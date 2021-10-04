package godis

import (
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strings"
)


// 在 Multi 过程中禁止执行的命令
var forbiddenInMulti = set.Make(
	"flushdb",
	"flushall",
)

// Watch set watching keys
//
func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	watching := conn.GetWatching()
	// 保存每个 key 的版本号到 conn.Watching[][] 中
	for _, arg := range args {
		key := string(arg)
		watching[key] = db.GetVersion(key)
	}
	return reply.MakeOkReply()
}

// 获取 key 的版本号，不存在返回 0
func execGetVersion(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ver := db.GetVersion(key)
	return reply.MakeIntReply(int64(ver))
}

func init() {
	RegisterCommand("GetVer", execGetVersion, readAllKeys, nil, 2)
}

// invoker should lock watching keys
//
// 检查 watching 的 keys 的版本号是否发生变化。
func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		currentVersion := db.GetVersion(key)
		if ver != currentVersion {
			return true
		}
	}
	return false
}

// StartMulti starts multi-command-transaction
func StartMulti(conn redis.Connection) redis.Reply {
	// Multi 命令不支持嵌套
	if conn.InMultiState() {
		return reply.MakeErrReply("ERR MULTI calls can not be nested")
	}
	// 设置 Multi 状态
	conn.SetMultiState(true)
	return reply.MakeOkReply()
}

// EnqueueCmd puts command line into `multi` pending queue
func EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if forbiddenInMulti.Has(cmdName) {
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if cmd.prepare == nil {
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}
	if !validateArity(cmd.arity, cmdLine) {
		// difference with redis: we won't enqueue command line with wrong arity
		return reply.MakeArgNumErrReply(cmdName)
	}
	conn.EnqueueCmd(cmdLine)
	return reply.MakeQueuedReply()
}

func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	cmdLines := conn.GetQueuedCmdLine()
	return db.ExecMulti(conn, conn.GetWatching(), cmdLines)
}

// ExecMulti executes multi commands transaction Atomically and Isolated
func (db *DB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	// prepare
	writeKeys := make([]string, 0) // may contains duplicate
	readKeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}

	// set watch
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)


	// 锁住
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	// 检查 watching 的 keys 的版本号是否发生变化。
	if isWatchingChanged(db, watching) {
		return reply.MakeEmptyMultiBulkReply() // watching keys changed, abort
	}

	// execute
	results := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		result := db.execWithLock(cmdLine)
		if reply.IsErrorReply(result) {
			aborted = true
			// don't rollback failed commands
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result)
	}
	if !aborted { //success
		db.addVersion(writeKeys...)
		return reply.MakeMultiRawReply(results)
	}
	// undo if aborted
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}
	return reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

// DiscardMulti drops MULTI pending commands
func DiscardMulti(conn redis.Connection) redis.Reply {

	// 只能在 Multi 状态下执行 Discard
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR DISCARD without MULTI")
	}

	// 清除状态
	conn.ClearQueuedCmds()

	// 清除 Multi 状态
	conn.SetMultiState(false)
	return reply.MakeQueuedReply()
}

// GetUndoLogs return rollback commands
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	// 查找命令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	// 是否支持回滚
	if cmd.undo == nil {
		return nil
	}
	// 返回回滚命令
	return cmd.undo(db, cmdLine[1:])
}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// GetRelatedKeys analysis related keys
func GetRelatedKeys(cmdLine [][]byte) ([]string, []string) {
	// 查找命令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}

	// 执行 prepare ，返回关联的 read/write keys 列表
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(cmdLine[1:])
}
