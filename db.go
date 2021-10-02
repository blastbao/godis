// Package godis is a memory database with redis compatible interface
package godis

import (
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/reply"
	"strings"
	"sync"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// DB stores data and execute user's commands
type DB struct {
	index int
	// key -> DataEntity
	// 存储数据
	data dict.Dict

	// key -> expireTime (time.Time)
	// 存储过期时间
	ttlMap dict.Dict

	// key -> version(uint32)
	// 存储版本号
	versionMap dict.Dict

	// dict.Dict will ensure concurrent-safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	//
	// 确保并发安全，用于复杂命令
	locker *lock.Locks

	// stop all data access for execFlushDB
	stopTheWorld sync.WaitGroup
	addAof       func(CmdLine)
}

// ExecFunc is interface for command executor args don't include cmd line
// 主逻辑
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi` returns related write keys and read keys
// PreFunc 解析命令行参数，得到关联的 读 keys / 写 keys 列表。
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
// 命令: cmd args
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line execute from head to tail when undo
// UndoFunc 返回回滚操作表
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker:     lock.Make(lockerSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeBasicDB create DB instance only with basic abilities.
// It is not concurrent safe
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeSimple(),
		ttlMap:     dict.MakeSimple(),
		versionMap: dict.MakeSimple(),
		locker:     lock.Make(1),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// 忽略大小写
	cmdName := strings.ToLower(string(cmdLine[0]))

	// 执行特殊命令
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	if c != nil && c.InMultiState() {
		EnqueueCmd(c, cmdLine)
		return reply.MakeQueuedReply()
	}

	// 执行普通命令
	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	// 查找命令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	// 参数(数目)检查
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	// 执行预处理: 解析入参，得到关联的 read/write keys 列表
	wKeys, rKeys := cmd.prepare(cmdLine[1:])

	// 增加版本号: version+=1
	db.addVersion(wKeys...)

	// 加读写锁
	db.RWLocks(wKeys, rKeys)
	defer db.RWUnLocks(wKeys, rKeys)

	// 执行主逻辑
	return cmd.executor(db, cmdLine[1:])
}

// 参数数目检查
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	// 锁
	db.stopTheWorld.Wait()

	// 查询 key 的 value
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}

	// 过期检查
	if db.IsExpired(key) {
		return nil, false
	}

	// 格式转换
	entity, _ := raw.(*database.DataEntity)

	// 返回
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	db.stopTheWorld.Wait()
	// 存储数据
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	db.stopTheWorld.Wait()
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	db.stopTheWorld.Wait()
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.stopTheWorld.Wait()
	// 移除数据
	db.data.Remove(key)
	// 移除 ttl
	db.ttlMap.Remove(key)
	// 取消 ttl 定时任务
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopTheWorld.Wait()
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	// 阻塞其它操作
	db.stopTheWorld.Add(1)
	defer db.stopTheWorld.Done()

	// ???
	db.data.Clear()
	db.ttlMap.Clear()
	db.locker = lock.Make(lockerSize)

}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopTheWorld.Wait()

	// 保存 ttl 信息
	db.ttlMap.Put(key, expireTime)

	// 创建 ttl 定时任务
	taskKey := genExpireTask(key)
	timewheel.At(
		expireTime, // 触发时间
		taskKey, 	// 任务标识
		func() {	// 回调函数
			keys := []string{key}
			// 写锁
			db.RWLocks(keys, nil)
			defer db.RWUnLocks(keys, nil)
			// check-lock-check, ttl may be updated during waiting lock
			logger.Info("expire " + key)
			// 查询过期时间
			rawExpireTime, ok := db.ttlMap.Get(key)
			if !ok {
				return
			}
			// 检查是否已过期
			expTime, _ := rawExpireTime.(time.Time)
			expired := time.Now().After(expTime)
			// 如果已经过期，就移除 key
			if expired {
				db.Remove(key)
			}
		},
	)
}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.stopTheWorld.Wait()
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	// 查询过期时间
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	// 检查是否已过期
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	// 已过期则移除
	if expired {
		db.Remove(key)
	}
	// 返回是否已过期
	return expired
}

/* --- add version --- */

// 把 keys 中每个 key 的 version+=1 ，意味着发生了变更。
func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		// 查询 key 的版本号
		version := db.GetVersion(key)
		// 增加 key 的版本号
		db.versionMap.Put(key, version+1)
	}
}

func (db *DB) GetVersion(key string) uint32 {
	// 查询 key 的版本号
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	// 返回版本号，不存在返回 0
	return entity.(uint32)
}

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		// 格式转换
		entity, _ := raw.(*database.DataEntity)
		// 获取过期时间
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		// 回调: key/value/expiration
		return cb(key, entity, expiration)
	})
}
