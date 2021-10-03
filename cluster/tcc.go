package cluster

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/reply"
)

// Transaction stores state and data for a try-commit-catch distributed transaction
type Transaction struct {
	id      string  			// 事务 ID ，transaction id
	cmdLine [][]byte 			// 命令，cmd cmdLine
	cluster *Cluster			// 集群
	conn    redis.Connection	// 连接
	dbIndex int					// DB

	writeKeys  []string			// 写 keys
	readKeys   []string			// 读 keys
	keysLocked bool				// 是否已加锁
	undoLog    []CmdLine		// 回滚命令

	status int8					// 事务状态
	mu     *sync.Mutex			// 锁
}

const (
	maxLockTime       = 3 * time.Second
	waitBeforeCleanTx = 2 * maxLockTime

	// 事务状态
	createdStatus    = 0	// 创建
	preparedStatus   = 1	// 准备
	committedStatus  = 2	// 提交
	rolledBackStatus = 3	// 回滚
)

// 事务 ID
func genTaskKey(txID string) string {
	return "tx:" + txID
}

// NewTransaction creates a try-commit-catch distributed transaction
func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:      id,
		cmdLine: cmdLine,
		cluster: cluster,
		conn:    c,
		dbIndex: c.GetDBIndex(),
		status:  createdStatus,
		mu:      new(sync.Mutex),
	}
}

// Reentrant
// invoker should hold tx.mu
func (tx *Transaction) lockKeys() {
	// 不要重复加锁
	if !tx.keysLocked {
		// 加锁
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		// 解锁
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// t should contains Keys and Id field
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 获取关联的 keys
	tx.writeKeys, tx.readKeys = godis.GetRelatedKeys(tx.cmdLine)

	// lock writeKeys
	// 加锁
	tx.lockKeys()

	// build undoLog
	// 获取回滚命令
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)

	// 设置事务状态
	tx.status = preparedStatus

	// 获取事务 ID
	taskKey := genTaskKey(tx.id)

	// 创建定时任务，异常时自动回滚事务
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			_ = tx.rollback()
		}
	})

	return nil
}

func (tx *Transaction) rollback() error {

	curStatus := tx.status

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 状态检查
	if tx.status != curStatus { // ensure status not changed by other goroutine
		return fmt.Errorf("tx %s status changed", tx.id)
	}

	// 已回滚，直接退出
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}

	// 加锁???
	tx.lockKeys()
	// 逐个执行回滚操作
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	// 解锁???
	tx.unLockKeys()

	// 设置状态为已回滚
	tx.status = rolledBackStatus
	return nil
}

// cmdLine: Prepare txId cmdName args...
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	// 参数检查
	if len(cmdLine) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'preparedel' command")
	}

	// 事务 ID
	txID := string(cmdLine[1])

	// 创建事务对象
	tx := NewTransaction(cluster, c, txID, cmdLine[2:])

	// 保存事务
	cluster.transactions.Put(txID, tx)

	// 执行事务
	err := tx.prepare()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}

	// 返回成功
	return &reply.OkReply{}
}

// execRollback rollbacks local transaction
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}

	// 根据 txId 获取事务
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	// 执行回滚
	err := tx.rollback()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}

	// clean transaction
	// 定时移除事务
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})

	// 返回值
	return reply.MakeIntReply(1)
}

// execCommit commits local transaction as a worker when receive execCommit command from coordinator
func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}

	// 根据 txId 获取事务
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	// 事务加锁
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// 执行 command
	result := cluster.db.ExecWithLock(c, tx.cmdLine)

	// 错误检查
	if reply.IsErrorReply(result) {
		// failed
		err2 := tx.rollback()
		return reply.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}

	// after committed

	// 解锁关联的 keys
	tx.unLockKeys()
	// 设置事务状态为已提交
	tx.status = committedStatus

	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})

	return result
}

// requestCommit commands all node to commit transaction as coordinator
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, peers map[string][]string) ([]redis.Reply, reply.ErrorReply) {
	var errReply reply.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]redis.Reply, 0, len(peers))
	for peer := range peers {
		var resp redis.Reply
		// 本机
		if peer == cluster.self {
			resp = execCommit(cluster, c, makeArgs("commit", txIDStr))
		// 非本机
		} else {
			resp = cluster.relay(peer, c, makeArgs("commit", txIDStr))
		}
		// 错误检查
		if reply.IsErrorReply(resp) {
			errReply = resp.(reply.ErrorReply)
			break
		}
		// 保存响应
		respList = append(respList, resp)
	}

	// 如果某个 peer 提交失败，则执行回滚
	if errReply != nil {
		requestRollback(cluster, c, txID, peers)
		return nil, errReply
	}

	// 返回响应列表
	return respList, nil
}

// requestRollback requests all node rollback transaction as coordinator
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, peers map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)
	// 逐个 peer 执行 rollback
	for peer := range peers {
		// 本机
		if peer == cluster.self {
			execRollback(cluster, c, makeArgs("rollback", txIDStr))
		// 非本机
		} else {
			cluster.relay(peer, c, makeArgs("rollback", txIDStr))
		}
	}
}
