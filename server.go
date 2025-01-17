package godis

import (
	"fmt"
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/reply"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// [核心对象]

type MultiDB struct {
	dbSet []*DB

	// handle publish/subscribe
	hub *pubsub.Hub

	// handle aof persistence
	aofHandler *aof.Handler
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other funtions
func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}

	// 默认的 dbs 数目
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	// 初始化 dbs
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}

	// 初始化 hub
	mdb.hub = pubsub.MakeHub()

	// 初始化 aof
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			db := db // avoid closure
			db.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(db.index, line)
			}
		}
	}

	return mdb
}

// MakeBasicMultiDB create a MultiDB only with basic abilities for aof rewrite and other usages
func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		mdb.dbSet[i] = makeBasicDB()
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
//
//
// [核心]
func (mdb *MultiDB) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {

	// 捕获 Panic
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	// 忽略大小写
	cmdName := strings.ToLower(string(cmdLine[0]))

	// 鉴权
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}

	// 鉴权
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}

	// special commands
	// 特殊命令

	// 订阅频道
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return reply.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(mdb.hub, c, cmdLine[1:])
	// 发布消息到指定频道
	} else if cmdName == "publish" {
		return pubsub.Publish(mdb.hub, cmdLine[1:])
	// 取消订阅
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(mdb.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(mdb, cmdLine[1:])
	} else if cmdName == "flushall" {
		return mdb.flushAll()
	// 选择 DB
	} else if cmdName == "select" {
		// 状态检查
		if c != nil && c.InMultiState() {
			return reply.MakeErrReply("cannot select database within multi")
		}
		// 参数检查
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		// 执行 Select
		return execSelect(c, mdb, cmdLine[1:])
	}

	// todo: support multi database transaction

	// normal commands
	// 普通命令

	// 选择 DB
	dbIndex := c.GetDBIndex()
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	selectedDB := mdb.dbSet[dbIndex]

	// 执行命令
	return selectedDB.Exec(c, cmdLine)
}

// AfterClientClose does some clean after client close connection
func (mdb *MultiDB) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(mdb.hub, c)
}

// Close graceful shutdown database
func (mdb *MultiDB) Close() {
	if mdb.aofHandler != nil {
		mdb.aofHandler.Close()
	}
}

func execSelect(c redis.Connection, mdb *MultiDB, args [][]byte) redis.Reply {
	// 参数解析
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}

	// 参数检查
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}

	// 更新 session
	c.SelectDB(dbIndex)

	// 返回 ok
	return reply.MakeOkReply()
}

func (mdb *MultiDB) flushAll() redis.Reply {
	for _, db := range mdb.dbSet {
		db.Flush()
	}
	if mdb.aofHandler != nil {
		mdb.aofHandler.AddAof(0, utils.ToCmdLine("FlushAll"))
	}
	return &reply.OkReply{}
}

func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	if dbIndex >= len(mdb.dbSet) {
		return
	}
	db := mdb.dbSet[dbIndex]
	db.ForEach(cb)
}

func (mdb *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	// 选择 DB
	if conn.GetDBIndex() >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	db := mdb.dbSet[conn.GetDBIndex()]
	// 执行 Multi
	return db.ExecMulti(conn, watching, cmdLines)
}

func (mdb *MultiDB) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	if dbIndex >= len(mdb.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := mdb.dbSet[dbIndex]
	db.RWLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	if dbIndex >= len(mdb.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := mdb.dbSet[dbIndex]
	db.RWUnLocks(writeKeys, readKeys)
}

func (mdb *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	if dbIndex >= len(mdb.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := mdb.dbSet[dbIndex]
	return db.GetUndoLogs(cmdLine)
}

func (mdb *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	if conn.GetDBIndex() >= len(mdb.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := mdb.dbSet[conn.GetDBIndex()]
	return db.execWithLock(cmdLine)
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	go db.aofHandler.Rewrite()
	return reply.MakeStatusReply("Background append only file rewriting started")
}

func RewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	db.aofHandler.Rewrite()
	return reply.MakeStatusReply("Background append only file rewriting started")
}
