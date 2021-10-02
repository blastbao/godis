package aof

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/reply"
	"io"
	"os"
	"strconv"
	"sync"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// 进行持久化时需要注意两个细节:
//	get 之类的读命令并不需要进行持久化
//	expire 命令要用等效的 expire at 命令替换。
//	举例说明，10:00 执行 expire a 3600 表示键 a 在 11:00 过期，在 10:30 载入 AOF 文件时执行 expire a 3600 就成了 11:30 过期与原数据不符。


type Handler struct {

	db          database.EmbedDB
	tmpDBMaker  func() database.EmbedDB

	aofChan     chan *payload	// 主线程使用此 channel 将要持久化的命令发送到异步协程
	aofFile     *os.File  		// append file 文件描述符
	aofFilename string			// append file 路径

	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shutdown
	// 当 aof 任务完成并准备关闭时，aof goroutine 将通过此通道向主 goroutine 发送消息
	aofFinished chan struct{}

	// buffer commands received during aof rewrite progress
	// aof 重写过程中收到的命令会写入缓冲管道
	aofRewriteBuffer chan *payload

	// pause aof for start/finish aof rewrite progress
	// 在必要的时候使用此字段暂停持久化操作
	pausingAof sync.RWMutex

	//
	currentDB  int
}

func NewAOFHandler(db database.EmbedDB, tmpDBMaker func() database.EmbedDB) (*Handler, error) {

	//
	handler := &Handler{}
	handler.aofFilename = config.Properties.AppendFilename	// AOF 文件名
	handler.db = db											// Database
	handler.tmpDBMaker = tmpDBMaker
	handler.LoadAof(0)

	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})

	go func() {
		handler.handleAof()
	}()

	return handler, nil
}

// AddAof send command to aof goroutine through channel
func (handler *Handler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into file
func (handler *Handler) handleAof() {
	// serialized execution
	handler.currentDB = 0


	for p := range handler.aofChan {

		// 异步协程在持久化之前会尝试获取锁，若其他协程持有锁则会暂停持久化操作；
		// 锁也保证了每次写入完整的一条指令，避免格式错误。
		handler.pausingAof.RLock() // prevent other goroutines from pausing aof


		//
		if handler.aofRewriteBuffer != nil {
			// replica during rewrite
			handler.aofRewriteBuffer <- p
		}

		if p.dbIndex != handler.currentDB {
			// select db
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue // skip this command
			}
			handler.currentDB = p.dbIndex
		}

		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}

		handler.pausingAof.RUnlock()
	}


	handler.aofFinished <- struct{}{}
}

// LoadAof read aof file
func (handler *Handler) LoadAof(maxBytes int) {

	// delete aofChan to prevent write again
	aofChan := handler.aofChan
	handler.aofChan = nil
	defer func(aofChan chan *payload) {
		handler.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}


	ch := parser.ParseStream(reader)
	fakeConn := &connection.FakeConn{} // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}

func (handler *Handler) Close() {
	if handler.aofFile != nil {
		close(handler.aofChan)
		<-handler.aofFinished // wait for aof finished
		err := handler.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}
