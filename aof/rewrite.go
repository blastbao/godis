package aof

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func (handler *Handler) newRewriteHandler() *Handler {
	h := &Handler{}
	h.aofFilename = handler.aofFilename
	h.db = handler.tmpDBMaker()
	return h
}

func (handler *Handler) Rewrite() {

	//
	tmpFile, fileSize, err := handler.startRewrite()
	if err != nil {
		logger.Warn(err)
		return
	}

	// load aof tmpFile
	tmpAof := handler.newRewriteHandler()
	tmpAof.LoadAof(int(fileSize))

	// rewrite aof tmpFile
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return
		}

		// dump db

		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}

	handler.finishRewrite(tmpFile)
}

func (handler *Handler) startRewrite() (*os.File, int64, error) {
	// 暂停 AOF 写入， 数据会在 db.aofChan 中暂时堆积
	handler.pausingAof.Lock() // pausing aof
	defer handler.pausingAof.Unlock()

	// 刷新 aof 文件
	err := handler.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, 0, err
	}

	// create rewrite channel
	// 创建重写缓冲区
	handler.aofRewriteBuffer = make(chan *payload, aofQueueSize)

	// get current aof file size
	// 读取当前 aof 文件大小
	fileInfo, _ := os.Stat(handler.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	// 创建临时文件
	file, err := ioutil.TempFile("", "aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, 0, err
	}

	return file, filesize, nil
}

func (handler *Handler) finishRewrite(tmpFile *os.File) {
	// 暂停 AOF 写入
	handler.pausingAof.Lock() // pausing aof
	defer handler.pausingAof.Unlock()

	// write commands created during rewriting to tmp file
	currentDB := -1

loop:
	for {

		// aof is pausing, there won't be any new commands in aofRewriteBuffer
		//
		// 将重写缓冲区内的数据写入临时文件
		// 因为 handleAof 已被暂停，在遍历期间 aofRewriteChan 中不会有新数据。


		select {
		case p := <-handler.aofRewriteBuffer:

			if p.dbIndex != currentDB {
				// select db
				// always do `select` during first loop 第一次进入循环时必须执行一次 select 确保数据库一致
				data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
				_, err := tmpFile.Write(data)
				if err != nil {
					logger.Warn(err)
					continue // skip this command
				}
				currentDB = p.dbIndex
			}

			data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
			_, err := tmpFile.Write(data)
			if err != nil {
				logger.Warn(err)
			}

		default:
			// channel is empty, break loop
			// 只有 channel 为空时才会进入此分支
			break loop
		}
	}

	// 释放重写缓冲区
	close(handler.aofRewriteBuffer)
	handler.aofRewriteBuffer = nil

	// replace current aof file by tmp file
	// 使用临时文件代替aof文件
	_ = handler.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), handler.aofFilename)

	// reopen aof file for further write
	// 重新打开文件描述符以保证正常写入
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	handler.aofFile = aofFile

	// reset selected db 重新写入一次 select 指令保证 aof 中的数据库与 handler.currentDB 一致
	data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDB))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
