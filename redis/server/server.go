package server

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.DB
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	// 数据库
	var db database.DB

	// 集群模式
	if config.Properties.Self != "" &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeCluster()
	// 单机模式
	} else {
		db = godis.NewStandaloneServer()
	}

	// 封装数据库操作对象
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *connection.Session) {
	// 关闭 net.Conn
	_ = client.Close()
	//
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {

	// 检查 server 是否被 closed
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}

	// 封装 conn 并保存
	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	// 不断从 conn 中读取 req
	ch := parser.ParseStream(conn)
	for payload := range ch {
		// 参数检查
		if payload.Err != nil {
			// 网络错误，直接关闭连接并退出
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			// 协议错误，回包并继续读取
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				// 回包出错，关闭连接并返回
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}

		// 参数检查
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}

		// 参数检查
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}

		// 执行请求
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	// 设置 Closed 标识
	h.closing.Set(true)
	// TODO: concurrent wait
	// 优雅退出，逐个关闭连接
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Session)
		_ = client.Close()
		return true
	})
	// 关闭数据库
	h.db.Close()
	return nil
}
