package client

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/reply"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// Client is a pipeline mode redis client
type Client struct {
	// tcp 连接
	conn        net.Conn

	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string

	//
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

// request is a message sends to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {
	// 创建连接
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	// 创建客户端
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	// 心跳协程
	go client.heartbeat()
	// 写协程
	go client.handleWrite()
	// 读协程
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	// 关闭心跳定时器
	client.ticker.Stop()

	// stop new request
	//
	close(client.pendingReqs)

	// wait stop process
	client.working.Wait()

	// clean
	_ = client.conn.Close()

	close(client.waitingReqs)
}

func (client *Client) handleConnectionError(err error) error {

	// 关闭连接
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}

	// 建立新连接
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}

	// 替换旧连接
	client.conn = conn
	go func() {
		_ = client.handleRead()
	}()

	return nil
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	// 构造请求
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)

	client.working.Add(1)	// 正在执行的请求数 +1
	defer client.working.Done()	// 正在执行的请求数 -1

	// 异步发送请求(同步)
	client.pendingReqs <- req

	// 同步等待响应
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server time out")
	}

	// 检查响应
	if req.err != nil {
		return reply.MakeErrReply("request failed")
	}

	// 返回响应
	return req.reply
}

func (client *Client) doHeartbeat() {
	// 构造心跳请求
	hbReq := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}

	hbReq.waiting.Add(1)

	// [重要]
	client.working.Add(1)
	defer client.working.Done()
	// [重要] 把请求推送到 client 的待发送管道中
	client.pendingReqs <- hbReq
	// [重要] 等待请求处理完毕 or 超时(3s)
	hbReq.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	// 参数检查
	if req == nil || len(req.args) == 0 {
		return
	}

	// 格式转换
	re := reply.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()

	// 发送请求
	_, err := client.conn.Write(bytes)

	// 重试控制
	for i := 0; err != nil && i < 3; i++ {
		if err = client.handleConnectionError(err); err == nil {
			_, err = client.conn.Write(bytes)
		}
	}

	// 发送成功: 把请求推入等待响应管道
	if err == nil {
		client.waitingReqs <- req
	// 发送出错: ...
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	// 取一个已发送、等待响应的请求
	req := <-client.waitingReqs
	if req == nil {
		return
	}

	// 设置响应值
	req.reply = reply

	// 设置请求为已完成
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (client *Client) handleRead() error {
	// 接受请求的管道
	ch := parser.ParseStream(client.conn)

	// 逐个处理请求
	for payload := range ch {
		// 出错
		if payload.Err != nil {
			client.finishRequest(reply.MakeErrReply(payload.Err.Error()))
			continue
		}
		// 成功
		client.finishRequest(payload.Data)
	}

	return nil
}
