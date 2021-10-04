package connection

import (
	"bytes"
	"github.com/hdt3213/godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Session represents a connection with a redis-cli
type Session struct {

	conn net.Conn

	// waiting until reply finished
	waitingReply wait.Wait

	// lock while server sending response
	mu sync.Mutex

	// subscribing channels
	//
	// 保存本会话订阅的频道
	subs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// queued commands for `multi`
	multiState bool
	queue      [][][]byte
	watching   map[string]uint32	// 保存每个 watching key 的版本号

	// selected db
	selectedDB int
}

// RemoteAddr returns the remote network address
func (c *Session) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close disconnect with the client
func (c *Session) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// NewConn creates Session instance
func NewConn(conn net.Conn) *Session {
	return &Session{
		conn: conn,
	}
}

// Write sends response to client over tcp connection
func (c *Session) Write(b []byte) error {
	// 参数检查
	if len(b) == 0 {
		return nil
	}

	// 并发写加锁
	c.mu.Lock()

	// 正在发送的请求数 +1
	c.waitingReply.Add(1)
	defer func() {
		// 正在发送的请求数 -1
		c.waitingReply.Done()
		// 解锁
		c.mu.Unlock()
	}()

	// 发送请求
	_, err := c.conn.Write(b)
	return err
}

// Subscribe add current connection into subscribers of the given channel
//
// 订阅频道
func (c *Session) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 保存本会话订阅的所有频道
	if c.subs == nil {
		c.subs = make(map[string]bool)
	}

	// 订阅 channel 频道
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
//
// 取消订阅频道
func (c *Session) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
//
// 订阅的频道总数
func (c *Session) SubsCount() int {
	return len(c.subs)
}

// GetChannels returns all subscribing channels
//
// 获取所有的订阅频道
func (c *Session) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// SetPassword stores password for authentication
func (c *Session) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Session) GetPassword() string {
	return c.password
}

func (c *Session) InMultiState() bool {
	return c.multiState
}

func (c *Session) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
	}
	c.multiState = state
}

func (c *Session) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

func (c *Session) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

func (c *Session) ClearQueuedCmds() {
	c.queue = nil
}

func (c *Session) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

func (c *Session) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB 保存当前 selectedDB 序号
func (c *Session) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

// FakeConn implements redis.Connection for test
type FakeConn struct {
	Session
	buf bytes.Buffer
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) error {
	c.buf.Write(b)
	return nil
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.buf.Reset()
}

// Bytes returns written data
func (c *FakeConn) Bytes() []byte {
	return c.buf.Bytes()
}
