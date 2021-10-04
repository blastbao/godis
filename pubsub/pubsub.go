package pubsub

import (
	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + reply.CRLF + t + reply.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + reply.CRLF + channel + reply.CRLF +
		":" + strconv.FormatInt(code, 10) + reply.CRLF)
}

/*
 * invoker should lock channel
 * return: is new subscribed
 */
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {

	// 订阅频道 channel
	client.Subscribe(channel)

	// add into hub.subs
	//
	// 查询频道 channel 的订阅者列表 subscribers ，不存在则创建
	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers)
	}

	// 检查是否已订阅
	if subscribers.Contains(client) {
		return false
	}

	// 添加订阅者
	subscribers.Add(client)
	return true
}

/*
 * invoker should lock channel
 * return: is actually un-subscribe
 */
func unsubscribe0(hub *Hub, channel string, client redis.Connection) bool {

	// 取消订阅频道 channel
	client.UnSubscribe(channel)

	// remove from hub.subs
	raw, ok := hub.subs.Get(channel)
	if ok {
		// 获取频道订阅者列表
		subscribers, _ := raw.(*list.LinkedList)
		// 移除订阅者
		subscribers.RemoveAllByVal(client)
		// 如果频道订阅者为空，就清除此频道
		if subscribers.Len() == 0 {
			// clean
			hub.subs.Remove(channel)
		}
		return true
	}
	return false
}

// Subscribe puts the given connection into the given channel
func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	// 解析参数，得到将订阅的频道列表
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	// 加锁，避免并发订阅产生冲突
	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	// 逐个订阅 channel
	for _, channel := range channels {
		if subscribe0(hub, channel, c) {
			// 订阅成功，返回响应 resp:="subscribe":{channel}:{subCounts}
			_ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
		}
	}

	return &reply.NoReply{}
}

// UnsubscribeAll removes the given connection from all subscribing channel
func UnsubscribeAll(hub *Hub, c redis.Connection) {
	// 获取所有已订阅的 channels
	channels := c.GetChannels()

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	// 逐个取消订阅
	for _, channel := range channels {
		unsubscribe0(hub, channel, c)
	}

}

// UnSubscribe removes the given connection from the given channel
func UnSubscribe(db *Hub, c redis.Connection, args [][]byte) redis.Reply {
	var channels []string

	// 如果指定了频道，就取消该指定频道，否则，取消所有已订阅的频道。
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = c.GetChannels()
	}

	// 加锁
	db.subsLocker.Locks(channels...)
	defer db.subsLocker.UnLocks(channels...)

	// 无可取消的频道
	if len(channels) == 0 {
		_ = c.Write(unSubscribeNothing)
		return &reply.NoReply{}
	}

	// 逐个取消订阅
	for _, channel := range channels {
		if unsubscribe0(db, channel, c) {
			_ = c.Write(makeMsg(_unsubscribe, channel, int64(c.SubsCount())))
		}
	}

	return &reply.NoReply{}
}

// Publish send msg to all subscribing client
func Publish(hub *Hub, args [][]byte) redis.Reply {
	// 参数检查
	if len(args) != 2 {
		return &reply.ArgNumErrReply{Cmd: "publish"}
	}

	// 参数1: 频道名
	channel := string(args[0])
	// 参数2: 消息体
	message := args[1]

	// 加锁
	hub.subsLocker.Lock(channel)
	defer hub.subsLocker.UnLock(channel)

	// 查询频道订阅者
	raw, ok := hub.subs.Get(channel)
	if !ok {
		return reply.MakeIntReply(0)
	}

	// 遍历订阅者列表，逐个推送消息
	subscribers, _ := raw.(*list.LinkedList)
	subscribers.ForEach(func(i int, c interface{}) bool {
		// 取出连接
		client, _ := c.(redis.Connection)
		// 构造消息
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes		// "message"
		replyArgs[1] = []byte(channel)	// $channel
		replyArgs[2] = message			// $message
		// 推送消息
		_ = client.Write(reply.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})

	// 返回订阅者数目
	return reply.MakeIntReply(int64(subscribers.Len()))
}
