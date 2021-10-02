package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)


// 雪花算法
// 使用 41bit 作为毫秒数，10bit 作为机器的ID（5个 bit 是数据中心，5个 bit 的机器ID），
// 12bit作为毫秒内的流水号（意味着每个节点在每毫秒可以产生 4096 个 ID），最后还有一个符号位，永远是 0 。



const (

	// epoch0 is set to the twitter snowflake epoch of Nov 04 2010 01:42:54 UTC in milliseconds
	// You may customize this to set a different epoch for your application.
	epoch0      int64 = 1288834974657

	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	timeLeft    uint8 = 22
	nodeLeft    uint8 = 10
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

// IDGenerator generates unique uint64 ID using snowflake algorithm
//
//
type IDGenerator struct {
	mu        *sync.Mutex
	lastStamp int64
	nodeID    int64
	sequence  int64
	epoch     time.Time
}

// MakeGenerator creates a new IDGenerator
func MakeGenerator(node string) *IDGenerator {

	//
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

// NextID returns next unique ID
func (w *IDGenerator) NextID() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 时间戳
	timestamp := time.Since(w.epoch).Nanoseconds() / 1000000

	// 系统时钟回退，抛出异常
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}

	// 如果是同一毫秒内，则递增序号
	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		// 序号溢出，需要阻塞到下一个毫秒，才能获得新的时间戳
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = time.Since(w.epoch).Nanoseconds() / 1000000
			}
		}
	// 时间戳增加，则重置序号
	} else {
		w.sequence = 0
	}

	// 更新当前时间戳
	w.lastStamp = timestamp

	// 拼接 ID
	id := (timestamp << timeLeft) | (w.nodeID << nodeLeft) | w.sequence
	//fmt.Printf("%d %d %d\n", timestamp, w.sequence, id)
	return id
}
