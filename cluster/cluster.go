// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"context"
	"fmt"
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/consistenthash"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/reply"
	"github.com/jolestar/go-commons-pool/v2"
	"runtime/debug"
	"strconv"
	"strings"
)

// Cluster represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
//
//
type Cluster struct {
	self string

	nodes          []string
	peerPicker     *consistenthash.Map
	peerConnection map[string]*pool.ObjectPool

	db           database.EmbedDB

	transactions *dict.SimpleDict // id -> Transaction

	idGenerator *idgenerator.IDGenerator
}

const (
	replicas = 4
	lockSize = 64
)

// if only one node involved in a transaction, just execute the command don't apply tcc procedure
var allowFastTransaction = true

// MakeCluster creates and starts a node of cluster
func MakeCluster() *Cluster {

	cluster := &Cluster{
		self: config.Properties.Self,

		db:             godis.NewStandaloneServer(),
		transactions:   dict.MakeSimple(),
		peerPicker:     consistenthash.New(replicas, nil),
		peerConnection: make(map[string]*pool.ObjectPool),

		idGenerator: idgenerator.MakeGenerator(config.Properties.Self),
	}

	// 汇总所有 peers (去重)
	exists := make(map[string]struct{})
	peers := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		if _, ok := exists[peer]; ok {
			continue
		}
		exists[peer] = struct{}{}
		peers = append(peers, peer)
	}
	peers = append(peers, config.Properties.Self)

	// 将 peers 添加到一致性 hash 环中
	cluster.peerPicker.AddNode(peers...)

	// 为每个 peer 创建连接池
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}

	cluster.nodes = peers
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdAndArgs [][]byte) redis.Reply

// Close stops current node of cluster
func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = makeRouter()

// 校验密码
func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	// 提取 Cmd
	cmdName := strings.ToLower(string(cmdLine[0]))

	// 执行 Auth
	if cmdName == "auth" {
		return godis.Auth(c, cmdLine[1:])
	}

	// 鉴权
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}

	// 执行 Multi
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return godis.StartMulti(c)
	// 执行 Discard
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return godis.DiscardMulti(c)
	// 执行 Exec
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return execMulti(cluster, c, nil)
	// 执行 Select
	} else if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return execSelect(c, cmdLine)
	}

	//
	if c != nil && c.InMultiState() {
		return godis.EnqueueCmd(c, cmdLine)
	}

	// 其它 Cmd
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}

	// 执行 Cmd
	result = cmdFunc(cluster, c, cmdLine)

	return
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func ping(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

/*----- utils -------*/

// 参数格式转换: []byte{cmd, args...}
func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmd)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}

// return peer -> keys
//
// 把 keys 按 peer 分组
func (cluster *Cluster) groupKeysByPeer(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		// 查找 key 归属的 peer (一致性哈希)
		peer := cluster.peerPicker.PickNode(key)
		// 把 key 添加到 peer 的分组中
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}

// 执行 select db
func execSelect(c redis.Connection, args [][]byte) redis.Reply {
	// 参数转换
	dbIndex, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	// 参数检查
	if dbIndex >= config.Properties.Databases {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	// 执行 select db
	c.SelectDB(dbIndex)
	// 返回响应
	return reply.MakeOkReply()
}
