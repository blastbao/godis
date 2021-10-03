package cluster

import (
	"fmt"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

// MGet atomically get multi key-value from cluster, writeKeys can be distributed on any node
func MGet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {

	// 参数检查
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mget' command")
	}

	// 参数解析
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}

	// key => response
	resultMap := make(map[string][]byte)

	// peer => keys
	peerKeys := cluster.groupKeysByPeer(keys)

	for peer, group := range peerKeys {
		// 转发 MGET 请求到 peer
		resp := cluster.relay(peer, c, makeArgs("MGET", group...))
		if reply.IsErrorReply(resp) {
			errReply := resp.(reply.ErrorReply)
			return reply.MakeErrReply(fmt.Sprintf("ERR during get %s occurs: %v", group[0], errReply.Error()))
		}
		// 保存响应
		arrReply, _ := resp.(*reply.MultiBulkReply)
		for i, v := range arrReply.Args {
			key := group[i]
			resultMap[key] = v
		}
	}

	// 结果聚合
	result := make([][]byte, len(keys))
	for i, key := range keys {
		result[i] = resultMap[key]
	}

	// 返回响应
	return reply.MakeMultiBulkReply(result)
}

// MSet atomically sets multi key-value in cluster, writeKeys can be distributed on any node
//
//
//
func MSet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	// 参数检查
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	// 参数解析
	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i+1])
		valueMap[keys[i]] = string(args[2*i+2])
	}

	// 根据 peer 分组
	groupMap := cluster.groupKeysByPeer(keys)

	// 若所有的 key 都在同一个节点，则直接执行，不使用较慢的 2pc 算法
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer := range groupMap {
			return cluster.relay(peer, c, args)
		}
	}

	// prepare
	// 开始准备阶段 ...

	var errReply redis.Reply

	// 事务 ID
	txID := cluster.idGenerator.NextID()		// 协调者使用 snowflake 算法创建事务 ID
	txIDStr := strconv.FormatInt(txID, 10)

	rollback := false

	// 向所有参与者发送 prepare 请求
	for peer, group := range groupMap {

		// 构造 prepare 命令参数
		peerArgs := []string{ txIDStr, "MSET" }
		for _, k := range group {
			peerArgs = append(peerArgs, k, valueMap[k])
		}

		// 响应结果
		var resp redis.Reply

		// 本机
		if peer == cluster.self {
			resp = execPrepare(cluster, c, makeArgs("Prepare", peerArgs...))
		// 转发
		} else {
			resp = cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		}

		// 如果出错，则需要回滚
		if reply.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}

	// 若 prepare 过程出错则执行回滚
	if rollback {
		requestRollback(cluster, c, txID, groupMap)		// rollback
	// 不需回滚，则提交，若提交失败，则返回错误
	} else {
		_, errReply = requestCommit(cluster, c, txID, groupMap)
		rollback = errReply != nil
	}


	if !rollback {
		return &reply.OkReply{}
	}

	return errReply

}

// MSetNX sets multi key-value in database, only if none of the given writeKeys exist and all given writeKeys are on the same node
func MSetNX(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}
	var peer string
	size := argCount / 2
	for i := 0; i < size; i++ {
		key := string(args[2*i])
		currentPeer := cluster.peerPicker.PickNode(key)
		if peer == "" {
			peer = currentPeer
		} else {
			if peer != currentPeer {
				return reply.MakeErrReply("ERR msetnx must within one slot in cluster mode")
			}
		}
	}
	return cluster.relay(peer, c, args)
}
