package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

// Del atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
func Del(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {

	// 参数检查
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}

	// 参数解析：从参数列表中取出要操作的 key
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}

	// 计算每个 key 所在的节点，并按照节点分组: peer => keys
	groupMap := cluster.groupKeysByPeer(keys)

	// 只有一个 peer 组，无需分布式事务，可以直接执行
	if len(groupMap) == 1 && allowFastTransaction { // do fast
		for peer, group := range groupMap { // only one peerKeys
			return cluster.relay(peer, c, makeArgs("DEL", group...))
		}
	}

	// prepare
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false

	// groupMap 的类型为 map[string][]string，key 是节点的地址，value 是 keys 中属于该节点的 key 列表
	for peer, peerKeys := range groupMap {
		peerArgs := []string{ txIDStr, "DEL" }
		peerArgs = append(peerArgs, peerKeys...)
		var resp redis.Reply
		if peer == cluster.self {
			resp = execPrepare(cluster, c, makeArgs("Prepare", peerArgs...))
		} else {
			// 向节点 peer 发送指令
			resp = cluster.relay(peer, c, makeArgs("Prepare", peerArgs...))
		}

		// 错误检查
		if reply.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}

	var respList []redis.Reply
	if rollback {
		// rollback
		// 回滚
		requestRollback(cluster, c, txID, groupMap)
	} else {
		// commit
		// 提交
		respList, errReply = requestCommit(cluster, c, txID, groupMap)
		if errReply != nil {
			rollback = true
		}
	}

	//
	if !rollback {
		var deleted int64 = 0
		for _, resp := range respList {
			intResp := resp.(*reply.IntReply)
			deleted += intResp.Code
		}
		return reply.MakeIntReply(int64(deleted))
	}


	return errReply
}
