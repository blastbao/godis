package cluster

import (
	"github.com/hdt3213/godis"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

const relayMulti = "_multi"
const innerWatch = "_watch"

var relayMultiBytes = []byte(relayMulti)

// cmdLine == []string{"exec"}
func execMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	// 只能在 Multi 状态下执行 Exec 命令
	if !conn.InMultiState() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}
	// 执行结束后，清除 Multi 状态
	defer conn.SetMultiState(false)

	// 获取缓存的 Cmds
	cmdLines := conn.GetQueuedCmdLine()

	// analysis related keys
	// 保存涉及到的 keys
	keys := make([]string, 0) // may contains duplicate
	for _, cl := range cmdLines {
		wKeys, rKeys := godis.GetRelatedKeys(cl)
		keys = append(keys, wKeys...)
		keys = append(keys, rKeys...)
	}

	// 把 watching 的 keys 也添加到 keys 列表中
	watching := conn.GetWatching()
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	keys = append(keys, watchingKeys...)


	// 如果不涉及任何 key ，则是一个空的事务，直接执行即可。
	if len(keys) == 0 {
		// empty transaction or only `PING`s
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}

	// 按 peers 分组
	groupMap := cluster.groupKeysByPeer(keys)

	// 要求 Multi 涉及的 keys 必须在一个 peer 上
	if len(groupMap) > 1 {
		return reply.MakeErrReply("ERR MULTI commands transaction must within one slot in cluster mode")
	}

	var peer string
	// assert len(groupMap) == 1
	for p := range groupMap {
		peer = p
	}

	// out parser not support reply.MultiRawReply, so we have to encode it
	//
	// 如果是本机，直接函数调用
	if peer == cluster.self {
		return cluster.db.ExecMulti(conn, watching, cmdLines)
	}

	// 如果非本机，就 rpc 转发
	return execMultiOnOtherNode(cluster, conn, peer, watching, cmdLines)
}

func execMultiOnOtherNode(cluster *Cluster, conn redis.Connection, peer string, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {

	defer func() {
		// 清理缓存的 cmds
		conn.ClearQueuedCmds()
		// 重置 multi 状态为 false
		conn.SetMultiState(false)
	}()

	// 构造 relay command
	//
	// relayCmd := {
	//	 "_multi"
	// }
	relayCmdLine := [][]byte{ // relay it to executing node
		relayMultiBytes,
	}

	// watching commands
	//
	// watchingCmd := {
	//	 "_watch",
	//	 ${key1},
	//	 ${version1},
	//	 ${key2},
	//	 ${version2},
	//   ...
	// }
	var watchingCmdLine = utils.ToCmdLine(innerWatch)
	for key, version := range watching {
		versionStr := strconv.FormatUint(uint64(version), 10)
		watchingCmdLine = append(watchingCmdLine, []byte(key), []byte(versionStr))
	}

	// 构造 relay command
	//
	// relayCmd := {
	//	 "_multi"
	//   "_watch",
	//	 ${key1},
	//	 ${version1},
	//	 ${key2},
	//	 ${version2},
	//   ...
	//   ${cmdLines},
	// }
	//
	relayCmdLine = append(relayCmdLine, encodeCmdLine([]CmdLine{watchingCmdLine})...)
	relayCmdLine = append(relayCmdLine, encodeCmdLine(cmdLines)...)


	// 执行 relayCmd
	var rawRelayResult redis.Reply
	if peer == cluster.self {
		// this branch just for testing
		rawRelayResult = execRelayedMulti(cluster, conn, relayCmdLine)
	} else {
		rawRelayResult = cluster.relay(peer, conn, relayCmdLine)
	}

	if reply.IsErrorReply(rawRelayResult) {
		return rawRelayResult
	}

	//
	_, ok := rawRelayResult.(*reply.EmptyMultiBulkReply)
	if ok {
		return rawRelayResult
	}

	relayResult, ok := rawRelayResult.(*reply.MultiBulkReply)
	if !ok {
		return reply.MakeErrReply("execute failed")
	}

	rep, err := parseEncodedMultiRawReply(relayResult.Args)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}

	return rep
}

// execRelayedMulti execute relayed multi commands transaction
// cmdLine format: _multi watch-cmdLine base64ed-cmdLine
// result format: base64ed-reply list
func execRelayedMulti(cluster *Cluster, conn redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return reply.MakeArgNumErrReply("_exec")
	}
	decoded, err := parseEncodedMultiRawReply(cmdLine[1:])
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	var txCmdLines []CmdLine
	for _, rep := range decoded.Replies {
		mbr, ok := rep.(*reply.MultiBulkReply)
		if !ok {
			return reply.MakeErrReply("exec failed")
		}
		txCmdLines = append(txCmdLines, mbr.Args)
	}
	watching := make(map[string]uint32)
	watchCmdLine := txCmdLines[0] // format: _watch key1 ver1 key2 ver2...
	for i := 2; i < len(watchCmdLine); i += 2 {
		key := string(watchCmdLine[i-1])
		verStr := string(watchCmdLine[i])
		ver, err := strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			return reply.MakeErrReply("watching command line failed")
		}
		watching[key] = uint32(ver)
	}
	rawResult := cluster.db.ExecMulti(conn, watching, txCmdLines[1:])
	_, ok := rawResult.(*reply.EmptyMultiBulkReply)
	if ok {
		return rawResult
	}
	resultMBR, ok := rawResult.(*reply.MultiRawReply)
	if !ok {
		return reply.MakeErrReply("exec failed")
	}
	return encodeMultiRawReply(resultMBR)
}

func execWatch(cluster *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeArgNumErrReply("watch")
	}

	// 取所有 keys
	args = args[1:]

	// 取当前的 watching 列表
	watching := conn.GetWatching()

	// 遍历每个 key
	for _, arg := range args {
		key := string(arg)
		// 查询 key 的 version
		peer := cluster.peerPicker.PickNode(key)
		result := cluster.relay(peer, conn, utils.ToCmdLine("GetVer", key))
		if reply.IsErrorReply(result) {
			return result
		}
		intResult, ok := result.(*reply.IntReply)
		if !ok {
			return reply.MakeErrReply("get version failed")
		}
		// 保存 key 的 version 到 watching 列表中
		watching[key] = uint32(intResult.Code)
	}
	return reply.MakeOkReply()
}
