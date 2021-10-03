package cluster

import (
	"context"
	"errors"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/reply"
	"strconv"
)

// 从连接池中取出同 peer 建立的 Client
func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

// 将 Client 归还给连接池
func (cluster *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// relay relays command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {

	// 若数据在本地则直接调用数据库引擎
	if peer == cluster.self {
		// to self db
		return cluster.db.Exec(c, args)
	}

	// 从连接池取一个与目标节点的连接
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}

	// 处理完成后将连接放回连接池
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()

	// 将指令发送到目标节点
	// (1) 发送切换 DB 命令
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	// (2) 发送 CMD
	return peerClient.Send(args)
}

// broadcast broadcasts command to all node in cluster
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	// 遍历集群节点
	for _, node := range cluster.nodes {
		// 发送请求
		res := cluster.relay(node, c, args)
		// 保存结果
		result[node] = res
	}
	return result
}
