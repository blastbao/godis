package cluster

import (
	"context"
	"errors"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/jolestar/go-commons-pool/v2"
)

// 实现 pool.PooledObjectFactory 接口，作为协程池的工厂类。
type connectionFactory struct {
	Peer string
}

func (f *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	// 创建同 peer 的 Client
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}

	// 启动 Client
	c.Start()

	// all peers of cluster should use the same password
	// 鉴权
	if config.Properties.RequirePass != "" {
		_ = c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
	}

	//
	return pool.NewPooledObject(c), nil
}

func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (f *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	// do validate
	return true
}

func (f *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	// do activate
	return nil
}

func (f *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	// do passivate
	return nil
}
