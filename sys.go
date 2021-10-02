package godis

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/reply"
)

// Ping the server
func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// Auth validate client's password
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	// 参数检查
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}

	if config.Properties.RequirePass == "" {
		return reply.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}

	// 保存密码
	passwd := string(args[0])
	c.SetPassword(passwd)

	// 校验密码
	if config.Properties.RequirePass != passwd {
		return reply.MakeErrReply("ERR invalid password")
	}

	// 鉴权成功
	return &reply.OkReply{}
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func init() {
	RegisterCommand("ping", Ping, noPrepare, nil, -1)
}
