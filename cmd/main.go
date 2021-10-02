package main

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	RedisServer "github.com/hdt3213/godis/redis/server"
	"github.com/hdt3213/godis/tcp"
	"os"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
}


// 检查文件是否存在
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	// 打印 logo
	print(banner)

	// 初始化日志
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	// 加载配置文件
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}

	// 启动网络服务
	if err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		},
		RedisServer.MakeHandler(),
	); err != nil {
		logger.Error(err)
	}

}
