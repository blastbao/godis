package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
// ListenAndServeWithSignal 监听中断信号并通过 closeChan 通知服务器关闭
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	// 退出管道
	closeChan := make(chan struct{})

	// 信号监听
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	// tcp 监听
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))

	// 启动服务
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
//
// 监听并提供服务，并在收到 closeChan 发来的关闭通知后关闭
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	// listen signal
	// 监听关闭通知
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	// listen port
	// 在异常退出后释放资源
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {

		// 监听端口, 阻塞直到收到新连接或者出现错误
		conn, err := listener.Accept()
		if err != nil {
			// 通常是由于 listener 被关闭无法继续监听导致的错误
			break
		}
		// handle
		logger.Info("accept link")
		waitDone.Add(1)

		// 开启 goroutine 来处理新连接
		go func() {
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()

	}
	waitDone.Wait()
}
