// Package amzwrap 包装 amz 客户端:接口化以供核心层注入 fake,并提供经 SOCKS5 代理的拨号。
package amzwrap

import (
	"context"
	"fmt"
	"net"

	"github.com/skye-z/amz"
	"golang.org/x/net/proxy"
)

// Logger 与 amz.Logger 同构的最小日志接口
type Logger interface {
	Printf(format string, args ...any)
}

// Status 客户端运行状态(amz.Status 中池所需字段子集)
type Status struct {
	Running       bool
	ListenAddress string
}

// Client amz 客户端最小接口,对齐 amz v0.2.3 公开方法
type Client interface {
	// Start 建立隧道并启动本地代理 listener(阻塞至就绪)
	Start(ctx context.Context) error
	// Run 维持隧道运行,阻塞直至 Close
	Run() error
	// Close 释放资源,幂等
	Close() error
	// Status 当前运行状态
	Status() Status
	// ListenAddress 本地代理监听地址
	ListenAddress() string
}

// Factory 创建 amz 客户端的工厂
type Factory interface {
	// NewClient 创建绑定 state 存储与监听地址的客户端
	NewClient(storagePath, listenAddr string, logger Logger) (Client, error)
}

// DefaultFactory 真实 amz 客户端工厂(HTTP+SOCKS5 双协议)
type DefaultFactory struct{}

// NewClient 创建真实 amz 客户端
func (DefaultFactory) NewClient(storagePath, listenAddr string, logger Logger) (Client, error) {
	inner, err := amz.NewClient(amz.Options{
		Storage: amz.StorageOptions{Path: storagePath},
		Listen:  amz.ListenOptions{Address: listenAddr},
		HTTP:    amz.HTTPOptions{Enabled: true},
		SOCKS5:  amz.SOCKS5Options{Enabled: true},
		Logger:  logger,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 amz 客户端失败: %w", err)
	}
	return &amzClient{inner: inner}, nil
}

// amzClient 真实客户端适配器
type amzClient struct {
	inner *amz.Client
}

// Start 实现 Client
func (c *amzClient) Start(ctx context.Context) error {
	if err := c.inner.Start(ctx); err != nil {
		return fmt.Errorf("amz 启动失败: %w", err)
	}
	return nil
}

// Run 实现 Client
func (c *amzClient) Run() error {
	if err := c.inner.Run(); err != nil {
		return fmt.Errorf("amz 运行失败: %w", err)
	}
	return nil
}

// Close 实现 Client
func (c *amzClient) Close() error {
	if err := c.inner.Close(); err != nil {
		return fmt.Errorf("amz 关闭失败: %w", err)
	}
	return nil
}

// Status 实现 Client
func (c *amzClient) Status() Status {
	s := c.inner.Status()
	return Status{Running: s.Running, ListenAddress: s.ListenAddress}
}

// ListenAddress 实现 Client
func (c *amzClient) ListenAddress() string { return c.inner.ListenAddress() }

// DialThroughProxy 经 SOCKS5 代理建立到 addr 的连接;到代理的连接固定 tcp,
// network 仅按 SOCKS5 语义尽力传递(由代理侧解释)
func DialThroughProxy(ctx context.Context, proxyAddr, network, addr string) (net.Conn, error) {
	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("创建 SOCKS5 拨号器失败: %w", err)
	}
	ctxDialer, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return nil, fmt.Errorf("SOCKS5 拨号器 %T 不支持 context", dialer)
	}
	conn, err := ctxDialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("经代理 %s 拨号 %s 失败: %w", proxyAddr, addr, err)
	}
	return conn, nil
}
