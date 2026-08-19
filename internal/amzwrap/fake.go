package amzwrap

import (
	"context"
	"sync"
)

// FakeClient 核心层测试用假客户端:并发安全,Run 阻塞至 Close
type FakeClient struct {
	StartErr error // Start 返回的错误
	RunErr   error // Run 返回的错误
	CloseErr error // Close 返回的错误

	listenAddr string
	stop       chan struct{}
	closeOnce  sync.Once

	mu      sync.Mutex
	started bool
	running bool
	closed  bool
}

// NewFakeClient 创建监听地址为 listenAddr 的假客户端
func NewFakeClient(listenAddr string) *FakeClient {
	return &FakeClient{listenAddr: listenAddr, stop: make(chan struct{})}
}

// Start 实现 Client;StartErr 非空时不进入运行态
func (c *FakeClient) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.StartErr != nil {
		return c.StartErr
	}
	c.started = true
	c.running = true
	return nil
}

// Run 实现 Client;阻塞直至 Close,随后返回 RunErr
func (c *FakeClient) Run() error {
	<-c.stop
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.RunErr
}

// Close 实现 Client;幂等,唤醒阻塞的 Run
func (c *FakeClient) Close() error {
	c.closeOnce.Do(func() { close(c.stop) })
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	c.closed = true
	return c.CloseErr
}

// Status 实现 Client
func (c *FakeClient) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Status{Running: c.running, ListenAddress: c.listenAddr}
}

// ListenAddress 实现 Client
func (c *FakeClient) ListenAddress() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listenAddr
}

// Started 是否已成功 Start
func (c *FakeClient) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

// Closed 是否已 Close
func (c *FakeClient) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// FakeFactory 假工厂:New 非 nil 时交由注入函数构造,否则创建默认 FakeClient
type FakeFactory struct {
	New func(storagePath, listenAddr string, logger Logger) (Client, error)
}

// NewClient 实现 Factory
func (f FakeFactory) NewClient(storagePath, listenAddr string, logger Logger) (Client, error) {
	if f.New != nil {
		return f.New(storagePath, listenAddr, logger)
	}
	return NewFakeClient(listenAddr), nil
}
