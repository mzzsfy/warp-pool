package amzwrap_test

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool/internal/amzwrap"
	"github.com/mzzsfy/warp-pool/internal/testutil"
)

// startEchoServer 起本地回显 TCP 服务,返回监听地址
func startEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("启动回显服务失败: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
	return ln.Addr().String()
}

// Given 默认工厂与临时 state 目录 When 创建客户端 Then 成功创建且可离线关闭
func TestDefaultFactory_NewClient_CreatesOfflineClient(t *testing.T) {
	factory := amzwrap.DefaultFactory{}
	client, err := factory.NewClient(filepath.Join(t.TempDir(), "state.json"), "127.0.0.1:40000", nil)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	if client == nil {
		t.Fatal("客户端不应为空")
	}
	if client.Status().Running {
		t.Fatal("未启动的客户端不应处于运行态")
	}
	if client.ListenAddress() != "" {
		t.Fatalf("未启动的客户端监听地址 = %q, 期望空", client.ListenAddress())
	}
	if err := client.Close(); err != nil {
		t.Fatalf("关闭客户端失败: %v", err)
	}
}

// Given 已启动且 Run 阻塞中的假客户端 When Close Then Run 返回、状态停转且重复 Close 幂等
func TestFakeClient_Lifecycle_CloseUnblocksRun(t *testing.T) {
	client := amzwrap.NewFakeClient("127.0.0.1:40001")
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	if !client.Status().Running {
		t.Fatal("启动后应处于运行态")
	}

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run() }()
	if err := client.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run 应正常返回, 实际 %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close 后 Run 未返回")
	}
	if client.Status().Running {
		t.Fatal("关闭后不应处于运行态")
	}
	if !client.Closed() {
		t.Fatal("Closed 应为真")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("重复 Close 应幂等: %v", err)
	}
}

// Given 注入 StartErr 的假客户端 When Start Then 返回该错误且不进入运行态
func TestFakeClient_StartErr_ReturnsInjectedError(t *testing.T) {
	injected := errors.New("启动失败")
	client := amzwrap.NewFakeClient("127.0.0.1:40002")
	client.StartErr = injected
	if err := client.Start(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Start 错误 = %v, 期望注入错误", err)
	}
	if client.Started() {
		t.Fatal("失败启动不应标记为已启动")
	}
	if client.Status().Running {
		t.Fatal("失败启动不应进入运行态")
	}
}

// Given 未注入构造函数的假工厂 When 创建客户端 Then 返回携带监听地址的 FakeClient
func TestFakeFactory_DefaultNew_CreatesFakeClient(t *testing.T) {
	client, err := (amzwrap.FakeFactory{}).NewClient("state.json", "127.0.0.1:40003", nil)
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	fake, ok := client.(*amzwrap.FakeClient)
	if !ok {
		t.Fatalf("客户端类型 = %T, 期望 *FakeClient", client)
	}
	if got := fake.ListenAddress(); got != "127.0.0.1:40003" {
		t.Fatalf("监听地址 = %q, 期望透传构造参数", got)
	}
}

// Given 注入构造函数的假工厂 When 创建客户端 Then 参数透传且错误原样返回
func TestFakeFactory_InjectedNew_PassesArgsAndError(t *testing.T) {
	injected := errors.New("构造失败")
	factory := amzwrap.FakeFactory{New: func(storagePath, listenAddr string, logger amzwrap.Logger) (amzwrap.Client, error) {
		if storagePath != "state.json" || listenAddr != "127.0.0.1:40004" {
			t.Fatalf("构造参数 = (%q, %q), 期望原样透传", storagePath, listenAddr)
		}
		return nil, injected
	}}
	if _, err := factory.NewClient("state.json", "127.0.0.1:40004", nil); !errors.Is(err, injected) {
		t.Fatalf("创建错误 = %v, 期望注入错误", err)
	}
}

// Given 本地 SOCKS5 代理与回显目标 When 经代理拨号 Then 数据可达目标并原样返回
func TestDialThroughProxy_ViaProxy_ReachesTarget(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })
	target := startEchoServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := amzwrap.DialThroughProxy(ctx, socks.Addr(), "tcp", target)
	if err != nil {
		t.Fatalf("经代理拨号失败: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	got := make([]byte, len("ping"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("回显 = %q, 期望 %q", got, "ping")
	}
	targets := socks.Targets()
	if len(targets) != 1 || targets[0] != target {
		t.Fatalf("代理目标 = %v, 期望 [%s]", targets, target)
	}
}

// Given 不可达的代理地址 When 经代理拨号 Then 返回包装错误
func TestDialThroughProxy_ProxyUnreachable_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := amzwrap.DialThroughProxy(ctx, "127.0.0.1:1", "tcp", "127.0.0.1:80"); err == nil {
		t.Fatal("代理不可达应返回错误")
	} else if !strings.Contains(err.Error(), "经代理") {
		t.Fatalf("错误 %q 应携带代理上下文", err)
	}
}

// Given 代理可达但目标拒绝 When 经代理拨号 Then 返回错误
func TestDialThroughProxy_TargetRefused_ReturnsError(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := amzwrap.DialThroughProxy(ctx, socks.Addr(), "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("目标不可达应返回错误")
	}
}
