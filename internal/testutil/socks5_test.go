package testutil_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool/internal/testutil"
	"golang.org/x/net/proxy"
)

// startEchoListener 起本地回显 TCP 监听,返回监听地址
func startEchoListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("启动回显监听失败: %v", err)
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

// Given SOCKS5 服务与回显目标 When 经 SOCKS5 拨号并读写 Then 数据回显且目标被记录
func TestSOCKS5Server_ProxyDial_ForwardsAndRecords(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })
	target := startEchoListener(t)

	dialer, err := proxy.SOCKS5("tcp", socks.Addr(), nil, proxy.Direct)
	if err != nil {
		t.Fatalf("创建拨号器失败: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctxDialer := dialer.(proxy.ContextDialer)
	conn, err := ctxDialer.DialContext(ctx, "tcp", target)
	if err != nil {
		t.Fatalf("经代理拨号失败: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	got := make([]byte, len("hello"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("回显 = %q, 期望 %q", got, "hello")
	}
	if targets := socks.Targets(); len(targets) != 1 || targets[0] != target {
		t.Fatalf("目标记录 = %v, 期望 [%s]", targets, target)
	}
}

// Given SOCKS5 服务 When 发送非 SOCKS5 版本握手 Then 连接被关闭且无目标记录
func TestSOCKS5Server_RejectsNonSocks5_ConnectionClosed(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })

	conn, err := net.DialTimeout("tcp", socks.Addr(), time.Second)
	if err != nil {
		t.Fatalf("连接服务器失败: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{4, 1, 0}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("非 SOCKS5 版本应被拒绝并关闭连接")
	}
	if targets := socks.Targets(); len(targets) != 0 {
		t.Fatalf("被拒握手不应记录目标, 实际 %v", targets)
	}
}

// Given SOCKS5 服务与回显目标 When 经代理按域名目标拨号 Then 转发成功且域名形式被记录
func TestSOCKS5Server_DomainTarget_ForwardsAndRecords(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })
	_, port, err := net.SplitHostPort(startEchoListener(t))
	if err != nil {
		t.Fatalf("拆分监听地址失败: %v", err)
	}
	domainTarget := net.JoinHostPort("localhost", port)

	dialer, err := proxy.SOCKS5("tcp", socks.Addr(), nil, proxy.Direct)
	if err != nil {
		t.Fatalf("创建拨号器失败: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialer.(proxy.ContextDialer).DialContext(ctx, "tcp", domainTarget)
	if err != nil {
		t.Fatalf("经代理拨号失败: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	got := make([]byte, len("hi"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if string(got) != "hi" {
		t.Fatalf("回显 = %q, 期望 %q", got, "hi")
	}
	if targets := socks.Targets(); len(targets) != 1 || targets[0] != domainTarget {
		t.Fatalf("目标记录 = %v, 期望 [%s]", targets, domainTarget)
	}
}

// Given SOCKS5 服务 When 经代理拨号 v6 环回的不可达端口 Then 连接被关闭且无成功目标记录
func TestSOCKS5Server_UpstreamUnreachable_ClosesConnection(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })

	dialer, err := proxy.SOCKS5("tcp", socks.Addr(), nil, proxy.Direct)
	if err != nil {
		t.Fatalf("创建拨号器失败: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := dialer.(proxy.ContextDialer).DialContext(ctx, "tcp", "[::1]:9"); err == nil {
		t.Fatal("上游不可达应返回错误")
	}
}

// Given 已启动的 SOCKS5 服务 When Close 两次 Then 均无错误
func TestSOCKS5Server_Close_Idempotent(t *testing.T) {
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	if socks.Addr() == "" {
		t.Fatal("监听地址不应为空")
	}
	if err := socks.Close(); err != nil {
		t.Fatalf("首次 Close 失败: %v", err)
	}
	if err := socks.Close(); err != nil {
		t.Fatalf("重复 Close 应幂等: %v", err)
	}
}
