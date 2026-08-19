package testutil_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool/internal/testutil"
)

// sendConnect 向代理发送 CONNECT 请求并读取应答状态码
func sendConnect(t *testing.T, addr, target string) int {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("连接代理失败: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("发送 CONNECT 失败: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("读取应答失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// Given HTTP CONNECT 服务与回显目标 When 经 CONNECT 建隧道 Then 数据回显且目标被记录
func TestHTTPConnectServer_TunnelForwardsAndRecords(t *testing.T) {
	proxy, err := testutil.NewHTTPConnectServer()
	if err != nil {
		t.Fatalf("启动 HTTP 代理失败: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	target := startEchoListener(t)

	conn, err := net.DialTimeout("tcp", proxy.Addr(), time.Second)
	if err != nil {
		t.Fatalf("连接代理失败: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("发送 CONNECT 失败: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("读取应答失败: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("应答状态 = %d, 期望 200", resp.StatusCode)
	}
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
	if targets := proxy.Targets(); len(targets) != 1 || targets[0] != target {
		t.Fatalf("目标记录 = %v, 期望 [%s]", targets, target)
	}
}

// Given 注入非 2xx 应答的 HTTP 服务 When 发送 CONNECT Then 返回注入状态且不记录目标
func TestHTTPConnectServer_InjectedStatus_RejectsConnect(t *testing.T) {
	proxy, err := testutil.NewHTTPConnectServer()
	if err != nil {
		t.Fatalf("启动 HTTP 代理失败: %v", err)
	}
	proxy.SetStatus(http.StatusProxyAuthRequired)
	t.Cleanup(func() { _ = proxy.Close() })

	if code := sendConnect(t, proxy.Addr(), "127.0.0.1:80"); code != http.StatusProxyAuthRequired {
		t.Fatalf("应答状态 = %d, 期望 407", code)
	}
	if targets := proxy.Targets(); len(targets) != 0 {
		t.Fatalf("被拒请求不应记录目标, 实际 %v", targets)
	}
}

// Given 目标不可达 When 发送 CONNECT Then 应答 502 且不记录目标
func TestHTTPConnectServer_UpstreamUnreachable_Replies502(t *testing.T) {
	proxy, err := testutil.NewHTTPConnectServer()
	if err != nil {
		t.Fatalf("启动 HTTP 代理失败: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	if code := sendConnect(t, proxy.Addr(), "127.0.0.1:1"); code != http.StatusBadGateway {
		t.Fatalf("应答状态 = %d, 期望 502", code)
	}
	if targets := proxy.Targets(); len(targets) != 0 {
		t.Fatalf("不可达目标不应记录, 实际 %v", targets)
	}
}

// Given 已启动的 HTTP 服务 When Close 两次 Then 均无错误
func TestHTTPConnectServer_Close_Idempotent(t *testing.T) {
	proxy, err := testutil.NewHTTPConnectServer()
	if err != nil {
		t.Fatalf("启动 HTTP 代理失败: %v", err)
	}
	if proxy.Addr() == "" {
		t.Fatal("监听地址不应为空")
	}
	if err := proxy.Close(); err != nil {
		t.Fatalf("首次 Close 失败: %v", err)
	}
	if err := proxy.Close(); err != nil {
		t.Fatalf("重复 Close 应幂等: %v", err)
	}
}
