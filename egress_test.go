package warppool_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
	"github.com/mzzsfy/warp-pool/internal/testutil"
)

// oversizedProbeBody 远超探测响应体上限的构造长度,验证超限防护
const oversizedProbeBody = 4096

// Given 三种 Keyer 与含零值/有效值/错族地址的出口 When 计算 Key Then 按策略映射或返回空
func TestDedupeKeyers(t *testing.T) {
	v4 := netip.MustParseAddr("203.0.113.7")
	v6 := netip.MustParseAddr("2001:db8::1")

	cases := []struct {
		name  string
		keyer warppool.DedupeKeyer
		eg    warppool.Egress
		want  string
	}{
		{"V4零值返回空", warppool.DedupeByV4{}, warppool.Egress{}, ""},
		{"V4有效返回文本", warppool.DedupeByV4{}, warppool.Egress{V4: v4}, "203.0.113.7"},
		{"V4字段存v6地址返回空", warppool.DedupeByV4{}, warppool.Egress{V4: v6}, ""},
		{"V6零值返回空", warppool.DedupeByV6{}, warppool.Egress{}, ""},
		{"V6有效返回文本", warppool.DedupeByV6{}, warppool.Egress{V6: v6}, "2001:db8::1"},
		{"V6字段存v4地址返回空", warppool.DedupeByV6{}, warppool.Egress{V6: v4}, ""},
		{"Both全零返回空", warppool.DedupeByBoth{}, warppool.Egress{}, ""},
		{"Both缺V4以空段拼接", warppool.DedupeByBoth{}, warppool.Egress{V6: v6}, "|2001:db8::1"},
		{"Both缺V6以空段拼接", warppool.DedupeByBoth{}, warppool.Egress{V4: v4}, "203.0.113.7|"},
		{"Both双栈有效返回拼接", warppool.DedupeByBoth{}, warppool.Egress{V4: v4, V6: v6}, "203.0.113.7|2001:db8::1"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.keyer.Key(c.eg); got != c.want {
				t.Fatalf("Key(%v) = %q, 期望 %q", c.eg, got, c.want)
			}
		})
	}
}

// startProbeStack 起本地 SOCKS5 代理与探测后端,返回经代理探测的 Prober
func startProbeStack(t *testing.T, handler http.Handler) (warppool.Prober, *testutil.SOCKS5Server, string) {
	t.Helper()
	socks, err := testutil.NewSOCKS5Server()
	if err != nil {
		t.Fatalf("启动 SOCKS5 服务器失败: %v", err)
	}
	t.Cleanup(func() { _ = socks.Close() })
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	prober := warppool.HTTPProber{V4URL: srv.URL + "/v4", V6URL: srv.URL + "/v6"}
	return prober, socks, strings.TrimPrefix(srv.URL, "http://")
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	addr, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("解析地址 %q 失败: %v", s, err)
	}
	return addr
}

// Given 双栈探测后端均返回明文 IP When 经代理探测 Then 解析双栈地址且两请求均经代理达后端
func TestHTTPProberBothStacks(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("203.0.113.10\n")) })
	mux.HandleFunc("/v6", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("2001:db8::1\n")) })
	prober, socks, backend := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err != nil {
		t.Fatalf("Probe 失败: %v", err)
	}
	if eg.V4 != mustAddr(t, "203.0.113.10") {
		t.Fatalf("V4 = %v, 期望 203.0.113.10", eg.V4)
	}
	if eg.V6 != mustAddr(t, "2001:db8::1") {
		t.Fatalf("V6 = %v, 期望 2001:db8::1", eg.V6)
	}

	targets := socks.Targets()
	if len(targets) != 2 {
		t.Fatalf("经代理请求数 = %d, 期望 2", len(targets))
	}
	for _, target := range targets {
		if target != backend {
			t.Fatalf("代理目标 %q, 期望后端 %q", target, backend)
		}
	}
}

// Given 单栈响应非法 When 探测 Then 不返回错误且失败栈为零值
func TestHTTPProberPartialFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("203.0.113.10\n")) })
	mux.HandleFunc("/v6", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not-an-ip\n")) })
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err != nil {
		t.Fatalf("单栈失败不应返回错误: %v", err)
	}
	if eg.V4 != mustAddr(t, "203.0.113.10") {
		t.Fatalf("V4 = %v, 期望 203.0.113.10", eg.V4)
	}
	if eg.V6.IsValid() {
		t.Fatalf("V6 = %v, 期望零值", eg.V6)
	}
}

// Given 双栈响应均非法 When 探测 Then 返回错误且 Egress 全零值
func TestHTTPProberAllFail(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("bad")) })
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err == nil {
		t.Fatal("双栈全失败应返回错误")
	}
	if eg.V4.IsValid() || eg.V6.IsValid() {
		t.Fatalf("全失败应返回零值 Egress, 实际 %v", eg)
	}
}

// Given V4 探测返回 v6 地址 When 探测 Then 该栈为零值,v6 栈正常
func TestHTTPProberWrongFamily(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("2001:db8::1\n")) })
	mux.HandleFunc("/v6", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("2001:db8::2\n")) })
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err != nil {
		t.Fatalf("v6 栈成功不应返回错误: %v", err)
	}
	if eg.V4.IsValid() {
		t.Fatalf("V4 = %v, 期望零值", eg.V4)
	}
	if eg.V6 != mustAddr(t, "2001:db8::2") {
		t.Fatalf("V6 = %v, 期望 2001:db8::2", eg.V6)
	}
}

// Given V4 响应体超长 When 探测 Then 该栈为零值,v6 栈正常
func TestHTTPProberBodyTooLarge(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("9", oversizedProbeBody)))
	})
	mux.HandleFunc("/v6", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("2001:db8::1\n")) })
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err != nil {
		t.Fatalf("v6 栈成功不应返回错误: %v", err)
	}
	if eg.V4.IsValid() {
		t.Fatalf("超限响应体应使 V4 为零值, 实际 %v", eg.V4)
	}
}

// Given V4 响应非 200 When 探测 Then 该栈为零值,v6 栈正常
func TestHTTPProberBadStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	mux.HandleFunc("/v6", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("2001:db8::1\n")) })
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err != nil {
		t.Fatalf("v6 栈成功不应返回错误: %v", err)
	}
	if eg.V4.IsValid() {
		t.Fatalf("非 200 状态应使 V4 为零值, 实际 %v", eg.V4)
	}
}

// Given 代理地址不可达 When 探测 Then 返回错误且 Egress 全零值
func TestHTTPProberProxyUnreachable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("203.0.113.10\n")) })
	prober, _, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eg, err := prober.Probe(ctx, "127.0.0.1:1")
	if err == nil {
		t.Fatal("代理不可达应返回错误")
	}
	if eg.V4.IsValid() || eg.V6.IsValid() {
		t.Fatalf("代理不可达应返回零值 Egress, 实际 %v", eg)
	}
}

// Given ctx 已取消 When 探测 Then 返回错误且 Egress 全零值
func TestHTTPProberContextCanceled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("203.0.113.10\n")) })
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err == nil {
		t.Fatal("ctx 已取消应返回错误")
	}
	if eg.V4.IsValid() || eg.V6.IsValid() {
		t.Fatalf("取消应返回零值 Egress, 实际 %v", eg)
	}
}

// Given 后端响应慢于 ctx 截止 When 探测 Then 按 ctx 截止返回错误
func TestHTTPProberContextTimeout(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte("203.0.113.10\n"))
	})
	prober, socks, _ := startProbeStack(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	eg, err := prober.Probe(ctx, socks.Addr())
	if err == nil {
		t.Fatal("超时应返回错误")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("探测耗时 %v, 未按 ctx 截止", time.Since(start))
	}
	if eg.V4.IsValid() || eg.V6.IsValid() {
		t.Fatalf("超时应返回零值 Egress, 实际 %v", eg)
	}
}
