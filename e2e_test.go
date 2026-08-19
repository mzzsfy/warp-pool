//go:build e2e

// e2e 测试:依赖真实网络、Cloudflare WARP 注册与出口探测服务,且有频控风险,
// CI 默认跳过,手动执行:GOROOT= go test -tags e2e -run E2E -count=1 -v .
// 单实例仅重播一次即断言,不做重播循环长测。
package warppool_test

import (
	"context"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
)

// e2e 时长:amz 启动(90s)+ 双栈探测 + 排空重播,均留裕量
const (
	e2eReadyDeadline  = 4 * time.Minute
	e2eReplayDeadline = 3 * time.Minute
	e2eDrainTimeout   = 3 * time.Second
	e2eProbeURL       = "https://api4.ipify.org"
	e2ePollInterval   = 500 * time.Millisecond
)

// fetchIP 请求明文 IP 服务并解析为 V4 地址
func fetchIP(t *testing.T, client *http.Client) netip.Addr {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e2eProbeURL, nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("请求 %s 失败: %v", e2eProbeURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		t.Fatalf("读取响应失败: %v", err)
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(string(body)))
	if err != nil {
		t.Fatalf("响应 %q 非合法 IP: %v", body, err)
	}
	if !addr.Is4() {
		t.Fatalf("响应 %q 非 V4 地址", body)
	}
	return addr
}

// pollUntil 轮询断言直至条件成立或超时
func pollUntil(t *testing.T, d time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.After(d)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal(msg)
		case <-time.After(e2ePollInterval):
		}
	}
}

// Given 真实 WARP 环境 When min=2 起池拨号 Then 出口非本机、IP 池内唯一,排空单实例一次重播回 Normal
func TestE2E_池生命周期_拨号排空重播(t *testing.T) {
	p, err := warppool.New(warppool.Options{
		Min:                2,
		Max:                2,
		StateDir:           t.TempDir(),
		DrainTimeout:       e2eDrainTimeout,
		ReplayBackoffStart: time.Second,
		ReplayBackoffMax:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造池失败: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// 等待两实例就绪
	pollUntil(t, e2eReadyDeadline, "等待 2 个 Normal 超时", func() bool {
		n := 0
		for _, info := range p.Instances() {
			if info.Status == warppool.StatusNormal {
				n++
			}
		}
		return n == 2
	})

	// 经池出口必须区别于本机出口
	localIP := fetchIP(t, &http.Client{})
	proxyClient := &http.Client{Transport: &http.Transport{DialContext: p.DialContext}}
	poolIP := fetchIP(t, proxyClient)
	if poolIP == localIP {
		t.Fatalf("经池出口 %s 与本机相同, 隧道未生效", poolIP)
	}

	// 池内 Normal 实例出口互异(撞车由池自动重播消除)
	pollUntil(t, e2eReadyDeadline, "等待实例出口互异超时", func() bool {
		seen := map[netip.Addr]warppool.ID{}
		for _, info := range p.Instances() {
			if info.Status != warppool.StatusNormal {
				continue
			}
			if _, dup := seen[info.Egress.V4]; dup {
				return false
			}
			seen[info.Egress.V4] = info.ID
		}
		return len(seen) == 2
	})

	// 单实例排空一次:超时强断后重播换新身份并回 Normal
	var victim warppool.ID
	for _, info := range p.Instances() {
		if info.Status == warppool.StatusNormal {
			victim = info.ID
			break
		}
	}
	if victim == "" {
		t.Fatal("无 Normal 实例可排空")
	}
	oldEgress := egressOf(p, victim)
	if err := p.SetStatus(victim, warppool.StatusDraining); err != nil {
		t.Fatalf("排空失败: %v", err)
	}
	pollUntil(t, e2eReplayDeadline, "等待排空实例重播回 Normal 超时", func() bool {
		return statusOf(p, victim) == warppool.StatusNormal
	})
	if got := egressOf(p, victim); got == oldEgress && oldEgress != (netip.Addr{}) {
		t.Logf("重播后出口未变化: %s(出口多样性由 CF 分配, 不阻断)", got)
	}

	// 重播后池仍可拨号
	if again := fetchIP(t, proxyClient); again == localIP {
		t.Fatalf("重播后经池出口 %s 与本机相同", again)
	}
	_ = p.Close()
}

// statusOf 查询实例当前状态
func statusOf(p *warppool.Pool, id warppool.ID) warppool.Status {
	for _, info := range p.Instances() {
		if info.ID == id {
			return info.Status
		}
	}
	return warppool.StatusDisabled
}

// egressOf 查询实例当前 V4 出口
func egressOf(p *warppool.Pool, id warppool.ID) netip.Addr {
	for _, info := range p.Instances() {
		if info.ID == id {
			return info.Egress.V4
		}
	}
	return netip.Addr{}
}
