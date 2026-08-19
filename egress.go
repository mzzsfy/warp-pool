package warppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"sync"

	"golang.org/x/net/proxy"
)

// maxProbeBodyBytes 探测响应体长度上限,最长 IP 文本亦远小于此
const maxProbeBodyBytes = 64

// dedupeKeySep 双栈去重键拼接分隔符
const dedupeKeySep = "|"

// Egress 实例出口地址;探测失败的字段为零值(invalid)
type Egress struct {
	V4, V6 netip.Addr
}

// Prober 经指定实例代理探测出口
type Prober interface {
	// Probe 经 proxyAddr 指向的实例代理探测出口双栈地址;
	// 单栈失败不返回错误(该栈零值),双栈全失败返回错误
	Probe(ctx context.Context, proxyAddr string) (Egress, error)
}

// HTTPProber 默认 Prober:经实例代理请求明文 IP 探测服务,双栈并发
type HTTPProber struct {
	V4URL string // V4 探测服务地址
	V6URL string // V6 探测服务地址
}

// Probe 实现 Prober
func (p HTTPProber) Probe(ctx context.Context, proxyAddr string) (Egress, error) {
	var (
		v4, v6     netip.Addr
		err4, err6 error
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		v4, err4 = p.probeStack(ctx, proxyAddr, p.V4URL, true)
	}()
	go func() {
		defer wg.Done()
		v6, err6 = p.probeStack(ctx, proxyAddr, p.V6URL, false)
	}()
	wg.Wait()
	if err4 != nil && err6 != nil {
		return Egress{}, fmt.Errorf("双栈出口探测失败: %w", errors.Join(err4, err6))
	}
	return Egress{V4: v4, V6: v6}, nil
}

// probeStack 探测指定协议栈(v4 为真表示 V4 栈);响应非该族地址或请求失败返回零值与错误
func (p HTTPProber) probeStack(ctx context.Context, proxyAddr, url string, v4 bool) (netip.Addr, error) {
	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("创建 SOCKS5 拨号器失败: %w", err)
	}
	ctxDialer, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return netip.Addr{}, fmt.Errorf("SOCKS5 拨号器 %T 不支持 context", dialer)
	}
	transport := &http.Transport{DialContext: ctxDialer.DialContext}
	defer transport.CloseIdleConnections()
	body, err := fetchBody(ctx, &http.Client{Transport: transport}, url)
	if err != nil {
		return netip.Addr{}, err
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(body))
	if err != nil {
		return netip.Addr{}, fmt.Errorf("解析出口地址 %q 失败: %w", body, err)
	}
	if addr.Is4() != v4 {
		return netip.Addr{}, fmt.Errorf("出口地址 %q 与探测协议族不符", body)
	}
	return addr, nil
}

// fetchBody 请求 url 返回明文响应体
func fetchBody(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("构造探测请求失败: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("探测请求失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("探测响应状态异常: %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBodyBytes+1))
	if err != nil {
		return "", fmt.Errorf("读取探测响应失败: %w", err)
	}
	if len(data) > maxProbeBodyBytes {
		return "", errors.New("探测响应体超限")
	}
	return string(data), nil
}

// DedupeKeyer 去重键策略:池内两实例 Key 相同即出口 IP 重复,后到者重播
type DedupeKeyer interface {
	// Key 将出口地址映射为去重键;空 Key 表示探测不完整,同样触发重播
	Key(Egress) string
}

// DedupeByV4 以 V4 文本为键(默认策略)
type DedupeByV4 struct{}

// Key V4 零值或字段非 v4 地址时返回空
func (DedupeByV4) Key(e Egress) string {
	if !e.V4.Is4() {
		return ""
	}
	return e.V4.String()
}

// DedupeByV6 以 V6 文本为键
type DedupeByV6 struct{}

// Key V6 零值或字段非 v6 地址时返回空
func (DedupeByV6) Key(e Egress) string {
	if !e.V6.Is6() {
		return ""
	}
	return e.V6.String()
}

// DedupeByBoth 以 V4 与 V6 文本拼接为键
type DedupeByBoth struct{}

// Key 双栈均有效时返回 "v4|v6" 拼接,任一栈缺失返回空
func (DedupeByBoth) Key(e Egress) string {
	v4, v6 := DedupeByV4{}.Key(e), DedupeByV6{}.Key(e)
	if v4 == "" || v6 == "" {
		return ""
	}
	return v4 + dedupeKeySep + v6
}
