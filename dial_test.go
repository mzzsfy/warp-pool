// 黑盒测试:验证拨号选路(轮询/亲和/失败重试/候选过滤)与在途连接登记,
// 仅经公开 API 与测试钩子注入 fake,故为外部测试包。
package warppool_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
	"github.com/mzzsfy/warp-pool/internal/amzwrap"
	"github.com/mzzsfy/warp-pool/internal/fakeamz"
	"github.com/mzzsfy/warp-pool/internal/testutil"
)

// 拨号测试环境常量(周期性任务关闭,避免与健康检查互相干扰)
const (
	dialBasePort     = 13100
	dialWaitDeadline = 5 * time.Second
	dialPollInterval = 2 * time.Millisecond
	intervalOff      = time.Hour
	dialKeyCount     = 5
)

// stubProber 按代理地址返回预设出口
type stubProber struct {
	mu      sync.Mutex
	results map[string]warppool.Egress
}

// Probe 实现 Prober
func (p *stubProber) Probe(_ context.Context, addr string) (warppool.Egress, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.results[addr], nil
}

// set 设置指定地址的探测出口
func (p *stubProber) set(addr string, eg warppool.Egress) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.results[addr] = eg
}

// dialEnv 黑盒拨号测试环境:每实例地址挂真实假 SOCKS5 服务
type dialEnv struct {
	t      *testing.T
	p      *warppool.Pool
	prober *stubProber
	target net.Listener

	mu      sync.Mutex
	servers map[string]*testutil.SOCKS5Server
}

// addrOf 第 i 个实例的代理地址
func addrOf(i int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(dialBasePort+i))
}

// egV4 仅 V4 出口
func egV4(ip string) warppool.Egress {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		panic(err)
	}
	return warppool.Egress{V4: addr}
}

// egBySeq 按序号构造互异出口
func egBySeq(i int) warppool.Egress { return egV4(fmt.Sprintf("203.0.113.%d", i)) }

// newDialEnv 构造注入 fake 的池并启动
func newDialEnv(t *testing.T, min, max int, mut func(*warppool.Options)) *dialEnv {
	t.Helper()
	env := &dialEnv{
		t:       t,
		prober:  &stubProber{results: map[string]warppool.Egress{}},
		servers: map[string]*testutil.SOCKS5Server{},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("启动目标服务失败: %v", err)
	}
	env.target = ln
	factory := fakeamz.FakeFactory{New: func(_, listenAddr string, _ amzwrap.Logger) (amzwrap.Client, error) {
		if err := env.resetServer(listenAddr); err != nil {
			return nil, err
		}
		return fakeamz.NewFakeClient(listenAddr), nil
	}}
	opts := warppool.Options{
		Min:                 min,
		Max:                 max,
		ListenBase:          addrOf(0),
		StateDir:            t.TempDir(),
		EgressProbeV4URL:    "http://v4",
		EgressProbeV6URL:    "http://v6",
		HealthInterval:      intervalOff,
		HealthTimeout:       time.Second,
		EgressCheckInterval: intervalOff,
		DrainTimeout:        time.Minute,
		ReplayBackoffStart:  5 * time.Millisecond,
		ReplayBackoffMax:    20 * time.Millisecond,
		ReplayConcurrency:   1,
	}
	if mut != nil {
		mut(&opts)
	}
	p, err := warppool.NewForTest(opts, factory, env.prober, nil)
	if err != nil {
		t.Fatalf("构造池失败: %v", err)
	}
	env.p = p
	t.Cleanup(func() {
		_ = p.Close()
		_ = ln.Close()
		env.closeAllServers()
	})
	return env
}

// setEgress 设定第 i 个实例的探测出口
func (e *dialEnv) setEgress(i int, eg warppool.Egress) { e.prober.set(addrOf(i), eg) }

// resetServer 重建指定地址的假 SOCKS5 服务
func (e *dialEnv) resetServer(addr string) error {
	e.mu.Lock()
	old := e.servers[addr]
	e.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	srv, err := testutil.NewSOCKS5ServerAt("tcp", addr)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.servers[addr] = srv
	e.mu.Unlock()
	return nil
}

// closeServer 关闭第 i 个实例的假 SOCKS5 服务(拨号即拒)
func (e *dialEnv) closeServer(i int) {
	e.mu.Lock()
	srv := e.servers[addrOf(i)]
	delete(e.servers, addrOf(i))
	e.mu.Unlock()
	if srv != nil {
		_ = srv.Close()
	}
}

// closeAllServers 关闭全部假 SOCKS5 服务
func (e *dialEnv) closeAllServers() {
	e.mu.Lock()
	servers := e.servers
	e.servers = map[string]*testutil.SOCKS5Server{}
	e.mu.Unlock()
	for _, srv := range servers {
		_ = srv.Close()
	}
}

// shutdownServers 关闭假 SOCKS5 服务与目标监听(池外资源,供泄漏断言前清理)
func (e *dialEnv) shutdownServers() {
	e.closeAllServers()
	_ = e.target.Close()
}

// targetAddr 拨号目标地址(仅监听不应答)
func (e *dialEnv) targetAddr() string { return e.target.Addr().String() }

// waitNormal 等待 Normal 实例数达到期望
func (e *dialEnv) waitNormal(n int) {
	e.t.Helper()
	e.waitFor(func() bool {
		count := 0
		for _, info := range e.p.Instances() {
			if info.Status == warppool.StatusNormal {
				count++
			}
		}
		return count == n
	}, fmt.Sprintf("等待 %d 个 Normal 超时", n))
}

// waitStatus 等待指定实例达到目标状态
func (e *dialEnv) waitStatus(id warppool.ID, s warppool.Status) {
	e.t.Helper()
	e.waitFor(func() bool {
		for _, info := range e.p.Instances() {
			if info.ID == id {
				return info.Status == s
			}
		}
		return false
	}, fmt.Sprintf("等待实例 %s 变为 %s 超时", id, s))
}

// waitFor 轮询断言直至条件成立或超时
func (e *dialEnv) waitFor(cond func() bool, msg string) {
	e.t.Helper()
	deadline := time.After(dialWaitDeadline)
	for !cond() {
		select {
		case <-deadline:
			e.t.Fatalf("%s, 当前: %v", msg, e.p.Instances())
		case <-time.After(dialPollInterval):
		}
	}
}

// dialOK 拨号并断言成功,返回携带实例信息的连接
func (e *dialEnv) dialOK(f func(ctx context.Context, network, addr string) (net.Conn, error)) warppool.InstanceConn {
	e.t.Helper()
	conn, err := f(context.Background(), "tcp", e.targetAddr())
	if err != nil {
		e.t.Fatalf("拨号失败: %v", err)
	}
	ic, ok := conn.(warppool.InstanceConn)
	if !ok {
		_ = conn.Close()
		e.t.Fatalf("返回连接类型 %T 未实现 InstanceConn", conn)
	}
	return ic
}

// normalInfos 当前 Normal 实例信息
func (e *dialEnv) normalInfos() map[warppool.ID]warppool.InstanceInfo {
	out := map[warppool.ID]warppool.InstanceInfo{}
	for _, info := range e.p.Instances() {
		if info.Status == warppool.StatusNormal {
			out[info.ID] = info
		}
	}
	return out
}

// Given 3 个 Normal 实例 When 连续轮询拨号 3 次 Then 依次覆盖全部实例且信息一致
func TestDial_RoundRobin_依次覆盖全部实例(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)
	normal := env.normalInfos()
	if len(normal) != 3 {
		t.Fatalf("Normal 实例数 = %d, 期望 3", len(normal))
	}

	seen := map[warppool.ID]bool{}
	for range 3 {
		ic := env.dialOK(env.p.DialContext)
		info := ic.Instance()
		want, ok := normal[info.ID]
		if !ok {
			t.Fatalf("连接来源实例 %s 不在 Normal 集合", info.ID)
		}
		if info.Status != warppool.StatusNormal {
			t.Fatalf("连接实例状态 = %s, 期望 Normal", info.Status)
		}
		if info.Egress != want.Egress {
			t.Fatalf("连接出口 %v 与实例 %v 不一致", info.Egress, want.Egress)
		}
		seen[info.ID] = true
		_ = ic.Close()
	}
	if len(seen) != 3 {
		t.Fatalf("轮询应覆盖全部实例, 实际命中 %v", seen)
	}
}

// Given 亲和拨号 When 同 key 连续拨号 Then 全落同一实例;实例禁用后同 key 稳定换到新实例
func TestDial_KeyAffinity_同键稳定且禁用后重分布(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)

	dialKey := func() warppool.InstanceConn {
		return env.dialOK(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return env.p.DialContextWithKey(ctx, "tenant-a", network, addr)
		})
	}
	var first warppool.ID
	for i := range dialKeyCount {
		ic := dialKey()
		if i == 0 {
			first = ic.Instance().ID
			continue
		}
		if got := ic.Instance().ID; got != first {
			t.Fatalf("同 key 第 %d 次拨号落 %s, 期望稳定落 %s", i+1, got, first)
		}
		_ = ic.Close()
	}

	// 禁用亲和目标:候选剔除后同 key 应换到另一实例且保持稳定
	if err := env.p.SetStatus(first, warppool.StatusDisabled); err != nil {
		t.Fatalf("禁用实例失败: %v", err)
	}
	env.waitStatus(first, warppool.StatusDisabled)
	var second warppool.ID
	for i := range dialKeyCount {
		ic := dialKey()
		got := ic.Instance().ID
		if got == first {
			_ = ic.Close()
			t.Fatalf("禁用实例 %s 不应再被选中", first)
		}
		if i == 0 {
			second = got
			continue
		}
		if got != second {
			t.Fatalf("禁用后同 key 应稳定落 %s, 实际 %s", second, got)
		}
		_ = ic.Close()
	}
}

// Given key 为空 When 亲和拨号 Then 退化为轮询依次覆盖全部实例
func TestDial_KeyEmpty_退化为轮询(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)

	seen := map[warppool.ID]bool{}
	for range 3 {
		ic := env.dialOK(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return env.p.DialContextWithKey(ctx, "", network, addr)
		})
		seen[ic.Instance().ID] = true
		_ = ic.Close()
	}
	if len(seen) != 3 {
		t.Fatalf("空 key 轮询应覆盖全部实例, 实际命中 %v", seen)
	}
}

// Given 首个实例代理拒连 When 轮询拨号经过该实例 Then 自动换下一实例成功且无业务错误
func TestDial_实例拒连_自动换下一实例(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)
	env.closeServer(0)

	for range 6 {
		ic := env.dialOK(env.p.DialContext)
		if got := ic.Instance().ID; got == "0" {
			_ = ic.Close()
			t.Fatalf("拒连实例 0 不应被成功选中")
		}
		_ = ic.Close()
	}
}

// Given 全部实例代理拒连 When 拨号 Then 返回最后一个拨号错误而非 ErrNoInstance
func TestDial_全部拒连_返回最后错误(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)
	for i := range 3 {
		env.closeServer(i)
	}

	conn, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr())
	if err == nil {
		_ = conn.Close()
		t.Fatal("全部拒连时拨号应失败")
	}
	if conn != nil {
		t.Fatal("失败时不应返回连接")
	}
	if errors.Is(err, warppool.ErrNoInstance) || errors.Is(err, warppool.ErrClosed) {
		t.Fatalf("应返回拨号错误而非 %v", err)
	}
}

// Given 无 Normal 候选 When 拨号 Then 立即返回 ErrNoInstance
func TestDial_无Normal候选_返回ErrNoInstance(t *testing.T) {
	t.Run("池未就绪", func(t *testing.T) {
		// 探测恒为空 Key,实例停留在 Probing
		env := newDialEnv(t, 1, 1, nil)
		env.waitFor(func() bool { return len(env.p.Instances()) > 0 }, "等待实例创建超时")
		conn, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr())
		if !errors.Is(err, warppool.ErrNoInstance) {
			t.Fatalf("err = %v, 期望 ErrNoInstance", err)
		}
		if conn != nil {
			t.Fatal("无候选时不应返回连接")
		}
	})
	t.Run("全部禁用", func(t *testing.T) {
		env := newDialEnv(t, 1, 1, nil)
		env.setEgress(0, egBySeq(1))
		env.waitNormal(1)
		if err := env.p.SetStatus("0", warppool.StatusDisabled); err != nil {
			t.Fatalf("禁用失败: %v", err)
		}
		env.waitStatus("0", warppool.StatusDisabled)
		if _, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr()); !errors.Is(err, warppool.ErrNoInstance) {
			t.Fatalf("err = %v, 期望 ErrNoInstance", err)
		}
	})
}

// Given Draining 实例存在在途连接 When 排空超时到点 Then 强断在途连接且新拨号不再选中
func TestDial_Draining_强断在途连接(t *testing.T) {
	env := newDialEnv(t, 1, 1, func(o *warppool.Options) {
		o.DrainTimeout = 50 * time.Millisecond
	})
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)

	ic := env.dialOK(env.p.DialContext)
	if info := ic.Instance(); info.ID != "0" || info.Egress != egBySeq(1) {
		t.Fatalf("连接信息 %+v 与实例不符", info)
	}
	if err := env.p.SetStatus("0", warppool.StatusDraining); err != nil {
		t.Fatalf("排空失败: %v", err)
	}
	if err := env.p.SetStatus("0", warppool.StatusDraining); err != nil {
		t.Fatalf("重复排空应幂等: %v", err)
	}

	// Draining 期间新拨号应无候选
	env.waitStatus("0", warppool.StatusDraining)
	if _, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr()); !errors.Is(err, warppool.ErrNoInstance) {
		t.Fatalf("Draining 期拨号 err = %v, 期望 ErrNoInstance", err)
	}

	// 排空超时强断在途连接
	deadline := time.After(dialWaitDeadline)
	for {
		_ = ic.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := ic.Read(make([]byte, 1)); err != nil && !os.IsTimeout(err) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("等待在途连接被强断超时")
		case <-time.After(dialPollInterval):
		}
	}
	_ = ic.Close()
}

// Given 上下文已取消 When 拨号 Then 返回取消错误且不再尝试候选
func TestDial_上下文取消_返回取消错误(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := env.p.DialContext(ctx, "tcp", env.targetAddr())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, 期望 context.Canceled", err)
	}
	if conn != nil {
		t.Fatal("取消时不应返回连接")
	}
}

// Given 真实拨号路径(假 SOCKS5) When 多次拨号并逐个关闭 Then 在途登记随 Close 精确清空
func TestDial_在途登记随Close清空(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)

	var conns []net.Conn
	for range 3 {
		ic := env.dialOK(env.p.DialContext)
		if got := warppool.InflightConnsForTest(env.p, "0"); got != len(conns)+1 {
			t.Fatalf("在途登记数 = %d, 期望 %d", got, len(conns)+1)
		}
		conns = append(conns, ic)
	}
	for _, c := range conns {
		if err := c.Close(); err != nil {
			t.Fatalf("关闭连接失败: %v", err)
		}
	}
	if got := warppool.InflightConnsForTest(env.p, "0"); got != 0 {
		t.Fatalf("全部关闭后在途登记应清空, 残留 %d 条", got)
	}
}

// Given 已占 Key 的实例被禁用 When 新实例探测出相同 Key Then Key 已释放可被确认复用
func TestPool_禁用释放去重键(t *testing.T) {
	env := newDialEnv(t, 1, 2, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	if err := env.p.SetStatus("0", warppool.StatusDisabled); err != nil {
		t.Fatalf("禁用失败: %v", err)
	}
	env.waitStatus("0", warppool.StatusDisabled)

	// 新实例与被禁实例出口相同:去重键已释放则可直接确认转 Normal
	env.setEgress(1, egBySeq(1))
	env.waitFor(func() bool {
		for _, info := range env.p.Instances() {
			if info.ID == "1" && info.Status == warppool.StatusNormal && info.Egress == egBySeq(1) {
				return true
			}
		}
		return false
	}, "等待新实例复用被禁实例出口转 Normal 超时")
}

// Given 巡检开启且实例出口变更 When 巡检到期 Then 实例出口视图刷新为新值
func TestPool_巡检回写实例出口(t *testing.T) {
	env := newDialEnv(t, 1, 1, func(o *warppool.Options) {
		o.EgressCheckInterval = 30 * time.Millisecond
	})
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)

	env.setEgress(0, egBySeq(2))
	env.waitFor(func() bool {
		infos := env.p.Instances()
		return len(infos) == 1 && infos[0].Egress == egBySeq(2)
	}, "等待巡检刷新实例出口超时")
}

// Given 5 实例的亲和落点 When 候选新增 1 实例 Then 绝大多数 key 落点不变(rendezvous)
func TestDial_KeyAffinity_候选扩容仅少量迁移(t *testing.T) {
	env := newDialEnv(t, 5, 6, nil)
	for i := range 5 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(5)

	// 亲和迁移阈值:候选从 5 增至 6,期望仅约 1/6 的 key 迁移
	const keyTotal = 60
	keepThreshold := 7 * keyTotal / 10
	placement := func() map[string]warppool.ID {
		out := map[string]warppool.ID{}
		for i := range keyTotal {
			key := fmt.Sprintf("tenant-%d", i)
			ic := env.dialOK(func(ctx context.Context, network, addr string) (net.Conn, error) {
				return env.p.DialContextWithKey(ctx, key, network, addr)
			})
			out[key] = ic.Instance().ID
			_ = ic.Close()
		}
		return out
	}
	before := placement()
	env.setEgress(5, egBySeq(6))
	if err := env.p.SetMin(6); err != nil {
		t.Fatalf("SetMin 失败: %v", err)
	}
	env.waitNormal(6)
	after := placement()

	same := 0
	for key, id := range before {
		if after[key] == id {
			same++
		}
	}
	if same < keepThreshold {
		t.Fatalf("扩容后落点不变数 = %d/%d, 期望 ≥ %d", same, keyTotal, keepThreshold)
	}
}

// Given 池已关闭 When SetStatus Then 对已终态实例返回 ErrClosed
func TestPool_SetStatus_池已关闭_返回ErrClosed(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	if err := env.p.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := env.p.SetStatus("0", warppool.StatusDisabled); !errors.Is(err, warppool.ErrClosed) {
		t.Fatalf("err = %v, 期望 ErrClosed", err)
	}
}

// Given 带亲和 key 拨号 When 无 Normal 候选或全部拒连 Then 分别返回 ErrNoInstance 与最后错误
func TestDial_KeyAffinity_无候选与全拒连(t *testing.T) {
	t.Run("全部禁用返回ErrNoInstance", func(t *testing.T) {
		env := newDialEnv(t, 1, 1, nil)
		env.setEgress(0, egBySeq(1))
		env.waitNormal(1)
		if err := env.p.SetStatus("0", warppool.StatusDisabled); err != nil {
			t.Fatalf("禁用失败: %v", err)
		}
		env.waitStatus("0", warppool.StatusDisabled)
		_, err := env.p.DialContextWithKey(context.Background(), "k", "tcp", env.targetAddr())
		if !errors.Is(err, warppool.ErrNoInstance) {
			t.Fatalf("err = %v, 期望 ErrNoInstance", err)
		}
	})
	t.Run("全部拒连返回最后错误", func(t *testing.T) {
		env := newDialEnv(t, 3, 3, nil)
		for i := range 3 {
			env.setEgress(i, egBySeq(i+1))
		}
		env.waitNormal(3)
		for i := range 3 {
			env.closeServer(i)
		}
		conn, err := env.p.DialContextWithKey(context.Background(), "k", "tcp", env.targetAddr())
		if err == nil {
			_ = conn.Close()
			t.Fatal("全部拒连时亲和拨号应失败")
		}
		if errors.Is(err, warppool.ErrNoInstance) || errors.Is(err, warppool.ErrClosed) {
			t.Fatalf("应返回拨号错误而非 %v", err)
		}
	})
}

// Given 池已关闭 When 拨号或再次关闭 Then 拨号返回 ErrClosed 且 Close 幂等
func TestDial_池已关闭_返回ErrClosed(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	if err := env.p.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := env.p.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	if _, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr()); !errors.Is(err, warppool.ErrClosed) {
		t.Fatalf("err = %v, 期望 ErrClosed", err)
	}
	if _, err := env.p.DialContextWithKey(context.Background(), "k", "tcp", env.targetAddr()); !errors.Is(err, warppool.ErrClosed) {
		t.Fatalf("err = %v, 期望 ErrClosed", err)
	}
}
