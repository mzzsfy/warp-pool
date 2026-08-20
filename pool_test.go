// 白盒测试:验证 pool 编排内部(reconcile/对齐/淘汰/唯一性/快照),
// 需注入 fake 工厂与假健康检查并读取未导出字段,故与实现同包。
package warppool

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool/internal/amzwrap"
	"github.com/mzzsfy/warp-pool/internal/fakeamz"
)

// pool 测试默认配置(周期与超时均压缩,禁长 sleep)
const (
	testListenBase     = "127.0.0.1:12000"
	testHealthInterval = 30 * time.Millisecond
	testEgressInterval = 60 * time.Millisecond
	testHealthTimeout  = 500 * time.Millisecond
	testPoolBackoff    = 2 * time.Millisecond
	testPoolBackoffMax = 10 * time.Millisecond
	testPoolDrain      = 40 * time.Millisecond
	egressCheckOff     = time.Hour // 默认关闭巡检的用例单独开启
)

// stubHealth 按代理地址返回健康/失败
type stubHealth struct {
	mu   sync.Mutex
	fail map[string]bool
}

func (h *stubHealth) check(_ context.Context, addr string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail[addr] {
		return errors.New("健康检查失败")
	}
	return nil
}

// setFail 标记指定地址不健康
func (h *stubHealth) setFail(addr string, fail bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail == nil {
		h.fail = map[string]bool{}
	}
	if fail {
		h.fail[addr] = true
		return
	}
	delete(h.fail, addr)
}

// poolEnv pool 测试环境:fake 工厂(落盘)+ 假探测 + 假健康检查
type poolEnv struct {
	t      *testing.T
	prober *stubProber
	health *stubHealth
	p      *pool

	mu         sync.Mutex
	clients    []*fakeamz.FakeClient
	stateAtNew []bool
}

// setResult 设置指定代理地址的探测出口
func (e *poolEnv) setResult(addr string, eg Egress) {
	e.prober.mu.Lock()
	defer e.prober.mu.Unlock()
	e.prober.results[addr] = eg
}

// newPoolForTest 构造注入 fake 的池并启动协调循环
func newPoolForTest(t *testing.T, mut func(o *Options)) *poolEnv {
	t.Helper()
	env := &poolEnv{
		t:      t,
		prober: &stubProber{results: map[string]Egress{}},
		health: &stubHealth{},
	}
	factory := fakeamz.FakeFactory{New: func(storagePath, listenAddr, endpoint string, _ amzwrap.Logger) (amzwrap.Client, error) {
		_, statErr := os.Stat(storagePath)
		c := &fileClient{FakeClient: fakeamz.NewFakeClient(listenAddr, endpoint), statePath: storagePath}
		env.mu.Lock()
		env.clients = append(env.clients, c.FakeClient)
		env.stateAtNew = append(env.stateAtNew, statErr == nil)
		env.mu.Unlock()
		return c, nil
	}}
	opts := Options{
		Min:                 1,
		Max:                 1,
		ListenBase:          testListenBase,
		StateDir:            t.TempDir(),
		DedupeKeyer:         DedupeByV4{}, // 测试出口数据仅 V4,显式固定键策略
		EgressProbeV4URL:    "http://v4",
		EgressProbeV6URL:    "http://v6",
		HealthInterval:      testHealthInterval,
		HealthTimeout:       testHealthTimeout,
		EgressCheckInterval: egressCheckOff,
		DrainTimeout:        testPoolDrain,
		ReplayBackoffStart:  testPoolBackoff,
		ReplayBackoffMax:    testPoolBackoffMax,
		ReplayConcurrency:   semCapOne,
	}
	if mut != nil {
		mut(&opts)
	}
	p, err := newPool(opts, factory, env.prober, env.health.check)
	if err != nil {
		t.Fatalf("构造池失败: %v", err)
	}
	env.p = p
	t.Cleanup(func() { p.Close() })
	return env
}

// clientCount 已创建客户端总数
func (e *poolEnv) clientCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.clients)
}

// eg 构造仅 V4 出口
func eg(ip string) Egress {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		panic(err)
	}
	return Egress{V4: addr}
}

// egBySeq 按序号构造互异出口
func egBySeq(i int) Egress { return eg(fmt.Sprintf("203.0.113.%d", i)) }

// waitNormalCount 等待快照中 Normal 实例数达到期望
func (e *poolEnv) waitNormalCount(n int) {
	e.t.Helper()
	deadline := time.After(testWaitDeadline)
	for {
		if countStatus(e.p.snapshot(), StatusNormal) == n {
			return
		}
		select {
		case <-deadline:
			e.t.Fatalf("等待 %d 个 Normal 超时, 当前 %d: %s", n, countStatus(e.p.snapshot(), StatusNormal), e.p.dumpStatus())
		case <-time.After(time.Millisecond):
		}
	}
}

// countStatus 统计快照内指定状态实例数
func countStatus(views []instanceView, s Status) int {
	n := 0
	for _, v := range views {
		if v.status() == s {
			n++
		}
	}
	return n
}

// waitTotalCount 等待池内实例总数达到期望
func (e *poolEnv) waitTotalCount(n int) {
	e.t.Helper()
	deadline := time.After(testWaitDeadline)
	for len(e.p.snapshot()) != n {
		select {
		case <-deadline:
			e.t.Fatalf("等待总数 %d 超时, 当前 %d", n, len(e.p.snapshot()))
		case <-time.After(time.Millisecond):
		}
	}
}

// normalViews 返回当前 Normal 实例视图
func (e *poolEnv) normalViews() []instanceView {
	var out []instanceView
	for _, v := range e.p.snapshot() {
		if v.status() == StatusNormal {
			out = append(out, v)
		}
	}
	return out
}

// Given min=3 When 启动池 Then 异步出现 3 个 Normal 且 state 目录落盘
func TestPool_StartsToMin(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 3, 3
	})
	for i, v := range []string{"127.0.0.1:12000", "127.0.0.1:12001", "127.0.0.1:12002"} {
		env.setResult(v, egBySeq(i+1))
	}
	env.waitNormalCount(3)
	files, err := os.ReadDir(env.p.opts.StateDir)
	if err != nil {
		t.Fatalf("读取 state 目录失败: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("state 文件数 = %d, 期望 3", len(files))
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given 某实例持续不健康 When 健康检查到期 Then 该实例排空重注册后回到 Normal
func TestPool_HealthCheckDrainsUnhealthy(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)
	env.health.setFail("127.0.0.1:12000", true)

	waitDrainThenRecover := func() {
		deadline := time.After(testWaitDeadline)
		drained := false
		for {
			s := env.p.snapshot()
			if !drained && len(s) == 1 && s[0].status() == StatusDraining {
				drained = true
			}
			if drained && len(s) == 1 && s[0].status() == StatusNormal {
				return
			}
			select {
			case <-deadline:
				t.Fatalf("期望 Draining→Normal 流转, drained=%v 当前 %s", drained, env.p.dumpStatus())
			case <-time.After(time.Millisecond):
			}
		}
	}
	waitDrainThenRecover()
	if env.clientCount() < 2 {
		t.Fatalf("排空应重注册客户端, 实际 %d 个", env.clientCount())
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given min=2 且一实例失联 When 失联事件到达 Then 池补建新实例回到 min
func TestPool_ReplacesLostNormal(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)
	first := env.client(0)

	if err := first.Close(); err != nil {
		t.Fatalf("外部关闭客户端失败: %v", err)
	}
	// 失联自愈异步:先等重建客户端,再等池回到 min
	waitClientCount(t, env, 3)
	env.waitNormalCount(2)
	if env.clientCount() < 3 {
		t.Fatalf("失联应触发补建, 客户端数 = %d", env.clientCount())
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// client 返回第 n 个创建的客户端
func (e *poolEnv) client(n int) *fakeamz.FakeClient {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.clients[n]
}

// Given min=2 max=3 且一实例失联 When 失联事件到达 Then 池补建新实例 Normal 回到 min
func TestPool_ReplacesLostWithNewInstance(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 3
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.setResult("127.0.0.1:12002", egBySeq(3))
	env.waitNormalCount(2)
	if err := env.client(0).Close(); err != nil {
		t.Fatalf("外部关闭客户端失败: %v", err)
	}

	// 失联实例(12000)重连复用自身槽位,同时池需保证 Normal≥min
	env.waitNormalCount(2)
	total := len(env.p.snapshot())
	if total < 2 {
		t.Fatalf("补建后实例总数 = %d, 期望 ≥ 2", total)
	}

	env.p.Close()
	waitNoLeak(t, before)
}

// Given 达 max 且需新建 When EvictOldest 决策 Then 老实例被 stop 腾位后新实例建立
func TestPool_EvictOldestMakesRoom(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
		o.Evictor = EvictOldest{}
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)
	oldest := env.normalViews()[0]

	// 禁用唯一实例:Normal 数 0 < min,总数 1 == max → EvictOldest stop 禁用者腾位新建
	env.p.setStatus("0", StatusDisabled)
	deadline := time.After(testWaitDeadline)
	for {
		gone := true
		for _, v := range env.p.snapshot() {
			if v.id == oldest.id {
				gone = false
			}
		}
		if gone && countStatus(env.p.snapshot(), StatusNormal) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("老实例 %s 应被淘汰移除且新实例就绪: %s", oldest.id, env.p.dumpStatus())
		case <-time.After(time.Millisecond):
		}
	}
	if env.clientCount() < 2 {
		t.Fatalf("淘汰后应新建客户端, 实际 %d 个", env.clientCount())
	}

	env.p.Close()
	waitNoLeak(t, before)
}

// Given EvictNone 且总数达 max When 需要新建 Then 新建挂起,腾位后继续
func TestPool_EvictNoneBackpressure(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)
	baseClients := env.clientCount()

	// 禁用一个实例:active=1<min 但总数=2==max 且 EvictNone → 不建新
	env.p.setStatus("0", StatusDisabled)
	deadline := time.After(200 * time.Millisecond)
	for {
		if allDisabledOrNormal(env) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("等待 Disabled 生效超时: %s", env.p.dumpStatus())
		case <-time.After(time.Millisecond):
		}
	}
	if n := env.clientCount(); n != baseClients {
		t.Fatalf("背压期不应新建客户端, %d → %d", baseClients, n)
	}

	// 手动 stop 被禁实例腾位 → 挂起的对齐继续建新
	for _, v := range env.p.snapshot() {
		if v.id == "0" {
			v.inst.send(command{kind: cmdStop})
		}
	}
	env.waitTotalCount(2)
	env.waitNormalCount(2)

	env.p.Close()
	waitNoLeak(t, before)
}

// allDisabledOrNormal 校验无 Probing/Draining 中间态
func allDisabledOrNormal(e *poolEnv) bool {
	for _, v := range e.p.snapshot() {
		if v.status() != StatusDisabled && v.status() != StatusNormal {
			return false
		}
	}
	return true
}

// Given 池内有 3 实例 When 降 min/max 至 2 Then 按淘汰策略缩减到 2
func TestPool_SetMaxShrinks(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 3, 3
		o.Evictor = EvictOldest{}
	})
	for i, addr := range []string{"127.0.0.1:12000", "127.0.0.1:12001", "127.0.0.1:12002"} {
		env.setResult(addr, egBySeq(i+1))
	}
	env.waitNormalCount(3)

	env.p.setMin(2)
	if err := env.p.setMax(2); err != nil {
		t.Fatalf("setMax 失败: %v", err)
	}
	env.waitTotalCount(2)
	env.waitNormalCount(2)

	env.p.Close()
	waitNoLeak(t, before)
}

// Given 先就绪实例已占 Key When 后实例探测出相同 Key Then 较新者删 state 重播换新身份
func TestPool_UniquenessCollisionReplaysNewer(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
	})
	same := egBySeq(1)
	env.setResult("127.0.0.1:12000", same)
	env.setResult("127.0.0.1:12001", same)
	env.waitNormalCount(1)

	// 先就绪者保持 Normal,后到者(动态识别)冲突重播;给予唯一出口后转 Normal
	first := env.normalViews()[0]
	other := "127.0.0.1:12001"
	if first.proxyAddr == "127.0.0.1:12001" {
		other = "127.0.0.1:12000"
	}
	env.setResult(other, egBySeq(2))
	env.waitNormalCount(2)
	if n := env.clientCount(); n < 3 {
		t.Fatalf("冲突者应重播重建客户端, 实际 %d 个", n)
	}

	env.p.Close()
	waitNoLeak(t, before)
}

// Given 空 Key 连续探测 When 达上限 Then 升级为重播换新身份
func TestPool_EmptyKeyReprobeThenReplay(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
		o.ReplayBackoffStart = 20 * time.Millisecond
		o.ReplayBackoffMax = 40 * time.Millisecond
		// Normal 后出口重发现依赖巡检,需开启
		o.EgressCheckInterval = testEgressInterval
	})
	env.waitTotalCount(1)
	// 重探测期(退避累计远未达上限)不应重建客户端
	time.Sleep(2 * testPoolBackoffMax)
	if n := env.clientCount(); n != 1 {
		t.Fatalf("空 Key 重探测期不应重建客户端, 实际 %d 个", n)
	}
	// 探测恢复有效 Key 后转 Normal,仍未重播
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)
	if n := env.clientCount(); n != 1 {
		t.Fatalf("恢复不应重播, 实际 %d 个客户端", n)
	}
	// 再次持续空 Key,达上限后升级重播
	env.setResult("127.0.0.1:12000", Egress{})
	waitClientCount(t, env, 2)

	env.p.Close()
	waitNoLeak(t, before)
}

// waitClientCount 等待已创建客户端数达到期望
func waitClientCount(t *testing.T, e interface{ clientCount() int }, n int) {
	t.Helper()
	deadline := time.After(testWaitDeadline)
	for e.clientCount() < n {
		select {
		case <-deadline:
			t.Fatalf("等待客户端数 %d 超时, 当前 %d", n, e.clientCount())
		case <-time.After(time.Millisecond):
		}
	}
}

// Given 巡检开启且实例出口迁移 When 巡检到期 Then Key 更新登记且不重播
func TestPool_EgressCheckUpdatesKey(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
		o.EgressCheckInterval = testEgressInterval
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)
	clients := env.clientCount()

	env.setResult("127.0.0.1:12000", egBySeq(2))
	// 轮询实例出口视图直至巡检生效(applyEgress 同时写回出口与 Key 登记),避免固定 sleep 抖动
	deadline := time.After(testWaitDeadline)
	for {
		if views := env.normalViews(); len(views) == 1 && views[0].egress() == egBySeq(2) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("等待巡检更新出口超时: %s", env.p.dumpStatus())
		case <-time.After(time.Millisecond):
		}
	}
	env.p.Close()
	// 协调循环已停,usedKeys 无并发写,可安全断言
	if id, ok := env.p.usedKeys[egBySeq(2).V4.String()]; !ok || id == "" {
		t.Fatalf("巡检应更新 Key 登记: %v", env.p.usedKeys)
	}
	if id, ok := env.p.usedKeys[egBySeq(1).V4.String()]; ok {
		t.Fatalf("旧 Key 应被替换, 仍被 %s 占用", id)
	}
	if n := env.clientCount(); n != clients {
		t.Fatalf("Key 迁移不应重播, 客户端 %d → %d", clients, n)
	}

	waitNoLeak(t, before)
}

// Given 巡检发现两 Normal 实例出口撞车 When 巡检到期 Then 较新者排空重播
func TestPool_EgressCheckCollisionDrainsNewer(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
		o.EgressCheckInterval = testEgressInterval
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)

	// 令老实例(12000)巡检时撞上新实例的 Key,较新者(12001)应排空重播
	env.setResult("127.0.0.1:12000", egBySeq(2))
	deadline := time.After(testWaitDeadline)
	for env.clientCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("巡检撞车应触发较新者排空重播, 客户端数 = %d", env.clientCount())
		case <-time.After(time.Millisecond):
		}
	}
	// 撞车者重播后给予唯一出口, 池应恢复 2 个 Normal
	env.setResult("127.0.0.1:12001", egBySeq(3))
	env.waitNormalCount(2)

	env.p.Close()
	waitNoLeak(t, before)
}

// Given 运行中的池 When Close Then 全部实例与协调循环退出且无 goroutine 泄漏
func TestPool_CloseNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)
	for _, c := range env.clientsSnapshot() {
		if c.Closed() {
			t.Fatal("关闭前客户端不应已 Close")
		}
	}
	env.p.Close()
	for _, c := range env.clientsSnapshot() {
		if !c.Closed() {
			t.Fatal("Close 后全部客户端应已 Close")
		}
	}
	waitNoLeak(t, before)
}

// clientsSnapshot 客户端列表副本
func (e *poolEnv) clientsSnapshot() []*fakeamz.FakeClient {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]*fakeamz.FakeClient(nil), e.clients...)
}

// Given 持续读写快照的并发负载 When 对齐与状态迁移进行 Then -race 无竞争且快照自洽
func TestPool_ConcurrentSnapshotRead(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 3
	})
	for i, addr := range []string{"127.0.0.1:12000", "127.0.0.1:12001", "127.0.0.1:12002"} {
		env.setResult(addr, egBySeq(i+1))
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, v := range env.p.snapshot() {
						_ = v.status()
						_ = v.egress()
					}
				}
			}
		}()
	}
	env.waitNormalCount(2)
	env.p.setStatus("0", StatusDisabled)
	env.waitNormalCount(2)
	close(stop)
	wg.Wait()

	env.p.Close()
	waitNoLeak(t, before)
}

// Given 已登记 Key 的实例处 Probing 且待确认 When 健康检查节拍补发确认 Then 实例恢复 Normal
// (确认命令被通道丢弃时的丢失防护)
func TestPool_ReconfirmPendingProbing(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)

	// 模拟确认丢失:实例停在已登记 Key 的待确认 Probing 态
	in := env.p.snapshot()[0].inst
	in.setStatus(StatusProbing)
	in.awaitConfirm.Store(true)
	waitStatus(t, in, StatusNormal)

	env.p.Close()
	waitNoLeak(t, before)
}

// panicKeyer 注入协调循环的唯一性判定异常
type panicKeyer struct{}

// Key 实现 DedupeKeyer
func (panicKeyer) Key(Egress) string { panic("注入异常") }

// Given 协调循环 panic When 业务关闭池 Then 实例与 goroutine 全部回收
func TestPool_ReconcilePanic_StillClosable(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.DedupeKeyer = panicKeyer{}
	})
	// 实例探测上报触发 handleReady panic,协调循环退出并置 closed
	deadline := time.After(testWaitDeadline)
	for !env.p.closed.Load() {
		select {
		case <-deadline:
			t.Fatal("等待协调循环 panic 超时")
		case <-time.After(time.Millisecond):
		}
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// dumpStatus 快照状态摘要(测试诊断输出)
func (p *pool) dumpStatus() string {
	var b strings.Builder
	for _, v := range p.snapshot() {
		fmt.Fprintf(&b, " [%s %s]", v.id, v.status())
	}
	return b.String()
}

// Given Endpoints 非空 When 池拉起多实例 Then 按创建序轮询绑定不同 endpoint
func TestPool_Endpoints_AssignedByCreationOrder(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
		o.Endpoints = []string{"162.159.192.1:2408", "162.159.193.10:500"}
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)
	eps := []string{"162.159.192.1:2408", "162.159.193.10:500"}
	for _, c := range env.clientsSnapshot() {
		off := 0
		switch c.ListenAddress() {
		case "127.0.0.1:12000":
			off = 0
		case "127.0.0.1:12001":
			off = 1
		default:
			t.Fatalf("未知监听地址 %s", c.ListenAddress())
		}
		if ep := c.Endpoint(); ep != eps[off] {
			t.Fatalf("监听 %s 应绑定 %s, 实际 %s", c.ListenAddress(), eps[off], ep)
		}
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given Endpoints 为空 When 实例创建 Then endpoint 为空串(自动选优)
func TestPool_Endpoints_EmptyList_AutoSelect(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, nil)
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)
	if ep := env.clientsSnapshot()[0].Endpoint(); ep != "" {
		t.Fatalf("空列表应自动选优, 实际 %q", ep)
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given 单元素 Endpoints When 实例数超过列表长度 Then 轮询复用同一 endpoint
func TestPool_Endpoints_ShorterThanInstances_ReuseByModulo(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 3, 3
		o.Endpoints = []string{"162.159.192.1:2408"}
	})
	for i, addr := range []string{"127.0.0.1:12000", "127.0.0.1:12001", "127.0.0.1:12002"} {
		env.setResult(addr, egBySeq(i+1))
	}
	env.waitNormalCount(3)
	for _, c := range env.clientsSnapshot() {
		if ep := c.Endpoint(); ep != "162.159.192.1:2408" {
			t.Fatalf("全部实例应绑定唯一 endpoint, 实际 %q", ep)
		}
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given 实例已绑定某 endpoint When 手动排空触发重播 Then 重建客户端轮换到下一个 endpoint
func TestPool_Endpoints_ReplayRotatesToNext(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
		o.Endpoints = []string{"10.0.0.1:2408", "10.0.0.2:2408"}
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)
	if err := env.p.setStatus("0", StatusDraining); err != nil {
		t.Fatalf("排空失败: %v", err)
	}
	waitClientCount(t, env, 3)
	env.waitNormalCount(2)
	snap := env.clientsSnapshot()
	if ep := snap[len(snap)-1].Endpoint(); ep != "10.0.0.2:2408" {
		t.Fatalf("重播后应轮换到下一个 endpoint, 实际 %q", ep)
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given ByV6 键策略且实例仅有互异 V4 出口 When 探测上报 Then 空键白名单放行,双双 Normal 零重播
func TestPool_EmptyKeyWhitelist_V4OnlyEnvironments_PassThrough(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
		o.DedupeKeyer = DedupeByV6{}
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.setResult("127.0.0.1:12001", egBySeq(2))
	env.waitNormalCount(2)
	if n := env.clientCount(); n != 2 {
		t.Fatalf("白名单放行不应重播, 实际 %d 个客户端", n)
	}
	if r := env.p.stats.replays.Load(); r != 0 {
		t.Fatalf("白名单放行不应计入重播, 实际 %d", r)
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given ByV6 键策略且两实例 V4 出口相同(键均为空) When 探测上报 Then 均放行,空键不去重不互斥
func TestPool_EmptyKeyWhitelist_SameV4Egress_BothPass(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 2, 2
		o.DedupeKeyer = DedupeByV6{}
	})
	same := egBySeq(1)
	env.setResult("127.0.0.1:12000", same)
	env.setResult("127.0.0.1:12001", same)
	env.waitNormalCount(2)
	if r := env.p.stats.replays.Load(); r != 0 {
		t.Fatalf("空键相同不应互斥重播, 实际 %d 次", r)
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given 全零出口(双栈探测失败) When 探测上报 Then 白名单不放行,保留重探测升级重播
func TestPool_EmptyKeyWhitelist_AllZeroEgress_StillReplays(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
		o.DedupeKeyer = DedupeByV6{}
	})
	// 不设探测结果: stub 返回全零
	env.waitTotalCount(1)
	deadline := time.After(testWaitDeadline)
	for env.p.stats.replays.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("全零出口应升级重播")
		case <-time.After(time.Millisecond):
		}
	}
	env.p.Close()
	waitNoLeak(t, before)
}

// Given 白名单 Normal 实例 When 巡检仍空键有效栈 Then 不排空零重播;巡检双栈全死 Then 排空自愈
func TestPool_EmptyKeyWhitelist_EgressCheckKeepsNormal(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newPoolForTest(t, func(o *Options) {
		o.Min, o.Max = 1, 1
		o.DedupeKeyer = DedupeByV6{}
		o.EgressCheckInterval = testEgressInterval
	})
	env.setResult("127.0.0.1:12000", egBySeq(1))
	env.waitNormalCount(1)
	// 轮询确认巡检至少跑过一轮(初探 1 次之后再次探测)再断言
	base := env.prober.callCount()
	deadline := time.After(testWaitDeadline)
	for env.prober.callCount() <= base {
		select {
		case <-deadline:
			t.Fatal("等待巡检执行超时")
		case <-time.After(time.Millisecond):
		}
	}
	if n := countStatus(env.p.snapshot(), StatusNormal); n != 1 {
		t.Fatalf("巡检空键有效栈应保持 Normal, 实际 %d", n)
	}
	if r := env.p.stats.replays.Load(); r != 0 {
		t.Fatalf("巡检空键有效栈应零重播, 实际 %d", r)
	}

	// 双栈全死: 巡检转排空重播自愈
	env.setResult("127.0.0.1:12000", Egress{})
	deadline = time.After(testWaitDeadline)
	for env.p.stats.replays.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("双栈全死应排空重播自愈")
		case <-time.After(time.Millisecond):
		}
	}
	env.p.Close()
	waitNoLeak(t, before)
}
