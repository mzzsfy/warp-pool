// 白盒测试:验证 instance 内部状态机(命令/事件 channel、重播退避、在途连接强断),
// 需注入 fake 并读取未导出字段,故与实现同包。
package warppool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool/internal/amzwrap"
	"github.com/mzzsfy/warp-pool/internal/fakeamz"
)

// 测试默认时长(全部远短于生产默认,禁长 sleep)
const (
	testBackoffStart = 5 * time.Millisecond
	testBackoffMax   = 40 * time.Millisecond
	testProbeTimeout = 2 * time.Second
	testDrainTimeout = 60 * time.Millisecond
	testWaitDeadline = 3 * time.Second
	semCapOne        = 1
	stateFileName    = "inst-%s.json"
)

// stubProber 按 proxyAddr 返回预设出口或错误,记录调用序
type stubProber struct {
	mu      sync.Mutex
	results map[string]Egress
	errs    map[string]error
	calls   []string
}

func (p *stubProber) Probe(_ context.Context, addr string) (Egress, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, addr)
	if err, ok := p.errs[addr]; ok {
		return Egress{}, err
	}
	return p.results[addr], nil
}

// setErr 设置指定代理地址的探测错误(空地址清除)
func (p *stubProber) setErr(addr string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		delete(p.errs, addr)
		return
	}
	if p.errs == nil {
		p.errs = map[string]error{}
	}
	p.errs[addr] = err
}

// calls 返回调用序副本
func (p *stubProber) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// fileClient 模拟 amz 落盘时机:Start(注册)成功才写 state 文件
type fileClient struct {
	*fakeamz.FakeClient
	statePath string
}

func (c *fileClient) Start(ctx context.Context) error {
	if err := c.FakeClient.Start(ctx); err != nil {
		return err
	}
	return os.WriteFile(c.statePath, []byte("{}"), 0o600)
}

// instEnv 单实例测试环境:fake 工厂落盘模拟 + 事件通道 + 信号量
type instEnv struct {
	t         *testing.T
	prober    *stubProber
	events    chan event
	sem       chan struct{}
	factory   fakeamz.FakeFactory
	stateDir  string
	endpoints []string
	stats     poolStats

	mu         sync.Mutex
	clients    []*fakeamz.FakeClient
	stateAtNew []bool // 每次创建客户端时 state 文件是否已存在
	startErrs  map[string]error
}

func newInstEnv(t *testing.T, results map[string]Egress) *instEnv {
	t.Helper()
	env := &instEnv{
		t:         t,
		prober:    &stubProber{results: results},
		events:    make(chan event, eventChanCap),
		sem:       make(chan struct{}, semCapOne),
		stateDir:  t.TempDir(),
		startErrs: map[string]error{},
	}
	env.factory = fakeamz.FakeFactory{New: func(storagePath, listenAddr, endpoint string, _ amzwrap.Logger) (amzwrap.Client, error) {
		_, statErr := os.Stat(storagePath)
		c := &fileClient{FakeClient: fakeamz.NewFakeClient(listenAddr, endpoint), statePath: storagePath}
		env.mu.Lock()
		seq := len(env.clients)
		env.clients = append(env.clients, c.FakeClient)
		env.stateAtNew = append(env.stateAtNew, statErr == nil)
		if err := env.startErrs[fmt.Sprint(seq)]; err != nil {
			c.StartErr = err
		}
		env.mu.Unlock()
		return c, nil
	}}
	return env
}

// setStartErr 为第 n 次创建的客户端注入 Start 错误
func (e *instEnv) setStartErr(n int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err == nil {
		delete(e.startErrs, fmt.Sprint(n))
		return
	}
	e.startErrs[fmt.Sprint(n)] = err
}

// clientCount 已创建客户端数
func (e *instEnv) clientCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.clients)
}

// client 返回第 n 个创建的客户端
func (e *instEnv) client(n int) *fakeamz.FakeClient {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.clients[n]
}

// stateExistedAtNew 第 n 次创建客户端时 state 文件是否存在
func (e *instEnv) stateExistedAtNew(n int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stateAtNew[n]
}

// startInst 构造并启动一个实例管理 goroutine
func (e *instEnv) startInst(id ID, proxyAddr string) *instance {
	e.t.Helper()
	in := newInstance(context.Background(), instConfig{
		id:           id,
		proxyAddr:    proxyAddr,
		statePath:    filepath.Join(e.stateDir, fmt.Sprintf(stateFileName, id)),
		endpoints:    e.endpoints,
		factory:      e.factory,
		prober:       e.prober,
		probeTimeout: testProbeTimeout,
		backoffStart: testBackoffStart,
		backoffMax:   testBackoffMax,
		drainTimeout: testDrainTimeout,
		replaySem:    e.sem,
		events:       e.events,
		logger:       discardLogger{},
		stats:        &e.stats,
	})
	return in
}

// waitEvent 等待指定类型事件,超时失败
func (e *instEnv) waitEvent(kinds ...evKind) event {
	e.t.Helper()
	deadline := time.After(testWaitDeadline)
	for {
		select {
		case ev := <-e.events:
			for _, k := range kinds {
				if ev.kind == k {
					return ev
				}
			}
		case <-deadline:
			e.t.Fatalf("等待事件 %v 超时", kinds)
		}
	}
}

// expectNoEvent 断言短期内无事件到达
func (e *instEnv) expectNoEvent() {
	e.t.Helper()
	select {
	case ev := <-e.events:
		e.t.Fatalf("不应有事件,实际 %v", ev.kind)
	case <-time.After(30 * time.Millisecond):
	}
}

// waitStatus 等待实例状态到达期望值
func waitStatus(t *testing.T, in *instance, want Status) {
	t.Helper()
	deadline := time.After(testWaitDeadline)
	for in.Status() != want {
		select {
		case <-deadline:
			t.Fatalf("实例 %s 状态停在 %s, 期望 %s", in.id, in.Status(), want)
		case <-time.After(time.Millisecond):
		}
	}
}

// waitNoLeak 等待 goroutine 数回落到基线,超时报泄漏
func waitNoLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(testWaitDeadline)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if now := runtime.NumGoroutine(); now > before {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine 泄漏: 基线 %d, 当前 %d\n%s", before, now, buf[:n])
	}
}

// stopInst 停止实例并等待管理 goroutine 退出
func stopInst(t *testing.T, in *instance) {
	t.Helper()
	in.send(command{kind: cmdStop})
	waitInstDone(t, in)
}

// mustAddrLocal 解析地址文本,失败即测试失败
func mustAddrLocal(s string) netip.Addr {
	addr, err := netip.ParseAddr(s)
	if err != nil {
		panic(err)
	}
	return addr
}

// Given 命令驱动的状态迁移 When 多 goroutine 并发读快照字段 Then 无竞争且结果一致(-race 验证)
func TestInstance_ConcurrentStatusRead(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)

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
					_ = in.Status()
					_ = in.EgressView()
					_ = in.view()
				}
			}
		}()
	}
	in.send(command{kind: cmdConfirm})
	waitStatus(t, in, StatusNormal)
	in.send(command{kind: cmdDrain})
	waitStatus(t, in, StatusProbing)
	env.waitEvent(evReady)
	close(stop)
	wg.Wait()

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given 全部命令与事件类型 When 转字符串 Then 返回对应名称或 Unknown
func TestKind_String_MapsAll(t *testing.T) {
	cmds := []struct {
		kind cmdKind
		want string
	}{
		{cmdConfirm, "Confirm"},
		{cmdReprobe, "Reprobe"},
		{cmdReplay, "Replay"},
		{cmdDrain, "Drain"},
		{cmdDisable, "Disable"},
		{cmdEnable, "Enable"},
		{cmdStop, "Stop"},
		{cmdKind(200), "Unknown"},
	}
	for _, c := range cmds {
		if got := c.kind.String(); got != c.want {
			t.Fatalf("cmdKind(%d).String() = %q, 期望 %q", c.kind, got, c.want)
		}
	}
	evs := []struct {
		kind evKind
		want string
	}{
		{evReady, "Ready"},
		{evLost, "Lost"},
		{evDrained, "Drained"},
		{evReplayed, "Replayed"},
		{evStopped, "Stopped"},
		{evKind(200), "Unknown"},
	}
	for _, e := range evs {
		if got := e.kind.String(); got != e.want {
			t.Fatalf("evKind(%d).String() = %q, 期望 %q", e.kind, got, e.want)
		}
	}
}

// waitInstDone 等待实例全部 goroutine 退出
func waitInstDone(t *testing.T, in *instance) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		in.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(testWaitDeadline):
		t.Fatalf("实例 %s goroutine 未退出", in.id)
	}
}

// Given 可探测出口的实例 When 启动并发送确认 Then 转 Normal 且事件按序上报
func TestInstance_StartProbingToNormal(t *testing.T) {
	before := runtime.NumGoroutine()
	eg := Egress{V4: mustAddrLocal("203.0.113.1")}
	env := newInstEnv(t, map[string]Egress{":1": eg})
	in := env.startInst("1", ":1")

	ev := env.waitEvent(evReady)
	if ev.egress != eg {
		t.Fatalf("evReady egress = %v, 期望 %v", ev.egress, eg)
	}
	if in.Status() != StatusProbing {
		t.Fatalf("探测完成后应保持 Probing, 实际 %s", in.Status())
	}
	if !env.client(0).Started() {
		t.Fatal("首个客户端应已 Start")
	}
	if !in.send(command{kind: cmdConfirm}) {
		t.Fatal("cmdConfirm 发送失败")
	}
	waitStatus(t, in, StatusNormal)
	if got := in.EgressView(); got != eg {
		t.Fatalf("Normal 后 egress = %v, 期望 %v", got, eg)
	}

	stopInst(t, in)
	if !env.client(0).Closed() {
		t.Fatal("stop 后客户端应已 Close")
	}
	env.waitEvent(evStopped)
	waitNoLeak(t, before)
}

// Given 探测全零出口的实例 When 上报后收到重探测命令 Then 不删 state 重新探测
func TestInstance_ReprobeKeepsState(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)
	statePath := in.statePath

	in.send(command{kind: cmdReprobe})
	env.waitEvent(evReady)
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("重探测不应删 state: %v", err)
	}
	if env.clientCount() != 1 {
		t.Fatalf("重探测不应重建客户端, 实际 %d 个", env.clientCount())
	}

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given 唯一性冲突需重播的实例 When 收到重播命令 Then 删 state 重建客户端并按指数退避
func TestInstance_ReplayRemovesStateAndBackoff(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)

	start := time.Now()
	in.send(command{kind: cmdReplay})
	env.waitEvent(evReady)
	if first := time.Since(start); first < testBackoffStart*4/5 {
		t.Fatalf("首次重播应至少退避 %v, 实际 %v", testBackoffStart, first)
	}
	if env.stateExistedAtNew(1) {
		t.Fatal("重播重建时旧 state 应已被删")
	}
	if env.clientCount() != 2 {
		t.Fatalf("重播应重建客户端, 实际 %d 个", env.clientCount())
	}

	// 连续重播验证指数退避:第二次间隔按 2×起点计
	secondStart := time.Now()
	in.send(command{kind: cmdReplay})
	env.waitEvent(evReady)
	if second := time.Since(secondStart); second < 2*testBackoffStart*4/5 {
		t.Fatalf("第二次重播应按指数退避 ≥ %v, 实际 %v", 2*testBackoffStart, second)
	}

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given Normal 实例存在在途连接 When 收到排空命令 Then 期满强断连接、删 state、重注册后进入探测
func TestInstance_DrainForceClosesConns(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)
	in.send(command{kind: cmdConfirm})
	waitStatus(t, in, StatusNormal)

	clientEnd, tracked := net.Pipe()
	defer clientEnd.Close()
	in.trackConn(tracked)

	in.send(command{kind: cmdDrain})
	waitStatus(t, in, StatusDraining)
	if in.Status() == StatusNormal {
		t.Fatal("排空后不应为 Normal")
	}
	env.waitEvent(evDrained)
	waitStatus(t, in, StatusProbing)
	if _, err := tracked.Read(make([]byte, 1)); err == nil {
		t.Fatal("在途连接应被强断")
	}
	env.waitEvent(evReady)
	if env.stateExistedAtNew(1) {
		t.Fatal("排空重注册前应删 state")
	}
	if env.clientCount() != 2 {
		t.Fatalf("排空后应重注册客户端, 实际 %d 个", env.clientCount())
	}

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given 任意状态实例 When 禁用再启用 Then 关闭客户端保留 state, 启用后复用身份重探测
func TestInstance_DisableEnableKeepsState(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)
	in.send(command{kind: cmdConfirm})
	waitStatus(t, in, StatusNormal)

	in.send(command{kind: cmdDisable})
	waitStatus(t, in, StatusDisabled)
	env.waitEvent(evDisabled)
	if !env.client(0).Closed() {
		t.Fatal("禁用应 Close 客户端")
	}
	if _, err := os.Stat(in.statePath); err != nil {
		t.Fatalf("禁用不应删 state: %v", err)
	}

	in.send(command{kind: cmdEnable})
	waitStatus(t, in, StatusProbing)
	if _, err := os.Stat(in.statePath); err != nil {
		t.Fatalf("启用不应删 state: %v", err)
	}
	waitClientCount(t, env, 2)
	if !env.stateExistedAtNew(1) {
		t.Fatal("启用重建应复用 state")
	}
	env.waitEvent(evReady)

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given 运行中实例 When 隧道失联 Then 上报失联并复用 state 立即重连
func TestInstance_LostReconnectsKeepState(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)
	in.send(command{kind: cmdConfirm})
	waitStatus(t, in, StatusNormal)

	if err := env.client(0).Close(); err != nil {
		t.Fatalf("外部关闭客户端失败: %v", err)
	}
	env.waitEvent(evLost)
	waitStatus(t, in, StatusProbing)
	env.waitEvent(evReady)
	if !env.stateExistedAtNew(1) {
		t.Fatal("失联重连应复用 state")
	}
	if env.clientCount() != 2 {
		t.Fatalf("失联应重连新客户端, 实际 %d 个", env.clientCount())
	}

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given Start 失败的实例 When 反复重试 Then 退避升级为删 state 重注册直至成功
func TestInstance_StartFailureRetriesWithoutState(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	env.setStartErr(0, errors.New("注册失败"))
	in := env.startInst("1", ":1")
	env.waitEvent(evReady) // 首个失败后第二个客户端成功并完成探测
	if env.clientCount() < 2 {
		t.Fatalf("Start 失败应重试, 实际 %d 个客户端", env.clientCount())
	}
	if env.client(0).Started() {
		t.Fatal("Start 失败的客户端不应处于启动态")
	}
	if !env.client(0).Closed() {
		t.Fatal("Start 失败的客户端应被 Close")
	}

	stopInst(t, in)
	waitNoLeak(t, before)
}

// Given 信号量为 1 When 两实例并发重播 Then 注册段串行执行
func TestInstance_ReplaySerializedBySemaphore(t *testing.T) {
	before := runtime.NumGoroutine()
	var mu sync.Mutex
	inReg, maxReg := 0, 0
	slowNew := func(_, listenAddr, endpoint string, _ amzwrap.Logger) (amzwrap.Client, error) {
		mu.Lock()
		inReg++
		if inReg > maxReg {
			maxReg = inReg
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		inReg--
		mu.Unlock()
		return fakeamz.NewFakeClient(listenAddr, endpoint), nil
	}
	env := newInstEnv(t, map[string]Egress{})
	env.factory = fakeamz.FakeFactory{New: slowNew}
	in1 := env.startInst("1", ":1")
	env.factory = fakeamz.FakeFactory{New: slowNew}
	in2 := env.startInst("2", ":1")
	env.waitEvent(evReady)
	env.waitEvent(evReady)

	in1.send(command{kind: cmdReplay})
	in2.send(command{kind: cmdReplay})
	env.waitEvent(evReady)
	env.waitEvent(evReady)

	mu.Lock()
	peak := maxReg
	mu.Unlock()
	if peak > semCapOne {
		t.Fatalf("注册并发峰值 = %d, 应受信号量约束 ≤ %d", peak, semCapOne)
	}

	stopInst(t, in1)
	stopInst(t, in2)
	waitNoLeak(t, before)
}

// Given 实例绑定 endpoint When 保留身份路径重建客户端(禁用启用/失联重连) Then endpoint 不轮换
func TestInstance_KeepStatePaths_DoNotRotateEndpoint(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newInstEnv(t, map[string]Egress{})
	env.endpoints = []string{"10.0.0.1:2408", "10.0.0.2:2408"}
	in := env.startInst("1", ":1")
	env.waitEvent(evReady)
	in.send(command{kind: cmdConfirm})
	waitStatus(t, in, StatusNormal)
	if ep := env.client(0).Endpoint(); ep != "10.0.0.1:2408" {
		t.Fatalf("初始应绑定首个 endpoint, 实际 %q", ep)
	}

	in.send(command{kind: cmdDisable})
	waitStatus(t, in, StatusDisabled)
	in.send(command{kind: cmdEnable})
	waitClientCount(t, env, 2)
	env.waitEvent(evReady)
	if ep := env.client(1).Endpoint(); ep != "10.0.0.1:2408" {
		t.Fatalf("保留身份重建不应轮换 endpoint, 实际 %q", ep)
	}

	// 失联重连同为保留身份路径,不轮换
	in.send(command{kind: cmdConfirm})
	waitStatus(t, in, StatusNormal)
	if err := env.client(1).Close(); err != nil {
		t.Fatalf("关闭客户端失败: %v", err)
	}
	waitClientCount(t, env, 3)
	env.waitEvent(evReady)
	if ep := env.client(2).Endpoint(); ep != "10.0.0.1:2408" {
		t.Fatalf("失联重连不应轮换 endpoint, 实际 %q", ep)
	}

	stopInst(t, in)
	waitNoLeak(t, before)
}
