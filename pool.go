package warppool

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mzzsfy/warp-pool/internal/amzwrap"
)

// probeFailLimit 空 Key 连续重探测上限,超限升级为重播
const probeFailLimit = 3

// stateFileFormat 实例 state 文件名格式
const stateFileFormat = "inst-%s.json"

// healthFunc 健康检查:经实例代理 listener 的连通性探测
type healthFunc func(ctx context.Context, proxyAddr string) error

// dialFunc 经实例代理 listener 的拨号实现(按 DialTransport 选择)
type dialFunc func(ctx context.Context, proxyAddr, network, addr string) (net.Conn, error)

// poolStats 池级原子计数(拨号热路径与实例管理 goroutine 直接累加,读端快照聚合)
type poolStats struct {
	replays      atomic.Int64
	dialTotal    atomic.Int64
	dialFailures atomic.Int64
}

// pool 实例集合编排核心;instances/usedKeys/keysByInst/emptyByInst/portOf 仅协调 goroutine 访问
type pool struct {
	opts        Options
	factory     amzwrap.Factory
	prober      Prober
	health      healthFunc
	dialThrough dialFunc

	ctx    context.Context
	cancel context.CancelFunc
	events chan event
	wake   chan struct{}
	wg     sync.WaitGroup

	instances   map[ID]*instance
	usedKeys    map[string]ID
	keysByInst  map[ID]string
	emptyByInst map[ID]int
	portOf      map[ID]int
	ports       *portAllocator
	replaySem   chan struct{}
	snap        atomic.Pointer[[]instanceView]
	stats       poolStats

	min       atomic.Int32
	max       atomic.Int32
	cursor    atomic.Uint64
	closed    atomic.Bool
	closeOnce atomic.Bool
	nextSeq   int
}

// newPool 校验配置并启动协调循环(依赖为 nil 时使用默认实现,异步拉起实例,不阻塞等待就绪)
func newPool(raw Options, factory amzwrap.Factory, prober Prober, health healthFunc) (*pool, error) {
	opts := raw.withDefaults()
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("配置非法: %w", err)
	}
	if factory == nil {
		factory = amzwrap.DefaultFactory{}
	}
	if prober == nil {
		prober = HTTPProber{V4URL: opts.EgressProbeV4URL, V6URL: opts.EgressProbeV6URL}
	}
	if health == nil {
		health = tcpHealth
	}
	dialThrough := dialFunc(amzwrap.DialThroughProxy)
	if opts.DialTransport == TransportHTTP {
		dialThrough = amzwrap.DialThroughProxyHTTP
	}
	if err := os.MkdirAll(opts.StateDir, 0o700); err != nil {
		return nil, fmt.Errorf("创建 state 目录失败: %w", err)
	}
	ports, err := newPortAllocator(opts.ListenBase)
	if err != nil {
		return nil, fmt.Errorf("ListenBase 非法: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &pool{
		opts:        opts,
		factory:     factory,
		prober:      prober,
		health:      health,
		dialThrough: dialThrough,
		ctx:         ctx,
		cancel:      cancel,
		events:      make(chan event, eventChanCap),
		wake:        make(chan struct{}, 1),
		instances:   make(map[ID]*instance),
		usedKeys:    make(map[string]ID),
		keysByInst:  make(map[ID]string),
		emptyByInst: make(map[ID]int),
		portOf:      make(map[ID]int),
		ports:       ports,
		replaySem:   make(chan struct{}, opts.ReplayConcurrency),
	}
	p.min.Store(int32(opts.Min))
	p.max.Store(int32(opts.Max))
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				opts.Logger.Printf("协调循环异常: %v", r)
				// 失去编排能力即拒绝新拨号,业务侧可感知重建池
				p.closed.Store(true)
			}
		}()
		p.reconcile()
	}()
	return p, nil
}

// reconcile 协调主循环:事件驱动 + 周期健康检查/巡检
func (p *pool) reconcile() {
	healthT := time.NewTicker(p.opts.HealthInterval)
	defer healthT.Stop()
	egressT := time.NewTicker(p.opts.EgressCheckInterval)
	defer egressT.Stop()
	p.align()
	for {
		select {
		case <-p.ctx.Done():
			return
		case ev := <-p.events:
			p.handleEvent(ev)
		case <-healthT.C:
			p.healthCheck()
			p.reconfirmProbing()
			p.align()
		case <-egressT.C:
			p.egressCheck()
			p.align()
		case <-p.wake:
			p.align()
		}
	}
}

// handleEvent 消费实例事件并驱动对齐
func (p *pool) handleEvent(ev event) {
	switch ev.kind {
	case evReady:
		p.handleReady(ev.inst, ev.egress)
	case evLost, evDrained, evReplayed, evDisabled:
		p.releaseKey(ev.inst.id)
		p.align()
	case evStopped:
		p.removeInstance(ev.inst)
		p.align()
	}
}

// handleReady 唯一性判定:空 Key 重探测(超限重播)、冲突较新者重播、唯一确认转 Normal
func (p *pool) handleReady(in *instance, eg Egress) {
	key := p.opts.DedupeKeyer.Key(eg)
	if key == "" {
		if n := p.emptyByInst[in.id] + 1; n < probeFailLimit {
			p.emptyByInst[in.id] = n
			in.send(command{kind: cmdReprobe})
			return
		}
		delete(p.emptyByInst, in.id)
		in.send(command{kind: cmdReplay})
		return
	}
	if owner, dup := p.usedKeys[key]; dup && owner != in.id {
		in.send(command{kind: cmdReplay})
		return
	}
	p.releaseKey(in.id)
	delete(p.emptyByInst, in.id)
	p.usedKeys[key] = in.id
	p.keysByInst[in.id] = key
	in.send(command{kind: cmdConfirm})
}

// releaseKey 释放实例占用的去重键
func (p *pool) releaseKey(id ID) {
	key, ok := p.keysByInst[id]
	if !ok {
		return
	}
	delete(p.keysByInst, id)
	if p.usedKeys[key] == id {
		delete(p.usedKeys, key)
	}
}

// removeInstance 移除终态实例并回收端口
func (p *pool) removeInstance(in *instance) {
	delete(p.instances, in.id)
	p.releaseKey(in.id)
	delete(p.emptyByInst, in.id)
	if off, ok := p.portOf[in.id]; ok {
		p.ports.release(off)
		delete(p.portOf, in.id)
	}
	p.publish()
}

// align 数量对齐:不足 min 补建(达 max 走淘汰,EvictNone 挂起等空位);超 max 按策略缩减
func (p *pool) align() {
	min, max := int(p.min.Load()), int(p.max.Load())
	if len(p.instances) > max {
		p.evictOnce()
		return
	}
	for p.activeCount() < min {
		if len(p.instances) >= max {
			p.evictOnce()
			return
		}
		if !p.createInstance() {
			return
		}
	}
}

// activeCount Normal 与 Probing 实例数
func (p *pool) activeCount() int {
	n := 0
	for _, in := range p.instances {
		if s := in.Status(); s == StatusNormal || s == StatusProbing {
			n++
		}
	}
	return n
}

// evictOnce 按策略淘汰一个实例腾位(EvictNone 即挂起)
func (p *pool) evictOnce() {
	victim, ok := p.opts.Evictor.Evict(p.views())
	if !ok {
		return
	}
	if in, exists := p.instances[victim]; exists {
		in.send(command{kind: cmdStop})
	}
}

// createInstance 分配端口并新建实例;端口耗尽返回 false
func (p *pool) createInstance() bool {
	addr, off, ok := p.ports.acquire()
	if !ok {
		p.opts.Logger.Printf("端口段耗尽, 无法新建实例")
		return false
	}
	seq := p.nextSeq
	p.nextSeq++
	id := ID(strconv.Itoa(seq))
	in := newInstance(p.ctx, instConfig{
		id:           id,
		proxyAddr:    addr,
		statePath:    filepath.Join(p.opts.StateDir, fmt.Sprintf(stateFileFormat, id)),
		endpoints:    p.opts.Endpoints,
		endpointIdx:  seq,
		factory:      p.factory,
		prober:       p.prober,
		probeTimeout: p.opts.HealthTimeout,
		backoffStart: p.opts.ReplayBackoffStart,
		backoffMax:   p.opts.ReplayBackoffMax,
		drainTimeout: p.opts.DrainTimeout,
		replaySem:    p.replaySem,
		events:       p.events,
		logger:       p.opts.Logger,
		stats:        &p.stats,
	})
	p.instances[id] = in
	p.portOf[id] = off
	p.publish()
	return true
}

// healthCheck 并发检查 Normal 实例,失败者下发排空
func (p *pool) healthCheck() {
	type result struct {
		in  *instance
		err error
	}
	var wg sync.WaitGroup
	ch := make(chan result, len(p.instances))
	for _, in := range p.instances {
		if in.Status() != StatusNormal {
			continue
		}
		wg.Add(1)
		go func(in *instance) {
			defer wg.Done()
			defer p.recoverProbe("健康检查")
			ctx, cancel := context.WithTimeout(p.ctx, p.opts.HealthTimeout)
			defer cancel()
			ch <- result{in: in, err: p.health(ctx, in.proxyAddr)}
		}(in)
	}
	wg.Wait()
	close(ch)
	for r := range ch {
		if r.err != nil {
			r.in.send(command{kind: cmdDrain, timeout: p.opts.DrainTimeout})
		}
	}
}

// egressCheck 并发重探测 Normal 实例出口,处理 Key 变更与撞车
func (p *pool) egressCheck() {
	type result struct {
		in  *instance
		eg  Egress
		err error
	}
	var wg sync.WaitGroup
	ch := make(chan result, len(p.instances))
	for _, in := range p.instances {
		if in.Status() != StatusNormal {
			continue
		}
		wg.Add(1)
		go func(in *instance) {
			defer wg.Done()
			defer p.recoverProbe("出口巡检")
			ctx, cancel := context.WithTimeout(p.ctx, p.opts.HealthTimeout)
			defer cancel()
			eg, err := p.prober.Probe(ctx, in.proxyAddr)
			ch <- result{in: in, eg: eg, err: err}
		}(in)
	}
	wg.Wait()
	close(ch)
	for r := range ch {
		p.applyEgress(r.in, r.eg, r.err)
	}
}

// recoverProbe 巡检类任务统一异常恢复
func (p *pool) recoverProbe(name string) {
	if r := recover(); r != nil {
		p.opts.Logger.Printf("%s任务异常: %v", name, r)
	}
}

// reconfirmProbing 补发可能丢失的唯一性确认(命令通道满时 send 会丢弃);
// 仅针对已登记 Key 且探测待确认的实例,空 Key 重探测循环不受影响
func (p *pool) reconfirmProbing() {
	for id, in := range p.instances {
		if _, decided := p.keysByInst[id]; decided && in.Status() == StatusProbing && in.awaitConfirm.Load() {
			in.send(command{kind: cmdConfirm})
		}
	}
}

// applyEgress 巡检结果处理:出口写回实例、不健康排空、Key 变更更新登记、撞车较新者排空
func (p *pool) applyEgress(in *instance, eg Egress, err error) {
	key := ""
	if err == nil {
		key = p.opts.DedupeKeyer.Key(eg)
		if eg != (Egress{}) {
			in.egressVal.Store(&eg)
		}
	}
	if key == "" {
		in.send(command{kind: cmdDrain, timeout: p.opts.DrainTimeout})
		return
	}
	if p.keysByInst[in.id] == key {
		return
	}
	if owner, dup := p.usedKeys[key]; dup && owner != in.id {
		victim := in
		if other, ok := p.instances[owner]; ok && other.createdAt.After(in.createdAt) {
			victim = other
		}
		victim.send(command{kind: cmdDrain, timeout: p.opts.DrainTimeout})
		return
	}
	p.releaseKey(in.id)
	p.usedKeys[key] = in.id
	p.keysByInst[in.id] = key
}

// views 淘汰决策候选快照
func (p *pool) views() []InstanceView {
	out := make([]InstanceView, 0, len(p.instances))
	for _, in := range p.instances {
		out = append(out, InstanceView{ID: in.id, Status: in.Status(), CreatedAt: in.createdAt})
	}
	return out
}

// publish 写时复制发布快照(按创建序排序,集合变化时调用;状态与出口经视图函数实时读)
func (p *pool) publish() {
	views := make([]instanceView, 0, len(p.instances))
	for _, in := range p.instances {
		views = append(views, in.view())
	}
	slices.SortFunc(views, viewLess)
	p.snap.Store(&views)
}

// viewLess 快照排序:ID 数值序即创建序
func viewLess(a, b instanceView) int {
	na, ea := strconv.Atoi(string(a.id))
	nb, eb := strconv.Atoi(string(b.id))
	if ea == nil && eb == nil {
		return cmp.Compare(na, nb)
	}
	return strings.Compare(string(a.id), string(b.id))
}

// snapshot 当前实例集合快照(dial 层只读消费)
func (p *pool) snapshot() []instanceView {
	if v := p.snap.Load(); v != nil {
		return *v
	}
	return nil
}

// setStatus 实例状态手动迁移;幂等返回 nil,非法迁移返回 ErrInvalidTransition,命令不可达返回 ErrClosed
func (p *pool) setStatus(id ID, target Status) error {
	var in *instance
	for _, v := range p.snapshot() {
		if v.id == id {
			in = v.inst
			break
		}
	}
	if in == nil {
		return errInstanceNotFound(id)
	}
	cur := in.Status()
	if cur == target {
		return nil
	}
	if !validTransition(cur, target) {
		return fmt.Errorf("%w: %s→%s", ErrInvalidTransition, cur, target)
	}
	var c command
	switch target {
	case StatusNormal:
		c = command{kind: cmdEnable}
	case StatusDraining:
		c = command{kind: cmdDrain, timeout: p.opts.DrainTimeout}
	case StatusDisabled:
		// 禁用即退出唯一性占用,由实例经 evDisabled 在协调循环释放
		c = command{kind: cmdDisable}
	}
	if !in.send(c) {
		return ErrClosed
	}
	return nil
}

// validTransition 手动迁移合法性:任意(Disabled 除外)→Draining、任意→Disabled 与 Disabled→Normal
func validTransition(cur, target Status) bool {
	switch target {
	case StatusDraining:
		return cur != StatusDisabled
	case StatusDisabled:
		return true
	case StatusNormal:
		return cur == StatusDisabled
	}
	return false
}

// setMin 调整实例数下限并触发对齐
func (p *pool) setMin(n int) error {
	if n <= 0 {
		return errors.New("Min 必须大于 0")
	}
	if int32(n) > p.max.Load() {
		return fmt.Errorf("Min(%d) 不能大于 Max(%d)", n, p.max.Load())
	}
	p.min.Store(int32(n))
	p.wakeup()
	return nil
}

// setMax 调整实例数上限并触发对齐(降低时按淘汰策略缩减)
func (p *pool) setMax(n int) error {
	if int32(n) < p.min.Load() {
		return fmt.Errorf("Max(%d) 不能小于 Min(%d)", n, p.min.Load())
	}
	p.max.Store(int32(n))
	p.wakeup()
	return nil
}

// wakeup 非阻塞唤醒协调循环
func (p *pool) wakeup() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Close 停止协调循环与全部实例,幂等;返回 nil
func (p *pool) Close() error {
	// closed 含 panic 置位场景,清理以 closeOnce 独立门控,保证异常后仍可回收
	if !p.closeOnce.CompareAndSwap(false, true) {
		return nil
	}
	p.closed.Store(true)
	p.cancel()
	p.wg.Wait()
	for _, in := range p.instances {
		in.waitStop()
	}
	return nil
}

// tcpHealth 经实例代理 listener 的 TCP 连通性探测
func tcpHealth(ctx context.Context, proxyAddr string) error {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return fmt.Errorf("代理连通性探测失败: %w", err)
	}
	return c.Close()
}

// portAllocator base+i 端口分配器,释放偏移回收复用
type portAllocator struct {
	host string
	base int
	next int
	used map[int]struct{}
	free []int
}

// newPortAllocator 解析监听基址
func newPortAllocator(listenBase string) (*portAllocator, error) {
	host, portStr, err := net.SplitHostPort(listenBase)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	if port < minListenPort || port > maxListenPort {
		return nil, fmt.Errorf("端口 %d 超出合法区间", port)
	}
	return &portAllocator{host: host, base: port, used: make(map[int]struct{})}, nil
}

// acquire 分配一个空闲端口偏移;耗尽返回 false
func (a *portAllocator) acquire() (string, int, bool) {
	if n := len(a.free); n > 0 {
		off := a.free[n-1]
		a.free = a.free[:n-1]
		a.used[off] = struct{}{}
		return a.addr(off), off, true
	}
	for ; a.base+a.next <= maxListenPort; a.next++ {
		if _, dup := a.used[a.next]; dup {
			continue
		}
		off := a.next
		a.used[off] = struct{}{}
		a.next++
		return a.addr(off), off, true
	}
	return "", 0, false
}

// release 回收端口偏移供复用
func (a *portAllocator) release(off int) {
	if _, ok := a.used[off]; !ok {
		return
	}
	delete(a.used, off)
	a.free = append(a.free, off)
}

// addr 偏移对应的监听地址
func (a *portAllocator) addr(off int) string {
	return net.JoinHostPort(a.host, strconv.Itoa(a.base+off))
}
