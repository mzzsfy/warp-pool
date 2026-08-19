package warppool

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mzzsfy/warp-pool/internal/amzwrap"
)

// amz 启动超时与内部通道容量
const (
	amzStartTimeout = 90 * time.Second
	cmdChanCap      = 16
	eventChanCap    = 32
	// runExitCap 失联信号缓冲:closeClient 使旧信号失效占位后,新失联仍可入队
	runExitCap = 2
	// backoffShiftLimit 退避指数移位上限,防溢出
	backoffShiftLimit = 30
)

// cmdKind pool → instance 的命令类型
type cmdKind uint8

const (
	cmdConfirm cmdKind = iota // Key 唯一确认,Probing→Normal
	cmdReprobe                // 空 Key 未超限,不删 state 重新探测
	cmdReplay                 // 删 state 重播,换取新设备身份
	cmdDrain                  // 排空:拒新拨号,期满强断在途并重注册
	cmdDisable                // 禁用:摘除服务,保留身份
	cmdEnable                 // 启用:复用 state 重新探测
	cmdStop                   // 终态:释放全部资源
)

// String 返回命令名
func (k cmdKind) String() string {
	switch k {
	case cmdConfirm:
		return "Confirm"
	case cmdReprobe:
		return "Reprobe"
	case cmdReplay:
		return "Replay"
	case cmdDrain:
		return "Drain"
	case cmdDisable:
		return "Disable"
	case cmdEnable:
		return "Enable"
	case cmdStop:
		return "Stop"
	}
	return "Unknown"
}

// command 实例管理命令
type command struct {
	kind    cmdKind
	timeout time.Duration // cmdDrain 的强断时限
}

// evKind instance → pool 的事件类型
type evKind uint8

const (
	evReady    evKind = iota // 探测完成待唯一性确认(含 Egress,可能全零)
	evLost                   // 隧道失联(Run 返回)
	evDrained                // 排空完成,已删 state 重注册进入探测
	evReplayed               // 重播完成进入探测
	evDisabled               // 已禁用,退出唯一性占用
	evStopped                // 终态退出
)

// event 实例上报事件
type event struct {
	inst   *instance
	kind   evKind
	egress Egress
}

// String 返回事件名
func (k evKind) String() string {
	switch k {
	case evReady:
		return "Ready"
	case evLost:
		return "Lost"
	case evDrained:
		return "Drained"
	case evReplayed:
		return "Replayed"
	case evDisabled:
		return "Disabled"
	case evStopped:
		return "Stopped"
	}
	return "Unknown"
}

// instConfig 实例构造参数
type instConfig struct {
	id           ID
	proxyAddr    string
	statePath    string
	factory      amzwrap.Factory
	prober       Prober
	probeTimeout time.Duration
	backoffStart time.Duration
	backoffMax   time.Duration
	drainTimeout time.Duration
	replaySem    chan struct{}
	events       chan event
	logger       Logger
}

// instance 单实例生命周期主体;状态写入仅发生在管理 goroutine,读端原子
type instance struct {
	id        ID
	createdAt time.Time
	proxyAddr string
	statePath string

	factory      amzwrap.Factory
	prober       Prober
	probeTimeout time.Duration
	backoffStart time.Duration
	backoffMax   time.Duration
	drainTimeout time.Duration
	replaySem    chan struct{}
	events       chan event
	logger       Logger

	ctx     context.Context
	cancel  context.CancelFunc
	cmds    chan command
	runExit chan uint64 // 失联通知,值为失联客户端的代次
	wg      sync.WaitGroup

	status       atomic.Uint32
	egressVal    atomic.Pointer[Egress]
	awaitConfirm atomic.Bool // 探测已上报待 pool 确认
	stopped      atomic.Bool

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	// 以下字段仅管理 goroutine 访问
	client   amzwrap.Client
	gen      uint64
	backoffN int
	pending  []command
}

// newInstance 构造实例并启动管理 goroutine(创建即启动)
func newInstance(parent context.Context, cfg instConfig) *instance {
	ctx, cancel := context.WithCancel(parent)
	in := &instance{
		id:           cfg.id,
		createdAt:    time.Now(),
		proxyAddr:    cfg.proxyAddr,
		statePath:    cfg.statePath,
		factory:      cfg.factory,
		prober:       cfg.prober,
		probeTimeout: cfg.probeTimeout,
		backoffStart: cfg.backoffStart,
		backoffMax:   cfg.backoffMax,
		drainTimeout: cfg.drainTimeout,
		replaySem:    cfg.replaySem,
		events:       cfg.events,
		logger:       cfg.logger,
		ctx:          ctx,
		cancel:       cancel,
		cmds:         make(chan command, cmdChanCap),
		runExit:      make(chan uint64, runExitCap),
		conns:        make(map[net.Conn]struct{}),
	}
	in.status.Store(uint32(StatusProbing))
	in.wg.Add(1)
	go func() {
		defer in.wg.Done()
		defer in.recoverManage()
		in.manage()
	}()
	return in
}

// recoverManage 管理循环退出收尾(含 panic 恢复)
func (in *instance) recoverManage() {
	if r := recover(); r != nil {
		in.logger.Printf("实例 %s 管理协程异常: %v", in.id, r)
	}
	in.finalize()
	// 终态上报尽力而为:非阻塞,池已关闭(缓冲满)时丢弃
	select {
	case in.events <- event{inst: in, kind: evStopped}:
	default:
	}
}

// instanceView dial 侧快照元素:集合成员不可变,状态与出口经函数实时原子读
type instanceView struct {
	id        ID
	status    func() Status
	egress    func() Egress
	proxyAddr string
	createdAt time.Time
	inst      *instance
}

// Status 当前状态(原子读)
func (in *instance) Status() Status { return Status(in.status.Load()) }

// EgressView 最近一次有效出口地址(原子读)
func (in *instance) EgressView() Egress {
	if p := in.egressVal.Load(); p != nil {
		return *p
	}
	return Egress{}
}

// view 生成 dial 侧只读视图
func (in *instance) view() instanceView {
	return instanceView{
		id:        in.id,
		status:    in.Status,
		egress:    in.EgressView,
		proxyAddr: in.proxyAddr,
		createdAt: in.createdAt,
		inst:      in,
	}
}

// send 非阻塞下发命令;实例已终态或通道满时丢弃并返回 false
func (in *instance) send(c command) bool {
	if in.stopped.Load() {
		return false
	}
	select {
	case in.cmds <- c:
		return true
	default:
		in.logger.Printf("实例 %s 命令通道满, 丢弃命令 %v", in.id, c.kind)
		return false
	}
}

// trackConn 登记在途连接(Draining 期满强断依据)
func (in *instance) trackConn(c net.Conn) {
	in.mu.Lock()
	in.conns[c] = struct{}{}
	in.mu.Unlock()
}

// untrackConn 移除在途连接登记
func (in *instance) untrackConn(c net.Conn) {
	in.mu.Lock()
	delete(in.conns, c)
	in.mu.Unlock()
}

// closeConns 强断全部在途连接并重置登记表
func (in *instance) closeConns() {
	in.mu.Lock()
	conns := in.conns
	in.conns = make(map[net.Conn]struct{})
	in.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// waitStop 等待实例全部 goroutine 退出
func (in *instance) waitStop() { in.wg.Wait() }

// manage 状态机主循环;返回即终态
func (in *instance) manage() {
	if !in.ensureClient(true, 0) {
		return
	}
	in.probeAndReport()
	for {
		if len(in.pending) > 0 {
			c := in.pending[0]
			in.pending = in.pending[1:]
			if in.handle(c) {
				return
			}
			continue
		}
		select {
		case <-in.ctx.Done():
			return
		case gen := <-in.runExit:
			if gen == in.gen && in.handleLost() {
				return
			}
		case c := <-in.cmds:
			if in.handle(c) {
				return
			}
		}
	}
}

// handle 处理单条命令;返回 true 表示进入终态
func (in *instance) handle(c command) (stop bool) {
	switch c.kind {
	case cmdConfirm:
		in.awaitConfirm.Store(false)
		if in.Status() == StatusProbing {
			in.setStatus(StatusNormal)
			in.backoffN = 0
		}
	case cmdReprobe:
		if in.Status() == StatusProbing && in.waitBackoff(in.backoffDelay()) {
			in.probeAndReport()
		}
	case cmdReplay:
		if s := in.Status(); s == StatusProbing || s == StatusNormal {
			in.setStatus(StatusProbing)
			if !in.ensureClient(false, in.backoffDelay()) {
				return true
			}
			in.emit(evReplayed, Egress{})
			in.probeAndReport()
		}
	case cmdDrain:
		if s := in.Status(); s == StatusNormal || s == StatusProbing {
			return in.drain(c.timeout)
		}
	case cmdDisable:
		if in.Status() != StatusDisabled {
			in.setStatus(StatusDisabled)
			in.closeClient()
			in.emit(evDisabled, Egress{})
		}
	case cmdEnable:
		if in.Status() == StatusDisabled {
			in.setStatus(StatusProbing)
			if !in.ensureClient(true, 0) {
				return true
			}
			in.probeAndReport()
		}
	case cmdStop:
		return true
	}
	return false
}

// handleLost 失联自愈:上报后复用 state 重连再探测;返回 true 表示终态
func (in *instance) handleLost() bool {
	in.setStatus(StatusProbing)
	in.emit(evLost, Egress{})
	if !in.ensureClient(true, 0) {
		return true
	}
	in.probeAndReport()
	return false
}

// drain 排空:拒新拨号(状态即 Draining),期满强断在途、删 state 重注册;返回 true 表示终态
func (in *instance) drain(timeout time.Duration) (stop bool) {
	if timeout <= 0 {
		timeout = in.drainTimeout
	}
	in.setStatus(StatusDraining)
	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			in.closeConns()
			in.setStatus(StatusProbing)
			in.emit(evDrained, Egress{})
			if !in.ensureClient(false, in.backoffDelay()) {
				return true
			}
			in.probeAndReport()
			return false
		case <-in.ctx.Done():
			return true
		case c := <-in.cmds:
			switch c.kind {
			case cmdStop:
				return true
			case cmdDisable:
				in.setStatus(StatusDisabled)
				in.closeClient()
				return false
			default:
				in.pending = append(in.pending, c)
			}
		}
	}
}

// ensureClient 确保存在运行中客户端:首轮等待 firstDelay,失败重试按指数退避并降级为删 state;返回 false 表示终态
func (in *instance) ensureClient(keepState bool, firstDelay time.Duration) bool {
	delay := firstDelay
	for {
		in.closeClient()
		if !keepState {
			in.removeState()
		}
		if delay > 0 && !in.waitBackoff(delay) {
			return false
		}
		c, ok := in.startUnderSem()
		if !ok {
			return false
		}
		if c != nil {
			in.client = c
			in.watchRun(c)
			return true
		}
		keepState = false
		delay = in.backoffDelay()
	}
}

// startUnderSem 全局重播并发约束内建立并启动客户端;
// ok 为 false 表示终态,client 为 nil 表示本轮失败可重试
func (in *instance) startUnderSem() (client amzwrap.Client, ok bool) {
	if !in.acquireSem() {
		return nil, false
	}
	defer in.releaseSem()
	c, err := in.factory.NewClient(in.statePath, in.proxyAddr, in.logger)
	if err == nil {
		err = in.startClient(c)
	}
	if err != nil {
		in.logger.Printf("实例 %s 客户端建立失败: %v", in.id, err)
		return nil, true
	}
	return c, true
}

// startClient 以固定超时启动客户端,失败即关闭
func (in *instance) startClient(c amzwrap.Client) error {
	ctx, cancel := context.WithTimeout(in.ctx, amzStartTimeout)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		_ = c.Close()
		return fmt.Errorf("amz 启动失败: %w", err)
	}
	return nil
}

// watchRun 启动 Run 维持协程,Run 返回即上报失联代次
func (in *instance) watchRun(c amzwrap.Client) {
	in.gen++
	gen := in.gen
	in.wg.Add(1)
	go func() {
		defer in.wg.Done()
		_ = c.Run()
		select {
		case in.runExit <- gen:
		default:
		}
	}()
}

// closeClient 关闭当前客户端并使旧失联信号失效
func (in *instance) closeClient() {
	in.gen++
	c := in.client
	in.client = nil
	if c != nil {
		if err := c.Close(); err != nil {
			in.logger.Printf("实例 %s 客户端关闭失败: %v", in.id, err)
		}
	}
}

// removeState 删除 state 文件(重播换取新身份)
func (in *instance) removeState() {
	if err := os.Remove(in.statePath); err != nil && !os.IsNotExist(err) {
		in.logger.Printf("实例 %s 删除 state 失败: %v", in.id, err)
	}
}

// backoffDelay 计算并推进当前指数退避时长
func (in *instance) backoffDelay() time.Duration {
	shift := min(in.backoffN, backoffShiftLimit)
	in.backoffN++
	d := in.backoffStart << shift
	if d > in.backoffMax || d <= 0 {
		return in.backoffMax
	}
	return d
}

// waitBackoff 等待指定时长(可被终态打断);期间收到的非终态命令转入待处理队列
func (in *instance) waitBackoff(d time.Duration) bool {
	for {
		t := time.NewTimer(d)
		select {
		case <-t.C:
			return true
		case <-in.ctx.Done():
			t.Stop()
			return false
		case c := <-in.cmds:
			t.Stop()
			if c.kind == cmdStop {
				return false
			}
			in.pending = append(in.pending, c)
		}
	}
}

// acquireSem 获取全局重播信号量
func (in *instance) acquireSem() bool {
	select {
	case in.replaySem <- struct{}{}:
		return true
	case <-in.ctx.Done():
		return false
	}
}

// releaseSem 释放全局重播信号量
func (in *instance) releaseSem() { <-in.replaySem }

// probeAndReport 单次出口探测并上报待确认(全零结果同样上报,由 pool 判空 Key)
func (in *instance) probeAndReport() {
	ctx, cancel := context.WithTimeout(in.ctx, in.probeTimeout)
	defer cancel()
	eg, _ := in.prober.Probe(ctx, in.proxyAddr)
	if eg != (Egress{}) {
		in.egressVal.Store(&eg)
	}
	in.awaitConfirm.Store(true)
	in.emit(evReady, eg)
}

// emit 上报事件;池关闭(ctx 结束)时放弃
func (in *instance) emit(kind evKind, eg Egress) {
	select {
	case in.events <- event{inst: in, kind: kind, egress: eg}:
	case <-in.ctx.Done():
	}
}

// finalize 终态收尾:释放客户端与在途连接,清理 state 文件(实例 ID 不复用)
func (in *instance) finalize() {
	in.stopped.Store(true)
	in.cancel()
	in.closeClient()
	in.closeConns()
	in.removeState()
}

// setStatus 状态迁移(仅管理 goroutine 调用)
func (in *instance) setStatus(s Status) { in.status.Store(uint32(s)) }
