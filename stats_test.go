// 黑盒测试:验证 Stats 快照(状态计数/重播累计/拨号累计)与操作精确对应,
// 重播精确计数依赖阻塞式探测逐步放行,经测试钩子注入 fake,故为外部测试包。
package warppool_test

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
	"github.com/mzzsfy/warp-pool/internal/fakeamz"
)

// statsPool 测试端口基址与时长(周期任务关闭,探测超时留足放行窗口)
const (
	statsBasePort   = 13400
	statsProbeWait  = 5 * time.Second
	statsEmptyLimit = 3 // 空 Key 连续探测上限(与实现一致)
)

// gateProber 阻塞式探测:每次探测挂起直至放行一个预设出口
type gateProber struct {
	release chan warppool.Egress
}

// Probe 实现 Prober
func (p *gateProber) Probe(ctx context.Context, _ string) (warppool.Egress, error) {
	select {
	case eg := <-p.release:
		return eg, nil
	case <-ctx.Done():
		return warppool.Egress{}, ctx.Err()
	}
}

// newReplayPool 构造可精确控制探测节奏的单实例池
func newReplayPool(t *testing.T) (*warppool.Pool, *gateProber) {
	t.Helper()
	prober := &gateProber{release: make(chan warppool.Egress)}
	p, err := warppool.NewForTest(warppool.Options{
		Min:                 1,
		Max:                 1,
		ListenBase:          net.JoinHostPort("127.0.0.1", strconv.Itoa(statsBasePort)),
		StateDir:            t.TempDir(),
		EgressProbeV4URL:    "http://v4",
		EgressProbeV6URL:    "http://v6",
		HealthInterval:      intervalOff,
		HealthTimeout:       statsProbeWait,
		EgressCheckInterval: intervalOff,
		DrainTimeout:        time.Minute,
		ReplayBackoffStart:  2 * time.Millisecond,
		ReplayBackoffMax:    10 * time.Millisecond,
		ReplayConcurrency:   1,
	}, fakeamz.FakeFactory{}, prober, nil)
	if err != nil {
		t.Fatalf("构造池失败: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, prober
}

// replayRound 放行一轮空 Key 探测(触发一次重播)
func (p *gateProber) replayRound() {
	for range statsEmptyLimit {
		p.release <- warppool.Egress{}
	}
}

// Given 阻塞探测逐步放行 When 空 Key 满一轮 Then 重播计数精确为一,再一轮为二
func TestStats_重播计数_逐轮精确累加(t *testing.T) {
	p, gate := newReplayPool(t)
	gate.replayRound()
	waitForStats(t, p, func(s warppool.Stats) bool { return s.Replays == 1 })
	gate.replayRound()
	waitForStats(t, p, func(s warppool.Stats) bool { return s.Replays == 2 })

	// 放行有效出口后实例转 Normal,状态计数同步
	gate.release <- egV4("203.0.113.10")
	waitForStats(t, p, func(s warppool.Stats) bool {
		return s.Normal == 1 && s.Total == 1 && s.Replays == 2
	})
}

// Given 多状态实例并存 When 查询快照 Then 状态计数与 Instances 完全一致
func TestStats_状态计数与实例快照一致(t *testing.T) {
	env := newDialEnv(t, 3, 3, nil)
	for i := range 3 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(3)
	if s := env.p.Stats(); s.Normal != 3 || s.Total != 3 || s.Probing != 0 || s.Draining != 0 || s.Disabled != 0 {
		t.Fatalf("全 Normal 快照 = %+v", s)
	}

	if err := env.p.SetStatus("0", warppool.StatusDisabled); err != nil {
		t.Fatalf("禁用失败: %v", err)
	}
	if err := env.p.SetStatus("1", warppool.StatusDraining); err != nil {
		t.Fatalf("排空失败: %v", err)
	}
	env.waitStatus("0", warppool.StatusDisabled)
	env.waitStatus("1", warppool.StatusDraining)

	s := env.p.Stats()
	if s.Normal != 1 || s.Draining != 1 || s.Disabled != 1 || s.Total != 3 {
		t.Fatalf("混合状态快照 = %+v", s)
	}
	// 与 Instances 逐一核对
	counts := map[warppool.Status]int{}
	for _, info := range env.p.Instances() {
		counts[info.Status]++
	}
	if counts[warppool.StatusNormal] != s.Normal || counts[warppool.StatusDraining] != s.Draining ||
		counts[warppool.StatusDisabled] != s.Disabled || len(counts) != s.Total {
		t.Fatalf("快照 %+v 与实例列表 %v 不一致", s, counts)
	}
}

// Given 单实例 When 拨号成功与失败各一次 Then 拨号累计与失败累计各精确计一
func TestStats_拨号成功失败各计(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	if s := env.p.Stats(); s.DialTotal != 0 || s.DialFails != 0 {
		t.Fatalf("拨号前计数应零: %+v", s)
	}

	ic := env.dialOK(env.p.DialContext)
	if s := env.p.Stats(); s.DialTotal != 1 || s.DialFails != 0 {
		t.Fatalf("一次成功后 = %+v, 期望 DialTotal 1 / DialFails 0", s)
	}
	_ = ic.Close()

	env.closeServer(0)
	if _, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr()); err == nil {
		t.Fatal("代理拒连时拨号应失败")
	}
	if s := env.p.Stats(); s.DialTotal != 2 || s.DialFails != 1 {
		t.Fatalf("一次失败后 = %+v, 期望 DialTotal 2 / DialFails 1", s)
	}
}

// statsDialWorkers 并发拨号测试参数
const (
	statsDialWorkers    = 4
	statsDialsPerWorker = 25
)

// Given 多 goroutine 并发拨号 When 全部成功 Then 拨号累计无丢失
func TestStats_并发拨号计数无丢失(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)

	var wg sync.WaitGroup
	errCh := make(chan error, statsDialWorkers)
	for range statsDialWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range statsDialsPerWorker {
				conn, err := env.p.DialContext(context.Background(), "tcp", env.targetAddr())
				if err != nil {
					errCh <- err
					return
				}
				_ = conn.Close()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发拨号失败: %v", err)
	}
	if s := env.p.Stats(); s.DialTotal != statsDialWorkers*statsDialsPerWorker || s.DialFails != 0 {
		t.Fatalf("并发计数 = %+v, 期望 DialTotal %d / DialFails 0", s, statsDialWorkers*statsDialsPerWorker)
	}
}

// waitForStats 轮询等待快照条件成立
func waitForStats(t *testing.T, p *warppool.Pool, cond func(warppool.Stats) bool) {
	t.Helper()
	deadline := time.After(dialWaitDeadline)
	for {
		if cond(p.Stats()) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("等待快照条件超时, 当前: %+v", p.Stats())
		case <-time.After(dialPollInterval):
		}
	}
}
