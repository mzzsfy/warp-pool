// 黑盒测试:验证公开门面(New 校验/Instances/SetStatus 迁移表/SetMin/SetMax/Close),
// 仅经公开 API 与测试钩子注入 fake,故为外部测试包。
package warppool_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
)

// TestNew_非法配置_返回错误 表驱动:New 在拉起实例前完成校验
func TestNew_非法配置_返回错误(t *testing.T) {
	cases := []struct {
		name string
		mut  func(o *warppool.Options)
	}{
		{"Min为零", func(o *warppool.Options) { o.Min, o.Max = 0, 1 }},
		{"Min大于Max", func(o *warppool.Options) { o.Min, o.Max = 2, 1 }},
		{"ListenBase非法形态", func(o *warppool.Options) { o.ListenBase = "127.0.0.1" }},
		{"ListenBase端口越界", func(o *warppool.Options) { o.ListenBase = "127.0.0.1:99999" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := warppool.Options{Min: 1, Max: 1, StateDir: t.TempDir()}
			c.mut(&opts)
			p, err := warppool.New(opts)
			if err == nil {
				_ = p.Close()
				t.Fatal("非法配置应返回错误")
			}
			if p != nil {
				t.Fatal("失败时不应返回池")
			}
		})
	}
}

// Given StateDir 父路径为普通文件 When New Then 创建 state 目录失败返回错误
func TestNew_StateDir不可创建_返回错误(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("{}"), 0o600); err != nil {
		t.Fatalf("创建占位文件失败: %v", err)
	}
	_, err := warppool.New(warppool.Options{
		Min: 1, Max: 1,
		StateDir:   filepath.Join(blocker, "sub"),
		ListenBase: addrOf(0),
	})
	if err == nil {
		t.Fatal("state 目录不可创建时应返回错误")
	}
}

// Given 仅必要配置 When 构造池 Then 零配置默认值跑通并等实例就绪
func TestNew_零配置默认值_跑通(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	_ = env.p.Close()
	env.shutdownServers()
	waitNoGoroutineLeak(t, before)
}

// TestPool_SetStatus_迁移表 表驱动:合法迁移受理,非法迁移返回 ErrInvalidTransition
func TestPool_SetStatus_迁移表(t *testing.T) {
	cases := []struct {
		name         string
		from         warppool.Status
		to           warppool.Status
		wantInvalid  bool
		wantAccepted bool
	}{
		{name: "Normal到Draining", from: warppool.StatusNormal, to: warppool.StatusDraining, wantAccepted: true},
		{name: "Normal到Disabled", from: warppool.StatusNormal, to: warppool.StatusDisabled, wantAccepted: true},
		{name: "Normal到Normal幂等", from: warppool.StatusNormal, to: warppool.StatusNormal, wantAccepted: true},
		{name: "Draining到Disabled", from: warppool.StatusDraining, to: warppool.StatusDisabled, wantAccepted: true},
		{name: "Draining到Draining幂等", from: warppool.StatusDraining, to: warppool.StatusDraining, wantAccepted: true},
		{name: "Draining到Normal非法", from: warppool.StatusDraining, to: warppool.StatusNormal, wantInvalid: true},
		{name: "Disabled到Normal", from: warppool.StatusDisabled, to: warppool.StatusNormal, wantAccepted: true},
		{name: "Disabled到Disabled幂等", from: warppool.StatusDisabled, to: warppool.StatusDisabled, wantAccepted: true},
		{name: "Disabled到Draining非法", from: warppool.StatusDisabled, to: warppool.StatusDraining, wantInvalid: true},
		{name: "Probing到Draining", from: warppool.StatusProbing, to: warppool.StatusDraining, wantAccepted: true},
		{name: "Probing到Disabled", from: warppool.StatusProbing, to: warppool.StatusDisabled, wantAccepted: true},
		{name: "Probing到Normal非法", from: warppool.StatusProbing, to: warppool.StatusNormal, wantInvalid: true},
		{name: "到Probing非法目标", from: warppool.StatusNormal, to: warppool.StatusProbing, wantInvalid: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newDialEnv(t, 1, 1, nil)
			env.prepareStatus(c.from)
			err := env.p.SetStatus("0", c.to)
			switch {
			case c.wantInvalid:
				if !errors.Is(err, warppool.ErrInvalidTransition) {
					t.Fatalf("err = %v, 期望 ErrInvalidTransition", err)
				}
			case c.wantAccepted:
				if err != nil {
					t.Fatalf("合法迁移应受理: %v", err)
				}
			}
		})
	}
}

// Given 目标实例不存在 When SetStatus Then 返回错误
func TestPool_SetStatus_实例不存在_返回错误(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	if err := env.p.SetStatus("no-such", warppool.StatusDisabled); err == nil {
		t.Fatal("实例不存在应返回错误")
	}
}

// Given 实例处于禁用态 When SetStatus(Normal) Then 受理并经重探测回到 Normal
func TestPool_SetStatus_禁用恢复_经重探测回Normal(t *testing.T) {
	env := newDialEnv(t, 1, 1, nil)
	env.setEgress(0, egBySeq(1))
	env.waitNormal(1)
	if err := env.p.SetStatus("0", warppool.StatusDisabled); err != nil {
		t.Fatalf("禁用失败: %v", err)
	}
	env.waitStatus("0", warppool.StatusDisabled)
	if err := env.p.SetStatus("0", warppool.StatusNormal); err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	env.waitStatus("0", warppool.StatusNormal)
}

// Given 2 个 Normal 实例 When Instances Then 返回创建序快照且字段与设定一致
func TestPool_Instances_快照字段与创建序(t *testing.T) {
	env := newDialEnv(t, 2, 2, nil)
	for i := range 2 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(2)
	infos := env.p.Instances()
	if len(infos) != 2 {
		t.Fatalf("实例数 = %d, 期望 2", len(infos))
	}
	for i, info := range infos {
		if info.ID != warppool.ID(strconv.Itoa(i)) {
			t.Fatalf("第 %d 个实例 ID = %s, 期望 %d", i, info.ID, i)
		}
		if info.Status != warppool.StatusNormal {
			t.Fatalf("实例 %s 状态 = %s, 期望 Normal", info.ID, info.Status)
		}
		if info.Egress != egBySeq(i+1) {
			t.Fatalf("实例 %s 出口 %v 与探测设定不符", info.ID, info.Egress)
		}
	}
}

// TestPool_SetMinSetMax_非法值 表驱动:越界调整返回错误且不影响现状
func TestPool_SetMinSetMax_非法值_返回错误(t *testing.T) {
	cases := []struct {
		name string
		call func(p *warppool.Pool) error
	}{
		{"SetMin为零", func(p *warppool.Pool) error { return p.SetMin(0) }},
		{"SetMin大于Max", func(p *warppool.Pool) error { return p.SetMin(2) }},
		{"SetMax小于Min", func(p *warppool.Pool) error { return p.SetMax(0) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newDialEnv(t, 1, 1, nil)
			env.setEgress(0, egBySeq(1))
			env.waitNormal(1)
			if err := c.call(env.p); err == nil {
				t.Fatal("非法值应返回错误")
			}
		})
	}
}

// Given min=1 max=3 When SetMin(2) Then 异步补建并等第二个实例 Normal
func TestPool_SetMin_扩容对齐(t *testing.T) {
	env := newDialEnv(t, 1, 3, nil)
	env.setEgress(0, egBySeq(1))
	env.setEgress(1, egBySeq(2))
	env.waitNormal(1)
	if err := env.p.SetMin(2); err != nil {
		t.Fatalf("SetMin 失败: %v", err)
	}
	env.waitNormal(2)
}

// Given min=2 max=2 且淘汰最老 When SetMin(1) 后 SetMax(1) Then 缩容到 1
func TestPool_SetMax_缩容对齐(t *testing.T) {
	env := newDialEnv(t, 2, 2, func(o *warppool.Options) {
		o.Evictor = warppool.EvictOldest{}
	})
	for i := range 2 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(2)
	if err := env.p.SetMin(1); err != nil {
		t.Fatalf("SetMin 失败: %v", err)
	}
	if err := env.p.SetMax(1); err != nil {
		t.Fatalf("SetMax 失败: %v", err)
	}
	env.waitFor(func() bool { return len(env.p.Instances()) == 1 }, "等待缩容到 1 超时")
	env.waitNormal(1)
}

// Given 运行中的池 When Close Then 全部资源释放、幂等且无 goroutine 泄漏
func TestPool_Close_幂等且无泄漏(t *testing.T) {
	before := runtime.NumGoroutine()
	env := newDialEnv(t, 2, 2, nil)
	for i := range 2 {
		env.setEgress(i, egBySeq(i+1))
	}
	env.waitNormal(2)
	if err := env.p.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := env.p.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	env.shutdownServers()
	waitNoGoroutineLeak(t, before)
}

// prepareStatus 构造实例起始状态(Probing 即不设探测结果,保持空 Key 循环)
func (e *dialEnv) prepareStatus(s warppool.Status) {
	e.t.Helper()
	switch s {
	case warppool.StatusProbing:
		e.waitFor(func() bool {
			for _, info := range e.p.Instances() {
				if info.ID == "0" {
					return info.Status == warppool.StatusProbing
				}
			}
			return false
		}, "等待实例进入 Probing 超时")
	case warppool.StatusNormal:
		e.setEgress(0, egBySeq(1))
		e.waitNormal(1)
	case warppool.StatusDraining:
		e.prepareStatus(warppool.StatusNormal)
		if err := e.p.SetStatus("0", warppool.StatusDraining); err != nil {
			e.t.Fatalf("构造 Draining 失败: %v", err)
		}
		e.waitStatus("0", warppool.StatusDraining)
	case warppool.StatusDisabled:
		e.prepareStatus(warppool.StatusNormal)
		if err := e.p.SetStatus("0", warppool.StatusDisabled); err != nil {
			e.t.Fatalf("构造 Disabled 失败: %v", err)
		}
		e.waitStatus("0", warppool.StatusDisabled)
	}
}

// waitNoGoroutineLeak 等待 goroutine 数回落到基线,超时报泄漏
func waitNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(dialWaitDeadline)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if now := runtime.NumGoroutine(); now > before {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine 泄漏: 基线 %d, 当前 %d\n%s", before, now, buf[:n])
	}
}
