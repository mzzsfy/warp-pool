package warppool_test

import (
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
)

func view(id warppool.ID, status warppool.Status, createdAt time.Time) warppool.InstanceView {
	return warppool.InstanceView{ID: id, Status: status, CreatedAt: createdAt}
}

// Given 任意候选快照 When EvictNone 决策 Then 恒不淘汰
func TestEvictNoneAlwaysFalse(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := [][]warppool.InstanceView{
		nil,
		{},
		{view("a", warppool.StatusNormal, base)},
		{view("a", warppool.StatusDisabled, base), view("b", warppool.StatusDraining, base.Add(time.Second)), view("c", warppool.StatusProbing, base.Add(2*time.Second))},
	}
	evictor := warppool.EvictNone{}
	for _, candidates := range cases {
		if id, ok := evictor.Evict(candidates); ok || id != "" {
			t.Fatalf("EvictNone.Evict(%v) = (%q, %v), 期望 (\"\", false)", candidates, id, ok)
		}
	}
}

// Given 各状态与创建时间组合的候选 When EvictOldest 决策 Then 按状态优先级与最老选出或返回 false
func TestEvictOldest(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := func(d time.Duration) time.Time { return base.Add(d) }

	cases := []struct {
		name       string
		candidates []warppool.InstanceView
		wantID     warppool.ID
		wantOK     bool
	}{
		{"空候选返回false", nil, "", false},
		{"仅Probing不可淘汰", []warppool.InstanceView{
			view("p1", warppool.StatusProbing, base),
			view("p2", warppool.StatusProbing, later(time.Second)),
		}, "", false},
		{"仅Normal取最老", []warppool.InstanceView{
			view("n2", warppool.StatusNormal, later(time.Second)),
			view("n1", warppool.StatusNormal, base),
			view("n3", warppool.StatusNormal, later(2*time.Second)),
		}, "n1", true},
		{"Draining优先于Normal", []warppool.InstanceView{
			view("n1", warppool.StatusNormal, base),
			view("d1", warppool.StatusDraining, later(10*time.Second)),
		}, "d1", true},
		{"Disabled优先于Draining与Normal", []warppool.InstanceView{
			view("n1", warppool.StatusNormal, base),
			view("d1", warppool.StatusDraining, later(time.Second)),
			view("x1", warppool.StatusDisabled, later(2*time.Second)),
		}, "x1", true},
		{"同类取最老", []warppool.InstanceView{
			view("d2", warppool.StatusDraining, later(time.Second)),
			view("d1", warppool.StatusDraining, base),
		}, "d1", true},
		{"Disabled更老优先", []warppool.InstanceView{
			view("x2", warppool.StatusDisabled, later(time.Second)),
			view("x1", warppool.StatusDisabled, base),
		}, "x1", true},
		{"跳过Probing其余可淘汰", []warppool.InstanceView{
			view("p1", warppool.StatusProbing, base),
			view("n1", warppool.StatusNormal, later(time.Second)),
		}, "n1", true},
		{"Probing最老不参与", []warppool.InstanceView{
			view("p1", warppool.StatusProbing, base),
			view("d1", warppool.StatusDraining, later(time.Second)),
			view("n1", warppool.StatusNormal, later(2*time.Second)),
		}, "d1", true},
		{"同级时间并列取先出现者", []warppool.InstanceView{
			view("a", warppool.StatusNormal, base),
			view("b", warppool.StatusNormal, base),
		}, "a", true},
	}

	evictor := warppool.EvictOldest{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, ok := evictor.Evict(c.candidates)
			if ok != c.wantOK || id != c.wantID {
				t.Fatalf("EvictOldest.Evict() = (%q, %v), 期望 (%q, %v)", id, ok, c.wantID, c.wantOK)
			}
		})
	}
}
