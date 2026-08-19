package warppool

import "time"

// InstanceView 淘汰决策所需的实例只读视图
type InstanceView struct {
	ID        ID
	Status    Status
	CreatedAt time.Time
}

// Evictor 决策池达上限时如何腾出实例槽位
type Evictor interface {
	// Evict 返回被淘汰实例 ID 与 true;返回 false 表示不淘汰(排队等待空位)。
	// candidates 为当前全部实例的只读视图,不可修改。
	Evict(candidates []InstanceView) (ID, bool)
}

// EvictNone 排队策略(默认):永不淘汰,新建请求等待空位
type EvictNone struct{}

// Evict 恒不淘汰
func (EvictNone) Evict([]InstanceView) (ID, bool) { return "", false }

// EvictOldest 杀最老策略:Disabled 最老 → Draining 最老 → Normal 最老(创建时间序);
// Probing 实例不可淘汰
type EvictOldest struct{}

// Evict 按状态优先级选最老候选
func (EvictOldest) Evict(candidates []InstanceView) (ID, bool) {
	best := -1
	for i, c := range candidates {
		pri, ok := evictPriority(c.Status)
		if !ok {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		bestPri, _ := evictPriority(candidates[best].Status)
		if pri < bestPri || (pri == bestPri && c.CreatedAt.Before(candidates[best].CreatedAt)) {
			best = i
		}
	}
	if best < 0 {
		return "", false
	}
	return candidates[best].ID, true
}

// evictPriority 淘汰优先级,值越小越先淘汰;ok 为 false 表示该状态不可淘汰
func evictPriority(s Status) (pri int, ok bool) {
	switch s {
	case StatusDisabled:
		return 0, true
	case StatusDraining:
		return 1, true
	case StatusNormal:
		return 2, true
	}
	return 0, false
}
