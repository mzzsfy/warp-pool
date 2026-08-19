// Package warppool 基于 amz 的 Cloudflare WARP 代理池。
// 管理一组 WARP 隧道实例,对外提供拨号级代理能力:实例数量维持(min/max)、
// 健康检查与故障切换、出口 IP 探测与池内唯一性、实例状态机与亲和选路。
package warppool

import (
	"errors"
	"fmt"
)

// 公开错误值
var (
	// ErrNoInstance 无可用实例(池未就绪或全部不可用)
	ErrNoInstance = errors.New("无可用实例")
	// ErrInvalidTransition 非法实例状态迁移
	ErrInvalidTransition = errors.New("非法状态迁移")
	// ErrClosed 池已关闭
	ErrClosed = errors.New("池已关闭")
)

// Pool WARP 代理池门面,业务唯一交互对象;零值不可用,须经 New 构造
type Pool struct {
	*pool
}

// New 按配置创建池:校验选项、创建 state 目录并异步拉起实例,不阻塞等待就绪
func New(opts Options) (*Pool, error) {
	p, err := newPool(opts, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: p}, nil
}

// Instances 返回全部实例只读快照(创建序)
func (p *Pool) Instances() []InstanceInfo {
	views := p.snapshot()
	out := make([]InstanceInfo, 0, len(views))
	for _, v := range views {
		out = append(out, InstanceInfo{ID: v.id, Status: v.status(), Egress: v.egress()})
	}
	return out
}

// Stats 池运行计数快照:状态计数为查询瞬间聚合,累计计数自池创建起单调递增
type Stats struct {
	Normal    int   // Normal 实例数
	Probing   int   // Probing 实例数
	Draining  int   // Draining 实例数
	Disabled  int   // Disabled 实例数
	Total     int   // 全部实例数
	Replays   int64 // 重播累计次数(cmdReplay 执行计)
	DialTotal int64 // 拨号尝试累计(成功与失败均计)
	DialFails int64 // 拨号失败累计
}

// Stats 返回池运行计数快照(被动查询,零回调)
func (p *Pool) Stats() Stats {
	views := p.snapshot()
	s := Stats{
		Total:     len(views),
		Replays:   p.stats.replays.Load(),
		DialTotal: p.stats.dialTotal.Load(),
		DialFails: p.stats.dialFailures.Load(),
	}
	for _, v := range views {
		switch v.status() {
		case StatusNormal:
			s.Normal++
		case StatusProbing:
			s.Probing++
		case StatusDraining:
			s.Draining++
		case StatusDisabled:
			s.Disabled++
		}
	}
	return s
}

// SetStatus 手动迁移实例状态;合法迁移为任意(Disabled 除外)→Draining、任意→Disabled、
// Disabled→Normal,幂等迁移返回 nil,非法迁移返回 ErrInvalidTransition,实例已终态返回 ErrClosed
func (p *Pool) SetStatus(id ID, status Status) error {
	return p.setStatus(id, status)
}

// SetMin 调整实例数下限并异步对齐
func (p *Pool) SetMin(n int) error { return p.setMin(n) }

// SetMax 调整实例数上限并异步对齐(降低时按淘汰策略缩减)
func (p *Pool) SetMax(n int) error { return p.setMax(n) }

// Close 关闭池与全部实例,幂等;关闭后拨号返回 ErrClosed
func (p *Pool) Close() error { return p.pool.Close() }

// errInstanceNotFound 目标实例不存在
func errInstanceNotFound(id ID) error { return fmt.Errorf("实例 %s 不存在", id) }
