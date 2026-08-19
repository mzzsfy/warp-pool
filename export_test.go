// 测试导出钩子:向黑盒测试(warppool_test)暴露依赖注入构造,仅测试构建可见。
package warppool

import (
	"strconv"

	"github.com/mzzsfy/warp-pool/internal/amzwrap"
)

// NewForTest 以注入依赖构造池,依赖为 nil 时使用默认实现
func NewForTest(opts Options, factory amzwrap.Factory, prober Prober, health healthFunc) (*Pool, error) {
	p, err := newPool(opts, factory, prober, health)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: p}, nil
}

// InflightConnsForTest 实例当前在途连接登记数;实例不存在返回 -1
func InflightConnsForTest(p *Pool, id ID) int {
	for _, v := range p.snapshot() {
		if v.id == id {
			v.inst.mu.Lock()
			defer v.inst.mu.Unlock()
			return len(v.inst.conns)
		}
	}
	return -1
}

// MakeViewsForTest 构造 n 个 ID 互异、状态恒 Normal 的候选视图(选路纯函数基准与分配断言)
func MakeViewsForTest(n int) []instanceView {
	normal := func() Status { return StatusNormal }
	out := make([]instanceView, n)
	for i := range out {
		out[i] = instanceView{id: ID(strconv.Itoa(i)), status: normal}
	}
	return out
}

// Fnv64aForTest 白盒暴露亲和散列纯函数
var Fnv64aForTest = fnv64a

// NormalCandidatesForTest 白盒暴露 Normal 候选过滤纯函数
var NormalCandidatesForTest = normalCandidates

// AffinityOrderForTest 白盒暴露亲和排序纯函数
var AffinityOrderForTest = affinityOrder

// PublishForTest 白盒暴露快照发布(写时复制;仅池静默态调用,协调 goroutine 不并发触碰实例表)
func (p *Pool) PublishForTest() { p.publish() }
