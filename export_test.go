// 测试导出钩子:向黑盒测试(warppool_test)暴露依赖注入构造,仅测试构建可见。
package warppool

import (
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
