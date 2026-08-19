package warppool

import (
	"cmp"
	"context"
	"net"
	"slices"
)

// 64 位 FNV-1a 参数与雪崩收尾常量(murmur3 64 位终混)
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
	mixPrimeA   uint64 = 0xff51afd7ed558ccd
	mixPrimeB   uint64 = 0xc4ceb9fe1a85ec53
	mixShift           = 33
)

// InstanceInfo 实例只读信息快照
type InstanceInfo struct {
	ID     ID
	Status Status
	Egress Egress
}

// InstanceConn 携带来源实例信息的代理连接;关闭时自动解除实例在途登记
type InstanceConn interface {
	net.Conn
	// Instance 返回建立该连接所选实例的信息
	Instance() InstanceInfo
}

// instanceConn InstanceConn 实现
type instanceConn struct {
	net.Conn
	info InstanceInfo
	inst *instance
}

// Instance 实现 InstanceConn
func (c *instanceConn) Instance() InstanceInfo { return c.info }

// Close 关闭连接并解除来源实例的在途登记(以 wrapper 自身为键,与登记键一致)
func (c *instanceConn) Close() error {
	c.inst.untrackConn(c)
	return c.Conn.Close()
}

// fnv64a 标准字符串散列加雪崩收尾(亲和选路);
// FNV 差异集中于低位,不混合时短数字后缀(实例 ID)的散列高位不变,
// rendezvous 排序高度相关导致落点集中
func fnv64a(s string) uint64 {
	h := fnvOffset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	h ^= h >> mixShift
	h *= mixPrimeA
	h ^= h >> mixShift
	h *= mixPrimeB
	h ^= h >> mixShift
	return h
}

// DialContext 轮询选择 Normal 实例经其实例代理拨号;
// 拨号失败自动换下一实例重试,全失败返回最后一个错误;无候选返回 ErrNoInstance
func (p *Pool) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	return p.dial(ctx, p.cursor.Add(1)-1, network, addr)
}

// DialContextWithKey 亲和拨号:同 key 稳定落同一实例(rendezvous 哈希),
// 候选增删仅迁移约 1/N 的 key;key 为空退化为轮询;失败换下一候选,全失败返回最后一个错误
func (p *Pool) DialContextWithKey(ctx context.Context, key, network, addr string) (net.Conn, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if key == "" {
		return p.dial(ctx, p.cursor.Add(1)-1, network, addr)
	}
	return p.dialKey(ctx, key, network, addr)
}

// dial 统一拨号主体:start 为候选下标起点(对候选数取模),失败按序顺延,ctx 取消即中止
func (p *Pool) dial(ctx context.Context, start uint64, network, addr string) (net.Conn, error) {
	candidates := normalCandidates(p.snapshot())
	if len(candidates) == 0 {
		return nil, ErrNoInstance
	}
	first := int(start % uint64(len(candidates)))
	var lastErr error
	for i := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := p.dialView(ctx, candidates[(first+i)%len(candidates)], network, addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// dialKey 亲和拨号主体:按 rendezvous 分数序尝试候选,失败顺延,ctx 取消即中止
func (p *Pool) dialKey(ctx context.Context, key, network, addr string) (net.Conn, error) {
	candidates := normalCandidates(p.snapshot())
	if len(candidates) == 0 {
		return nil, ErrNoInstance
	}
	var lastErr error
	for _, v := range affinityOrder(key, candidates) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := p.dialView(ctx, v, network, addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// affinityOrder rendezvous 序:key 与实例 ID 联合散列取分数,分数高者优先;
// 候选增删仅改变少数 key 的最优落点
func affinityOrder(key string, candidates []instanceView) []instanceView {
	order := slices.Clone(candidates)
	slices.SortFunc(order, func(a, b instanceView) int {
		return cmp.Compare(fnv64a(key+string(b.id)), fnv64a(key+string(a.id)))
	})
	return order
}

// normalCandidates 快照中接受新拨号的实例(按快照序)
func normalCandidates(views []instanceView) []instanceView {
	out := make([]instanceView, 0, len(views))
	for _, v := range views {
		if v.status() == StatusNormal {
			out = append(out, v)
		}
	}
	return out
}

// dialView 按池配置传输经实例代理拨号并包装为 InstanceConn(登记在途集合,累计拨号计数)
func (p *pool) dialView(ctx context.Context, v instanceView, network, addr string) (net.Conn, error) {
	p.stats.dialTotal.Add(1)
	conn, err := p.dialThrough(ctx, v.proxyAddr, network, addr)
	if err != nil {
		p.stats.dialFailures.Add(1)
		return nil, err
	}
	wrapped := &instanceConn{
		Conn: conn,
		info: InstanceInfo{ID: v.id, Status: v.status(), Egress: v.egress()},
		inst: v.inst,
	}
	v.inst.trackConn(wrapped)
	return wrapped, nil
}
