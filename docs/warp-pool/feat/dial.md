# dial

## 职责

从 pool 快照中选择 Normal 实例,经该实例代理 listener 建立到目标的连接,包装为 InstanceConn 返回。轮询与亲和两种选路,失败自动换实例重试。

## 接口签名

```go
// Pool 公开方法(实现在本模块)
func (p *Pool) DialContext(ctx context.Context, network, addr string) (net.Conn, error)
func (p *Pool) DialContextWithKey(ctx context.Context, key, network, addr string) (net.Conn, error)

// 返回的连接携带实例信息
type InstanceConn interface {
    net.Conn
    Instance() InstanceInfo
}

type InstanceInfo struct {
    ID     ID
    Status Status
    Egress Egress
}
```

## 选路算法

- 轮询:pool 维护全局原子游标,`cursor++ % len(候选)`;候选=快照中 Normal 实例
- 亲和:`fnv64a(key) % len(候选)`——对候选列表长度取模,列表变化(实例增删)时仅受影响的 key 重分布,保持最大稳定性
- 失败重试:拨号错误(连接建立失败)→ 候选中排除该实例,按原策略取下一个,最多尝试 len(候选) 次;全失败返回最后错误。ctx 取消即中止
- 候选为空:返回 ErrNoInstance(池未就绪/全部不可用)

## 数据流

DialContext → snapshot()(atomic.Pointer 读,无锁)→ 候选过滤(Normal)→ 选实例 → 经实例 proxyAddr SOCKS5 拨号 → 成功:包装 InstanceConn(登记到实例在途集合,Draining 强断依据)→ 返回。

传输方式固定为 SOCKS5(amz 实例 listener 双协议,SOCKS5 拨号语义最直接,支持任意 network 的 addr;HTTP CONNECT 仅兜底不需要)。

## 验收点(BDD)

- 拨号成功:conn 目标可达,断言 InstanceConn 拿到 ID/Egress 与所选实例一致
- 轮询:N 个请求依次落到 N 个实例(均匀,无饿死)
- 亲和:同 key 连续 M 次拨号全落同一实例;实例故障(非 Normal)后同 key 自动换实例且新选择稳定
- 失败重试:首个实例 listener 拒连,请求成功落在下一实例,无业务可见错误
- 候选空:立即返回 ErrNoInstance,不阻塞
- Draining 实例不接新拨号(候选过滤验证)
