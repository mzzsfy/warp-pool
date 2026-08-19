# api

## 职责

warppool 包的公开面:Options 全部可配项、Pool 公开方法、错误值。业务唯一 import 面,薄封装,逻辑全在 pool/instance/dial。

## Options(全部可配项)

```go
type Options struct {
    Min, Max           int                  // 必须 0 < Min <= Max
    ListenBase         string               // 实例 listener 起点 "127.0.0.1:51367"(默认)
    StateDir           string               // 实例 state 目录(默认 "./warp-state")
    Evictor            Evictor              // 默认 EvictNone(背压)
    DedupeKeyer        DedupeKeyer          // 默认 DedupeByV4
    EgressProbeV4URL   string               // 默认 https://api4.ipify.org
    EgressProbeV6URL   string               // 默认 https://api6.ipify.org
    HealthInterval     time.Duration        // 周期健康检查,默认 30s
    HealthTimeout      time.Duration        // 单次 HealthCheck 超时,默认 10s
    EgressCheckInterval time.Duration       // 出口巡检周期,默认 5min
    DrainTimeout       time.Duration        // Draining 强断超时,默认 60s
    ReplayBackoffStart time.Duration        // 重播退避起点,默认 1s
    ReplayBackoffMax   time.Duration        // 重播退避上限,默认 60s
    ReplayConcurrency  int                  // 全局重播并发,默认 1
    DialTransport      DialTransport        // 拨号传输方式:TransportSOCKS5(默认)/TransportHTTP
    Logger             Logger               // Printf 接口,默认静默
}
```

## Pool 公开方法

```go
func New(opts Options) (*Pool, error)   // 校验选项;创建目录;拉起实例与协调循环(异步就绪)
func (p *Pool) DialContext(ctx context.Context, network, addr string) (net.Conn, error)
func (p *Pool) DialContextWithKey(ctx context.Context, key, network, addr string) (net.Conn, error)
func (p *Pool) Instances() []InstanceInfo
func (p *Pool) Stats() Stats // 池计数快照:状态实例数、重播累计、拨号累计与失败,被动查询
func (p *Pool) SetStatus(id ID, status Status) error // 非法迁移(如 Disabled→Draining)返回 ErrInvalidTransition
func (p *Pool) SetMin(n int) error // 新值 Min<=0 或 Min>当前 Max 拒
func (p *Pool) SetMax(n int) error // 新值 Max<当前 Min 拒
func (p *Pool) Close() error // 幂等
```

公开错误:`ErrNoInstance`(无可用实例)、`ErrInvalidTransition`、`ErrClosed`。

SetStatus 合法迁移:非 Disabled→Draining、任意→Disabled、Disabled→Normal(经重探测)、同状态幂等返回 nil。

## 验收点(BDD)

- New 校验:Min<=0 / Min>Max / 非法 ListenBase 返回明确错误
- 全部默认值可零配置跑通(仅 Min/Max 必填)
- Close 幂等;Close 后 DialContext 返回 ErrClosed
- godoc 可读性:包注释 + 每公开符号注释(中文,含义式)
