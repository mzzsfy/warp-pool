# 技术架构

## 技术栈

| 层 | 选型 | 原因 |
|----|------|------|
| 语言 | Go 1.26 | 与 amz SDK 同语言;业务方为 Go 项目 |
| 核心依赖 | github.com/skye-z/amz v0.2.3(核心外部依赖) | 单实例 WARP 隧道全生命周期已由其解决 |
| 拨号传输 | golang.org/x/net/proxy(SOCKS5) | 已在 amz 依赖树;SOCKS5 拨号语义最直接,支持任意 network 目标 |
| 其余 | 标准库(net/netip/sync/time/context) | 库最小依赖原则 |
| 测试 | 标准 testing + httptest;amz 真实隧道仅在 e2e 标签下 | 单测不依赖网络 |
| 日志 | `Logger interface{ Printf(string, ...any) }` 注入,默认静默 | 与 amz Logger 契约一致,零依赖适配 slog/log |

## 模块图

```
            ┌─────────────────────────────┐
            │  Pool(api/pool) 编排         │← 对外唯一入口
            └──────┬──────────┬───────────┘
                   │          │
        持有/编排   │          │ 查询可用实例快照
                   ▼          ▼
            ┌──────────┐   ┌─────────────────┐
            │ instance │   │ dial(选路)       │
            │ 状态机    │   │ 轮询/亲和/失败重试 │
            └────┬─────┘   └─────────────────┘
                 │ 探测自身出口
                 ▼
            ┌──────────┐    ┌──────────────┐
            │ egress   │    │ eviction     │ ← Pool 引用(策略对象)
            │ 探测+Keyer│    │ 淘汰策略接口  │
            └──────────┘    └──────────────┘
```

依赖方向无环:Pool → instance/egress/eviction/dial;instance → amz/egress;dial 只读消费 Pool 快照,不反向依赖 Pool;策略(Evictor/DedupeKeyer/Logger)为业务注入的值对象。

并发模型:每 instance 一个管理 goroutine(状态机串行,无锁);Pool 一个协调循环 goroutine(健康检查/数量对齐/淘汰/唯一性巡检);DialContext 实例集合快照读无锁(写时复制,atomic.Pointer;在途连接登记另有互斥锁)。

## 外部依赖

| 依赖 | 版本 | 用途与约束 |
|------|------|-----------|
| amz | v0.2.3 | 注册/选点/隧道/本地代理 listener;TUN 不使用 |
| Cloudflare WARP 注册 API(经 amz) | - | 重播=新设备身份;频控风险:重播全局并发可配(`ReplayConcurrency` 默认 1),指数退避参数可配(`ReplayBackoffStart` 默认 1s / `ReplayBackoffMax` 默认 60s,重试次数不设限) |
| CF 边缘(162.159.192.0/24) | - | 隧道流量;出口 IP 由 CF 分配,不承诺多样性 |
| 出口 IP 探测服务(`EgressProbeV4URL`/`EgressProbeV6URL` 可配) | - | 默认 api4.ipify.org + api6.ipify.org;探测失败按 Keyer 语义处理(空 Key→重播) |
| 本地端口段(`ListenBase` 可配,base+i,默认 127.0.0.1:51367) | - | 每实例独立 listener;端口耗尽即 max 实际上限 |

## 接口契约

### 对外(Pool 公开 API)

```go
func New(opts Options) (*Pool, error)          // 校验 min<=max、创建 state 目录、拉起 min 个实例(异步),不阻塞等就绪
func (p *Pool) DialContext(ctx, network, addr) (net.Conn, error)  // 轮询选实例;拨号失败自动换下一实例;全失败返回最后一个错误
func (p *Pool) DialContextWithKey(ctx, key, network, addr) (net.Conn, error) // key=="" 退化为轮询
type InstanceConn interface { net.Conn; Instance() InstanceInfo } // 返回连接可断言
type InstanceInfo struct { ID ID; Status Status; Egress Egress }
func (p *Pool) SetStatus(id ID, status Status) error  // Normal/Draining/Disabled 迁移,非法迁移返回错误
func (p *Pool) Instances() []InstanceInfo              // 只读快照
func (p *Pool) SetMin(n int) error / SetMax(n int) error // 触发协调循环异步对齐
func (p *Pool) Close() error                            // 幂等,关闭全部实例
```

### 状态机(instance 内部,Status 对外可见)

```
Normal ──SetStatus(Draining)/健康检查失败──▶ Draining ──超时强断在途+删state重注册──▶ Probing ──IP唯一──▶ Normal
Probing ──SetStatus(Draining)──▶ Draining(同上)
Normal/Probing/Draining ──SetStatus(Disabled)──▶ Disabled ──SetStatus(Normal):cmdEnable──▶ Probing(复用 state 重探测)
```

Probing 为内部瞬态(启动/重播后探测+去重中),对外可见(业务可区分"重建中")。就绪门槛(Normal)= amz Start 成功 + 双栈探测完成 + DedupeKeyer Key 池内唯一。

### 模块间

- instance → pool:就绪/失联/排空/重播/退出事件(channel);不直接改池状态
- pool → instance:command(直发各实例命令通道):confirm/reprobe/replay/drain(timeout)/disable/enable/stop
- dial → pool:`snapshot() []instanceView`(只读);`pickRR()/pickKeyed(key)` 纯函数于快照
- 策略注入:`Evictor.Evict(candidates []InstanceView) (victim ID, ok)`、`DedupeKeyer.Key(Egress) string`(Key 计算与冲突判定在 pool 侧,instance 不自判)
