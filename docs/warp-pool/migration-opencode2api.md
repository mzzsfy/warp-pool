# 从 opencode2api 迁移

opencode2api 在 warp_amz.go 与 pool.go 中自研了两层脚手架:WarpPool 管 amz 客户端生命周期与 Rebuild 换 IP,transportPool/nodePool 管代理健康与会话亲和 rebind。warp-pool 把这两层下沉为库能力,业务侧只保留"请求 → 会话标识 → 拨号"的胶水。本文给出能力映射、代码对照与收益说明。

## 能力映射

| 原 opencode2api | warp-pool | 说明 |
|-----------------|-----------|------|
| `NewWarpPool(cfg, logger)` 按 token 数建实例,手工预计算 listener(base+i)与 state 路径 | `New(Options{Min, Max, ListenBase, StateDir, Logger})` | 实例数由 min/max 维持,免配 token,自动注册补位 |
| `warp.Start(ctx)` 阻塞等待全部实例就绪 | `New` 校验后异步拉起,不阻塞 | 未就绪时拨号返回 ErrNoInstance,业务侧重试即可 |
| `WarpPool.Rebuild(proxy)`:close → 删 state → 重注册 | `SetStatus(id, StatusDraining)` | 排空(DrainTimeout 内强断在途连接)后删 state 重注册换身份 |
| `queryWarpProxyPublicIP` + "IP did not change" 重试环 | 内置 HTTPProber 巡检 + DedupeKeyer 去重 | 重复出口自动重播(退避可配),直至池内唯一 |
| `proxyTransport`(healthy/cooling/generation)+ `StartProxyHealthChecks` 定时器 | 池内置周期 HealthCheck(HealthInterval/HealthTimeout) | 检查失败自动转 Draining 走重建,业务侧无健康循环 |
| `nodePool.CursorFor(affinity)` fnv 哈希选起点 + `cursor.Next()` 跳过冷却代理 | `DialContextWithKey(ctx, key, network, addr)` | rendezvous 哈希,同 key 稳定落同一实例,候选增删仅迁移约 1/N 的 key |
| `nodePool.RebindProxy` / `RestoreProxy`(代理故障迁移/恢复回迁) | 亲和拨号内建:失败换下一候选 | 实例摘除时 key 落到次优候选,重建后自然回落,无需绑定计数 |
| `warpIndexFor` 地址反查实例 | 断言 `InstanceConn` 后 `Instance().ID` | 直接取连接所属实例,免地址推断 |
| `proxy.PublicIP()` | `InstanceInfo.Egress.V4/V6` | 双栈出口随连接与 Instances 快照可读 |
| `WarpPool.Close` | `Pool.Close` | 均幂等 |

## 代码对照

以下"原写法"均节选自 opencode2api 仓库,只保留要点。

### 池构造与启动

原写法(warp_amz.go):token 列表决定实例数,预计算端口与 state 路径,Start 阻塞拉起全部 amz 客户端。

```go
func NewWarpPool(cfg WarpConfig, logger *slog.Logger) (*WarpPool, error) {
	// ...逐 token 预计算 listen(base+i)与 state 路径
	for i, token := range cfg.Tokens {
		inst := &warpInstance{
			index: i, token: token,
			path:   filepath.Join(cfg.StateDir, "token-"+strconv.Itoa(i)+".state.json"),
			listen: net.JoinHostPort(baseHost, strconv.Itoa(basePort+i)),
			runErr: make(chan error, 1),
		}
		pool.instances = append(pool.instances, inst)
	}
	return pool, nil
}

func (p *WarpPool) Start(ctx context.Context) error {
	for _, inst := range p.instances {
		if err := p.startInstance(ctx, inst); err != nil { // amz.NewClient + Start,90s 超时
			p.closeAll()
			return fmt.Errorf("warp token %d: %w", inst.index, err)
		}
	}
	return nil
}
```

迁移后:一个 New 调用,实例数与端口、state、注册全部由库托管。

```go
pool, err := warppool.New(warppool.Options{
	Min: 2, Max: 5,
	ListenBase: "127.0.0.1:51367",
	StateDir:   "./warp-state",
	Logger:     log.Default(), // *log.Logger 自带 Printf 方法,满足 Logger 接口
})
if err != nil {
	return err
}
defer pool.Close()
```

### 会话亲和选路

原写法(pool.go + gateway.go):cursor 按哈希选起点,Next 跳过不可用代理,取 node 当前绑定的 proxy 发请求,外层再包 attempt 重试环。

```go
cursor := nodes.CursorFor(ids.Session) // fnv 哈希稳定起点
for attempt := 1; attempt <= g.cfg.Retry.MaxAttempts; attempt++ {
	node := cursor.Next() // 跳过绑定冷却代理的节点
	if node == nil {
		break
	}
	proxy := nodes.Proxy(node) // 双重检查绑定的代理仍可用
	if proxy == nil {
		continue
	}
	resp, err := proxy.client.Do(req) // 每代理一个 http.Client
	// ...按状态码决定冷却/重试
}
// 代理冷却时:RebindProxy 把其上全部 key 迁到最少负载代理;恢复时 RestoreProxy 回迁
```

迁移后:单 `http.Transport` 复用连接池,亲和键从 ctx 取;失败换候选、冷却跳过、绑定迁移全部内建。

```go
// dial 同会话稳定落同一实例;拨号失败自动换下一候选,全失败返回最后一个错误
func (u *WarpUpstream) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	session, _ := ctx.Value(sessionKey{}).(string)
	conn, err := u.pool.DialContextWithKey(ctx, session, network, addr)
	if err != nil {
		return nil, err
	}
	if ic, ok := conn.(warppool.InstanceConn); ok {
		u.remember(session, ic.Instance().ID) // 记录会话落点,供换 IP 用
	}
	return conn, nil
}
```

### 封禁换 IP

原写法(warp.go):冷却入口识别 warp 代理,起 goroutine 循环 Rebuild(同步 close + 删 state + 重启),再查公网 IP 验证变化,IP 未变则退避重试。

```go
func (g *Gateway) startWarpRebuild(proxy *proxyTransport, retryDelay time.Duration) {
	for attempt := 1; ; attempt++ {
		rebuildErr := g.warp.Rebuild(proxy) // close -> 删 state -> 重注册
		if rebuildErr == nil {
			newIP, err := queryWarpProxyPublicIP(g.ctx, proxy) // 自查出口 IP
			if err == nil && (oldIP == "" || newIP != oldIP) {
				g.finishProxyCooldown(proxy, oldIP, newIP, attempt, "WARP IP changed")
				return
			}
		}
		if !sleepContext(g.ctx, retryDelay) { // 退避重试
			return
		}
	}
}
```

迁移后:一行 SetStatus,排空/强断/重注册/新 IP 去重校验全部由实例状态机完成。

```go
// Rotate 会话发现封禁(403/429)时换出口 IP:
// Draining 不接新请求,超时强断在途连接并删 state 重注册;
// 新出口经 DedupeKeyer 校验,与池内既有实例重复则继续重播。
func (u *WarpUpstream) Rotate(session string) error {
	u.mu.Lock()
	id := u.last[session]
	u.mu.Unlock()
	if id == "" {
		return nil
	}
	return u.pool.SetStatus(id, warppool.StatusDraining)
}
```

## 迁移后完整骨架

完整可编译骨架(123 行含注释,替代 warp_amz.go 全部与 pool.go 的选路部分):

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/mzzsfy/warp-pool"
)

// sessionKey 请求级会话标识的 ctx 键
type sessionKey struct{}

// WithSession 把会话标识放进 ctx,拨号时取出做亲和
func WithSession(ctx context.Context, session string) context.Context {
	return context.WithValue(ctx, sessionKey{}, session)
}

// WarpUpstream 替代 warp_amz.go 的 WarpPool 与 pool.go 的 nodePool
type WarpUpstream struct {
	pool     *warppool.Pool
	affinity *http.Client
	mu       sync.Mutex
	last     map[string]warppool.ID // 会话最近一次落点实例
}

// New 替代 NewWarpPool + warp.Start:异步拉起,不阻塞
func New() (*WarpUpstream, error) {
	pool, err := warppool.New(warppool.Options{
		Min:                 2,
		Max:                 5,
		ListenBase:          "127.0.0.1:51367",
		StateDir:            "./warp-state",
		HealthInterval:      30 * time.Second,
		HealthTimeout:       10 * time.Second,
		EgressCheckInterval: 5 * time.Minute,
		DrainTimeout:        60 * time.Second,
		Logger:              log.Default(),
	})
	if err != nil {
		return nil, err
	}
	u := &WarpUpstream{pool: pool, last: make(map[string]warppool.ID)}
	u.affinity = &http.Client{Transport: &http.Transport{
		DialContext:           u.dial,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}}
	return u, nil
}

// dial 替代 CursorFor + cursor.Next + nodes.Proxy:同会话稳定落同一实例,
// 拨号失败自动换下一候选;session 为空退化为轮询
func (u *WarpUpstream) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	session, _ := ctx.Value(sessionKey{}).(string)
	conn, err := u.pool.DialContextWithKey(ctx, session, network, addr)
	if err != nil {
		return nil, err
	}
	if ic, ok := conn.(warppool.InstanceConn); ok {
		u.remember(session, ic.Instance().ID)
	}
	return conn, nil
}

func (u *WarpUpstream) remember(session string, id warppool.ID) {
	if session == "" {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.last[session] = id
}

// Do 发请求:轮询场景 session 传空,退化为 pool.DialContext 同款轮询
func (u *WarpUpstream) Do(ctx context.Context, session, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(WithSession(ctx, session), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return u.affinity.Do(req)
}

// Rotate 替代 startWarpRebuild:封禁时对会话落点实例触发排空 + 重播换 IP
func (u *WarpUpstream) Rotate(session string) error {
	u.mu.Lock()
	id := u.last[session]
	u.mu.Unlock()
	if id == "" {
		return nil
	}
	return u.pool.SetStatus(id, warppool.StatusDraining)
}

// Close 替代 WarpPool.Close
func (u *WarpUpstream) Close() error { return u.pool.Close() }

func main() {
	u, err := New()
	if err != nil {
		log.Fatal(err)
	}
	defer u.Close()

	resp, err := u.Do(context.Background(), "session-1", "https://api4.ipify.org")
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()
	// 上游判定封禁时换 IP:
	// if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
	// 	_ = u.Rotate("session-1")
	// }

	for _, info := range u.pool.Instances() {
		fmt.Println("实例", info.ID, info.Status, info.Egress.V4)
	}
}
```

go.mod 变更:`go get github.com/mzzsfy/warp-pool`;`skye-z/amz` 从直接依赖降为间接依赖(经 warp-pool 引入),业务不再 import amz。

## 行为对齐

| 原 WarpPool 行为 | 迁移后表现 |
|-----------------|-----------|
| 轮询:每代理一个 http.Client,Cursor 轮转起点 | `DialContext` 全局游标轮询,单 Transport 复用连接池 |
| 健康检查:定时器只重查不健康代理,任何 HTTP 响应即视为恢复 | 库按 HealthInterval 周期检查,失败转 Draining 自动重建,无需业务区分"重查哪些" |
| 重播:Rebuild 循环直到公网 IP 变化 | Draining 重注册后经出口探测与 DedupeKeyer 去重,重复即继续重播(退避区间可配),不依赖业务自查 IP |
| 亲和:fnv 哈希起点 + 冷却跳过 + Rebind/Restore | rendezvous 哈希稳定落点,失败顺延候选,实例重建后 key 自然回落 |
| 关闭:closeAll 逐实例停,幂等 | Close 幂等,关闭后拨号返回 ErrClosed |

差异:

- 启动语义:原 Start 阻塞至全部就绪;New 异步拉起。需要就绪门禁的业务以 `Instances()` 快照轮询 Normal 数,或直接依赖拨号重试。
- 换 IP 语义:原 Rebuild 同步完成并返回新 IP;SetStatus 只提交状态迁移,排空与重注册异步进行,不回传新 IP。拨号侧自动避开 Draining 实例,业务无需等待。
- 代理范围:原 transportPool 同时承载普通外部代理(direct/http/socks5)与 warp 代理;warp-pool 只接管 warp 部分。仍需普通代理路由的业务保留自家 transportPool,把 warp 条目从其 proxies 配置中移除即可。

## 迁移收益

- 代码缩减:删除 warp_amz.go 全部(约 320 行)与 pool.go 的 proxyTransport/transportPool/nodePool/nodeCursor(约 370 行),换成约 120 行胶水 + 一个 import;warp_test.go 对应测试一并退役,由库自带单测与 e2e 覆盖。
- 配置缩减:warp.tokens 不再需要(免注册 token 获取与轮换),实例数改由 Min/Max 表达,可运行时 SetMin/SetMax 伸缩。
- 新增能力:出口 IP 池内唯一性硬约束(重复自动重播)、双栈出口探测、实例状态机(Probing/Normal/Draining/Disabled)、达上限淘汰策略(排队背压/杀最老)、重播并发与退避控制。
- 自愈面扩大:原实现只在业务请求路径发现故障时冷却;库对全部实例周期巡检,故障实例无条件进入重建,不依赖流量触达。
