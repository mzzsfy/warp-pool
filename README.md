# warp-pool

基于 [amz](https://github.com/skye-z/amz) 的 Cloudflare WARP 代理池 Go 库。

管理一组 WARP 隧道实例,对外提供拨号级代理能力:实例数量维持(min/max)、健康检查与故障切换、出口 IP 探测与池内唯一性(重复 IP 自动重播换新身份)、实例状态机(Probing/Normal/Draining/Disabled)、亲和选路。

## 特性

- 同进程多 WARP 实例,自动注册、选点、重连(amz 提供)
- 出口 IP 双栈探测(v4/v6),池内唯一性硬约束:DedupeKeyer 去重策略(默认 v6),重复必重播(删 state 重注册)
- 常驻 min 个 Normal 实例;总数上限 max;达 max 时按淘汰策略腾位(排队背压 / 杀最老)
- 实例状态机:Draining 不接新请求、超时强断在途连接并重播换 IP;Disabled 摘除待手动恢复
- 拨号 API 标准签名,可直接作 `http.Transport.DialContext`;亲和拨号同 key 稳定粘实例
- 返回连接可断言 `InstanceConn` 获取实例 ID/出口 IP,支撑"发现封禁 → 主动重启"闭环
- 运行时 SetMin/SetMax/SetStatus/Instances

## 快速开始

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/mzzsfy/warp-pool"
)

func main() {
	pool, err := warppool.New(warppool.Options{Min: 2, Max: 5})
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	// 轮询拨号:可直接接入 http.Client
	client := &http.Client{Transport: &http.Transport{
		DialContext: pool.DialContext,
	}}
	resp, err := client.Get("https://api4.ipify.org")
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	// 亲和拨号:同 key 稳定走同一实例(会话保持)
	conn, err := pool.DialContextWithKey(context.Background(), "session-1", "tcp", "example.com:443")
	if err != nil {
		panic(err)
	}
	// 断言获取实例信息,发现封禁时 SetStatus 主动重启换 IP
	if ic, ok := conn.(warppool.InstanceConn); ok {
		info := ic.Instance()
		fmt.Println("经实例", info.ID, "出口", info.Egress.V4)
		_ = conn.Close()
		// pool.SetStatus(info.ID, warppool.StatusDraining) // 触发排空+重播
	}
}
```

## 配置

`Options` 全部字段(零值走默认):

| 字段 | 默认 | 说明 |
|------|------|------|
| Min / Max | 必填 / 必填 | 常驻 Normal 数下限 / 总数上限,需 0 < Min <= Max |
| ListenBase | 127.0.0.1:51367 | 实例代理监听起点(base+i 逐实例递增) |
| StateDir | ./warp-state | 实例 state 目录(amz 注册态,重播即删) |
| Evictor | EvictNone | 达 max 淘汰策略:排队背压 / EvictOldest 杀最老 |
| DedupeKeyer | DedupeByV6 | 出口去重键:DedupeByV4 / DedupeByV6 / DedupeByBoth(v6 唯一性最好;纯 v4 环境请改 ByV4/ByBoth) |
| EgressProbeV4URL / V6URL | api4/api6.ipify.org | 出口探测服务 |
| HealthInterval / HealthTimeout | 30s / 10s | 健康检查周期 / 单次超时 |
| EgressCheckInterval | 5min | 出口 IP 巡检周期 |
| DrainTimeout | 60s | Draining 强断在途连接的超时 |
| ReplayBackoffStart / Max | 1s / 60s | 重播退避起点 / 上限 |
| ReplayConcurrency | 1 | 全局重播并发 |
| DialTransport | socks5 | 拨号传输:socks5 / http(经实例 listener 的 CONNECT 隧道) |
| Endpoints | 自动选优 | 实例 endpoint(`host:port`)列表;空为 amz 自动选优,非空按创建序轮询分配,重播时轮换下一个 |
| Logger | 静默 | `Printf(string, ...any)` 接口 |

### 出口 IP 多样性

WARP 免费版 IPv4 出口为机房级共享 NAT:全池实例自动选优时大概率落同一机房、同一批 IPv4。
IPv6 出口为每设备唯一,`DedupeByV6`/`DedupeByBoth` 下天然互异。要分散 IPv4,配置
`Endpoints` 让实例分别绑定不同机房(列表可从 WARP 官方 endpoint 段挑选):

```go
warppool.Options{Endpoints: []string{"162.159.192.1:2408", "162.159.193.10:500"}}
```

## 测试

```bash
go test ./... -race -count=1 -cover   # 离线单测(默认)
go test -tags e2e -run E2E ./... -v   # 真实 WARP 端到端(需网络)
go test -run xxx -bench . -benchmem   # 性能基准(并发拨号项建议 -benchtime 20000x,见下注)
```

## 代理测试入口

`cmd/proxy` 将池包装为本地 HTTP 代理,用于真实网络手动验收:

```bash
go run ./cmd/proxy                          # 默认 127.0.0.1:8080, 实例 2-2, socks5 传输
go run ./cmd/proxy -listen 127.0.0.1:8080 -min 2 -max 4 -transport http -state ./warp-state
go run ./cmd/proxy -endpoints 162.159.192.1:2408,162.159.193.10:500   # 实例分机房绑定

curl -x http://127.0.0.1:8080 https://api4.ipify.org   # 经池出口, 多次请求观察轮换
```

每次 CONNECT 打印所选实例与出口 IP,周期输出池状态计数;Ctrl-C 排空在途请求后退出。

## 性能基准

环境:Windows 10 Pro / Intel i5-8500(6C6T)/ go1.26.1 windows/amd64;`-benchmem -benchtime 1s`。
全链路基准经假 SOCKS5 代理(127.0.0.1 loopback,真实 TCP 路径),非直连 WARP。

| 基准 | 规模 | ns/op | B/op | allocs/op |
|------|------|-------|------|-----------|
| DialContext 轮询全链路 | 10 实例 | 482 291 | 71 119 | 87 |
| DialContextWithKey 亲和全链路 | 10 实例 | 484 676 | 71 980 | 88 |
| DialContextConcurrent 并发拨号 | 10 实例,≥100 goroutine | 1 666 392 | 71 275 | 89 |
| Snapshot 并发读(Instances) | 10 / 50 实例 | 243 / 1 100 | 768 / 4 096 | 1 |
| Snapshot 读写并发(1ms 周期重发布) | 10 / 50 实例 | 234 / 1 093 | 768 / 4 099 | 1 |
| Snapshot 写发布(写时复制) | 10 / 50 实例 | 1 554 / 11 852 | 1 240 / 5 720 | 22 / 102 |
| AffinityOrder 亲和排序 | 候选 10 / 50 / 100 | 2 336 / 17 059 / 43 151 | 896 / 4 096 / 8 192 | 1 |
| NormalCandidates 候选过滤 | 实例 10 / 50 / 100 | 431 / 2 232 / 4 209 | 896 / 4 096 / 8 192 | 1 |
| Fnv64a 亲和散列 | - | 9.8 | 0 | 0 |

结论:

- 选路开销微秒级:轮询(候选过滤+取模)约 0.43µs/10 实例,亲和(过滤+rendezvous 排序)约 2.3µs/10 实例,占全链路拨号(482µs)不足 0.5%。
- 选路纯函数零分配:散列与排序比较路径 0 分配;候选过滤/亲和排序恒 1 次分配(结果切片拷贝),不随规模增长(`Test选路纯函数_零分配` 断言)。
- 快照读写不互斥:1ms 周期后台重发布下,读写并发与纯读耗时持平(±1%),无锁读成立;写发布为写时复制,仅实例集合变化时发生。
- 分布均匀性(10/50/100 实例,100 实例监听端口贴近 65535 上限):轮询精确均匀;亲和落点卡方检验达标(阈值=自由度+4σ,由 `TestDial_选路分布均匀性` 断言)。
- 并发拨号无锁竞争:≥100 goroutine 下 mutex profile 中池自有互斥(实例在途登记)全程累计等待 7µs/2 万次拨号;CPU 热点 99% 为网络 syscall(`runtime.cgocall`),无池函数热点。

注:Windows 对 TIME_WAIT 端口总量有限制,close 密集的并发拨号基准长时间运行会耗尽端口预算(表现为 WSAEADDRINUSE/拒连),故并发项按迭代数封顶执行(`-benchtime 20000x`),其 ns/op 含少量重试等待,量级参考即可。

## 设计文档

`docs/warp-pool/`(overview / architecture / api-index / project-design / data-design / feat 模块设计 / steps 步骤)。

注意:库保证池内出口 IP 唯一(探测可得范围内),不承诺多样性——同机出口 IP 受 Cloudflare 分配约束。

## 从 opencode2api 迁移

自研 WarpPool(Rebuild 换 IP)与 nodePool(会话亲和 rebind)可整体替换为库能力:`DialContext` 接 `http.Transport`,`DialContextWithKey` 做会话粘性,`InstanceConn` 断言 + `SetStatus` 做封禁换 IP。能力映射、可编译对照示例与收益说明见 [migration-opencode2api.md](docs/warp-pool/migration-opencode2api.md)。
