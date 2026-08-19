# warp-pool

基于 [amz](https://github.com/skye-z/amz) 的 Cloudflare WARP 代理池 Go 库。

管理一组 WARP 隧道实例,对外提供拨号级代理能力:实例数量维持(min/max)、健康检查与故障切换、出口 IP 探测与池内唯一性(重复 IP 自动重播换新身份)、实例状态机(Probing/Normal/Draining/Disabled)、亲和选路。

## 特性

- 同进程多 WARP 实例,自动注册、选点、重连(amz 提供)
- 出口 IP 双栈探测(v4/v6),池内唯一性硬约束:DedupeKeyer 去重策略(默认 v4),重复必重播(删 state 重注册)
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
| DedupeKeyer | DedupeByV4 | 出口去重键:DedupeByV4 / DedupeByV6 / DedupeByBoth |
| EgressProbeV4URL / V6URL | api4/api6.ipify.org | 出口探测服务 |
| HealthInterval / HealthTimeout | 30s / 10s | 健康检查周期 / 单次超时 |
| EgressCheckInterval | 5min | 出口 IP 巡检周期 |
| DrainTimeout | 60s | Draining 强断在途连接的超时 |
| ReplayBackoffStart / Max | 1s / 60s | 重播退避起点 / 上限 |
| ReplayConcurrency | 1 | 全局重播并发 |
| Logger | 静默 | `Printf(string, ...any)` 接口 |

## 测试

```bash
go test ./... -race -count=1 -cover   # 离线单测(默认)
go test -tags e2e -run E2E ./... -v   # 真实 WARP 端到端(需网络)
```

## 设计文档

`docs/warp-pool/`(overview / architecture / api-index / project-design / data-design / feat 模块设计 / steps 步骤)。

注意:库保证池内出口 IP 唯一(探测可得范围内),不承诺多样性——同机出口 IP 受 Cloudflare 分配约束。
