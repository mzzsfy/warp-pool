# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目定位

基于 [amz](https://github.com/skye-z/amz) 的 Cloudflare WARP 代理池 **Go 库**(module `github.com/mzzsfy/warp-pool`),非服务进程。管理一组 WARP 隧道实例:min/max 数量维持、健康检查、出口 IP 探测与池内唯一性(重复 IP 删 state 重播换身份)、实例状态机(Normal/Probing/Draining/Disabled)、轮询+亲和选路。对外仅暴露 `DialContext` 拨号级 API。

## 常用命令

```bash
go test ./... -race -count=1 -cover   # 离线单测(默认,无网络依赖)
go test -tags e2e -run E2E ./... -v   # 真实 WARP 端到端(需网络,有频控风险,勿频繁跑)
go test -run xxx -bench . -benchmem   # 基准;并发拨号项须 -benchtime 20000x(Windows TIME_WAIT 端口耗尽)
go run ./cmd/proxy                    # 手动验收:本地 HTTP 代理包装池,-min/-max/-transport/-listen/-state 可配
```

无 lint 配置;`go vet ./...` 语义由 `go test` 编译链覆盖。

## 架构

模块图与依赖方向(无环):`pool → instance / dial / egress / eviction;dial 只读消费 pool 快照,不反向依赖`。

- 根包 `warppool`:`warppool.go` 门面(`Pool` 包装内部 `pool`),`pool.go` 协调核心,`instance.go` 每实例状态机,`dial.go` 选路(轮询取模 / 亲和 rendezvous 哈希),`egress.go` 出口探测与 `DedupeKeyer`,`eviction.go` 淘汰策略,`options.go` 配置默认值。
- `internal/amzwrap`:amz 客户端接口化(`Client`/`Factory`),核心层依赖接口而非 amz 本体。
- `internal/fakeamz`:amzwrap 假实现,仅被 `*_test.go` 导入,不进产物二进制。
- `internal/testutil`:离线单测基建(最小 SOCKS5 / HTTP CONNECT 假服务器,走真实 TCP loopback)。
- `cmd/proxy`:真实网络手动验收入口,非库产物。

### 并发模型(改代码前必读)

- **每 instance 一个管理 goroutine**:状态机在其内串行执行,实例内部无锁。
- **pool 一个协调循环 goroutine**:`instances/usedKeys/keysByInst/emptyByInst/portOf` 仅它访问;健康检查、数量对齐、淘汰、唯一性巡检都在此循环。
- **拨号热路径无锁读**:`snap atomic.Pointer` 存实例集合快照,写时复制,仅实例集合变化时重新发布;在途连接登记另有实例级互斥锁。
- 通信:`instance → pool` 走 event channel;`pool → instance` 走各实例 command channel;instance 不直接改池状态。

### 测试注入

`newPool(opts, factory, prober, health)` 四参依赖注入;`export_test.go` 的 `NewForTest` 暴露给黑盒测试(`warppool_test` 包),另白盒暴露选路纯函数与快照发布钩子。单测全部走 `fakeamz` + `testutil`,零网络。

## 设计约束(overview.md 的"不做"清单,改动前先对照)

- 纯库,不提供 HTTP/SOCKS5 对外协议服务(`cmd/proxy` 仅测试用);不管理 TUN。
- 无事件回调/订阅 API:不收敛场景走无限退避重试(`ReplayBackoffStart/Max`、`ReplayConcurrency` 可配),不引入通知通道。
- 换 IP 唯一手段是删 state 重注册(重播);连接级重连无意义,Draining 超时即强断在途连接。
- 库只承诺池内出口 IP 唯一性,不承诺多样性(同机 IP 受 Cloudflare 分配约束)。

## 设计文档

`docs/warp-pool/`:overview(目标/边界/关键决策)、architecture(技术栈/并发模型/接口契约)、api-index、data-design、`feat/`(模块级设计)、`steps/`(分步实施记录)。改架构前先读 overview 与 architecture;公开 API 变更同步 README 与 api-index。
