# MVP

## 范围

库全量核心能力一次交付(库无法半成品可用,MVP 边界=砍掉非核心外围):Options 全量配置项、instance 状态机四态全路径、pool 协调(数量对齐/健康检查/淘汰/唯一性巡检)、dial(轮询+亲和+失败重试+InstanceConn)、egress 双栈探测+三 Keyer、eviction 两策略、api 门面。

不含(后续步骤):opencode2api 迁移示例(02)、性能基准(03)、HTTP CONNECT 兜底通道(04)、状态导出/监控钩子(05)。

## 模块连通

- [ ] api(Pool 门面+Options+校验)最小实现
- [ ] options 全量配置与默认值
- [ ] egress(Prober+DedupeKeyer 三内置)最小实现
- [ ] eviction(EvictNone/EvictOldest)最小实现
- [ ] instance(四态状态机+amz 生命周期+重播退避)最小实现
- [ ] pool(reconcile 编排)最小实现
- [ ] dial(轮询/亲和/重试+InstanceConn)最小实现
- [ ] internal/amzwrap(amz 接口化+SOCKS5 拨号)最小实现

## 端到端路径

`DialContext` → 亲和/轮询选实例 → 经实例 SOCKS5 代理达目标 → `InstanceConn.Instance()` 拿 ID/Egress → `SetStatus(Draining)` → 超时强断 → 重播 → 回 Normal 全链路(fake 注入验证;真实 WARP 在 e2e tag)。

## 骨架

- [ ] 目录结构(project-design.md 目录树)
- [ ] 配置(Options 结构体,无配置文件)
- [ ] 测试框架(标准 testing + httptest + fake 注入 + e2e build tag)
- [ ] 依赖管理(go.mod,amz + x/net/proxy)

## 验收点

- `New` → 拨号成功 → `Close`,进程无泄漏(-race + goroutine 检查)
- 单测全离线:不联网、不起真 amz,`go test ./... -race -count=1` 全绿
- e2e(build tag `e2e`):真实 WARP 起池→拨号→SetStatus→重播验证,CI 默认跳过
- 状态机迁移表、选路/亲和/重试、淘汰两策略、去重三 Keyer 各自测试覆盖
- `go vet ./...` 干净;README 含 10 行可跑示例
