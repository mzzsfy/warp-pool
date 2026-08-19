# warp-pool

## 目标

管理一组 Cloudflare WARP 隧道实例,对外提供拨号级代理能力,实例数量、状态、出口 IP 可控。

amz 解决"一条 WARP 隧道"的注册、选点、重连;warp-pool 解决"一组隧道"的编排——数量维持(min/max)、健康检查与故障切换、出口 IP 探测与池内唯一性(重复必重播)、实例状态机(正常/排空/禁用)、亲和选路。业务方 import 即用,替代各项目各自维护的 WarpPool/nodePool 脚手架(如 opencode2api)。给需要多出口 IP 的 Go 服务使用:API 探针、爬虫、多账号会话保持、规避单实例限速。

## 做什么

- 实例生命周期管理:同进程多 amz.Client,自动注册/启动/关闭
- 出口 IP 探测(v4/v6 双栈),DedupeKeyer 去重策略(内置 v4/v6/双栈,默认 v4),重复 IP 自动重播(删 state 重注册)直至池内唯一
- 数量模型:总数上限 max,维持 Normal+Probing ≥ min(常态为 min 个 Normal);实例摘除时旧实例占坑排空、新实例自动补位,总数波动于 min~max
- 淘汰策略接口:内置"排队(背压)"与"杀最老(创建时间+状态优先级)"
- 实例状态机:Normal / Draining(不接新请求,超时强断在途连接并重播换 IP)/ Disabled(摘除待手动恢复)/ Probing(重建探测中,对外可见)
- 选路:轮询(默认,失败自动换实例重试)+ 亲和选路(同 key 稳定粘实例,故障自动跳过);拨号传输 SOCKS5(默认)/HTTP CONNECT(兜底)可配
- 拨号 API:标准签名 `DialContext(ctx, network, addr)`,返回连接可断言 `InstanceConn` 获取实例信息(ID/出口 IP/状态),支撑业务主动重启闭环
- 运行时调整:SetMin/SetMax(自动伸缩对齐)、SetStatus、Instances 查询、Stats 计数快照(状态数/重播累计/拨号累计与失败,被动查询式)
- 周期健康检查:HealthCheck 失败自动转 Draining 走重建

## 不做

- 独立服务/进程形态(纯库;需要服务的业务自行包 main)
- HTTP/SOCKS5 协议封装(仅暴露 DialContext,协议由业务组装;amz 实例自身 listener 仍可用;SOCKS5 与 HTTP CONNECT 为经实例拨号的两种内置传输,非对外协议服务)
- TUN 模式管理(仅 HTTP/SOCKS5 通道;amz TUN 能力不纳入池管)
- Team WARP / license key 管理(amz 路线图未支持)
- 池元数据持久化(state 文件由 amz 自管,池重启按 min 重建并复用注册态)
- 事件回调/订阅 API(重播不收敛走无限退避重试,不引入通知通道)
- 跨进程/分布式协调
- 承诺出口 IP 多样性(同机出口 IP 受 CF 分配约束,库只保证池内唯一性,唯一性无法满足时无限退避重播)

## 关键决策

| 决策项 | 选择 | 原因 |
|--------|------|------|
| 形态 | Go 库 | 多项目复用,业务 import 使用 |
| 池化目标 | 高可用 + 分摊流量 + IP 轮换 | 三目标全要 |
| 请求 API | DialContext + InstanceConn 断言 | 最底层通用;业务自组装 http.Transport 或直连用 |
| IP 唯一性 | 硬约束,重播直至唯一 | 封禁规避场景重复 IP 无价值 |
| 去重维度 | DedupeKeyer 接口,默认 v4 | WARP v6 出口单机几乎同 /64,按 v6 去重死循环;接口留扩展 |
| 重启语义 | Draining 超时强断 + 重注册换身份 | 连接级重连对换 IP 无效,重注册是唯一换 IP 手段 |
| 淘汰策略 | 接口 + 排队/杀最老两内置 | 可配置要求 |
| 不收敛行为 | 无限退避重试(参数可配) | 语义最简,不引入回调面 |
| 依赖 | amz + x/net + 标准库 | 库最小依赖原则 |
