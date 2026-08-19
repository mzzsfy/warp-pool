# 全局接口索引

公开面仅一个入口类型 `Pool` + 配置 `Options` + 策略/数据类型。内部模块接口见各 feat 文档,此处汇总全部公开符号(库形态无鉴权列,N/A)。

| 接口 | 所属模块 | 签名 | 请求格式 | 响应格式 | 鉴权 | 调用方 |
|------|---------|------|---------|---------|------|--------|
| 构造 | api | `New(opts Options) (*Pool, error)` | Options 结构体 | Pool / error | N/A | 业务 |
| 轮询拨号 | dial | `DialContext(ctx, network, addr string) (net.Conn, error)` | 标准拨号参数 | net.Conn(可断言 InstanceConn) | N/A | 业务(可直接作 http.Transport.DialContext) |
| 亲和拨号 | dial | `DialContextWithKey(ctx, key, network, addr string) (net.Conn, error)` | key+标准拨号参数 | 同上 | N/A | 业务(会话粘性) |
| 实例查询 | api | `Instances() []InstanceInfo` | 无 | InstanceInfo 列表 | N/A | 业务(封禁检测/监控) |
| 状态设置 | api | `SetStatus(id ID, status Status) error` | 实例ID+目标状态 | error | N/A | 业务(主动重启闭环) |
| 数量调整 | api | `SetMin(n int) error` / `SetMax(n int) error` | 新 min/max | error | N/A | 业务(运行时伸缩) |
| 关闭 | api | `Close() error` | 无 | error | N/A | 业务 |
| 连接实例信息 | dial | `InstanceConn.Instance() InstanceInfo` | 无 | ID/Status/Egress | N/A | 业务(断言后调用) |

数据类型:Options(api)、InstanceInfo/InstanceConn(dial)、Status/ID(instance)、Egress/Prober/HTTPProber(egress)、Evictor/EvictNone/EvictOldest(eviction)、DedupeKeyer/DedupeByV4/DedupeByV6/DedupeByBoth(egress)、Logger(api)、ErrNoInstance/ErrInvalidTransition/ErrClosed(api)。

一致性检查:命名与标准库 DialContext 签名一致;策略类型名词式;无重复接口;全部需求有对应公开符号,模块间调用全部落在内部,公开面无遗漏。
