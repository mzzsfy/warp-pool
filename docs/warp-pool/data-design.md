# 数据流

## 数据流图

主链路(业务拨号):

```
业务 DialContext(ctx, network, addr)
  → dial: snapshot 读(atomic,无锁)
  → 候选过滤(Normal) → 轮询游标 / fnv64a(key) 取模
  → internal/amzwrap: SOCKS5 拨号至实例 proxyAddr(127.0.0.1:base+i)
      → amz Client listener → WARP 隧道(QUIC/CONNECT-IP) → CF 边缘 → 目标
  → 包装 InstanceConn(登记实例在途集合) → 返回业务
  → 业务断言 Instance() → {ID, Status, Egress} → 发现封禁 → SetStatus(id, Draining)
```

实例就绪链路:

```
New/补位 → pool 建 instance(state=StateDir/inst-<id>.json,端口=base+i)
  → instance: amz.NewClient → Start(90s) → go Run()
  → Probing: Prober.Probe(经自身代理) → Egress{V4,V6} → 上报 evReady(instance 不自判)
  → pool 判定唯一性:DedupeKeyer.Key(egress) 对照 usedKeys
      空 Key → cmdReprobe 重探测(连续达 probeFailLimit 后 cmdReplay 退避重播)
      冲突  → cmdReplay → 删 state → 重播(全局信号量 ReplayConcurrency)→ 新身份 → 重新探测
      唯一  → cmdConfirm → Normal → 快照发布(业务 Instances() 可见)
```

状态迁移链路:

```
HealthCheck 失败 / SetStatus(Draining)
  → cmdDrain → Draining(不接新拨号)
  → DrainTimeout 到点 → 强 Close 在途连接 → 删 state → 重注册 → Probing → Normal
SetStatus(Disabled) → cmdDisable → Disabled(占坑,不重建,不接新请求)
SetStatus(Normal on Disabled) → cmdEnable → Probing(复用 state,身份不变)
```

编排链路(reconcile,单 goroutine):

```
事件(evReady/evLost/evDrained/evReplayed) 或周期到点
  → 健康检查到期? → 并发 HealthCheck(Normal) → 失败者 cmdDrain
  → Normal+Probing < min? → 总数==max? → Evictor.Evict → 受害者 cmdStop / 无则挂起
                          → 否则建新 instance
  → 出口巡检到期? → 重探测 Normal → 空 Key/探测失败 → cmdDrain;Key 变更 → 更新登记;撞车 → 较新者重播
  → 快照发布(写时复制)
```

## 数据变换

| 变换点 | 输入 | 输出 | 持久化 | 缓存策略 |
|--------|------|------|--------|---------|
| 拨号选路 | 快照+key | 目标实例 | 无 | 快照 atomic 只读,变更写时复制 |
| 出口探测 | 实例 proxyAddr | Egress{V4,V6} | 无 | 实例内存字段,巡检周期刷新 |
| 去重键计算 | Egress | string Key | 无 | pool.usedKeys 登记,实例退出释放 |
| WARP 注册态 | - | state JSON | StateDir/inst-<id>.json(amz 自管) | 跨池重启复用,重播即删 |
| 在途连接登记 | InstanceConn | 实例连接集合 | 无 | Draining 超时强断依据,关闭即移除 |
| 池元数据(实例表/游标) | 事件 | 内存结构 | 无(不持久化) | 池重启按 min 重建 |

自洽检查:拨号热路径不依赖事件循环(纯快照),reconcile 阻塞不影响业务拨号;instance→pool 单向事件、pool→instance 单向命令、dial→pool 单向只读快照,无环;就绪探测与巡检共用 Prober,无重复探测路径。
