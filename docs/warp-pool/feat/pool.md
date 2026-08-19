# pool

## 职责

实例集合的编排核心:数量维持(min/max)、淘汰触发、周期健康检查、唯一性登记、命令/事件中转、快照发布。一个协调循环 goroutine 串行处理全部决策(避免锁域复杂化)。

## 接口签名

```go
// 内部结构(不对外)
type pool struct {
    opts      Options
    instances map[ID]*instance
    usedKeys  map[string]ID        // DedupeKeyer Key → 占用实例
    ports     portAllocator        // base+i 分配与回收
    replaySem chan struct{}        // ReplayConcurrency 信号量
    snapshot  atomic.Pointer[[]instanceView] // dial 只读
    events    chan event
}

// 协调循环(单 goroutine)
func (p *pool) reconcile(ctx context.Context) // 周期触发 + 事件触发
```

## 协调逻辑(reconcile 每轮)

1. 健康检查到期 → 对 Normal 实例并发检查(纯 TCP 拨通实例自身代理 listener 即健康,带超时;amz.Client 未导出 HealthCheck,以代理连通性为准),失败者下发 cmdDrain
2. 数量对齐:Normal+Probing 数 < min → 需建新;总数 == max → 走 Evictor;EvictNone → 挂起新建等待空位(evStopped 后唤醒继续)
3. 唯一性判定:evReady 事件 → DedupeKeyer 计算 Key → 空且未超上限(probeFailLimit=3,硬编码不可配) → cmdReprobe;空且超限/与池内冲突 → cmdReplay(后完成者);唯一 → 登记 usedKeys + cmdConfirm(实例转 Normal)
4. 周期巡检:对 Normal 实例重新探测出口 IP(CF 侧迁移检测),Key 变化 → 更新登记;空 Key/探测失败 → cmdDrain;撞车 → 较新者 cmdDrain(标准排空重播路径)
5. 实例退出:evStopped → 移出集合、释放端口、释放 Key、唤醒对齐
6. 快照发布:实例集合变化后写时复制发布(状态与出口经视图函数实时原子读)

Key 释放时机:evLost / evDrained / evReplayed / evStopped(实例离开 Normal 即释放其占用 Key)。
事件触发与周期触发合并去抖(事件到达即提前唤醒,不等到期)。

## 数据流

instance.event → pool.events → reconcile 消费决策 → instance.command → instance 执行 → 新事件回环。dial 只读 snapshot,不进此循环。

## 验收点(BDD)

- 启动:min=3 时池逐步出现 3 个 Normal(异步,New 不阻塞);state 目录 3 个文件
- 故障自愈:1 个实例 HealthCheck 持续失败 → 转 Draining 重播 → 池自动补新实例,Normal 数回到 min
- 淘汰:EvictOldest + 总数达 max + 需新建 → 最老 Disabled 实例被 stop,新实例建立
- 背压:EvictNone + 总数达 max + 需新建 → 新建挂起;某实例 stop 后挂起的新建继续
- SetMax 降低:超过新 max 的多余实例按淘汰策略缩减
- 唯一性巡检:人为令两实例出口 IP 相同 → 巡检后较新者进入重播
- Close:协调循环退出、实例全部终止(ctx 取消驱动,通道不 close)、无 goroutine 泄漏(-race + goroutine 数量前后一致检查)
