# eviction

## 职责

池内实例总数达 max 且需要新建实例时,决策"腾位置"的方式。纯策略对象,无状态,无 goroutine。

## 接口签名

```go
// Evictor 决策达 max 时如何腾出实例槽位
type Evictor interface {
    // Evict 返回被淘汰实例 ID 与 true;返回 false 表示不淘汰(排队策略)
    // candidates 为当前全部实例的只读视图(含状态与创建时间)
    Evict(candidates []InstanceView) (ID, bool)
}

type InstanceView struct {
    ID        ID
    Status    Status       // Normal / Draining / Disabled / Probing
    CreatedAt time.Time
}

// 内置实现
EvictNone{}        // 排队(背压):永不淘汰,新建请求挂起等待空位
EvictOldest{}      // 杀最老:Disabled 最老 → Draining 最老 → Normal 最老(创建时间序);Probing 不可淘汰
```

## 数据流

pool 协调循环检测到「Normal+Probing 数 < min 且 总数 == max」→ 取快照 → Evictor.Evict(snapshot) → 有受害者:向该实例发 stop 命令,腾位后继续建新实例;无受害者(EvictNone):新建任务进入等待队列,直到实例数 < max(Stop/淘汰/禁用后释放)。

## 验收点(BDD)

- EvictNone:任意快照恒返回 false;池达 max 时新建挂起,Close 某实例后队列中的新建继续
- EvictOldest:快照含 Disabled+Draining+Normal 各若干,返回三者中各自最老且优先级 Disabled > Draining > Normal
- EvictOldest:候选为空返回 false
- Options 未指定时默认 EvictNone(背压语义安全,不误杀)
