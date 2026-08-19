# instance

## 职责

单实例全生命周期:amz Client 创建/启动/关闭、状态机迁移、出口探测协调、重播退避、Draining 超时强断。每实例一个管理 goroutine,状态变更仅发生在该 goroutine 内(串行,无锁)。

## 接口签名

```go
type Status uint8 // StatusProbing / StatusNormal / StatusDraining / StatusDisabled

type ID string // 实例稳定标识(创建序号文本)

// 对 pool 呈现的命令(经 command channel,非阻塞语义)
type command struct {
    kind    cmdKind // cmdConfirm / cmdReprobe / cmdReplay / cmdDrain / cmdDisable / cmdEnable / cmdStop
    timeout time.Duration // cmdDrain 的超时
}

// 对 pool 上报的事件(经 event channel)
type event struct {
    inst   *instance
    kind   evKind  // evReady(探测完成待唯一性确认) / evLost(异常失联) / evDrained / evReplayed / evStopped(终态退出)
    egress Egress
}

// dial 侧视图(快照元素:集合成员不可变;status/egress 为实时原子读函数,非发布快照)
type instanceView struct {
    id        ID
    status    func() Status // 实时原子读当前状态
    egress    func() Egress // 实时原子读最近有效出口
    proxyAddr string
    createdAt time.Time
    inst      *instance // 反向引用(SetStatus 定位实例)
}
```

## 状态机

```
(创建) → Probing --探测完成,上报 evReady--> [等 pool 唯一性判定]
    --cmdConfirm(Key唯一)--> Normal
    --cmdReprobe(空Key未超限)--> 重探测(不删 state,短退避)
    --cmdReplay(空Key超限/Key冲突)--> [删state重建Client,重播退避] → Probing
Normal --cmdDrain/健康检查失败--> Draining --超时强断在途+删state重注册--> Probing;Probing --cmdDrain--> Draining(同路)
Normal/Probing/Draining --cmdDisable--> Disabled --cmdEnable--> Probing(重探测,复用state)
任意 --cmdStop--> 终态(资源释放,evStopped)
```

Key 唯一性由 pool 判定(instance 不持有 DedupeKeyer,不自判);探测失败(双栈全失败)同样上报 evReady(egress 全零,Key 必空),与空 Key 同路处理。

## 数据流

pool 下发 command → instance 管理循环执行 → 事件回报 pool。在途连接登记:实例建立 outbound conn 时登记,Draining 超时点统一 Close;conn 关闭时移除。重播受全局信号量约束(ReplayConcurrency,默认 1)。

amz 用法(沿 opencode2api 验证模式):`amz.NewClient`(Storage=实例 state 路径,Listen=base+i,SOCKS5+HTTP enabled)→ `Start(ctx)`(90s 超时)→ `go Run()` 维持 → Close 释放。重播 = Close → 删 state 文件 → 重新 NewClient+Start。失联(Run 返回)= Close → 复用 state 重新 NewClient+Start(不删 state;再失败才删 state 重注册)。

Disabled = Close 释放隧道与 listener(state 保留,身份不变);Enable = 复用 state 重新 NewClient+Start → 探测。

## 验收点(BDD)

- Probing 探测完成上报 evReady;pool 判定 Key 唯一下发 cmdConfirm → 转 Normal
- Probing Key 冲突/空 Key 超限 → state 文件被删、新 Client 创建、重新探测;退避间隔按 ReplayBackoffStart 起指数增长封顶 ReplayBackoffMax
- Draining:不再接受新拨号;在途连接到期(timeout)被强制 Close;随后重注册转 Probing
- Disabled:SetStatus 后立即停止接新请求;Enable 后重新探测(不删 state,身份保留)
- cmdStop:amz Close、listener 释放、管理 goroutine 退出、evStopped 上报
- 同一实例同时只存在一个管理 goroutine;状态迁移全部串行(竞争测试 -race 通过)
