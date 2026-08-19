# egress

## 职责

1. 经实例自身代理 listener 探测出口双栈 IP
2. DedupeKeyer 将 Egress 映射为去重键,池据此判定实例间 IP 重复

纯函数 + 注入策略,无状态。

## 接口签名

```go
// Egress 实例出口地址;探测失败的字段为零值(invalid)
type Egress struct {
    V4, V6 netip.Addr
}

// Prober 经指定实例代理探测出口(实现内用 http.Client{Transport: 代理指向实例 listener})
type Prober interface {
    Probe(ctx context.Context, proxyAddr string) (Egress, error)
}

// DedupeKeyer 去重键策略:池内两实例 Key 相同 = IP 重复,后到者重播
type DedupeKeyer interface {
    Key(Egress) string // 空 Key = 探测不完整,同样触发重播
}

// 内置实现
DedupeByV4{}   // 默认:V4 文本;V4 零值返回空
DedupeByV6{}   // V6 文本;V6 零值返回空
DedupeByBoth{} // V4 与 V6 拼接;任一零值返回空
```

## 数据流

实例 Probing 阶段:Prober.Probe(ctx, 实例监听地址) → Egress → 上报 pool,由 pool 侧 DedupeKeyer.Key(egress) 判定唯一性:
- Key 为空 → 视为探测不完整,按重播退避重试(不删 state,仅重探测;连续失败达上限 probeFailLimit=3,硬编码不可配,后退避重播)
- Key 与池内其他 Normal 实例冲突 → 删 state 重播
- 唯一 → 实例转 Normal

配置:`EgressProbeV4URL`(默认 https://api4.ipify.org)、`EgressProbeV6URL`(默认 https://api6.ipify.org);探测超时复用 `HealthTimeout`,重试间隔复用重播退避参数(`ReplayBackoffStart`/`ReplayBackoffMax`)。

## 验收点(BDD)

- Probe:经代理返回明文 IP 文本,解析为 netip.Addr;非法响应体返回错误
- DedupeByV4:V4 零值 → 空 Key;有效 V4 → 其文本
- DedupeByBoth:任一栈零值 → 空 Key;双栈有效 → "v4|v6" 拼接
- 两实例 Key 相同时,后完成探测者触发重播,先完成者保持 Normal
