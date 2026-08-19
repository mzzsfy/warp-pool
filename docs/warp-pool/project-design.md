# 目录结构

## 目录树

```
warp-pool/
├── warppool.go        包注释 + Pool 门面 + 公开错误(api 模块)
├── options.go         Options + 默认值 + 校验
├── pool.go            pool 编排:reconcile 循环 / 快照发布 / 事件命令中转
├── instance.go        instance:状态机 / amz 生命周期 / 重播退避 / 在途连接登记
├── dial.go            DialContext / DialContextWithKey / InstanceConn / 选路
├── egress.go          Egress / Prober / DedupeKeyer 三内置
├── eviction.go        Evictor / EvictNone / EvictOldest
├── status.go          Status 枚举 + ID
├── *_test.go          同名单元测试(fake 注入,离线)
├── e2e_test.go        build tag `e2e`,真实 WARP 验证
├── internal/
│   ├── amzwrap/       amz Client 接口化包装(可 fake;SOCKS5 拨号)
│   └── testutil/      测试辅助(socks5 假服务/假探测服务等)
├── go.mod             module github.com/mzzsfy/warp-pool
├── README.md          特性 + 10 行示例 + 配置表
└── docs/warp-pool/    设计文档(本目录树)
```

目录树为 MVP 时点快照,后续 steps 会新增 bench_test.go 等文件。

## 目录职责

| 目录 | 职责 | 命名规范 |
|------|------|---------|
| 根 | 库公开面与各模块实现,文件名=模块名,与 feat/ 文档一一对应 | 小写单词,与 feat/ 文档同名 |
| internal/amzwrap | 唯一 import amz 的位置,接口化供 fake;提供经代理的 SOCKS5 拨号 | 对外仅暴露接口与默认实现 |
| internal/testutil | 跨模块测试辅助,不进公开 API | 仅供 *_test.go 引用 |
| docs/warp-pool | 设计文档树(bootstrap 产物) | 见 dev-flow 文档总览 |
| StateDir(运行时) | 实例 state 文件(默认 ./warp-state),运行时生成 | inst-<id>.json |

约定:公开符号集中于 warppool.go/options.go/dial.go/egress.go/eviction.go/status.go;pool/instance 仅 ID/Status/event 等最小公开;state 目录运行时生成并加入 .gitignore。
