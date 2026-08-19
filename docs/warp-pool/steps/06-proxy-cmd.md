# cmd 代理测试入口

## 目标

提供真实 main 入口,将池 DialContext 包装为本地 HTTP 代理服务,支撑真实网络下的手动验收(curl -x / 浏览器),同时作为库接入示例。

## 改动点

- cmd/proxy/main.go:flag 配置(listen/min/max/state/transport),HTTP CONNECT 隧道(hijack + 双向拷贝)+ 明文 HTTP 绝对形式转发,stdlib 实现,零新依赖
- README:增补测试入口用法

## 设计决策

- HTTP CONNECT 而非 SOCKS5:stdlib 即可完成,零新依赖;testutil 假服务为测试断言导向,不复用
- 不复用 testutil:其约定"仅供 *_test.go 引用",且监听地址/记录行为面向断言

## 验收点

- go build ./... 通过,go vet 干净
- 真实启动:经代理 curl 出口 IP 非本机,池内实例出口互异
- Ctrl-C 优雅退出(server.Shutdown + pool.Close),无进程泄漏
