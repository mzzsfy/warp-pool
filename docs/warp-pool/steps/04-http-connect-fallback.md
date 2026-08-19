# HTTP CONNECT 兜底通道

## 目标

SOCKS5 拨号不可用场景(极端网络环境)下提供 HTTP CONNECT 备选拨号方式,经 amz 实例 listener 的 HTTP 代理协议。

## 改动点

- internal/amzwrap:新增 DialThroughProxyHTTP(CONNECT 隧道实现)
- options.go:拨号传输方式配置(socks5 默认 / http)
- dial.go:按配置选择拨号实现

## 验收点

- 两种传输方式对业务透明(InstanceConn 行为一致)
- http 模式经 httptest 假代理服务测试通过
- 默认行为不变(socks5)
