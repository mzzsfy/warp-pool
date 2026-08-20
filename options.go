package warppool

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Logger 最小日志接口,与 amz.Logger 同构;默认静默
type Logger interface {
	Printf(format string, args ...any)
}

// 全部配置默认值
const (
	defaultListenBase          = "127.0.0.1:51367"
	defaultStateDir            = "./warp-state"
	defaultEgressProbeV4URL    = "https://api4.ipify.org"
	defaultEgressProbeV6URL    = "https://api6.ipify.org"
	defaultHealthInterval      = 30 * time.Second
	defaultHealthTimeout       = 10 * time.Second
	defaultEgressCheckInterval = 5 * time.Minute
	defaultDrainTimeout        = 60 * time.Second
	defaultReplayBackoffStart  = 1 * time.Second
	defaultReplayBackoffMax    = 60 * time.Second
	defaultReplayConcurrency   = 1
	defaultDialTransport       = TransportSOCKS5
)

// DialTransport 经实例代理拨号的传输方式
type DialTransport string

const (
	// TransportSOCKS5 经实例 listener 的 SOCKS5 代理拨号(默认)
	TransportSOCKS5 DialTransport = "socks5"
	// TransportHTTP 经实例 listener 的 HTTP CONNECT 隧道拨号
	TransportHTTP DialTransport = "http"
)

// 端口合法区间
const (
	minListenPort = 1
	maxListenPort = 65535
)

// Options 池全部可配项;零值字段在 New 时填充默认值
type Options struct {
	Min, Max            int           // 实例数下限/上限,必须 0 < Min <= Max <= 65535
	ListenBase          string        // 实例 listener 端口起点(base+i)
	StateDir            string        // 实例 state 目录
	Evictor             Evictor       // 达上限时的淘汰策略;EvictNone 下缩容在实例自然退出后生效
	DedupeKeyer         DedupeKeyer   // 出口去重键策略
	EgressProbeV4URL    string        // V4 出口探测服务地址
	EgressProbeV6URL    string        // V6 出口探测服务地址
	HealthInterval      time.Duration // 周期健康检查间隔
	HealthTimeout       time.Duration // 单次健康检查超时
	EgressCheckInterval time.Duration // 出口巡检周期
	DrainTimeout        time.Duration // Draining 强断在途连接的超时
	ReplayBackoffStart  time.Duration // 重播退避起点
	ReplayBackoffMax    time.Duration // 重播退避上限
	ReplayConcurrency   int           // 全局重播并发
	DialTransport       DialTransport // 拨号传输方式(socks5 默认 / http)
	Endpoints           []string      // 实例 endpoint(host:port)列表;空为自动选优,非空按创建序轮询分配,重播时轮换
	Logger              Logger        // 日志输出
}

// withDefaults 返回零值字段已填默认的副本
func (o Options) withDefaults() Options {
	if o.ListenBase == "" {
		o.ListenBase = defaultListenBase
	}
	if o.StateDir == "" {
		o.StateDir = defaultStateDir
	}
	if o.Evictor == nil {
		o.Evictor = EvictNone{}
	}
	if o.DedupeKeyer == nil {
		o.DedupeKeyer = DedupeByV6{}
	}
	if o.EgressProbeV4URL == "" {
		o.EgressProbeV4URL = defaultEgressProbeV4URL
	}
	if o.EgressProbeV6URL == "" {
		o.EgressProbeV6URL = defaultEgressProbeV6URL
	}
	if o.HealthInterval <= 0 {
		o.HealthInterval = defaultHealthInterval
	}
	if o.HealthTimeout <= 0 {
		o.HealthTimeout = defaultHealthTimeout
	}
	if o.EgressCheckInterval <= 0 {
		o.EgressCheckInterval = defaultEgressCheckInterval
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = defaultDrainTimeout
	}
	if o.ReplayBackoffStart <= 0 {
		o.ReplayBackoffStart = defaultReplayBackoffStart
	}
	if o.ReplayBackoffMax <= 0 {
		o.ReplayBackoffMax = defaultReplayBackoffMax
	}
	if o.ReplayConcurrency <= 0 {
		o.ReplayConcurrency = defaultReplayConcurrency
	}
	if o.DialTransport == "" {
		o.DialTransport = defaultDialTransport
	}
	if o.Logger == nil {
		o.Logger = discardLogger{}
	}
	return o
}

// Validate 校验生效配置(应先经默认值填充),返回首个非法项错误
func (o Options) Validate() error {
	if o.Min <= 0 {
		return errors.New("Min 必须大于 0")
	}
	if o.Min > o.Max {
		return fmt.Errorf("Min(%d) 不能大于 Max(%d)", o.Min, o.Max)
	}
	if o.Max > maxListenPort {
		return fmt.Errorf("Max(%d) 不能大于端口空间上限 %d", o.Max, maxListenPort)
	}
	if err := validateHostPort(o.ListenBase); err != nil {
		return fmt.Errorf("ListenBase 非法: %w", err)
	}
	if o.StateDir == "" {
		return errors.New("StateDir 不能为空")
	}
	if o.Evictor == nil {
		return errors.New("Evictor 未设置")
	}
	if o.DedupeKeyer == nil {
		return errors.New("DedupeKeyer 未设置")
	}
	if o.EgressProbeV4URL == "" || o.EgressProbeV6URL == "" {
		return errors.New("出口探测 URL 不能为空")
	}
	if o.HealthInterval <= 0 || o.HealthTimeout <= 0 || o.EgressCheckInterval <= 0 {
		return errors.New("健康检查与巡检周期必须为正")
	}
	if o.DrainTimeout <= 0 {
		return errors.New("DrainTimeout 必须为正")
	}
	if o.ReplayBackoffStart <= 0 || o.ReplayBackoffMax < o.ReplayBackoffStart {
		return errors.New("重播退避区间非法")
	}
	if o.ReplayConcurrency <= 0 {
		return errors.New("ReplayConcurrency 必须为正")
	}
	switch o.DialTransport {
	case TransportSOCKS5, TransportHTTP:
	default:
		return fmt.Errorf("DialTransport %q 非法, 仅支持 %s/%s", o.DialTransport, TransportSOCKS5, TransportHTTP)
	}
	for _, ep := range o.Endpoints {
		if err := validateHostPort(ep); err != nil {
			return fmt.Errorf("Endpoint %q 非法: %w", ep, err)
		}
	}
	return nil
}

// validateHostPort 校验 host:port 形态且端口在合法区间
func validateHostPort(s string) error {
	_, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("解析 host:port 失败: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("端口 %q 非数字: %w", portStr, err)
	}
	if port < minListenPort || port > maxListenPort {
		return fmt.Errorf("端口 %d 超出合法区间", port)
	}
	return nil
}

// discardLogger 静默日志实现
type discardLogger struct{}

// Printf 丢弃全部日志
func (discardLogger) Printf(string, ...any) {}
