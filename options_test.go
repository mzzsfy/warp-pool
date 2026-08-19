// 白盒例外:默认值应用 withDefaults 为未导出方法,需同包访问验证
package warppool

import (
	"testing"
	"time"
)

// Given 全零 Options When 应用默认值 Then 每个字段均为既定默认
func TestOptionsWithDefaults_ZeroOptions_FillsDefaults(t *testing.T) {
	got := Options{}.withDefaults()
	cases := []struct {
		name string
		got  any
		want any
	}{
		{"ListenBase", got.ListenBase, defaultListenBase},
		{"StateDir", got.StateDir, defaultStateDir},
		{"Evictor", got.Evictor, EvictNone{}},
		{"DedupeKeyer", got.DedupeKeyer, DedupeByV4{}},
		{"EgressProbeV4URL", got.EgressProbeV4URL, defaultEgressProbeV4URL},
		{"EgressProbeV6URL", got.EgressProbeV6URL, defaultEgressProbeV6URL},
		{"HealthInterval", got.HealthInterval, defaultHealthInterval},
		{"HealthTimeout", got.HealthTimeout, defaultHealthTimeout},
		{"EgressCheckInterval", got.EgressCheckInterval, defaultEgressCheckInterval},
		{"DrainTimeout", got.DrainTimeout, defaultDrainTimeout},
		{"ReplayBackoffStart", got.ReplayBackoffStart, defaultReplayBackoffStart},
		{"ReplayBackoffMax", got.ReplayBackoffMax, defaultReplayBackoffMax},
		{"ReplayConcurrency", got.ReplayConcurrency, defaultReplayConcurrency},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("%s = %v, 期望 %v", c.name, c.got, c.want)
			}
		})
	}
	if _, ok := got.Logger.(discardLogger); !ok {
		t.Fatalf("Logger = %T, 期望 discardLogger", got.Logger)
	}
}

// Given 全字段自定义的 Options When 应用默认值 Then 原值全部保留
func TestOptionsWithDefaults_CustomOptions_KeepsNonZero(t *testing.T) {
	in := Options{
		Min:                 2,
		Max:                 5,
		ListenBase:          "0.0.0.0:10000",
		StateDir:            "/tmp/warp-pool-test",
		Evictor:             EvictOldest{},
		DedupeKeyer:         DedupeByBoth{},
		EgressProbeV4URL:    "http://v4.example/ip",
		EgressProbeV6URL:    "http://v6.example/ip",
		HealthInterval:      time.Second,
		HealthTimeout:       500 * time.Millisecond,
		EgressCheckInterval: time.Minute,
		DrainTimeout:        2 * time.Second,
		ReplayBackoffStart:  100 * time.Millisecond,
		ReplayBackoffMax:    30 * time.Second,
		ReplayConcurrency:   3,
		Logger:              discardLogger{},
	}
	if got := in.withDefaults(); got != in {
		t.Fatalf("withDefaults() = %+v, 期望原样保留 %+v", got, in)
	}
}

// Given 时长与并发为非正的 Options When 应用默认值 Then 按零值处理填充默认
func TestOptionsWithDefaults_NonPositiveValues_FillDefaults(t *testing.T) {
	got := Options{
		HealthInterval:     -time.Second,
		HealthTimeout:      0,
		DrainTimeout:       -time.Second,
		ReplayBackoffStart: 0,
		ReplayBackoffMax:   -time.Second,
		ReplayConcurrency:  -1,
	}.withDefaults()
	cases := []struct {
		name string
		got  any
		want any
	}{
		{"HealthInterval", got.HealthInterval, defaultHealthInterval},
		{"HealthTimeout", got.HealthTimeout, defaultHealthTimeout},
		{"DrainTimeout", got.DrainTimeout, defaultDrainTimeout},
		{"ReplayBackoffStart", got.ReplayBackoffStart, defaultReplayBackoffStart},
		{"ReplayBackoffMax", got.ReplayBackoffMax, defaultReplayBackoffMax},
		{"ReplayConcurrency", got.ReplayConcurrency, defaultReplayConcurrency},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("%s = %v, 期望 %v", c.name, c.got, c.want)
			}
		})
	}
}

// Given 合法基线配置逐项注入非法值 When 校验 Then 每项均返回错误
func TestOptions_Validate_IllegalFields_ReturnError(t *testing.T) {
	base := Options{Min: 2, Max: 4}.withDefaults()
	cases := []struct {
		name   string
		mutate func(Options) Options
	}{
		{"Min为零", func(o Options) Options { o.Min = 0; return o }},
		{"Min为负", func(o Options) Options { o.Min = -1; return o }},
		{"Min大于Max", func(o Options) Options { o.Min = base.Max + 1; return o }},
		{"Max超端口上限", func(o Options) Options { o.Max = maxListenPort + 1; return o }},
		{"ListenBase缺端口", func(o Options) Options { o.ListenBase = "127.0.0.1"; return o }},
		{"ListenBase端口非数字", func(o Options) Options { o.ListenBase = "127.0.0.1:abc"; return o }},
		{"ListenBase端口为零", func(o Options) Options { o.ListenBase = "127.0.0.1:0"; return o }},
		{"ListenBase端口超上限", func(o Options) Options { o.ListenBase = "127.0.0.1:65536"; return o }},
		{"StateDir为空", func(o Options) Options { o.StateDir = ""; return o }},
		{"Evictor未设置", func(o Options) Options { o.Evictor = nil; return o }},
		{"DedupeKeyer未设置", func(o Options) Options { o.DedupeKeyer = nil; return o }},
		{"V4探测URL为空", func(o Options) Options { o.EgressProbeV4URL = ""; return o }},
		{"V6探测URL为空", func(o Options) Options { o.EgressProbeV6URL = ""; return o }},
		{"健康间隔非正", func(o Options) Options { o.HealthInterval = 0; return o }},
		{"健康超时非正", func(o Options) Options { o.HealthTimeout = 0; return o }},
		{"巡检周期非正", func(o Options) Options { o.EgressCheckInterval = -time.Second; return o }},
		{"DrainTimeout非正", func(o Options) Options { o.DrainTimeout = 0; return o }},
		{"退避起点非正", func(o Options) Options { o.ReplayBackoffStart = 0; return o }},
		{"退避上限小于起点", func(o Options) Options {
			o.ReplayBackoffStart = time.Minute
			o.ReplayBackoffMax = time.Second
			return o
		}},
		{"重播并发非正", func(o Options) Options { o.ReplayConcurrency = 0; return o }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.mutate(base).Validate(); err == nil {
				t.Fatalf("%s 应返回错误", c.name)
			}
		})
	}
}

// Given 仅设置 Min/Max 的最小配置 When 默认填充后校验 Then 通过
func TestOptions_Validate_MinimalOptions_Passes(t *testing.T) {
	if err := (Options{Min: 1, Max: 1}.withDefaults()).Validate(); err != nil {
		t.Fatalf("最小配置应通过校验: %v", err)
	}
}

// Given 静默日志 When 输出任意内容 Then 不产生副作用
func TestDiscardLogger_Printf_DiscardsAll(t *testing.T) {
	discardLogger{}.Printf("格式 %d %s", 1, "文本")
}
