// 性能基准与分布均匀性验证:拨号全链路(轮询/亲和/并发)、快照读写、选路纯函数;
// 全链路环境复用 dial_test 的 dialEnv(经假 SOCKS5 服务的真实网络路径),
// 均匀性与零分配断言为普通测试,随默认跑批执行;基准仅在 -bench 时运行。
package warppool_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mzzsfy/warp-pool"
)

// 基准环境常量
const (
	dialBenchInsts = 10  // 全链路拨号基准的实例数
	concDialers    = 100 // 并发拨号基准的 goroutine 目标数
	allocRuns      = 100 // AllocsPerRun 断言的执行轮数
	routeCopyAlloc = 1   // 选路纯函数结果切片拷贝的固有分配次数
	roundRobinTurn = 2   // 轮询均匀性断言的完整轮转圈数
	keysPerInst    = 10  // 亲和均匀性断言的每实例 key 数
	chiSigma       = 4   // 卡方阈值的方差倍数(误检概率约 1e-4 量级)
	nearMaxInsts   = 100 // 贴近监听端口区间顶端的实例数
	maxTCPPort     = 1<<16 - 1
	affinityKey    = "bench-tenant"

	snapshotWriteInterval = time.Millisecond // 读写并发基准的后台快照重发布间隔
	sinkCount             = 32               // 拨号基准下沉服务端口数
	sinkAcceptors         = 2                // 每下沉服务的并发接受者数(抗调度饿死)
)

// benchSinks 多端口下沉服务池(接受后丢弃读,随对端关闭回收),拨号基准的拨号目标;
// Windows loopback 下单目标端口在高频建连时受两重限制:积压队列溢出即拒连、
// TIME_WAIT 端口预算有限,多端口轮询摊薄压力;端口由 OS 分配,不与手动绑定冲突
type benchSinks struct {
	listeners []net.Listener
	cursor    atomic.Int64
}

// newBenchSinks 启动 sinkCount 个下沉服务并随测试清理
func newBenchSinks(tb testing.TB) *benchSinks {
	tb.Helper()
	s := &benchSinks{listeners: make([]net.Listener, sinkCount)}
	tb.Cleanup(func() {
		for _, ln := range s.listeners {
			_ = ln.Close()
		}
	})
	for i := range s.listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("启动下沉服务失败: %v", err)
		}
		s.listeners[i] = ln
		for range sinkAcceptors {
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					go func(c net.Conn) {
						_, _ = io.Copy(io.Discard, c)
						_ = c.Close()
					}(c)
				}
			}()
		}
	}
	return s
}

// addr 轮询返回下沉服务地址
func (s *benchSinks) addr() string {
	i := s.cursor.Add(1) - 1
	return s.listeners[int(i)%len(s.listeners)].Addr().String()
}

// 纯函数基准候选数与快照基准实例数
var (
	routeBenchInsts    = []int{10, 50, nearMaxInsts}
	snapshotBenchInsts = []int{10, 50}
)

// routeAddr 指定监听基址下第 i 个实例的代理地址
func routeAddr(base, i int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(base+i))
}

// newRouteEnv 构造 n 实例拨号环境并等待全部 Normal
func newRouteEnv(tb testing.TB, n, base int) *dialEnv {
	tb.Helper()
	env := newDialEnv(tb, n, n, func(o *warppool.Options) {
		o.ListenBase = routeAddr(base, 0)
	})
	for i := range n {
		env.prober.set(routeAddr(base, i), egBySeq(i+1))
	}
	env.waitNormal(n)
	// 目标作为下沉服务消费接入连接(读到对端关闭即回收):
	// 从不 Accept 会令积压队列耗尽,接受即关会向在途握手发 RST,二者均致上游拨号失败
	go func() {
		for {
			c, err := env.target.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_, _ = io.Copy(io.Discard, c)
				_ = c.Close()
			}(c)
		}
	}()
	return env
}

// BenchmarkDialContext 轮询拨号全链路基准(选路 + 经假 SOCKS5 的真实网络拨号)
func BenchmarkDialContext(b *testing.B) {
	env := newRouteEnv(b, dialBenchInsts, dialBasePort)
	sinks := newBenchSinks(b)
	b.ReportAllocs()
	for b.Loop() {
		conn, err := env.p.DialContext(context.Background(), "tcp", sinks.addr())
		if err != nil {
			b.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDialContextWithKey 亲和拨号全链路基准(同 key 稳定落同一实例)
func BenchmarkDialContextWithKey(b *testing.B) {
	env := newRouteEnv(b, dialBenchInsts, dialBasePort)
	sinks := newBenchSinks(b)
	b.ReportAllocs()
	for b.Loop() {
		conn, err := env.p.DialContextWithKey(context.Background(), affinityKey, "tcp", sinks.addr())
		if err != nil {
			b.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDialContextConcurrent 并发拨号全链路基准,goroutine 数不低于 concDialers(锁竞争观测入口);
// Windows 主机 TIME_WAIT 端口总量有限,建议按迭代数封顶执行:-benchtime 20000x
func BenchmarkDialContextConcurrent(b *testing.B) {
	env := newRouteEnv(b, dialBenchInsts, dialBasePort)
	sinks := newBenchSinks(b)
	b.ReportAllocs()
	procs := runtime.GOMAXPROCS(0)
	b.SetParallelism((concDialers + procs - 1) / procs)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := env.p.DialContext(context.Background(), "tcp", sinks.addr())
			if err != nil {
				b.Errorf("并发拨号失败: %v", err)
				return
			}
			_ = conn.Close()
		}
	})
}

// BenchmarkSnapshot 快照并发基准:公开读(Instances)、读写并发(后台周期重发布)与写时复制发布
func BenchmarkSnapshot(b *testing.B) {
	for _, n := range snapshotBenchInsts {
		b.Run(fmt.Sprintf("实例%d", n), func(b *testing.B) {
			env := newRouteEnv(b, n, dialBasePort)
			b.Run("并发读", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_ = env.p.Instances()
					}
				})
			})
			b.Run("读写并发", func(b *testing.B) {
				stop := make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					tk := time.NewTicker(snapshotWriteInterval)
					defer tk.Stop()
					for {
						select {
						case <-stop:
							return
						case <-tk.C:
							env.p.PublishForTest()
						}
					}
				}()
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_ = env.p.Instances()
					}
				})
				close(stop)
				<-done
			})
			b.Run("写发布", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					env.p.PublishForTest()
				}
			})
		})
	}
}

// BenchmarkAffinityOrder 亲和排序纯函数基准(rendezvous 散列 + 候选重排)
func BenchmarkAffinityOrder(b *testing.B) {
	for _, n := range routeBenchInsts {
		views := warppool.MakeViewsForTest(n)
		b.Run(fmt.Sprintf("候选%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if order := warppool.AffinityOrderForTest(affinityKey, views); len(order) != n {
					b.Fatalf("排序结果长度 = %d, 期望 %d", len(order), n)
				}
			}
		})
	}
}

// BenchmarkNormalCandidates Normal 候选过滤纯函数基准(轮询选路前置)
func BenchmarkNormalCandidates(b *testing.B) {
	for _, n := range routeBenchInsts {
		views := warppool.MakeViewsForTest(n)
		b.Run(fmt.Sprintf("实例%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := warppool.NormalCandidatesForTest(views); len(got) != n {
					b.Fatalf("过滤结果长度 = %d, 期望 %d", len(got), n)
				}
			}
		})
	}
}

// BenchmarkFnv64a 亲和散列纯函数基准(入参预拼接:key+ID 拼接在库内同包直调下零分配,
// 经导出钩子间接调用会使拼接结果按逃逸处理,失真)
func BenchmarkFnv64a(b *testing.B) {
	s := affinityKey + string(warppool.ID("7"))
	b.ReportAllocs()
	for b.Loop() {
		_ = warppool.Fnv64aForTest(s)
	}
}

// Test选路纯函数_零分配 散列与排序比较路径零分配;
// 候选过滤与亲和排序整体恒 routeCopyAlloc 次(结果切片拷贝),不随候选数增长,
// 即比较期 key+ID 拼接与散列均零分配(拼接同包直调不逃逸,钩子间接调用会失真故单独预拼接)
func Test选路纯函数_零分配(t *testing.T) {
	s := affinityKey + string(warppool.ID("7"))
	if got := testing.AllocsPerRun(allocRuns, func() {
		_ = warppool.Fnv64aForTest(s)
	}); got != 0 {
		t.Fatalf("fnv64a 分配 %v 次/调用, 期望 0", got)
	}
	for _, n := range routeBenchInsts {
		views := warppool.MakeViewsForTest(n)
		if got := testing.AllocsPerRun(allocRuns, func() {
			_ = warppool.NormalCandidatesForTest(views)
		}); got != routeCopyAlloc {
			t.Fatalf("候选过滤(%d 实例)分配 %v 次/调用, 期望恒 %d", n, got, routeCopyAlloc)
		}
		if got := testing.AllocsPerRun(allocRuns, func() {
			_ = warppool.AffinityOrderForTest(affinityKey, views)
		}); got != routeCopyAlloc {
			t.Fatalf("亲和排序(%d 候选)分配 %v 次/调用, 期望恒 %d(仅重排拷贝, 比较路径零分配)", n, got, routeCopyAlloc)
		}
	}
}

// TestDial_选路分布均匀性 多实例(10/50/贴近端口区间顶端)下轮询精确均匀、亲和卡方达标
func TestDial_选路分布均匀性(t *testing.T) {
	for _, tc := range []struct {
		name  string
		insts int
		base  int
	}{
		{"实例10", 10, dialBasePort},
		{"实例50", 50, dialBasePort},
		{"实例100贴近端口上限", nearMaxInsts, maxTCPPort - nearMaxInsts + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newRouteEnv(t, tc.insts, tc.base)
			t.Run("轮询精确均匀", func(t *testing.T) {
				counts := map[warppool.ID]int{}
				for range tc.insts * roundRobinTurn {
					ic := env.dialOK(env.p.DialContext)
					counts[ic.Instance().ID]++
					_ = ic.Close()
				}
				for _, info := range env.p.Instances() {
					if got := counts[info.ID]; got != roundRobinTurn {
						t.Fatalf("轮询实例 %s 落点 %d 次, 期望精确 %d", info.ID, got, roundRobinTurn)
					}
				}
			})
			t.Run("亲和卡方达标", func(t *testing.T) {
				total := tc.insts * keysPerInst
				counts := map[warppool.ID]int{}
				for i := range total {
					key := fmt.Sprintf("tenant-%d", i)
					ic := env.dialOK(func(ctx context.Context, network, addr string) (net.Conn, error) {
						return env.p.DialContextWithKey(ctx, key, network, addr)
					})
					counts[ic.Instance().ID]++
					_ = ic.Close()
				}
				expect := float64(total) / float64(tc.insts)
				chi := 0.0
				// 全实例计入(0 落点实例同样偏离期望),检出轻度饿死
				for _, info := range env.p.Instances() {
					d := float64(counts[info.ID]) - expect
					chi += d * d / expect
				}
				df := tc.insts - 1
				threshold := float64(df) + chiSigma*math.Sqrt(2*float64(df))
				if chi > threshold {
					t.Fatalf("亲和落点卡方 %.1f 超阈值 %.1f(自由度 %d), 落点分布 %v", chi, threshold, df, counts)
				}
			})
		})
	}
}
