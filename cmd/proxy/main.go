// cmd/proxy: warp-pool 真实代理测试入口。
// 将池拨号包装为本地 HTTP 代理(CONNECT 隧道 + 明文绝对形式转发),
// 手动验收:curl -x http://127.0.0.1:8080 https://api4.ipify.org
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mzzsfy/warp-pool"
)

// 默认配置
const (
	defaultProxyListen = "127.0.0.1:8080"
	defaultMin         = 1
	defaultMax         = 4
	defaultStateDir    = "./warp-state"
	statusInterval     = 30 * time.Second
	shutdownGrace      = 10 * time.Second
	connectEstablished = "HTTP/1.1 200 Connection Established\r\n\r\n"
)

// hopByHopHeaders 逐跳头,转发前剥离
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

func main() {
	var (
		listen    = flag.String("listen", defaultProxyListen, "代理监听地址")
		minN      = flag.Int("min", defaultMin, "实例数下限")
		maxN      = flag.Int("max", defaultMax, "实例数上限")
		stateDir  = flag.String("state", defaultStateDir, "实例 state 目录")
		transport = flag.String("transport", string(warppool.TransportSOCKS5), "拨号传输方式(socks5/http)")
		endpoints = flag.String("endpoints", "", "endpoint 列表(逗号分隔 host:port,空为自动选优),实例轮询绑定,重播轮换")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := warppool.Options{
		Min:           *minN,
		Max:           *maxN,
		StateDir:      *stateDir,
		DialTransport: warppool.DialTransport(*transport),
		Logger:        log.Default(),
		DedupeKeyer:   warppool.DedupeByBoth{},
	}
	if *endpoints != "" {
		opts.Endpoints = strings.Split(*endpoints, ",")
	}
	pool, err := warppool.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = pool.Close() }()

	handler := proxyHandler{
		transport: &http.Transport{DialContext: pool.DialContext},
		pool:      pool,
	}
	srv := &http.Server{Addr: *listen, Handler: handler}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("代理监听 %s, 实例 %d-%d, 传输 %s, state %s", *listen, *minN, *maxN, *transport, *stateDir)
	if len(opts.Endpoints) > 0 {
		log.Printf("endpoint 轮询: %v", opts.Endpoints)
	}

	go statusLoop(ctx, pool)
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("代理服务退出: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("收到退出信号, 排空在途请求")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// proxyHandler HTTP 代理:CONNECT 走隧道,其余请求经池转发
type proxyHandler struct {
	transport *http.Transport
	pool      *warppool.Pool
}

// ServeHTTP 按请求形态分发
func (h proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		h.tunnelConnect(w, r)
		return
	}
	h.forward(w, r)
}

// tunnelConnect 劫持客户端连接,经池拨号目标后双向透传
func (h proxyHandler) tunnelConnect(w http.ResponseWriter, r *http.Request) {
	hij, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "代理服务不支持连接劫持", http.StatusInternalServerError)
		return
	}
	target, err := h.pool.DialContext(r.Context(), "tcp", r.Host)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = target.Close() }()
	if ic, ok := target.(warppool.InstanceConn); ok {
		info := ic.Instance()
		log.Printf("CONNECT %s -> 实例 %s 出口 %s", r.Host, info.ID, info.Egress.V4)
	}
	client, _, err := hij.Hijack()
	if err != nil {
		return
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Write([]byte(connectEstablished)); err != nil {
		return
	}
	relay(client, target)
}

// forward 明文 HTTP 绝对形式请求经池转发,应答原样回写
func (h proxyHandler) forward(w http.ResponseWriter, r *http.Request) {
	if r.URL.Host == "" {
		http.Error(w, "非代理形式请求", http.StatusBadRequest)
		return
	}
	r.RequestURI = ""
	for _, name := range hopByHopHeaders {
		r.Header.Del(name)
	}
	resp, err := h.transport.RoundTrip(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for name, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// relay 双向透传,任一方向结束即返回(由 defer 关闭双方收尾)
func relay(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
}

// statusLoop 周期输出池状态与实例出口,退出信号即止
func statusLoop(ctx context.Context, pool *warppool.Pool) {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		st := pool.Stats()
		log.Printf("池状态: normal=%d probing=%d draining=%d disabled=%d total=%d replays=%d dials=%d fails=%d",
			st.Normal, st.Probing, st.Draining, st.Disabled, st.Total, st.Replays, st.DialTotal, st.DialFails)
		for _, info := range pool.Instances() {
			log.Printf("  实例 %s %s 出口 %s", info.ID, info.Status, info.Egress.V4)
		}
	}
}
