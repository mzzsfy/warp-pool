// 假 HTTP CONNECT 代理服务:与 SOCKS5Server 同构,支持注入应答状态码。
package testutil

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"sync"
)

// HTTPConnectServer 最小 HTTP CONNECT 代理测试服务器:解析 CONNECT 请求,
// 转发到真实目标并记录目标地址;应答状态码可注入用于失败路径测试;
// PreambleAfterReply 开启后在应答同包紧随预发隧道数据,用于预发保留路径测试
type HTTPConnectServer struct {
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup

	// PreambleAfterReply 应答后立即预发的隧道数据(空即关闭)
	PreambleAfterReply []byte

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	targets []string
	status  int
}

// NewHTTPConnectServer 在 127.0.0.1 随机端口启动 HTTP CONNECT 测试服务器
func NewHTTPConnectServer() (*HTTPConnectServer, error) {
	return NewHTTPConnectServerAt("tcp", "127.0.0.1:0")
}

// NewHTTPConnectServerAt 在指定地址启动 HTTP CONNECT 测试服务器
func NewHTTPConnectServerAt(network, addr string) (*HTTPConnectServer, error) {
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	s := &HTTPConnectServer{
		listener: ln,
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
		status:   http.StatusOK,
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr 服务器监听地址
func (s *HTTPConnectServer) Addr() string { return s.listener.Addr().String() }

// SetStatus 注入应答状态码(默认 200)
func (s *HTTPConnectServer) SetStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
}

// Targets 已被请求的目标地址列表(按请求顺序)
func (s *HTTPConnectServer) Targets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

// Close 关闭服务器并等待连接处理结束,可重复调用
func (s *HTTPConnectServer) Close() error {
	select {
	case <-s.done:
		return nil
	default:
	}
	close(s.done)
	err := s.listener.Close()
	s.mu.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *HTTPConnectServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.track(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			defer conn.Close()
			s.handle(conn)
		}()
	}
}

func (s *HTTPConnectServer) track(conn net.Conn) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
}

func (s *HTTPConnectServer) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *HTTPConnectServer) handle(conn net.Conn) {
	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil || req.Method != http.MethodConnect {
		return
	}
	target := req.URL.Host
	if s.currentStatus() != http.StatusOK {
		writeConnectResponse(conn, s.currentStatus())
		return
	}
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		writeConnectResponse(conn, http.StatusBadGateway)
		return
	}
	defer upstream.Close()
	if _, err := writeConnectResponse(conn, http.StatusOK); err != nil {
		return
	}
	s.record(target)
	if len(s.PreambleAfterReply) > 0 {
		if _, err := conn.Write(s.PreambleAfterReply); err != nil {
			return
		}
	}
	relay(conn, upstream)
}

// currentStatus 当前注入的应答状态码
func (s *HTTPConnectServer) currentStatus() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// writeConnectResponse 写应答状态行与空首部
func writeConnectResponse(conn net.Conn, code int) (int, error) {
	return fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\n\r\n", code, http.StatusText(code))
}

func (s *HTTPConnectServer) record(target string) {
	s.mu.Lock()
	s.targets = append(s.targets, target)
	s.mu.Unlock()
}
