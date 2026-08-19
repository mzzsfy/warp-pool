// Package testutil 提供离线单测基建
package testutil

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
)

// SOCKS5 协议常量
const (
	socks5Version      = 5
	socks5CmdConnect   = 1
	socks5AtypIPv4     = 1
	socks5AtypDomain   = 3
	socks5AtypIPv6     = 4
	socks5NoAuthMethod = 0
)

// socks5ReplySuccess 无认证 CONNECT 成功回复(绑定地址置零)
var socks5ReplySuccess = []byte{socks5Version, 0, 0, socks5AtypIPv4, 0, 0, 0, 0, 0, 0}

// SOCKS5Server 最小 SOCKS5 代理测试服务器:仅支持无认证 CONNECT,转发到真实目标并记录目标地址
type SOCKS5Server struct {
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	targets []string
}

// NewSOCKS5Server 在 127.0.0.1 随机端口启动 SOCKS5 测试服务器
func NewSOCKS5Server() (*SOCKS5Server, error) {
	return NewSOCKS5ServerAt("tcp", "127.0.0.1:0")
}

// NewSOCKS5ServerAt 在指定地址启动 SOCKS5 测试服务器
func NewSOCKS5ServerAt(network, addr string) (*SOCKS5Server, error) {
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	s := &SOCKS5Server{
		listener: ln,
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr 服务器监听地址
func (s *SOCKS5Server) Addr() string { return s.listener.Addr().String() }

// Targets 已被请求的目标地址列表(按请求顺序)
func (s *SOCKS5Server) Targets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

// Close 关闭服务器并等待连接处理结束,可重复调用
func (s *SOCKS5Server) Close() error {
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

func (s *SOCKS5Server) acceptLoop() {
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

func (s *SOCKS5Server) track(conn net.Conn) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
}

func (s *SOCKS5Server) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *SOCKS5Server) handle(conn net.Conn) {
	target, err := s.handshake(conn)
	if err != nil {
		return
	}
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer upstream.Close()
	if _, err := conn.Write(socks5ReplySuccess); err != nil {
		return
	}
	s.record(target)
	relay(conn, upstream)
}

// handshake 完成无认证协商与 CONNECT 请求解析,返回目标地址
func (s *SOCKS5Server) handshake(conn net.Conn) (string, error) {
	header, err := readFull(conn, 2)
	if err != nil {
		return "", err
	}
	if header[0] != socks5Version {
		return "", errors.New("非 SOCKS5 版本")
	}
	if _, err := readFull(conn, int(header[1])); err != nil {
		return "", err
	}
	if _, err := conn.Write([]byte{socks5Version, socks5NoAuthMethod}); err != nil {
		return "", err
	}
	req, err := readFull(conn, 4)
	if err != nil {
		return "", err
	}
	if req[0] != socks5Version || req[1] != socks5CmdConnect {
		return "", errors.New("仅支持 CONNECT")
	}
	host, err := readHost(conn, req[3])
	if err != nil {
		return "", err
	}
	portBytes, err := readFull(conn, 2)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes)))), nil
}

// readHost 按 ATYP 读取目标主机
func readHost(conn net.Conn, atyp byte) (string, error) {
	switch atyp {
	case socks5AtypIPv4:
		b, err := readFull(conn, 4)
		if err != nil {
			return "", err
		}
		return net.IP(b).String(), nil
	case socks5AtypDomain:
		lenByte, err := readFull(conn, 1)
		if err != nil {
			return "", err
		}
		b, err := readFull(conn, int(lenByte[0]))
		if err != nil {
			return "", err
		}
		return string(b), nil
	case socks5AtypIPv6:
		b, err := readFull(conn, 16)
		if err != nil {
			return "", err
		}
		return net.IP(b).String(), nil
	}
	return "", errors.New("不支持的 ATYP")
}

func (s *SOCKS5Server) record(target string) {
	s.mu.Lock()
	s.targets = append(s.targets, target)
	s.mu.Unlock()
}

// relay 双向转发直至任一侧关闭
func relay(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); done <- struct{}{} }()
	go func() { io.Copy(b, a); done <- struct{}{} }()
	<-done
	a.Close()
	b.Close()
}

func readFull(conn net.Conn, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
