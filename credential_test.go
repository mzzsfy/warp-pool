package warppool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// stubCredentialSource 测试用凭据源:按序弹出,空槽返回 nil;并发安全(管理 goroutine 读,测试 goroutine 改)
type stubCredentialSource struct {
	mu    sync.Mutex
	creds []*Credential
	err   error
	calls int
}

// Acquire 实现 CredentialSource
func (s *stubCredentialSource) Acquire() (*Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	if len(s.creds) == 0 {
		return nil, nil
	}
	c := s.creds[0]
	s.creds = s.creds[1:]
	return c, nil
}

// recover 解除故障并放入凭据
func (s *stubCredentialSource) recover(c *Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = nil
	s.creds = append(s.creds, c)
}

// callCount 已调用次数
func (s *stubCredentialSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// validCred 构造字段齐全的测试凭据
func validCred(id string) *Credential {
	return &Credential{DeviceID: id, Token: "tok-" + id, PrivateKey: "key-" + id}
}

// Given 凭据源返回有效凭据 When 写入实例 state Then 文件为 amz 可复用格式(三元组齐全)
func TestWriteCredentialState_ValidCredential_WritesReusableState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inst-0.json")
	if err := writeCredentialState(path, validCred("dev-1")); err != nil {
		t.Fatalf("写入凭据 state 失败: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 state 失败: %v", err)
	}
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("state 非合法 JSON: %v", err)
	}
	if state["device_id"] != "dev-1" {
		t.Fatalf("device_id = %v", state["device_id"])
	}
	cert, _ := state["certificate"].(map[string]any)
	if cert == nil || cert["private_key"] != "key-dev-1" {
		t.Fatalf("certificate.private_key 缺失或错误: %v", cert)
	}
	if state["token"] != "tok-dev-1" {
		t.Fatalf("token = %v", state["token"])
	}
}

// Given 三元组缺字段的凭据 When 写入 Then 返回错误且不落盘
func TestWriteCredentialState_IncompleteCredential_ReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inst-0.json")
	for _, mutate := range []func(*Credential){
		func(c *Credential) { c.DeviceID = "" },
		func(c *Credential) { c.Token = "" },
		func(c *Credential) { c.PrivateKey = "" },
	} {
		c := validCred("dev-x")
		mutate(c)
		if err := writeCredentialState(path, c); err == nil {
			t.Fatal("缺字段凭据应返回错误")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("失败时不应落盘")
		}
	}
}
