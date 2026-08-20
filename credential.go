package warppool

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Credential amz 设备凭据:三元组齐全时实例复用该身份注册(Enroll),代替匿名注册
type Credential struct {
	DeviceID   string // 设备 ID(amz state device_id)
	Token      string // 设备令牌(amz state token)
	PrivateKey string // secp256r1 ECDSA 私钥,base64(x509 DER);wgcf 的 x25519 格式不兼容
}

// incomplete 三元组任一为空(调用方保证 c 非 nil)
func (c *Credential) incomplete() bool {
	return c.DeviceID == "" || c.Token == "" || c.PrivateKey == ""
}

// CredentialSource 身份供给钩子:实例需要新身份(创建/重播)时调用一次;
// 返回 nil 即本次走匿名注册,返回错误则本轮启动失败按既有退避重试。
// 何时给号、何时给空的策略完全由实现方决定。
// 契约:多实例管理 goroutine 会并发调用,实现须并发安全;
// 须快速返回或自带超时,阻塞会卡住实例管理协程(该实例 Stop/Disable 均不可达);
// 取号即消耗——写盘后重试/销毁路径会删 state,凭据不回收。
type CredentialSource interface {
	Acquire() (*Credential, error)
}

// amzStateFile 写出的 amz state 最小结构(仅复用判定所需字段,Enroll 会回写完整态)
type amzStateFile struct {
	Version     string             `json:"version"`
	DeviceID    string             `json:"device_id,omitempty"`
	Token       string             `json:"token,omitempty"`
	Certificate amzStateCredential `json:"certificate,omitempty"`
}

// amzStateCredential amz state 的 certificate 节
type amzStateCredential struct {
	PrivateKey string `json:"private_key,omitempty"`
}

// amzStateCurrentVersion 对齐 amz v0.2.3 storage.CurrentVersion
const amzStateCurrentVersion = "1"

// ErrIncompleteCredential 凭据三元组不齐全
var ErrIncompleteCredential = errors.New("凭据三元组(DeviceID/Token/PrivateKey)不齐全")

// writeCredentialState 将凭据写为 amz 可复用 state 文件;字段不齐返回 ErrIncompleteCredential 且不落盘
func writeCredentialState(path string, c *Credential) error {
	if c.incomplete() {
		return ErrIncompleteCredential
	}
	data, err := json.MarshalIndent(amzStateFile{
		Version:     amzStateCurrentVersion,
		DeviceID:    c.DeviceID,
		Token:       c.Token,
		Certificate: amzStateCredential{PrivateKey: c.PrivateKey},
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化凭据 state 失败: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("创建 state 目录失败: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("写入凭据 state 失败: %w", err)
	}
	return nil
}
