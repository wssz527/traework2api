// auth.go 解析 TRAE SOLO 凭证（嵌套形 auth+account / 扁平形），
// 提供并发安全的 token 读写与设备标识。
//
// 从 traework2api/internal/auth/auth.go 迁移。差异：
//   - 类型名 Auth → traeAuth（避免与宿主 pluginapi.AuthData 混淆）
//   - FilePath / SaveAtomic 删除：落盘改由宿主 auth store 管（host.auth.save）
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// traeAuth 归一化后的账号凭证。
//
// 并发模型：mu 保护可变字段（AccessToken/RefreshToken/ExpiresAt），
// 写路径（refreshLocked）持写锁整段执行 ExchangeToken；读路径（JWT /
// RefreshTokenValue / NeedsRefresh）持读锁读快照，杜绝与写并发时的数据竞争。
// 其余字段加载后不变，直接读。
type traeAuth struct {
	mu sync.RWMutex

	AccessToken        string // Cloud-IDE-JWT 头用
	RefreshToken       string // 每次 ExchangeToken 轮换
	ExpiresAt          int64  // Unix 秒（accessToken 过期时刻）
	Domain             string // "trae.cn"
	ApiHost            string // "https://api.trae.com.cn"（ExchangeToken host）
	MachineID          string // 模型通道 x-machine-id
	DeviceID           string // 模型通道 x-device-id
	CheckinDeviceID    string // 签到 x-device-id
	CheckinDeviceBrand string // 签到 x-device-brand
	CheckinDeviceType  string // 签到 x-device-type
	UID                string
	EnterpriseID       string
	Nickname           string

	// Disabled 与宿主凭证文件顶层 disabled 同步：冷却/禁用写盘时为 true，
	// 否则每次重写凭证都会把宿主的 disabled 抹回 false（冷却跨重启失效）。
	Disabled bool
}

// Lock 供改写 Auth 字段期间加写锁。
func (a *traeAuth) Lock() { a.mu.Lock() }

// Unlock 释放 a.Lock 获取的写锁。
func (a *traeAuth) Unlock() { a.mu.Unlock() }

// RLock 供读路径持有读锁（与写锁互斥，读读不互斥）。
func (a *traeAuth) RLock() { a.mu.RLock() }

// RUnlock 释放 a.RLock 获取的读锁。
func (a *traeAuth) RUnlock() { a.mu.RUnlock() }

// JWT 返回当前 accessToken 的读锁快照，防与 RefreshToken 写并发竞态。
func (a *traeAuth) JWT() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.AccessToken
}

// RefreshTokenValue 返回当前 refreshToken 的读锁快照。
func (a *traeAuth) RefreshTokenValue() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.RefreshToken
}

// NeedsRefresh 报告 token 是否将在 within 内过期（或已过期/无 expiry）。
func (a *traeAuth) NeedsRefresh(within time.Duration) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.needsRefreshLocked(within)
}

// needsRefreshLocked 是 NeedsRefresh 的持锁内部版本。
func (a *traeAuth) needsRefreshLocked(within time.Duration) bool {
	if a.ExpiresAt <= 0 {
		return true
	}
	return time.Now().Add(within).Unix() >= a.ExpiresAt
}

// parseNested 兼容现有 trae-*.json 嵌套形：{"account":{...},"auth":{...}}
func parseNested(raw []byte) (*traeAuth, error) {
	var n struct {
		Auth struct {
			AccessToken        string `json:"accessToken"`
			RefreshToken       string `json:"refreshToken"`
			ExpiresAt          int64  `json:"expiresAt"`
			Domain             string `json:"domain"`
			ApiHost            string `json:"apiHost"`
			MachineID          string `json:"machineId"`
			DeviceID           string `json:"deviceId"`
			CheckinDeviceID    string `json:"checkinDeviceId"`
			CheckinDeviceBrand string `json:"checkinDeviceBrand"`
			CheckinDeviceType  string `json:"checkinDeviceType"`
		} `json:"auth"`
		Account struct {
			UID          string `json:"uid"`
			EnterpriseID string `json:"enterpriseId"`
			Nickname     string `json:"nickname"`
		} `json:"account"`
	}
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	return &traeAuth{
		AccessToken:        n.Auth.AccessToken,
		RefreshToken:       n.Auth.RefreshToken,
		ExpiresAt:          n.Auth.ExpiresAt,
		Domain:             n.Auth.Domain,
		ApiHost:            n.Auth.ApiHost,
		MachineID:          n.Auth.MachineID,
		DeviceID:           n.Auth.DeviceID,
		CheckinDeviceID:    n.Auth.CheckinDeviceID,
		CheckinDeviceBrand: n.Auth.CheckinDeviceBrand,
		CheckinDeviceType:  n.Auth.CheckinDeviceType,
		UID:                n.Account.UID,
		EnterpriseID:       n.Account.EnterpriseID,
		Nickname:           n.Account.Nickname,
		Disabled:           parseDisabledFromAuthJSON(raw),
	}, nil
}

// parseFlat 兼容扁平形（CPA 面板手建等）。
func parseFlat(raw []byte) (*traeAuth, error) {
	var f struct {
		AccessToken        string `json:"accessToken"`
		RefreshToken       string `json:"refreshToken"`
		ExpiresAt          int64  `json:"expiresAt"`
		Domain             string `json:"domain"`
		ApiHost            string `json:"apiHost"`
		MachineID          string `json:"machineId"`
		DeviceID           string `json:"deviceId"`
		CheckinDeviceID    string `json:"checkinDeviceId"`
		CheckinDeviceBrand string `json:"checkinDeviceBrand"`
		CheckinDeviceType  string `json:"checkinDeviceType"`
		UID                string `json:"uid"`
		EnterpriseID       string `json:"enterpriseId"`
		Nickname           string `json:"nickname"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	return &traeAuth{
		AccessToken:        f.AccessToken,
		RefreshToken:       f.RefreshToken,
		ExpiresAt:          f.ExpiresAt,
		Domain:             f.Domain,
		ApiHost:            f.ApiHost,
		MachineID:          f.MachineID,
		DeviceID:           f.DeviceID,
		CheckinDeviceID:    f.CheckinDeviceID,
		CheckinDeviceBrand: f.CheckinDeviceBrand,
		CheckinDeviceType:  f.CheckinDeviceType,
		UID:                f.UID,
		EnterpriseID:       f.EnterpriseID,
		Nickname:           f.Nickname,
		Disabled:           parseDisabledFromAuthJSON(raw),
	}, nil
}

// parseStored 兼容两种磁盘形态：嵌套形（登录脚本产出）与扁平形（手建）。
func parseStored(raw []byte) (*traeAuth, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty auth storage")
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	var (
		a   *traeAuth
		err error
	)
	if _, nested := probe["auth"]; nested {
		a, err = parseNested(raw)
	} else {
		a, err = parseFlat(raw)
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(a.AccessToken) == "" {
		return nil, fmt.Errorf("parse_error: missing accessToken")
	}
	return a, nil
}

// marshalNested 以嵌套形序列化（与登录脚本产出的格式一致）。
func (a *traeAuth) marshalNested() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	doc := map[string]any{
		"auth": map[string]any{
			"accessToken":        a.AccessToken,
			"refreshToken":       a.RefreshToken,
			"expiresAt":          a.ExpiresAt,
			"domain":             a.Domain,
			"apiHost":            a.ApiHost,
			"machineId":          a.MachineID,
			"deviceId":           a.DeviceID,
			"checkinDeviceId":    a.CheckinDeviceID,
			"checkinDeviceBrand": a.CheckinDeviceBrand,
			"checkinDeviceType":  a.CheckinDeviceType,
		},
		"account": map[string]any{
			"uid":          a.UID,
			"enterpriseId": a.EnterpriseID,
			"nickname":     a.Nickname,
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return authFileJSON(raw, a.Disabled)
}
