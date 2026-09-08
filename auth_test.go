package main

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// 现有 trae-*.json 的真实结构（敏感字段已脱敏为占位）。
const existingFormat = `{
  "account": {"uid": "1000000000000000", "enterpriseId": "test-ent-id", "nickname": "测试用户"},
  "auth": {
    "accessToken": "at-placeholder", "refreshToken": "rt-placeholder", "expiresAt": 1786805537,
    "domain": "trae.cn", "apiHost": "https://api.trae.com.cn",
    "machineId": "abcdef0123456789abcdef0123456789", "deviceId": "0123456789abcdef0123456789abcdef",
    "checkinDeviceId": "1111222233334444", "checkinDeviceBrand": "Mac16,10", "checkinDeviceType": "mac"
  }
}`

func TestParseExistingFormat(t *testing.T) {
	a, err := parseStored([]byte(existingFormat))
	if err != nil {
		t.Fatalf("parse existing format: %v", err)
	}
	if a.UID != "1000000000000000" || a.EnterpriseID != "test-ent-id" || a.Nickname != "测试用户" {
		t.Errorf("account: %+v", a)
	}
	if a.AccessToken != "at-placeholder" || a.RefreshToken != "rt-placeholder" || a.ExpiresAt != 1786805537 {
		t.Errorf("tokens: %+v", a)
	}
	if a.Domain != "trae.cn" || a.ApiHost != "https://api.trae.com.cn" {
		t.Errorf("hosts: %+v", a)
	}
	if len(a.MachineID) != 32 || len(a.DeviceID) != 32 {
		t.Errorf("ids: machine=%q device=%q", a.MachineID, a.DeviceID)
	}
	if a.CheckinDeviceID != "1111222233334444" || a.CheckinDeviceBrand != "Mac16,10" || a.CheckinDeviceType != "mac" {
		t.Errorf("checkin device: id=%q brand=%q type=%q", a.CheckinDeviceID, a.CheckinDeviceBrand, a.CheckinDeviceType)
	}
}

func TestParseFlat(t *testing.T) {
	raw := []byte(`{"accessToken":"at","refreshToken":"rt","expiresAt":1753600000,"uid":"u2","nickname":"n2","machineId":"m1","deviceId":"d1","checkinDeviceId":"1111222233334444","checkinDeviceBrand":"90SB001GCD","checkinDeviceType":"windows"}`)
	a, err := parseStored(raw)
	if err != nil || a.UID != "u2" || a.AccessToken != "at" || a.MachineID != "m1" || a.DeviceID != "d1" ||
		a.CheckinDeviceID != "1111222233334444" || a.CheckinDeviceBrand != "90SB001GCD" || a.CheckinDeviceType != "windows" {
		t.Fatalf("flat: %+v %v", a, err)
	}
}

func TestParseMissingToken(t *testing.T) {
	if _, err := parseStored([]byte(`{"uid":"u3"}`)); err == nil {
		t.Fatal("want error for missing accessToken")
	}
	if _, err := parseStored([]byte(``)); err == nil {
		t.Fatal("want error for empty storage")
	}
	if _, err := parseStored([]byte(`not json`)); err == nil {
		t.Fatal("want error for invalid json")
	}
}

// TestMarshalNestedRoundtrip 插件落盘格式（嵌套 + 顶层 type）必须能被自己解析回来，
// 且 SOLO 自定义字段不丢。
func TestMarshalNestedRoundtrip(t *testing.T) {
	a, err := parseStored([]byte(existingFormat))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := a.marshalNested()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var top map[string]any
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// 迁移报告 §3.2：顶层必须有 "type":"trae"，否则宿主不会把凭证路由给本插件。
	if top["type"] != providerName {
		t.Errorf("top-level type=%v want %q", top["type"], providerName)
	}
	b, err := parseStored(raw)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if b.MachineID != a.MachineID || b.DeviceID != a.DeviceID || b.ApiHost != a.ApiHost {
		t.Errorf("SOLO fields lost: %+v", b)
	}
	if b.CheckinDeviceID != "1111222233334444" || b.CheckinDeviceBrand != "Mac16,10" || b.CheckinDeviceType != "mac" {
		t.Errorf("checkin device fields lost: %+v", b)
	}
	if b.UID != a.UID || b.Nickname != a.Nickname || b.EnterpriseID != a.EnterpriseID {
		t.Errorf("account fields lost: %+v", b)
	}
}

func TestNeedsRefresh(t *testing.T) {
	a := &traeAuth{ExpiresAt: 0}
	if !a.NeedsRefresh(0) {
		t.Error("zero expiry should need refresh")
	}
	a.ExpiresAt = 9999999999
	if a.NeedsRefresh(0) {
		t.Error("far future should not need refresh")
	}
	a.ExpiresAt = 1
	if !a.NeedsRefresh(24 * 3600 * 1e9) {
		t.Error("past expiry should need refresh")
	}
}

// TestConcurrentTokenReadsAndWrites 在 -race 下验证 token 字段读写并发安全：
// 并发写（模拟 RefreshToken）与读（JWT/RefreshTokenValue/NeedsRefresh）无竞争。
func TestConcurrentTokenReadsAndWrites(t *testing.T) {
	a := &traeAuth{
		AccessToken:  "at",
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(-time.Minute).Unix(),
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { // 写路径
			defer wg.Done()
			a.Lock()
			a.AccessToken = "new-at"
			a.RefreshToken = "new-rt"
			a.ExpiresAt = time.Now().Add(time.Hour).Unix()
			a.Unlock()
		}()
		wg.Add(1)
		go func() { // 读路径
			defer wg.Done()
			_ = a.JWT()
			_ = a.RefreshTokenValue()
			_ = a.NeedsRefresh(time.Hour)
		}()
	}
	wg.Add(1)
	go func() { // 并发落盘序列化（读锁）
		defer wg.Done()
		_, _ = a.marshalNested()
	}()
	wg.Wait()
	if a.RefreshTokenValue() == "" {
		t.Error("refresh token should not be empty")
	}
}

func TestNeedsRefreshLocked(t *testing.T) {
	a := &traeAuth{ExpiresAt: time.Now().Add(-time.Minute).Unix()}
	a.RLock()
	need := a.needsRefreshLocked(time.Hour)
	a.RUnlock()
	if !need {
		t.Error("expired token should need refresh under lock")
	}
}

// TestMaskUID 账号列表接口必须脱敏 UID（不能把完整 UID/token 吐给面板）。
func TestMaskUID(t *testing.T) {
	cases := []struct{ in, want string }{
		{"1000000000000000", "1000********0000"},
		{"12345", "1***5"},
		{"ab", "**"},
		{"", ""},
	}
	for _, c := range cases {
		if got := maskUID(c.in); got != c.want {
			t.Errorf("maskUID(%q)=%q want %q", c.in, got, c.want)
		}
	}
	// 脱敏后不得含完整 UID
	uid := "1000000000000000"
	if strings.Contains(maskUID(uid), uid) {
		t.Error("masked UID must not contain the full UID")
	}
}

// TestAuthFileNameFor 文件名必须带 trae- 前缀（宿主 watcher 与 parse 都靠它）。
func TestAuthFileNameFor(t *testing.T) {
	if got := authFileNameFor(&traeAuth{UID: "12345"}); got != "trae-12345.json" {
		t.Errorf("got %q", got)
	}
	if got := authFileNameFor(nil); got != authFileName {
		t.Errorf("fallback got %q", got)
	}
}
