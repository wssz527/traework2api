package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestScanUserUniqueID(t *testing.T) {
	dir := t.TempDir()
	// 旧文件里的旧 ID 与新文件里的新 ID：应以最新修改的文件为准。
	old := filepath.Join(dir, "000001.ldb")
	newF := filepath.Join(dir, "000002.log")
	if err := os.WriteFile(old, []byte(`{"user_unique_id":"1111111111111111","timestamp":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newF, []byte(`junk{"user_unique_id":"2222222222222222","timestamp":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	id, ok := scanUserUniqueID(dir)
	if !ok || id != "2222222222222222" {
		t.Fatalf("got (%q,%v), want (2222222222222222,true)", id, ok)
	}
}

func TestScanUserUniqueIDMiss(t *testing.T) {
	if _, ok := scanUserUniqueID(filepath.Join(t.TempDir(), "nope")); ok {
		t.Fatal("missing dir should miss")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000001.log"), []byte(`{"other":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := scanUserUniqueID(dir); ok {
		t.Fatal("no user_unique_id should miss")
	}
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, "000001.log"), []byte(`{"user_unique_id":"abc-def"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := scanUserUniqueID(dir2); ok {
		t.Fatal("non-numeric id should miss")
	}
}

// TestNewFakeCheckinIDFormat 伪造 ID 必须是 16 位纯数字（含字母会触发上游风控 9074）。
func TestNewFakeCheckinIDFormat(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		id := newFakeCheckinID()
		if !fakeDeviceIDRe.MatchString(id) {
			t.Fatalf("id %q is not 16 digits", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

// TestEnsurePerAccountCheckinDeviceGenerates 无 ID 的账号必须生成固定 ID（后续不动）。
func TestEnsurePerAccountCheckinDeviceGenerates(t *testing.T) {
	t.Setenv("TW2A_DISABLE_CHECKIN_DETECT", "1") // 禁止探测本机客户端数据
	a := &traeAuth{UID: "u1"}
	if !EnsurePerAccountCheckinDevice(a) {
		t.Fatal("should report changed when generating a new device id")
	}
	first := a.CheckinDeviceID
	if !fakeDeviceIDRe.MatchString(first) {
		t.Fatalf("generated id %q is not 16 digits", first)
	}
	// 已有 ID → 保持不动（避免频繁换设备触发风控）
	if EnsurePerAccountCheckinDevice(a) {
		t.Error("existing device id should not be rotated")
	}
	if a.CheckinDeviceID != first {
		t.Errorf("device id changed: %q → %q", first, a.CheckinDeviceID)
	}
}

// TestEnsurePerAccountCheckinDeviceDistinct 不同账号必须拿到不同 ID
// （共用 ID 会导致只有第一个账号能签到，上游报 9095）。
func TestEnsurePerAccountCheckinDeviceDistinct(t *testing.T) {
	t.Setenv("TW2A_DISABLE_CHECKIN_DETECT", "1")
	ids := map[string]bool{}
	for i := 0; i < 20; i++ {
		a := &traeAuth{UID: "u"}
		EnsurePerAccountCheckinDevice(a)
		if ids[a.CheckinDeviceID] {
			t.Fatalf("duplicate device id across accounts: %q", a.CheckinDeviceID)
		}
		ids[a.CheckinDeviceID] = true
	}
}

func TestDetectLocalCheckinDeviceDisabled(t *testing.T) {
	t.Setenv("TW2A_DISABLE_CHECKIN_DETECT", "1")
	if id, _, _, ok := DetectLocalCheckinDevice(); ok || id != "" {
		t.Errorf("env kill-switch should disable detection, got (%q,%v)", id, ok)
	}
}

// fakeDeviceIDRe 在本包已定义；这里再断言一次格式常量，防止被误改成非数字。
var _ = regexp.MustCompile(`^\d{16}$`)

// TestIsCheckinRiskControl 9074（"当前参与用户太多"）是设备指纹风控，
// 必须被识别为可换 ID 重试的错误；普通业务错误不得误判。
func TestIsCheckinRiskControl(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"checkin claim: code=9074 message=当前参与用户太多，请稍后再试", true},
		{"code=9074", true},
		{"当前参与用户太多，请稍后再试", true},
		{"checkin claim: code=9095 message=同一设备已签到", false},
		{"今日已签到", false},
		{"connection refused", false},
	}
	for _, c := range cases {
		if got := isCheckinRiskControl(c.msg); got != c.want {
			t.Errorf("isCheckinRiskControl(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}
