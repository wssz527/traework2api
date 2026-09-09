package main

import (
	"encoding/json"
	"testing"
)

// 回归：宿主下发的 config_yaml 是 YAML 文本（[]byte），不是 map。
// 之前声明成 map[string]any 导致整体 Unmarshal 失败、配置从未生效。
func TestConfigureParsesConfigYAMLText(t *testing.T) {
	restore := setSchedulerMode(schedulerModeOff)
	defer restore()
	origHour := loadedCheckinHour()
	defer setCheckinHour(origHour)
	origHours := loadedRefreshHours()
	defer setRefreshHours(origHours)

	// 宿主按 []byte 下发，JSON 里是 base64——测试必须同构模拟。
	raw, _ := json.Marshal(struct {
		ConfigYAML []byte `json:"config_yaml"`
	}{ConfigYAML: []byte("enabled: true\ncheckin_hour: 21\nrefresh_hours: [3, 15]\nscheduler_mode: credits\n")})
	configure(raw)

	if got := loadedSchedulerMode(); got != schedulerModeCredits {
		t.Errorf("scheduler_mode=%q, want credits", got)
	}
	if got := loadedCheckinHour(); got != 21 {
		t.Errorf("checkin_hour=%d, want 21", got)
	}
	if got := loadedRefreshHours(); len(got) != 2 || got[0] != 3 || got[1] != 15 {
		t.Errorf("refresh_hours=%v, want [3 15]", got)
	}
}

// 损坏的 config_yaml 不得影响注册（保持默认值，不 panic）。
func TestConfigureMalformedConfigYAMLKeepsDefaults(t *testing.T) {
	restore := setSchedulerMode(schedulerModeOff)
	defer restore()
	raw, _ := json.Marshal(struct {
		ConfigYAML []byte `json:"config_yaml"`
	}{ConfigYAML: []byte("::: not yaml at all\n\n# comment only\n")})
	configure(raw)
	if got := loadedSchedulerMode(); got != schedulerModeOff {
		t.Errorf("scheduler_mode=%q, want off (default)", got)
	}
}
