// config.go 解析 plugin.register / plugin.reconfigure 下发的插件配置。
//
// 插件不能 flag.Parse()，配置只能来自宿主的 plugin.reconfigure。
package main

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// pluginConfigRequest 宿主下发配置的已知形状。不同 CPA 版本字段位置略有差异，
// 这里做最大兼容：config 可能是 map，也可能嵌在 config_yaml / values 里。
type pluginConfigRequest struct {
	Config    map[string]any `json:"config"`
	ConfigMap map[string]any `json:"config_yaml"`
	Values    map[string]any `json:"values"`
	Enabled   *bool          `json:"enabled"`
}

// configure 应用插件配置并启动调度器。任何解析失败都不影响插件注册。
func configure(raw []byte) {
	if len(raw) == 0 {
		ensureScheduler()
		return
	}
	var req pluginConfigRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		ensureScheduler()
		return
	}
	fields := firstConfigMap(req)

	if v, ok := fields["checkin_auto"]; ok {
		if b, ok := parseBool(v); ok {
			setCheckinAuto(b)
		}
	}
	if v, ok := fields["checkin_hour"]; ok {
		if h, ok := parseHour(v); ok {
			setCheckinHour(h)
		}
	}
	if v, ok := fields["refresh_hours"]; ok {
		if hours := parseHours(v); len(hours) > 0 {
			setRefreshHours(hours)
		}
	}
	if v, ok := fields["refresh_skew"]; ok {
		if s, ok := v.(string); ok {
			if d, err := time.ParseDuration(strings.TrimSpace(s)); err == nil && d > 0 {
				setRefreshSkew(d)
			}
		}
	}
	if v, ok := fields["lifecycle_auto"]; ok {
		if b, ok := parseBool(v); ok {
			setLifecycleAuto(b)
		}
	}
	if v, ok := fields["scheduler_mode"]; ok {
		if s, ok := v.(string); ok {
			mode := strings.ToLower(strings.TrimSpace(s))
			if mode == schedulerModeCredits || mode == schedulerModeOff {
				restore := setSchedulerMode(mode)
				_ = restore // 保持到进程结束
			}
		}
	}
	ensureScheduler()
}

// firstConfigMap 从候选字段中挑第一个非空配置 map。
func firstConfigMap(req pluginConfigRequest) map[string]any {
	for _, m := range []map[string]any{req.Config, req.ConfigMap, req.Values} {
		if len(m) > 0 {
			return m
		}
	}
	return nil
}

var (
	refreshSkew   = defaultRefreshSkew
	refreshSkewMu sync.RWMutex
)

func setRefreshSkew(d time.Duration) {
	refreshSkewMu.Lock()
	refreshSkew = d
	refreshSkewMu.Unlock()
}

func loadedRefreshSkew() time.Duration {
	refreshSkewMu.RLock()
	defer refreshSkewMu.RUnlock()
	if refreshSkew <= 0 {
		return defaultRefreshSkew
	}
	return refreshSkew
}
