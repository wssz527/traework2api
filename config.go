// config.go 解析 plugin.register / plugin.reconfigure 下发的插件配置。
//
// 插件不能 flag.Parse()，配置只能来自宿主的 plugin.reconfigure。
package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

// pluginConfigRequest 宿主下发配置的已知形状。不同 CPA 版本字段位置略有差异，
// 这里做最大兼容：config 可能是 map，也可能嵌在 config_yaml / values 里。
//
// 注意：config_yaml 是**字符串**（宿主把本插件的 config 段渲染成 YAML 文本，
// 见 pluginhost.rpcLifecycleRequest ConfigYAML []byte），不是 map。之前声明成
// map 导致整体 json.Unmarshal 失败、配置从未生效（所有值都是默认值）。
type pluginConfigRequest struct {
	Config     map[string]any `json:"config"`
	ConfigYAML []byte         `json:"config_yaml"`
	Values     map[string]any `json:"values"`
	Enabled    *bool          `json:"enabled"`
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
	if v, ok := fields["version_track"]; ok {
		if b, ok := parseBool(v); ok {
			setVersionTracking(b)
		}
	}
	if v, ok := fields["version_track_interval"]; ok {
		if sv, ok := v.(string); ok {
			if d, err := time.ParseDuration(strings.TrimSpace(sv)); err == nil {
				setVersionInterval(d)
			}
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

// firstConfigMap 从候选字段中挑第一个非空配置 map；config_yaml 是 YAML 文本，
// 用简易行解析转成 map（本插件配置只有一层：标量 + 行内列表）。
func firstConfigMap(req pluginConfigRequest) map[string]any {
	for _, m := range []map[string]any{req.Config, req.Values} {
		if len(m) > 0 {
			return m
		}
	}
	return parseConfigYAMLText(req.ConfigYAML)
}

// parseConfigYAMLText 把单层的 `key: value` YAML 文本解析成 map。
// 支持：bool、数字、行内整型列表（[3] / [3, 21]）、字符串（去引号）。
// 不认识或嵌套的行直接跳过——配置解析绝不能导致注册失败。
func parseConfigYAMLText(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]any)
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" || v == "" {
			continue
		}
		out[k] = parseConfigYAMLValue(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseConfigYAMLValue(v string) any {
	if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
		inner := strings.TrimSpace(v[1 : len(v)-1])
		if inner == "" {
			return []any{}
		}
		var list []any
		for _, item := range strings.Split(inner, ",") {
			item = strings.TrimSpace(item)
			if n, err := strconv.Atoi(item); err == nil {
				list = append(list, n)
			}
		}
		return list
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return strings.Trim(v, "\"'")
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
