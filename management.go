// management.go 管理路由 + 面板资源。
//
// 宿主把路由挂在固定前缀下：/v0/management/plugins/trae/*
// 页面资源：/v0/resource/plugins/trae/*
package main

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type managementRoute struct {
	Method      string `json:"method"`
	Path        string `json:"path"`
	Description string `json:"description,omitempty"`
}

type resourceRoute struct {
	Path        string `json:"path"`
	Menu        string `json:"menu,omitempty"`
	Description string `json:"description,omitempty"`
}

type managementRegistrationResponse struct {
	Routes    []managementRoute `json:"routes,omitempty"`
	Resources []resourceRoute   `json:"resources,omitempty"`
}

// managementBasePathCache 缓存宿主注入的 BasePath（不硬编码 /v0/management）。
var (
	managementBasePathCache   = "/v0/management"
	managementBasePathCacheMu sync.RWMutex
)

func loadedManagementBasePath() string {
	managementBasePathCacheMu.RLock()
	defer managementBasePathCacheMu.RUnlock()
	return managementBasePathCache
}

func setManagementBasePath(p string) {
	p = strings.TrimRight(strings.TrimSpace(p), "/")
	if p == "" {
		return
	}
	managementBasePathCacheMu.Lock()
	managementBasePathCache = p
	managementBasePathCacheMu.Unlock()
}

func managementRegistration() managementRegistrationResponse {
	base := "/plugins/" + providerName
	return managementRegistrationResponse{
		Routes: []managementRoute{
			{Method: http.MethodGet, Path: base + "/accounts", Description: "List Trae accounts with credits and status (masked UID)."},
			{Method: http.MethodPost, Path: base + "/refresh", Description: "Force refresh credits for all accounts."},
			{Method: http.MethodPost, Path: base + "/checkin", Description: "Check in one account (body auth_index) or all."},
			{Method: http.MethodPost, Path: base + "/checkin/config", Description: "Toggle auto check-in / set checkin_hour."},
			{Method: http.MethodGet, Path: base + "/credits", Description: "Query credits for one (auth_index) or all accounts."},
			{Method: http.MethodPost, Path: base + "/import", Description: "Import a Trae credential JSON into the host auth store."},
			{Method: http.MethodPost, Path: base + "/select", Description: "Select the active account used for chat routing (body auth_index)."},
			{Method: http.MethodPost, Path: base + "/keepalive", Description: "Manually refresh access tokens for all accounts (or one with auth_index)."},
			{Method: http.MethodGet, Path: base + "/status", Description: "Scheduler status: checkin hour, refresh hours, modes."},
			{Method: http.MethodGet, Path: base + "/version", Description: "Upstream client version tracking: current, built-in, cache and sources."},
			{Method: http.MethodPost, Path: base + "/version/refresh", Description: "Force an upstream version re-probe (background; non-blocking)."},
		},
		Resources: []resourceRoute{
			{Path: "/panel", Menu: "Trae", Description: "Trae dashboard: credits, check-in, account select."},
		},
	}
}

func handleManagement(raw []byte) (resp []byte, err error) {
	defer recoverTo(&err, "management.handle")
	var req pluginapi.ManagementRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	path := strings.TrimRight(req.Path, "/")

	// 面板页面资源（/v0/resource/plugins/trae/*）
	resPrefix := "/v0/resource/plugins/" + providerName
	if req.Method == http.MethodGet && strings.HasPrefix(path, resPrefix) {
		sub := strings.TrimPrefix(path, resPrefix)
		return okEnvelope(mgmtHTMLResponse(servePanel(sub)))
	}

	base := loadedManagementBasePath() + "/plugins/" + providerName
	switch {
	case req.Method == http.MethodGet && path == base+"/accounts":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, map[string]any{
			"accounts": buildAccounts(false),
			"active":   getActiveAuthID(),
		}))
	case req.Method == http.MethodPost && path == base+"/refresh":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, map[string]any{
			"accounts": buildAccounts(true),
			"active":   getActiveAuthID(),
		}))
	case req.Method == http.MethodPost && path == base+"/checkin":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleManualCheckin(req)))
	case req.Method == http.MethodPost && path == base+"/checkin/config":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleCheckinConfig(req)))
	case req.Method == http.MethodGet && path == base+"/credits":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleCreditsQuery(req)))
	case req.Method == http.MethodPost && path == base+"/import":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleImportAuth(req)))
	case req.Method == http.MethodPost && path == base+"/select":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleSelectAuth(req)))
	case req.Method == http.MethodPost && path == base+"/keepalive":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleKeepalive(req)))
	case req.Method == http.MethodGet && path == base+"/status":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, handleStatus()))
	case req.Method == http.MethodGet && path == base+"/version":
		return okEnvelope(mgmtJSONResponse(http.StatusOK, versionStatus()))
	case req.Method == http.MethodPost && path == base+"/version/refresh":
		go refreshUpstreamVersion()
		return okEnvelope(mgmtJSONResponse(http.StatusOK, map[string]any{"started": true}))
	}
	return okEnvelope(mgmtJSONResponse(http.StatusNotFound, map[string]any{"error": "not found: " + path}))
}

// handleManualCheckin POST /checkin：单号（body.auth_index）或全量。
func handleManualCheckin(req pluginapi.ManagementRequest) map[string]any {
	var body struct {
		AuthIndex string `json:"auth_index"`
	}
	_ = json.Unmarshal(req.Body, &body)
	authIndex := strings.TrimSpace(body.AuthIndex)

	files, err := hostAuthList()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	var targets []pluginapi.HostAuthFileEntry
	for _, f := range files {
		if authIndex == "" || f.AuthIndex == authIndex {
			targets = append(targets, f)
		}
	}
	if len(targets) == 0 {
		return map[string]any{"error": "no matching account"}
	}

	results := make([]map[string]any, len(targets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i := range targets {
		wg.Add(1)
		go func(idx int, f pluginapi.HostAuthFileEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = map[string]any{"auth_index": f.AuthIndex, "error": "internal error"}
					pluginLogf("panic in manual checkin %s: %v", f.AuthIndex, r)
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[idx] = checkinOneAccount(f)
		}(i, targets[i])
	}
	wg.Wait()

	successN, failN, alreadyN := 0, 0, 0
	for _, out := range results {
		if out == nil {
			continue
		}
		if out["error"] != nil {
			failN++
			continue
		}
		if reason, _ := out["reason"].(string); reason == "already" {
			alreadyN++
			continue
		}
		if out["success"] == true {
			successN++
		} else {
			failN++
		}
	}
	return map[string]any{
		"results": results,
		"summary": map[string]any{
			"total":     len(targets),
			"success":   successN,
			"already":   alreadyN,
			"fail":      failN,
			"attempted": len(targets),
		},
	}
}

// handleCheckinConfig POST /checkin/config：开关自动签到 / 配置签到时刻。
func handleCheckinConfig(req pluginapi.ManagementRequest) map[string]any {
	var body struct {
		Enabled *bool `json:"enabled"`
		Hour    *int  `json:"checkin_hour"`
	}
	_ = json.Unmarshal(req.Body, &body)
	if body.Enabled != nil {
		setCheckinAuto(*body.Enabled)
	}
	if body.Hour != nil {
		setCheckinHour(*body.Hour)
	}
	return map[string]any{
		"checkin_auto": loadedCheckinAuto(),
		"checkin_hour": loadedCheckinHour(),
		"persistent":   false, // CPA 无插件配置写回回调；重启后 config_yaml 的值生效
	}
}

// handleImportAuth POST /import：导入凭证 JSON 到宿主 auth store。
func handleImportAuth(req pluginapi.ManagementRequest) map[string]any {
	var body struct {
		JSON json.RawMessage `json:"json"`
		Raw  string          `json:"raw"`
	}
	_ = json.Unmarshal(req.Body, &body)
	raw := []byte(strings.TrimSpace(body.Raw))
	if len(body.JSON) > 0 {
		raw = body.JSON
	}
	if len(raw) == 0 {
		return map[string]any{"success": false, "error": "missing json/raw credential payload"}
	}
	sa, err := parseStored(raw)
	if err != nil {
		return map[string]any{"success": false, "error": redactSecrets(err.Error())}
	}
	// 缺签到设备 ID 的新凭证直接补上，保证导入后就能签到。
	if EnsurePerAccountCheckinDevice(sa) {
		// 忽略失败：凭证仍可导入，签到时候会重试生成。
	}
	if err := persistAuth(sa, false); err != nil {
		return map[string]any{"success": false, "error": err.Error()}
	}
	return map[string]any{
		"success":  true,
		"file":     authFileNameFor(sa),
		"uid":      maskUID(sa.UID),
		"nickname": sa.Nickname,
	}
}

// handleSelectAuth POST /select：切换面板选中（即 chat 路由优先）的账号。
func handleSelectAuth(req pluginapi.ManagementRequest) map[string]any {
	var body struct {
		AuthIndex string `json:"auth_index"`
	}
	_ = json.Unmarshal(req.Body, &body)
	authIndex := strings.TrimSpace(body.AuthIndex)
	if authIndex == "" {
		return map[string]any{"error": "auth_index is required", "active_auth": getActiveAuthID()}
	}
	files, err := hostAuthList()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	for _, f := range files {
		if f.AuthIndex != authIndex {
			continue
		}
		if f.Disabled {
			return map[string]any{"error": "账号已禁用，无法选中", "auth_index": authIndex}
		}
		sa, err := hostAuthGet(f.AuthIndex)
		if err != nil {
			return map[string]any{"error": redactSecrets(err.Error()), "auth_index": authIndex}
		}
		setActiveAuthID(f.ID)
		return map[string]any{
			"ok":          true,
			"active_auth": f.ID,
			"nickname":    sa.Nickname,
			"uid":         maskUID(sa.UID),
		}
	}
	return map[string]any{"error": "account not found", "auth_index": authIndex}
}

// handleCreditsQuery GET /credits：单号（?auth_index=）或全量积分。
func handleCreditsQuery(req pluginapi.ManagementRequest) map[string]any {
	authIndex := ""
	if vals := req.Query["auth_index"]; len(vals) > 0 {
		authIndex = strings.TrimSpace(vals[0])
	}
	files, err := hostAuthList()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	type acctCredits struct {
		AuthIndex    string         `json:"auth_index"`
		Nickname     string         `json:"nickname,omitempty"`
		UID          string         `json:"uid,omitempty"`
		Credits      *int64         `json:"credits,omitempty"`
		CreditsTotal int64          `json:"credits_total,omitempty"`
		CreditsUsed  int64          `json:"credits_used,omitempty"`
		PackCount    int            `json:"pack_count,omitempty"`
		CreditsEx    *creditsDetail `json:"credits_detail,omitempty"`
		Error        string         `json:"error,omitempty"`
	}
	var out []acctCredits
	for _, f := range files {
		if authIndex != "" && f.AuthIndex != authIndex {
			continue
		}
		sa, err := hostAuthGet(f.AuthIndex)
		if err != nil {
			out = append(out, acctCredits{AuthIndex: f.AuthIndex, Error: redactSecrets(err.Error())})
			continue
		}
		ac := acctCredits{AuthIndex: f.AuthIndex, Nickname: sa.Nickname, UID: maskUID(sa.UID)}
		if u, qerr := currentClient().UserEntUsage(sa); qerr != nil {
			ac.Error = redactSecrets(qerr.Error())
		} else {
			remain := u.Remain
			ac.Credits = &remain
			ac.CreditsTotal = u.Limit
			ac.CreditsUsed = u.Used
			ac.PackCount = u.PackCount
			now := nowUTC()
			ac.CreditsEx = newCreditsDetail(u, now)
			storeCreditsUsage(f.ID, u, now)
		}
		out = append(out, ac)
	}
	if authIndex != "" && len(out) == 0 {
		return map[string]any{"error": "account not found"}
	}
	return map[string]any{"accounts": out}
}

// handleKeepalive POST /keepalive：手动刷新 token（单号或全量）。
func handleKeepalive(req pluginapi.ManagementRequest) map[string]any {
	var body struct {
		AuthIndex string `json:"auth_index"`
	}
	_ = json.Unmarshal(req.Body, &body)
	authIndex := strings.TrimSpace(body.AuthIndex)
	files, err := hostAuthList()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	var out []map[string]any
	for _, f := range files {
		if authIndex != "" && f.AuthIndex != authIndex {
			continue
		}
		out = append(out, refreshOneAccount(f))
	}
	if authIndex != "" && len(out) == 0 {
		return map[string]any{"error": "account not found"}
	}
	return map[string]any{"results": out}
}

// handleStatus GET /status：调度器与生命周期配置。
func handleStatus() map[string]any {
	now := nowUTC()
	return map[string]any{
		"provider":       providerName,
		"version":        version,
		"checkin_auto":   loadedCheckinAuto(),
		"checkin_hour":   loadedCheckinHour(),
		"refresh_hours":  loadedRefreshHours(),
		"scheduler_mode": loadedSchedulerMode(),
		"lifecycle_auto": lifecycleEnabled(),
		"active_auth":    getActiveAuthID(),
		"server_time":    now.Format("2006-01-02 15:04:05"),
		"server_date":    now.Format("2006-01-02"),
	}
}

func mgmtJSONResponse(status int, v any) pluginapi.ManagementResponse {
	body, _ := json.Marshal(v)
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=utf-8")
	return pluginapi.ManagementResponse{StatusCode: status, Headers: h, Body: body}
}

func mgmtHTMLResponse(body []byte) pluginapi.ManagementResponse {
	h := http.Header{}
	h.Set("Content-Type", "text/html; charset=utf-8")
	return pluginapi.ManagementResponse{StatusCode: http.StatusOK, Headers: h, Body: body}
}

// parseHour 解析 0-23 的小时参数（配置与脚本共用）。
func parseHour(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, t >= 0 && t <= 23
	case float64:
		h := int(t)
		return h, h >= 0 && h <= 23
	case string:
		h, err := strconv.Atoi(strings.TrimSpace(t))
		if err != nil {
			return 0, false
		}
		return h, h >= 0 && h <= 23
	}
	return 0, false
}

// parseHours 解析小时数组（refresh_hours 配置）。
func parseHours(v any) []int {
	switch t := v.(type) {
	case []int:
		return t
	case []any:
		out := make([]int, 0, len(t))
		for _, item := range t {
			if h, ok := parseHour(item); ok {
				out = append(out, h)
			}
		}
		return out
	case string:
		out := make([]int, 0)
		for _, part := range strings.Split(t, ",") {
			if h, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && h >= 0 && h <= 23 {
				out = append(out, h)
			}
		}
		return out
	}
	return nil
}

// parseBool 解析布尔配置（接受 bool 与常见字符串）。
func parseBool(v any) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1", "yes", "on":
			return true, true
		case "false", "0", "no", "off":
			return false, true
		}
	}
	return false, false
}
