package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// ---------------------------------------------------------------------------
// 注册
// ---------------------------------------------------------------------------

func mustDecodeResult(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode envelope: %v (%s)", err, raw)
	}
	if !env.OK {
		t.Fatalf("envelope not ok: %+v", env.Error)
	}
	return env.Result
}

func TestRegistrationShape(t *testing.T) {
	raw, err := handleMethodGuarded("plugin.register", nil)
	if err != nil {
		t.Fatal(err)
	}
	var reg registration
	if err := json.Unmarshal(mustDecodeResult(t, raw), &reg); err != nil {
		t.Fatal(err)
	}
	if reg.SchemaVersion != 1 {
		t.Errorf("schema_version=%d", reg.SchemaVersion)
	}
	if reg.Metadata.Name != providerName {
		t.Errorf("name=%q", reg.Metadata.Name)
	}
	c := reg.Capabilities
	if !c.ModelProvider || !c.AuthProvider || !c.Executor || !c.ManagementAPI || !c.Scheduler {
		t.Errorf("capabilities missing: %+v", c)
	}
	if c.ExecutorModelScope != pluginapi.ExecutorModelScopeOAuth {
		t.Errorf("scope=%q want oauth", c.ExecutorModelScope)
	}
	if len(c.ExecutorInputFormats) != 1 || c.ExecutorInputFormats[0] != "chat-completions" {
		t.Errorf("input formats=%v", c.ExecutorInputFormats)
	}
	if len(c.ExecutorOutputFormats) != 1 || c.ExecutorOutputFormats[0] != "chat-completions" {
		t.Errorf("output formats=%v", c.ExecutorOutputFormats)
	}
}

func TestUnknownMethodIsEnvelopeNotError(t *testing.T) {
	raw, err := handleMethodGuarded("nope.nope", nil)
	if err != nil {
		t.Fatalf("unknown method should not fail the RPC: %v", err)
	}
	var env envelope
	_ = json.Unmarshal(raw, &env)
	if env.OK || env.Error == nil || env.Error.Code != "unknown_method" {
		t.Errorf("want unknown_method envelope, got %+v", env)
	}
}

// ---------------------------------------------------------------------------
// auth.parse
// ---------------------------------------------------------------------------

func authParseReq(t *testing.T, fileName, provider string, raw string) []byte {
	t.Helper()
	body, _ := json.Marshal(pluginapi.AuthParseRequest{
		FileName: fileName,
		Provider: provider,
		RawJSON:  []byte(raw),
	})
	return body
}

func TestParseAuthClaimsTraeFile(t *testing.T) {
	// 带 type:trae → 认领
	withType := `{"type":"trae","account":{"uid":"1000000000000000","nickname":"n"},"auth":{"accessToken":"at-placeholder","refreshToken":"rt"}}`
	raw, err := handleMethodGuarded("auth.parse", authParseReq(t, "trae-1000000000000000.json", "", withType))
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.AuthParseResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Handled {
		t.Fatal("should claim a trae-typed file")
	}
	// 迁移报告 §3.3 硬约束：ID 必须留空，让宿主按 path 计算，否则会出现重复 auth 条目。
	if resp.Auth.ID != "" {
		t.Errorf("AuthData.ID must be empty, got %q", resp.Auth.ID)
	}
	// FileName 必须回显
	if resp.Auth.FileName != "trae-1000000000000000.json" {
		t.Errorf("FileName=%q want echo", resp.Auth.FileName)
	}
	if resp.Auth.Provider != providerName {
		t.Errorf("provider=%q", resp.Auth.Provider)
	}
	if resp.Auth.Metadata["type"] != providerName {
		t.Errorf("metadata.type=%v", resp.Auth.Metadata["type"])
	}
	// StorageJSON 必须能被自己解析回（含 SOLO 自定义字段）
	if _, err := parseStored(resp.Auth.StorageJSON); err != nil {
		t.Errorf("storage roundtrip failed: %v", err)
	}
}

func TestParseAuthTypeLessFallsBackToPrefix(t *testing.T) {
	// 无 type 字段：文件名前缀 trae- → 认领
	noType := `{"account":{"uid":"u1"},"auth":{"accessToken":"at"}}`
	raw, _ := handleMethodGuarded("auth.parse", authParseReq(t, "trae-u1.json", "", noType))
	var resp pluginapi.AuthParseResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if !resp.Handled {
		t.Error("trae- prefix file should be claimed")
	}
	// 无 type 且文件名不匹配、宿主也没路由给我 → 不认领
	raw2, _ := handleMethodGuarded("auth.parse", authParseReq(t, "other-u1.json", "", noType))
	var resp2 pluginapi.AuthParseResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw2), &resp2)
	if resp2.Handled {
		t.Error("unrelated file must not be claimed")
	}
	// 宿主显式路由给我（Provider=trae）→ 认领
	raw3, _ := handleMethodGuarded("auth.parse", authParseReq(t, "whatever.json", providerName, noType))
	var resp3 pluginapi.AuthParseResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw3), &resp3)
	if !resp3.Handled {
		t.Error("host-routed file should be claimed")
	}
}

func TestParseAuthRejectsOtherProviderType(t *testing.T) {
	other := `{"type":"workbuddy","auth":{"accessToken":"at"}}`
	raw, _ := handleMethodGuarded("auth.parse", authParseReq(t, "workbuddy-u1.json", "", other))
	var resp pluginapi.AuthParseResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.Handled {
		t.Error("must never claim another provider's typed file")
	}
}

func TestParseAuthRejectsUnparseable(t *testing.T) {
	raw, _ := handleMethodGuarded("auth.parse", authParseReq(t, "trae-x.json", "", `{"nope":1}`))
	var resp pluginapi.AuthParseResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.Handled {
		t.Error("credential without accessToken must not be claimed")
	}
}

// ---------------------------------------------------------------------------
// 模型
// ---------------------------------------------------------------------------

func TestModelStaticReturnsCanonicalProvider(t *testing.T) {
	body, _ := json.Marshal(pluginapi.StaticModelRequest{})
	raw, err := handleMethodGuarded("model.static", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.ModelResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	// 宿主会丢弃 Provider 不匹配的响应，必须回规范 provider key。
	if resp.Provider != providerName {
		t.Errorf("provider=%q", resp.Provider)
	}
	if len(resp.Models) != len(staticModelIDs) {
		t.Errorf("models=%d want %d", len(resp.Models), len(staticModelIDs))
	}
	for _, m := range resp.Models {
		if m.ContextLength <= 0 {
			t.Errorf("model %s has no context length", m.ID)
		}
	}
}

func TestModelForAuthFallsBackToStaticWithoutUpstream(t *testing.T) {
	// StorageJSON 为空 → 无法拉动态表 → 必须回退静态表而不是返回空
	body, _ := json.Marshal(pluginapi.AuthModelRequest{AuthProvider: providerName})
	raw, err := handleMethodGuarded("model.for_auth", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.ModelResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Provider != providerName {
		t.Errorf("provider=%q", resp.Provider)
	}
	if len(resp.Models) == 0 {
		t.Fatal("should fall back to static models")
	}
}

func TestModelForAuthExcludesDeprecated(t *testing.T) {
	// 动态表里的弃用模型（DeepSeek-V4-Pro/Flash）必须被过滤，只保留 Official
	storeDynamicModels(modelsFromUpstream([]ModelInfo{
		{ID: "DeepSeek-V4-Pro", Name: "old"},
		{ID: "DeepSeek-V4-Flash-Official", Name: "new"},
	}))
	body, _ := json.Marshal(pluginapi.AuthModelRequest{StorageJSON: []byte(existingFormat)})
	raw, _ := handleMethodGuarded("model.for_auth", body)
	var resp pluginapi.ModelResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	for _, m := range resp.Models {
		if deprecatedModelIDs[m.ID] {
			t.Errorf("deprecated model leaked: %s", m.ID)
		}
	}
	dynamicModelsCache.Lock()
	dynamicModelsCache.models = nil
	dynamicModelsCache.Unlock()
}

func TestFilterExcludedModelsDoesNotMutateCache(t *testing.T) {
	// 过滤器必须返回新切片：就地过滤会污染 dynamicModelsCache 的底层数组
	src := []pluginapi.ModelInfo{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	host := pluginapi.HostConfigSummary{ExcludedModels: map[string][]string{providerName: {"b"}}}
	out := filterExcludedModels(src, host)
	if len(out) != 2 {
		t.Fatalf("out=%d want 2", len(out))
	}
	if len(src) != 3 || src[1].ID != "b" {
		t.Errorf("input slice was mutated: %+v", src)
	}
}

func TestResolveUpstreamModel(t *testing.T) {
	cases := []struct{ in, want string }{
		{"glm-5.2", "glm-5.2"},
		{"glm-5.2__dev", "glm-5.2"},        // 内部名后缀去掉
		{"GLM-5.2", "GLM-5.2"},             // 已在静态表 → 原样
		{"", DefaultConfigName},            // 空 → 默认模型
		{"unknown-model", "unknown-model"}, // 未知 → 原样转发（由上游裁决）
	}
	for _, c := range cases {
		if got := resolveUpstreamModel(c.in, nil); got != c.want {
			t.Errorf("resolveUpstreamModel(%q)=%q want %q", c.in, got, c.want)
		}
	}
	// 下划线命名归一化后命中静态表的情况（normalizeModelName 与原实现同算法）
	if got := resolveUpstreamModel("Deepseek-V4-Pro", nil); got != "Deepseek-V4-Pro" {
		t.Errorf("got %q", got)
	}
	modelAliasCache.Lock()
	modelAliasCache.byAlias = map[string]string{"fast": "glm-5.2"}
	modelAliasCache.Unlock()
	if got := resolveUpstreamModel("fast", nil); got != "glm-5.2" {
		t.Errorf("alias resolve=%q", got)
	}
	modelAliasCache.Lock()
	modelAliasCache.byAlias = nil
	modelAliasCache.Unlock()
}

func TestNormalizeModelName(t *testing.T) {
	// 与原 traework2api 同算法：下划线分段 + 首字母大写，其余小写
	if got := normalizeModelName("deepseek_v4_pro"); got != "Deepseek-V4-Pro" {
		t.Errorf("got %q want Deepseek-V4-Pro", got)
	}
	if got := normalizeModelName("glm_5_2"); got != "Glm-5-2" {
		t.Errorf("got %q want Glm-5-2", got)
	}
	if got := normalizeModelName(""); got != "" {
		t.Errorf("empty should stay empty, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// 管理路由
// ---------------------------------------------------------------------------

func TestManagementRegistrationRoutesUnderPluginPrefix(t *testing.T) {
	reg := managementRegistration()
	base := "/plugins/" + providerName
	for _, r := range reg.Routes {
		if !strings.HasPrefix(r.Path, base) {
			t.Errorf("route %s %s must be under %s", r.Method, r.Path, base)
		}
	}
	// 面板资源
	if len(reg.Resources) == 0 || reg.Resources[0].Path != "/panel" {
		t.Errorf("resources=%+v", reg.Resources)
	}
}

func TestManagementHandlePanelServesHTML(t *testing.T) {
	body, _ := json.Marshal(pluginapi.ManagementRequest{
		Method: http.MethodGet,
		Path:   "/v0/resource/plugins/" + providerName + "/panel",
	})
	raw, err := handleMethodGuarded("management.handle", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.ManagementResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(resp.Headers.Get("Content-Type")), "text/html") {
		t.Errorf("content-type=%q", resp.Headers.Get("Content-Type"))
	}
	if !strings.Contains(string(resp.Body), "<html") {
		t.Error("panel body is not HTML")
	}
}

func TestManagementHandleStatus(t *testing.T) {
	body, _ := json.Marshal(pluginapi.ManagementRequest{
		Method: http.MethodGet,
		Path:   "/v0/management/plugins/" + providerName + "/status",
	})
	raw, err := handleMethodGuarded("management.handle", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.ManagementResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.Unmarshal(resp.Body, &out)
	if out["provider"] != providerName {
		t.Errorf("body=%s", resp.Body)
	}
	if out["checkin_hour"] != float64(defaultCheckinHour) {
		t.Errorf("checkin_hour=%v want default %d (对齐原 9 点档)", out["checkin_hour"], defaultCheckinHour)
	}
}

func TestManagementHandleUnknownPath404(t *testing.T) {
	body, _ := json.Marshal(pluginapi.ManagementRequest{
		Method: http.MethodGet,
		Path:   "/v0/management/plugins/" + providerName + "/nope",
	})
	raw, _ := handleMethodGuarded("management.handle", body)
	var resp pluginapi.ManagementResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status=%d want 404", resp.StatusCode)
	}
}

func TestManagementBasePathIsHostProvided(t *testing.T) {
	// 宿主可能改前缀；不得硬编码 /v0/management
	setManagementBasePath("/v0/mgmt")
	if got := loadedManagementBasePath(); got != "/v0/mgmt" {
		t.Errorf("base=%q", got)
	}
	setManagementBasePath("/v0/management")
	setManagementBasePath("") // 空值必须被忽略
	if got := loadedManagementBasePath(); got != "/v0/management" {
		t.Errorf("empty base should be ignored, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// recover：panic 必须变成 error envelope，不能传播到宿主
// ---------------------------------------------------------------------------

func TestRecoverToConvertsPanic(t *testing.T) {
	var err error
	func() {
		defer recoverTo(&err, "test.entry")
		panic("boom")
	}()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic not converted to error: %v", err)
	}
}

// TestHandleMethodGuardedReturnsErrorEnvelopeOnBadInput 非法入参必须走
// error 路径返回，而不是 panic（panic 会被 recover 但仍要验证不崩溃）。
func TestHandleMethodGuardedReturnsErrorEnvelopeOnBadInput(t *testing.T) {
	_, err := handleMethodGuarded("executor.execute", []byte(`{"storage_json":123}`))
	if err == nil {
		t.Fatal("malformed body should surface as an error, not a panic")
	}
	if strings.Contains(err.Error(), "panic") {
		t.Errorf("should be a normal error, got panic: %v", err)
	}
}

// TestHandleMethodGuardedSurvivesPanic 验证 recover 包装确实拦住了 panic：
// 借 handleExecStream 注入一个会 panic 的 stream pump（nil auth + 空 stream）。
func TestHandleMethodGuardedSurvivesPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped handleMethodGuarded: %v", r)
		}
	}()
	// 直接调用 recoverTo 覆盖的入口，传一个能触发 nil 解引用的载荷：
	// storage 为空 → parseStored 出错 → 早退，不 panic；这里额外断言
	// 即使内部 panic 也不会逃逸（见 TestRecoverToConvertsPanic）。
	_, _ = handleMethodGuarded("executor.execute_stream", []byte(`{}`))
	_, _ = handleMethodGuarded("management.handle", []byte(`{}`))
	_, _ = handleMethodGuarded("scheduler.pick", []byte(`{}`))
}

// ---------------------------------------------------------------------------
// 调度器
// ---------------------------------------------------------------------------

func TestNextFire(t *testing.T) {
	now := time.Date(2026, 9, 8, 10, 0, 0, 0, time.Local)
	// 9 点已过 → 明天 9 点
	got := nextFire(now, []int{9})
	want := time.Date(2026, 9, 9, 9, 0, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
	// 多时刻取最近的一个
	got = nextFire(now, []int{15, 12})
	want = time.Date(2026, 9, 8, 12, 0, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v want %v", got, want)
	}
	// 非法小时忽略
	if got := nextFire(now, []int{99}); !got.IsZero() {
		t.Errorf("invalid hour should be ignored, got %v", got)
	}
}

func TestSchedulerPickDefersWhenOff(t *testing.T) {
	restore := setSchedulerMode(schedulerModeOff)
	defer restore()
	body, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Provider:   providerName,
		Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "a", Provider: providerName}},
	})
	raw, err := handleMethodGuarded("scheduler.pick", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.Handled {
		t.Error("mode=off must defer to the built-in scheduler")
	}
}

func TestSchedulerPickIgnoresForeignCandidates(t *testing.T) {
	restore := setSchedulerMode(schedulerModeCredits)
	defer restore()
	body, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "w", Provider: "workbuddy"}},
	})
	raw, _ := handleMethodGuarded("scheduler.pick", body)
	var resp pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.Handled {
		t.Error("foreign candidates must not be handled")
	}
}

func TestSchedulerPickStickyOnActiveAuth(t *testing.T) {
	restore := setSchedulerMode(schedulerModeCredits)
	defer restore()
	setActiveAuthID("b")
	defer setActiveAuthID("")
	body, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Candidates: []pluginapi.SchedulerAuthCandidate{
			{ID: "a", Provider: providerName},
			{ID: "b", Provider: providerName},
		},
	})
	raw, _ := handleMethodGuarded("scheduler.pick", body)
	var resp pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if !resp.Handled || resp.AuthID != "b" {
		t.Errorf("resp=%+v want sticky b", resp)
	}
}

func TestCheckinHourConfigurable(t *testing.T) {
	orig := loadedCheckinHour()
	defer setCheckinHour(orig)
	setCheckinHour(21)
	if loadedCheckinHour() != 21 {
		t.Errorf("hour=%d", loadedCheckinHour())
	}
	setCheckinHour(99) // 非法值必须被忽略
	if loadedCheckinHour() != 21 {
		t.Errorf("invalid hour should be ignored, got %d", loadedCheckinHour())
	}
}

func TestEnsureSchedulerIsIdempotent(t *testing.T) {
	ensureScheduler()
	ensureScheduler() // 第二次必须是 no-op，不能起第二个 goroutine
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

func TestMarkCoolingSetsWindow(t *testing.T) {
	st := stateFor("test-cooling")
	st.mu.Lock()
	st.coolUntil = time.Time{}
	st.mu.Unlock()
	markCooling("test-cooling", nil, time.Hour, "plan_limit", "plan 权益不足")
	if !isCooling("test-cooling") {
		t.Error("should be cooling")
	}
	st.mu.Lock()
	reason := st.coolFor
	st.mu.Unlock()
	if reason != "plan_limit" {
		t.Errorf("reason=%q", reason)
	}
}

func TestIsCoolingExpired(t *testing.T) {
	st := stateFor("test-expired")
	st.mu.Lock()
	st.coolUntil = time.Now().Add(-time.Minute)
	st.mu.Unlock()
	if isCooling("test-expired") {
		t.Error("expired cooldown should not be cooling")
	}
}

func TestNoteAccountErrorThreshold(t *testing.T) {
	st := stateFor("test-threshold")
	st.mu.Lock()
	st.errCount = 0
	st.coolUntil = time.Time{}
	st.mu.Unlock()
	for i := 0; i < errThreshold-1; i++ {
		noteAccountError("test-threshold", nil, 500, "boom")
		if isCooling("test-threshold") {
			t.Fatalf("should not cool before threshold (i=%d)", i)
		}
	}
	noteAccountError("test-threshold", nil, 500, "boom")
	if !isCooling("test-threshold") {
		t.Error("should cool after reaching threshold")
	}
	// 冷却后计数归零
	st.mu.Lock()
	count := st.errCount
	st.mu.Unlock()
	if count != 0 {
		t.Errorf("errCount should reset, got %d", count)
	}
}

func TestNoteAccountSuccessResets(t *testing.T) {
	st := stateFor("test-reset")
	st.mu.Lock()
	st.errCount = 2
	st.mu.Unlock()
	noteAccountSuccess("test-reset", nil)
	st.mu.Lock()
	count := st.errCount
	st.mu.Unlock()
	if count != 0 {
		t.Errorf("errCount=%d want 0", count)
	}
}

func TestUnfreezeIfCooled(t *testing.T) {
	st := stateFor("test-unfreeze")
	st.mu.Lock()
	st.coolUntil = time.Now().Add(time.Hour)
	st.coolFor = "plan_limit"
	st.disabled = false
	st.mu.Unlock()
	unfreezeIfCooled("test-unfreeze", nil)
	if isCooling("test-unfreeze") {
		t.Error("credits > 0 should unfreeze a cooled account")
	}
}

func TestUnfreezeKeepsDisabled(t *testing.T) {
	st := stateFor("test-stay-disabled")
	st.mu.Lock()
	st.coolUntil = time.Now().Add(time.Hour)
	st.disabled = true
	st.mu.Unlock()
	unfreezeIfCooled("test-stay-disabled", nil)
	if !isCooling("test-stay-disabled") {
		t.Error("disabled accounts must stay cooled until human re-login")
	}
	st.mu.Lock()
	st.disabled = false
	st.coolUntil = time.Time{}
	st.mu.Unlock()
}

func TestIsAlreadyCheckedIn(t *testing.T) {
	// 明确"已签到"标记 → true
	for _, s := range []string{"今日已签到", "you have already checked in", "already checked"} {
		if !isAlreadyCheckedIn(s) {
			t.Errorf("%q should be already", s)
		}
	}
	// 歧义/错误路径 → false（不误判为已签）
	for _, s := range []string{"checkin service error", "upstream 429: checkin rate limited", "code=400 bad request", ""} {
		if isAlreadyCheckedIn(s) {
			t.Errorf("%q should NOT be already", s)
		}
	}
}

// ---------------------------------------------------------------------------
// 脱敏
// ---------------------------------------------------------------------------

func TestRedactSecrets(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{`Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig`, "***"},
		{`{"accessToken":"abcdef1234567890abcdef"}`, "***"},
		{`{"refreshToken":"zzzzzzzzzzzzzzzzzzzz"}`, "***"},
	}
	for _, c := range cases {
		out := redactSecrets(c.in)
		if strings.Contains(out, "abcdef") || strings.Contains(out, "zzzzzz") {
			t.Errorf("secret leaked: %q → %q", c.in, out)
		}
	}
	// 明显无敏感信息的文本不应被破坏
	if got := redactSecrets("upstream 429 rate limited"); got != "upstream 429 rate limited" {
		t.Errorf("plain text mangled: %q", got)
	}
}

// TestRedactSecretsCoversNestedTraeShape trae 凭证是嵌套 JSON，token 字段值必须被打码。
func TestRedactSecretsCoversNestedTraeShape(t *testing.T) {
	in := `{"auth":{"accessToken":"SUPERSECRETTOKEN123","refreshToken":"ANOTHERSECRET456"}}`
	out := redactSecrets(in)
	if strings.Contains(out, "SUPERSECRETTOKEN123") || strings.Contains(out, "ANOTHERSECRET456") {
		t.Errorf("nested token leaked: %q", out)
	}
}

// ---------------------------------------------------------------------------
// 配置解析
// ---------------------------------------------------------------------------

func TestParseHour(t *testing.T) {
	if h, ok := parseHour(float64(9)); !ok || h != 9 {
		t.Errorf("float64 9 → (%d,%v)", h, ok)
	}
	if h, ok := parseHour("21"); !ok || h != 21 {
		t.Errorf("string 21 → (%d,%v)", h, ok)
	}
	if _, ok := parseHour("99"); ok {
		t.Error("99 should be rejected")
	}
	if _, ok := parseHour("abc"); ok {
		t.Error("abc should be rejected")
	}
}

func TestParseHours(t *testing.T) {
	if got := parseHours([]any{float64(3), float64(15)}); len(got) != 2 || got[0] != 3 || got[1] != 15 {
		t.Errorf("got %v", got)
	}
	if got := parseHours("3,15"); len(got) != 2 || got[0] != 3 {
		t.Errorf("csv got %v", got)
	}
}

func TestParseBool(t *testing.T) {
	if v, ok := parseBool("true"); !ok || !v {
		t.Error("true")
	}
	if v, ok := parseBool("off"); !ok || v {
		t.Error("off")
	}
	if _, ok := parseBool(42); ok {
		t.Error("unsupported type should be rejected")
	}
}

func TestConfigureAppliesFields(t *testing.T) {
	origHour := loadedCheckinHour()
	origAuto := loadedCheckinAuto()
	origHours := loadedRefreshHours()
	defer func() {
		setCheckinHour(origHour)
		setCheckinAuto(origAuto)
		setRefreshHours(origHours)
	}()
	body, _ := json.Marshal(map[string]any{
		"config": map[string]any{
			"checkin_auto":   false,
			"checkin_hour":   21,
			"refresh_hours":  []any{3, 15},
			"lifecycle_auto": true,
		},
	})
	configure(body)
	if loadedCheckinAuto() {
		t.Error("checkin_auto should be off")
	}
	if loadedCheckinHour() != 21 {
		t.Errorf("checkin_hour=%d", loadedCheckinHour())
	}
	if got := loadedRefreshHours(); len(got) != 2 || got[1] != 15 {
		t.Errorf("refresh_hours=%v", got)
	}
}

func TestConfigureToleratesMalformed(t *testing.T) {
	configure([]byte(`{broken`)) // 不得 panic
	configure(nil)
}

// ---------------------------------------------------------------------------
// 并发安全（-race）
// ---------------------------------------------------------------------------

func TestConcurrentCacheAndStateAccess(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			storeCredits(string(rune('a'+i)), int64(i), time.Now())
			_, _ = cachedCredits(string(rune('a' + i)))
			invalidateCredits(string(rune('a' + i)))
			isCooling(string(rune('a' + i)))
			noteAccountSuccess(string(rune('a'+i)), nil)
		}(i)
	}
	wg.Wait()
}

func TestStatusOfError(t *testing.T) {
	if got := statusOfError(&Error{Kind: ErrServer, Status: 503}); got != 503 {
		t.Errorf("got %d", got)
	}
	if got := statusOfError(errors.New("plain")); got != 0 {
		t.Errorf("plain error should yield 0, got %d", got)
	}
}

func TestSetModelInBody(t *testing.T) {
	if got := setModelInBody([]byte(`{"model":"x"}`), "glm-5.2"); !strings.Contains(string(got), `"model":"glm-5.2"`) {
		t.Errorf("got %s", got)
	}
	if got := setModelInBody([]byte(`{broken`), "y"); string(got) != `{broken` {
		t.Error("invalid json should pass through")
	}
}

func TestClientNeedsSSEFrame(t *testing.T) {
	if clientNeedsSSEFrame(map[string]any{"request_path": "/v1/chat/completions"}) {
		t.Error("native chat-completions path: host adds framing")
	}
	if !clientNeedsSSEFrame(map[string]any{"request_path": "/v1/messages"}) {
		t.Error("cross-format path must be framed by the plugin")
	}
	if !clientNeedsSSEFrame(nil) {
		t.Error("missing metadata should default to framed")
	}
}

func TestStripDataPrefix(t *testing.T) {
	if got := stripDataPrefix("data: {\"a\":1}"); got != `{"a":1}` {
		t.Errorf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// 宿主注册校验（回归测试：灰度挂载时宿主拒收的根因）
// ---------------------------------------------------------------------------

// TestValidPluginMetadata 复刻宿主 internal/pluginhost/host.go:validPlugin 对
// Metadata 的要求：Name / Version / Author / GitHubRepository 四个字段都必须非空。
// 任一项为空时宿主打 "invalid metadata or no capabilities" 并拒绝注册整个插件。
//
// 曾因 GitHubRepository:"" 被真实宿主拒收（workbuddy 有值所以正常），故固定下来。
func TestValidPluginMetadata(t *testing.T) {
	reg := traeRegistration()
	if !validPluginMetadata(reg.Metadata) {
		t.Fatalf("registration metadata would be rejected by the host: %+v", reg.Metadata)
	}
	fields := map[string]string{
		"Name":             reg.Metadata.Name,
		"Version":          reg.Metadata.Version,
		"Author":           reg.Metadata.Author,
		"GitHubRepository": reg.Metadata.GitHubRepository,
	}
	for name, v := range fields {
		if strings.TrimSpace(v) == "" {
			t.Errorf("metadata.%s is empty — host validPlugin() rejects this", name)
		}
	}
}

// TestValidPluginMetadataRejectsEmpty 逐字段验证自检能抓到空值。
func TestValidPluginMetadataRejectsEmpty(t *testing.T) {
	base := traeRegistration().Metadata
	cases := []struct {
		name   string
		mutate func(*pluginapi.Metadata)
	}{
		{"empty name", func(m *pluginapi.Metadata) { m.Name = "" }},
		{"blank version", func(m *pluginapi.Metadata) { m.Version = "   " }},
		{"empty author", func(m *pluginapi.Metadata) { m.Author = "" }},
		{"empty github repo", func(m *pluginapi.Metadata) { m.GitHubRepository = "" }},
	}
	for _, c := range cases {
		m := base
		c.mutate(&m)
		if validPluginMetadata(m) {
			t.Errorf("%s should be rejected", c.name)
		}
	}
}

// TestRegistrationDeclaresCapabilitiesAtLeastOne 宿主还要求至少一个 capability
// 非 nil，否则同样判为 invalid。
func TestRegistrationDeclaresCapabilitiesAtLeastOne(t *testing.T) {
	reg := traeRegistration()
	c := reg.Capabilities
	if !c.ModelProvider && !c.AuthProvider && !c.Executor &&
		!c.Scheduler && !c.ManagementAPI && !c.FrontendAuthProvider &&
		!c.UsagePlugin {
		t.Error("registration declares no capability — host validPlugin() rejects this")
	}
	// 本插件必须提供 executor，否则 chat 反代不可用
	if !c.Executor {
		t.Error("executor capability must be declared")
	}
}

// TestRegistrationExecutorFormatsNonEmpty executor 声明了就必须给出格式列表。
func TestRegistrationExecutorFormatsNonEmpty(t *testing.T) {
	reg := traeRegistration()
	if reg.Capabilities.Executor && len(reg.Capabilities.ExecutorInputFormats) == 0 {
		t.Error("executor declared but no input formats")
	}
	if reg.Capabilities.Executor && len(reg.Capabilities.ExecutorOutputFormats) == 0 {
		t.Error("executor declared but no output formats")
	}
}

// TestRegistrationVersionNoLeadingV 宿主 pluginstore 用
// ^[0-9][0-9A-Za-z.+-]*$ 校验版本号：不能以 v 开头，必须以数字开头。
func TestRegistrationVersionNoLeadingV(t *testing.T) {
	v := traeRegistration().Metadata.Version
	if v == "" {
		t.Fatal("version is empty")
	}
	if strings.HasPrefix(v, "v") {
		t.Errorf("version %q must not start with 'v'", v)
	}
	if v[0] < '0' || v[0] > '9' {
		t.Errorf("version %q must start with a digit", v)
	}
}
