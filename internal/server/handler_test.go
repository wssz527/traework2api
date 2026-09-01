package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/pool"
	"traework2api/internal/upstream"
)

// 模拟 SOLO SSE 响应（glm-5.2 回答"你好"）。
const soloSSE = "event:metadata\ndata:{\"model\":\"\",\"session_id\":\"s1\"}\n\n" +
	"event:output\ndata:{\"response\":\"你好\",\"reasoning_content\":\"想一下\",\"tool_calls\":null}\n\n" +
	"event:token_usage\ndata:{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}\n\n" +
	"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"

// newFakeUpstream 返回 ChatStream 走 fake 的 upstream.Client。
func newFakeUpstream(t *testing.T, behavior func(auth string) (status int, body string, isStream bool)) *upstream.Client {
	t.Helper()
	return &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			authz := r.Header.Get("Authorization")
			status, body, isStream := behavior(authz)
			ct := "application/json"
			if isStream {
				ct = "text/event-stream"
			}
			return &http.Response{
				StatusCode: status,
				Header:     http.Header{"Content-Type": []string{ct}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
		AgentHost: "https://fake.example",
		UgHost:    "https://fake.example",
		OAuthHost: "https://fake.example",
		ClientID:  upstream.ClientID,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testPoolWith(auths ...*auth.Auth) *pool.Pool {
	p := pool.New("")
	for _, a := range auths {
		p.Add(a)
		p.SetCredits(a.UID, 1000)
	}
	return p
}

// newOfflineUpstream 返回指向本地死端口的 Client。冷缓存时 mapModel 会经
// fetchDynamicModels 真实出网拉模型表（测试池是假 token，必失败但依赖外网
// 且慢）；三个 host 指到 127.0.0.1:1 立刻连接拒绝走静态回退，单测完全
// 离线且行为不变。
func newOfflineUpstream() *upstream.Client {
	c := upstream.New()
	c.AgentHost = "http://127.0.0.1:1"
	c.UgHost = "http://127.0.0.1:1"
	c.OAuthHost = "http://127.0.0.1:1"
	return c
}

// snapshotDynamicModelsCache 隔离全局动态模型缓存：测试开始时清空、
// 结束时恢复原快照。多 个用例的 get_detail_param mock 各自返回不同模型表，而
// dynamicModelsCache 是包级全局（成功 TTL 1h / 失败负缓存 5min）——
// 只恢复不 清空的话，先跑用例的模型表（以及失败留下的 lastFail）会污染
// 本用例；清空后本 用例的 mock 表独立生效，结束时原样还原全局状态。
func snapshotDynamicModelsCache(t *testing.T) {
	t.Helper()
	dynamicModelsCache.Lock()
	ids, fetched, lastFail := dynamicModelsCache.ids, dynamicModelsCache.fetched, dynamicModelsCache.lastFail
	dynamicModelsCache.ids, dynamicModelsCache.fetched, dynamicModelsCache.lastFail = nil, time.Time{}, time.Time{}
	dynamicModelsCache.Unlock()
	t.Cleanup(func() {
		dynamicModelsCache.Lock()
		dynamicModelsCache.ids, dynamicModelsCache.fetched, dynamicModelsCache.lastFail = ids, fetched, lastFail
		dynamicModelsCache.Unlock()
	})
}

// isolatedConvStore 隔离会话注册表：ConvStorePath 指向独立临时目录下的
// conversations.json，避免用例读写共享的 data/conversations.json（跨用例/跨
// 运行的绑定残留会让 turn1 直接命中旧会话、跳过建会话，导致 creates 断言失败）。
// 每次调用生成新目录，同用例内多个 handler 也不共享注册表。
var isolatedConvStoreSeq int

func isolatedConvStore() string {
	isolatedConvStoreSeq++
	return filepath.Join(os.TempDir(), fmt.Sprintf("tw2api-conv-test-%d-%d", os.Getpid(), isolatedConvStoreSeq), "conversations.json")
}

func TestChatNonStreamAggregates(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		if authz != "Cloud-IDE-JWT at1" {
			t.Errorf("auth=%q", authz)
		}
		return 200, soloSSE, true
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("resp not json: %v body=%s", err, rec.Body)
	}
	if resp["object"] != "chat.completion" {
		t.Errorf("object=%v", resp["object"])
	}
	msg := resp["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "你好" {
		t.Errorf("content=%q", msg["content"])
	}
	if msg["reasoning_content"] != "想一下" {
		t.Errorf("reasoning=%q", msg["reasoning_content"])
	}
}

func TestChatStreamConvertsLegacySOLOToOpenAI(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, soloSSE, true
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("ct=%q", ct)
	}
	body := rec.Body.String()
	if strings.Contains(body, "event:output") {
		t.Errorf("SOLO event leaked to OpenAI client: %q", body)
	}
	if !strings.Contains(body, `data: {"`) ||
		!strings.Contains(body, `"object":"chat.completion.chunk"`) ||
		!strings.Contains(body, `"content":"你好"`) ||
		!strings.Contains(body, "data: [DONE]\n\n") {
		t.Errorf("body=%q", body)
	}
}

func TestWriteRemoteReplyFormatsOpenAISSE(t *testing.T) {
	h := &Handler{}
	rec := httptest.NewRecorder()

	startRemoteStream(rec)
	h.writeRemoteReply(rec, true, "glm-5.3", "OK")

	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type=%q", ct)
	}
	frames := strings.Split(strings.TrimSuffix(rec.Body.String(), "\n\n"), "\n\n")
	if len(frames) != 3 {
		t.Fatalf("frames=%d body=%q", len(frames), rec.Body.String())
	}
	for i, frame := range frames[:2] {
		if !strings.HasPrefix(frame, "data: ") {
			t.Errorf("frame %d is not SSE data: %q", i, frame)
			continue
		}
		var chunk map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(frame, "data: ")), &chunk); err != nil {
			t.Errorf("frame %d is not JSON: %v", i, err)
		}
		if chunk["object"] != "chat.completion.chunk" {
			t.Errorf("frame %d object=%v", i, chunk["object"])
		}
	}
	if frames[2] != "data: [DONE]" {
		t.Errorf("last frame=%q", frames[2])
	}
}

func TestRemotePromptPreservesStructuredOpenAIMessages(t *testing.T) {
	body := []byte(`{"messages":[
		{"role":"system","content":"规则"},
		{"role":"user","content":[{"type":"text","text":"只回复 OK"}]},
		{"role":"user","content":[{"type":"text","text":"<system-reminder>日期</system-reminder>"}]}
	]}`)
	want := "system:\n规则\n\nuser:\n只回复 OK\n\nuser:\n<system-reminder>日期</system-reminder>"

	if got := remotePrompt(body); got != want {
		t.Errorf("prompt=%q want=%q", got, want)
	}
}

func TestChatRotatesOnPlanLimit(t *testing.T) {
	calls := map[string]int{}
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		calls[authz]++
		if authz == "Cloud-IDE-JWT at-bad" {
			return 200, "event:error\ndata:{\"code\":1005,\"message\":\"plan limit\",\"extra\":{\"plan\":2}}\n\n", true
		}
		return 200, soloSSE, true
	})
	p := testPoolWith(
		&auth.Auth{UID: "bad", AccessToken: "at-bad", ExpiresAt: 9999999999},
		&auth.Auth{UID: "good", AccessToken: "at-good", ExpiresAt: 9999999999},
	)
	p.SetCredits("bad", 2000)
	p.SetCredits("good", 1000)
	h := NewHandler(Config{Pool: p, Upstream: up})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if calls["Cloud-IDE-JWT at-bad"] != 1 || calls["Cloud-IDE-JWT at-good"] != 1 {
		t.Errorf("calls=%v", calls)
	}
}

// TestChatStreamCooldownOnStreamError 流式请求中上游 event:error（如 5xx/参数错误）
// 应触发 NoteError 冷却（而非仅透传不冷却）。
func TestChatStreamCooldownOnStreamError(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, "event:error\ndata:{\"code\":5001,\"message\":\"upstream broke\"}\n\n", true
	})
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: up})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// 流内错误不应中断整个请求的 SSE 输出（仍有 event:error + [DONE]）。
	body := rec.Body.String()
	if !strings.Contains(body, "solo error") || !strings.Contains(body, "data: [DONE]") {
		t.Errorf("body=%q want error event + [DONE]", body)
	}
	st, _ := p.Status("u1")
	if st.ErrCount == 0 {
		t.Errorf("stream error should bump errCount: %+v", st)
	}
}

func TestChatAllUnavailableReturns503(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 429, `rate limited`, false
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Errorf("code=%d body=%s", rec.Code, rec.Body)
	}
	var e map[string]any
	json.Unmarshal(rec.Body.Bytes(), &e)
	if e["error"] == nil {
		t.Errorf("want error envelope: %s", rec.Body)
	}
}

func TestChatSessionDeadDisables(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 401, `{"code":1001,"msg":"login required"}`, false
	})
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: up})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Errorf("code=%d", rec.Code)
	}
	st, _ := p.Status("u1")
	if !st.Disabled {
		t.Errorf("account should be disabled: %+v", st)
	}
}

// TestMapModelMaxSuffix P0："-max" 后缀剥离与 maxMode 返回。
func TestMapModelMaxSuffix(t *testing.T) {
	snapshotDynamicModelsCache(t)
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}), Upstream: newOfflineUpstream()})
	cases := []struct {
		in    string
		want  string
		isMax bool
	}{
		{"glm-5.3", "glm-5.3", false},
		{"glm-5.3-max", "glm-5.3", true},
		{"DeepSeek-V4-Flash-Official-max", "DeepSeek-V4-Flash-Official", true},
		{"glm-5.3__dev", "glm-5.3", false},
		{"", "glm-5.2", false}, // 默认模型
	}
	for _, tc := range cases {
		got, isMax, err := h.mapModel(tc.in)
		if err != nil {
			t.Errorf("mapModel(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want || isMax != tc.isMax {
			t.Errorf("mapModel(%q) = (%q,%v) want (%q,%v)", tc.in, got, isMax, tc.want, tc.isMax)
		}
	}
	if _, _, err := h.mapModel("does-not-exist-max"); err == nil {
		t.Error("未知模型带 -max 后缀也应 400")
	}
}

func TestChatUnknownModel400(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}), Upstream: newOfflineUpstream()})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"does-not-exist-xyz","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Errorf("code=%d body=%s", rec.Code, rec.Body)
	}
	var e map[string]any
	json.Unmarshal(rec.Body.Bytes(), &e)
	if e["error"] == nil {
		t.Errorf("want error envelope: %s", rec.Body)
	}
}

func TestModelsEndpoint(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}), Upstream: newOfflineUpstream()})
	req := httptest.NewRequest("GET", "/v1/models", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	var resp map[string]any
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["object"] != "list" {
		t.Errorf("object=%v", resp["object"])
	}
	data := resp["data"].([]any)
	if len(data) != 32 {
		t.Errorf("models count=%d want 32", len(data))
	}
	found := false
	for _, m := range data {
		if m.(map[string]any)["id"] == "glm-5.2" {
			found = true
		}
	}
	if !found {
		t.Error("glm-5.2 missing")
	}
}

func TestAPIKeyAuth(t *testing.T) {
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: newOfflineUpstream(),
		APIKey:   "test-key",
	})
	// 无 key → 401
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("no key: code=%d", rec.Code)
	}
	// 错 key → 401
	req = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("wrong key: code=%d", rec.Code)
	}
	// 对 key → 200（models）
	req = httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("right key: code=%d", rec.Code)
	}
}

func TestRequestBodyTooLarge(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}), Upstream: newOfflineUpstream()})
	big := strings.Repeat("a", maxBodyBytes+1)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(big))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("code=%d body=%s", rec.Code, rec.Body)
	}
	var e map[string]any
	json.Unmarshal(rec.Body.Bytes(), &e)
	if e["error"] == nil {
		t.Errorf("want error envelope: %s", rec.Body)
	}
}

func TestAPIKeyAuthCaseInsensitivePrefix(t *testing.T) {
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: newOfflineUpstream(),
		APIKey:   "test-key",
	})
	// 大小写不同的 Bearer 前缀也应接受（按规范，前缀大小写不敏感）。
	req := httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "bearer test-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("lowercase bearer: code=%d", rec.Code)
	}
	// 错误 key 仍拒绝。
	req = httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("wrong key: code=%d", rec.Code)
	}
}

func TestStatusEndpoint(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", Nickname: "nick", AccessToken: "at", ExpiresAt: 9999999999})
	p.SetCredits("u1", 42)
	h := NewHandler(Config{Pool: p, Upstream: newOfflineUpstream()})
	req := httptest.NewRequest("GET", "/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"uid":"u1"`) || !strings.Contains(body, `"credits":42`) {
		t.Errorf("body=%s", body)
	}
	if strings.Contains(body, "AccessToken") || strings.Contains(body, `"at"`) {
		t.Error("token leaked in status output")
	}
}

func TestHealthz(t *testing.T) {
	h := NewHandler(Config{Pool: pool.New(""), Upstream: newOfflineUpstream()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("code=%d", rec.Code)
	}
}

func TestChatGLM53RoutesThroughRemoteSession(t *testing.T) {
	snapshotDynamicModelsCache(t) // 隔离全局动态模型缓存：本用例自己 mock get_detail_param
	var gotPaths []string
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotPaths = append(gotPaths, r.Method+" "+r.URL.Path)
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"s1"}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":"OK"}]}}`), nil
			case r.Method == http.MethodDelete && r.URL.Path == "/api/remote/v1/chat_sessions/s1":
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
		AgentHost: "https://fake.example",
		UgHost:    "https://fake.example",
		OAuthHost: "https://fake.example",
		ClientID:  upstream.ClientID,
	}
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: "-",
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	wantPaths := []string{
		"POST /api/ide/v1/get_detail_param",
		"POST /api/remote/v1/chat_sessions",
		"POST /api/remote/v1/chat_sessions/s1/messages",
		"GET /api/remote/v1/chat_sessions/s1/messages",
		"DELETE /api/remote/v1/chat_sessions/s1",
	}
	if strings.Join(gotPaths, "\n") != strings.Join(wantPaths, "\n") {
		t.Errorf("paths=%v want=%v", gotPaths, wantPaths)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"content":"OK"`) || !strings.Contains(body, "data: [DONE]\n\n") {
		t.Errorf("stream body=%q", body)
	}
}

func remoteTestHook() (restore func()) {
	oldWait, oldKeep, oldBusy := remoteWaitTotal, remoteKeepaliveInterval, remoteBusyRetryWait
	remoteWaitTotal, remoteKeepaliveInterval, remoteBusyRetryWait = 200*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond
	// P0 事件流拨号走 remoteHost；单测里指向本地死端口使其立即失败、
	// 静默降级——不出网、不拖慢用例、也不受外部网络环境干扰。
	restoreHost := upstream.SetRemoteHost("http://127.0.0.1:1")
	return func() {
		remoteWaitTotal, remoteKeepaliveInterval, remoteBusyRetryWait = oldWait, oldKeep, oldBusy
		restoreHost()
	}
}

// TestChatRemoteStreamTimeoutEmitsKeepaliveAndError 任务迟迟不完成时：

func TestChatRemoteStreamTimeoutEmitsKeepaliveAndError(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	var creates, deletes int
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				creates++
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"s1"}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"user","status":"in_progress"}]}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				// mapModel → fetchDynamicModels 的模型表请求（与 remoteStreamTestUpstream 同款）。
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodDelete && r.URL.Path == "/api/remote/v1/chat_sessions/s1":
				deletes++
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
		AgentHost: "https://fake.example",
		UgHost:    "https://fake.example",
		OAuthHost: "https://fake.example",
		ClientID:  upstream.ClientID,
	}
	p := testPoolWith(
		&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999},
		&auth.Auth{UID: "u2", AccessToken: "at2", ExpiresAt: 9999999999},
	)
	h := NewHandler(Config{Pool: p, Upstream: up, ConvStorePath: "-"})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if creates != 1 || deletes != 1 {
		t.Errorf("creates=%d deletes=%d want 1/1（超时不得换号重发）", creates, deletes)
	}
	body := rec.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Errorf("stream should contain keepalive comments: %q", body)
	}
	if !strings.Contains(body, "remote_task_failed") || !strings.Contains(body, "data: [DONE]\n\n") {
		t.Errorf("stream should end with error frame + DONE: %q", body)
	}
}

// TestChatRemoteBusyRetriesThenRotates 发消息遇 429 并发槽满：

func TestChatRemoteBusyRetriesThenRotates(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	var creates int
	var sends int
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				creates++
				return jsonHTTPResponse(200, fmt.Sprintf(`{"code":0,"data":{"chat_session_id":"s%d"}}`, creates)), nil
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages"):
				sends++
				return jsonHTTPResponse(429, `{"code":991502,"error":{"reason":"solo_agent_parallel_limit","limit":2,"running":2},"message":"solo agent parallel limit reached"}`), nil
			case r.Method == http.MethodDelete:
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
		AgentHost: "https://fake.example",
		UgHost:    "https://fake.example",
		OAuthHost: "https://fake.example",
		ClientID:  upstream.ClientID,
	}
	p := testPoolWith(
		&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999},
		&auth.Auth{UID: "u2", AccessToken: "at2", ExpiresAt: 9999999999},
	)
	h := NewHandler(Config{Pool: p, Upstream: up, ConvStorePath: "-"})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 503 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if creates != 2 { // 每账号一个会话
		t.Errorf("creates=%d want 2", creates)
	}
	if sends != 6 { // 每会话 3 次发送重试 × 2 账号
		t.Errorf("sends=%d want 6", sends)
	}
	st, _ := p.Status("u1")
	if st.ErrCount != 0 {
		t.Errorf("busy 不应累计账号错误: %+v", st)
	}
}

// TestChatRemoteClientDisconnectCleansSession 客户端断开后应停止轮询并删除会话，

func TestChatRemoteClientDisconnectCleansSession(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	var deletes int
	done := make(chan struct{})
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"s1"}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"user","status":"in_progress"}]}}`), nil
			case r.Method == http.MethodDelete && r.URL.Path == "/api/remote/v1/chat_sessions/s1":
				deletes++
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
		AgentHost: "https://fake.example",
		UgHost:    "https://fake.example",
		OAuthHost: "https://fake.example",
		ClientID:  upstream.ClientID,
	}
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: "-",
	})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after client disconnect")
	}
	if deletes != 1 {
		t.Errorf("deletes=%d want 1（断开后必须删除会话释放并发槽）", deletes)
	}
}

func jsonHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestChatDeepSeekStaysOnLegacyChannel(t *testing.T) {
	snapshotDynamicModelsCache(t)
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"DeepSeek-V4-Flash-Official","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	// DeepSeek-V4-Flash-Official 已加入 cloudAgentModels，走 remote 通道（云端沙盒）。
	if creates != 1 {
		t.Errorf("DeepSeek should go remote (cloud sandbox), creates=%d want 1", creates)
	}
}

// ---------------------------------------------------------------------------
// 本地 Work 通道接线（tools_local:true 触发）
// ---------------------------------------------------------------------------
