package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// soloServer 起一个假 SOLO 上游：/api/agent/v3/llm_utils_chat 返回 SSE。
// 返回被命中的最后一次请求体，供断言改写结果。
func soloServer(t *testing.T, sse string, status int) (*httptest.Server, *capturedReq) {
	t.Helper()
	cap := &capturedReq{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.mu.Lock()
		cap.body = body
		cap.path = r.URL.Path
		cap.auth = r.Header.Get("Authorization")
		cap.accept = r.Header.Get("Accept")
		cap.mu.Unlock()
		if status >= 400 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"code":1005,"message":"plan limit"}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, sse)
	}))
	t.Cleanup(srv.Close)
	return srv, cap
}

type capturedReq struct {
	mu     sync.Mutex
	body   []byte
	path   string
	auth   string
	accept string
}

func (c *capturedReq) snapshot() capturedReq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return capturedReq{body: c.body, path: c.path, auth: c.auth, accept: c.accept}
}

// withFakeUpstream 把默认客户端临时指向假上游。
func withFakeUpstream(t *testing.T, url string, status int, sse string) *capturedReq {
	t.Helper()
	srv, cap := soloServer(t, sse, status)
	orig := defaultClient.Load()
	fake := &Client{
		HTTP:       srv.Client(),
		StreamHTTP: srv.Client(),
		AgentHost:  srv.URL,
		UgHost:     srv.URL,
		OAuthHost:  srv.URL,
		ClientID:   ClientID,
	}
	setDefaultClient(fake)
	t.Cleanup(func() { setDefaultClient(orig) })
	return cap
}

// storageOf 合成一个含假 token 的凭证（永不读真实 auths）。
func storageOf() []byte {
	raw := `{"type":"trae","account":{"uid":"1000000000000000","nickname":"测试"},
	  "auth":{"accessToken":"at-placeholder","refreshToken":"rt-placeholder",
	  "expiresAt":9999999999,"domain":"trae.cn","apiHost":"https://api.trae.com.cn",
	  "machineId":"m1","deviceId":"d1","checkinDeviceId":"1111222233334444",
	  "checkinDeviceBrand":"Mac16,10","checkinDeviceType":"mac"}}`
	return []byte(raw)
}

func TestExecutorExecuteAggregatesNonStream(t *testing.T) {
	cap := withFakeUpstream(t, "", 200, soloSSEFixture)
	body, _ := json.Marshal(pluginapi.ExecutorRequest{
		AuthID:          "trae-1000000000000000",
		Model:           "glm-5.2",
		Format:          "chat-completions",
		Stream:          false,
		StorageJSON:     storageOf(),
		OriginalRequest: []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	})
	raw, err := handleMethodGuarded("executor.execute", body)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var resp pluginapi.ExecutorResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, resp.Payload)
	}
	if out["object"] != "chat.completion" {
		t.Errorf("object=%v", out["object"])
	}
	// 非流式必须折叠成一条 message，不是 chunk
	choices := out["choices"].([]any)
	msg := choices[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "中国的首都是北京。" {
		t.Errorf("content=%q", msg["content"])
	}
	// 上游返回的 model 为空 → 回填客户端请求的 model
	if out["model"] != "glm-5.2" {
		t.Errorf("model=%v want glm-5.2", out["model"])
	}
	snap := cap.snapshot()
	if snap.path != EpChat {
		t.Errorf("path=%s want %s", snap.path, EpChat)
	}
	// 上游只接受 stream=true，插件必须强制改写
	if !strings.Contains(string(snap.body), `"stream":true`) {
		t.Errorf("upstream body not forced to stream: %s", snap.body)
	}
	if !strings.Contains(string(snap.body), `"function":"solo_work_lite"`) {
		t.Errorf("function missing: %s", snap.body)
	}
	if snap.auth != "Cloud-IDE-JWT at-placeholder" {
		t.Errorf("auth header=%q", snap.auth)
	}
}

func TestExecutorExecuteUpstreamErrorSurfaces(t *testing.T) {
	withFakeUpstream(t, "", 500, "")
	body, _ := json.Marshal(pluginapi.ExecutorRequest{
		Model:           "glm-5.2",
		StorageJSON:     storageOf(),
		OriginalRequest: []byte(`{"model":"glm-5.2","messages":[]}`),
	})
	_, err := handleMethodGuarded("executor.execute", body)
	if err == nil {
		t.Fatal("upstream 500 should surface as an error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should carry the status: %v", err)
	}
}

// TestExecutorExecutePlanLimitCoolsAccount 1005 plan 权益不足 → 账号进入 12h 冷却。
func TestExecutorExecutePlanLimitCoolsAccount(t *testing.T) {
	withFakeUpstream(t, "", 200, "event:error\ndata:{\"code\":1005,\"message\":\"plan limit\"}\n\n")
	authID := "trae-planlimit"
	st := stateFor(authID)
	st.mu.Lock()
	st.coolUntil = time.Time{}
	st.disabled = false
	st.mu.Unlock()

	body, _ := json.Marshal(pluginapi.ExecutorRequest{
		AuthID:          authID,
		Model:           "glm-5.2",
		StorageJSON:     storageOf(),
		OriginalRequest: []byte(`{"model":"glm-5.2","messages":[]}`),
	})
	if _, err := handleMethodGuarded("executor.execute", body); err == nil {
		t.Fatal("want error for plan limit")
	}
	if !isCooling(authID) {
		t.Error("plan limit should put the account into cooldown")
	}
	st.mu.Lock()
	reason := st.coolFor
	st.mu.Unlock()
	if reason != "plan_limit" {
		t.Errorf("cool reason=%q want plan_limit", reason)
	}
}

func TestExecutorStreamCollectsChunks(t *testing.T) {
	cap := withFakeUpstream(t, "", 200, soloSSEFixture)
	body, _ := json.Marshal(executorStreamRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			AuthID:          "trae-1",
			Model:           "glm-5.2",
			Format:          "chat-completions",
			Stream:          true,
			StorageJSON:     storageOf(),
			OriginalRequest: []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"stream":true}`),
			Metadata:        map[string]any{"request_path": "/v1/chat/completions"},
		},
		// 无 StreamID → 同步收集路径
	})
	raw, err := handleMethodGuarded("executor.execute_stream", body)
	if err != nil {
		t.Fatalf("execute_stream: %v", err)
	}
	var resp streamResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Chunks) == 0 {
		t.Fatal("no chunks collected")
	}
	if got := resp.Headers.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("content-type=%q", got)
	}
	joined := ""
	for _, c := range resp.Chunks {
		joined += string(c.Payload)
	}
	if !strings.Contains(joined, `"object":"chat.completion.chunk"`) {
		t.Errorf("chunks are not OpenAI chunks: %q", joined)
	}
	// 内容分片是两个 chunk，拼接后必须完整
	if !strings.Contains(joined, `"content":"中国"`) || !strings.Contains(joined, `"content":"的首都是北京。"`) {
		t.Errorf("content delta lost: %q", joined)
	}
	// 宿主 chat-completions 通道自己加 data: 前缀，插件不应重复加
	if strings.Contains(joined, "data: ") {
		t.Errorf("chunks should be unframed for the native path: %q", joined)
	}
	// [DONE] 由宿主补，插件不应重复发
	if strings.Contains(joined, "[DONE]") {
		t.Errorf("host appends its own terminator: %q", joined)
	}
	snap := cap.snapshot()
	if !strings.Contains(string(snap.body), `"config_name":"glm-5.2"`) {
		t.Errorf("config_name missing: %s", snap.body)
	}
	if snap.accept != "text/event-stream" {
		t.Errorf("accept=%q want text/event-stream", snap.accept)
	}
}

// TestExecutorStreamFramedForCrossFormat 跨格式入口（如 /v1/messages）要求
// payload 自带 "data: " 帧。
func TestExecutorStreamFramedForCrossFormat(t *testing.T) {
	withFakeUpstream(t, "", 200, soloSSEFixture)
	body, _ := json.Marshal(executorStreamRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			AuthID:          "trae-1",
			Model:           "glm-5.2",
			Stream:          true,
			StorageJSON:     storageOf(),
			OriginalRequest: []byte(`{"model":"glm-5.2","messages":[],"stream":true}`),
			Metadata:        map[string]any{"request_path": "/v1/messages"},
		},
	})
	raw, err := handleMethodGuarded("executor.execute_stream", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp streamResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	joined := ""
	for _, c := range resp.Chunks {
		joined += string(c.Payload)
	}
	if !strings.Contains(joined, "data: ") {
		t.Errorf("cross-format chunks must be SSE framed: %q", joined)
	}
}

// TestExecutorStreamAsyncReturnsImmediately 带 StreamID 时必须立即返回空 chunks，
// 由 goroutine 泵流（这里是 no-host 环境，emit 失败即中止，不得阻塞）。
func TestExecutorStreamAsyncReturnsImmediately(t *testing.T) {
	withFakeUpstream(t, "", 200, soloSSEFixture)
	body, _ := json.Marshal(executorStreamRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			AuthID:          "trae-1",
			Model:           "glm-5.2",
			Stream:          true,
			StorageJSON:     storageOf(),
			OriginalRequest: []byte(`{"model":"glm-5.2","messages":[],"stream":true}`),
		},
		StreamID: "stream-1",
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		raw, err := handleMethodGuarded("executor.execute_stream", body)
		if err != nil {
			t.Errorf("execute_stream async: %v", err)
			return
		}
		var resp streamResponse
		if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		if len(resp.Chunks) != 0 {
			t.Errorf("async path must return no chunks, got %d", len(resp.Chunks))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("async execute_stream blocked instead of returning immediately")
	}
	// 给后台 pump 一点时间跑完（无宿主时 emit 会失败并提前返回）
	time.Sleep(100 * time.Millisecond)
}

func TestExecutorRejectsUnparseableCredential(t *testing.T) {
	body, _ := json.Marshal(pluginapi.ExecutorRequest{
		Model:       "glm-5.2",
		StorageJSON: []byte(`{"nope":1}`),
	})
	if _, err := handleMethodGuarded("executor.execute", body); err == nil {
		t.Fatal("unparseable credential should error")
	}
}

func TestCountTokensReturnsZeroEstimate(t *testing.T) {
	raw, err := handleMethodGuarded("executor.count_tokens", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.ExecutorResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(resp.Payload), "input_tokens") {
		t.Errorf("payload=%s", resp.Payload)
	}
}

func TestAuthRefreshExchangesToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, EpExchange) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"Result":{"Token":"new-at","RefreshToken":"new-rt","TokenExpireAt":1786847930141}}`)
	}))
	defer srv.Close()
	orig := defaultClient.Load()
	setDefaultClient(&Client{
		HTTP:      srv.Client(),
		AgentHost: srv.URL, UgHost: srv.URL, OAuthHost: srv.URL, ClientID: ClientID,
	})
	defer setDefaultClient(orig)

	sa, err := parseStored(storageOf())
	if err != nil {
		t.Fatal(err)
	}
	sa.ApiHost = srv.URL
	sa.ExpiresAt = 1
	storage, _ := sa.marshalNested()

	body, _ := json.Marshal(pluginapi.AuthRefreshRequest{StorageJSON: storage})
	raw, err := handleMethodGuarded("auth.refresh", body)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	var resp pluginapi.AuthRefreshResponse
	if err := json.Unmarshal(mustDecodeResult(t, raw), &resp); err != nil {
		t.Fatal(err)
	}
	// 宿主自己持久化；ID/FileName 必须留空让它回填
	if resp.Auth.ID != "" || resp.Auth.FileName != "" {
		t.Errorf("ID=%q FileName=%q must both be empty", resp.Auth.ID, resp.Auth.FileName)
	}
	after, err := parseStored(resp.Auth.StorageJSON)
	if err != nil {
		t.Fatalf("reparse refreshed storage: %v", err)
	}
	if after.AccessToken != "new-at" || after.RefreshToken != "new-rt" {
		t.Errorf("tokens not refreshed: %+v", after)
	}
	if after.ExpiresAt != 1786847930 {
		t.Errorf("expiresAt=%d want 1786847930 (ms→s)", after.ExpiresAt)
	}
	// SOLO 自定义字段不得在 refresh 后丢失
	if after.MachineID != "m1" || after.CheckinDeviceID != "1111222233334444" {
		t.Errorf("SOLO fields lost: %+v", after)
	}
}
