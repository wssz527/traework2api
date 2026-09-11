package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// mustMarshal 测试辅助：序列化失败直接 Fatal。
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// stubLoginExchange 替换真实兑换（不触网），返回一个固定的假凭证。
func stubLoginExchange(t *testing.T, fn func(*loginFlow) (*traeAuth, error)) {
	t.Helper()
	orig := loginExchange
	loginExchange = fn
	t.Cleanup(func() { loginExchange = orig })
}

// loginFlows 在测试间必须清干净：否则 18080 监听与状态会串。
func resetLoginFlows(t *testing.T) {
	t.Helper()
	loginFlows.Lock()
	for state, f := range loginFlows.m {
		f.cleanup()
		delete(loginFlows.m, state)
	}
	loginFlows.Unlock()
	t.Cleanup(func() {
		loginFlows.Lock()
		for state, f := range loginFlows.m {
			f.cleanup()
			delete(loginFlows.m, state)
		}
		loginFlows.Unlock()
	})
}

func decodeStart(t *testing.T, raw []byte) pluginapi.AuthLoginStartResponse {
	t.Helper()
	var env struct {
		OK     bool                             `json:"ok"`
		Result pluginapi.AuthLoginStartResponse `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !env.OK {
		t.Fatalf("envelope not ok")
	}
	return env.Result
}

func decodePoll(t *testing.T, raw []byte) pluginapi.AuthLoginPollResponse {
	t.Helper()
	var env struct {
		OK     bool                            `json:"ok"`
		Result pluginapi.AuthLoginPollResponse `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !env.OK {
		t.Fatalf("envelope not ok")
	}
	return env.Result
}

func TestStartLoginBuildsURLAndState(t *testing.T) {
	resetLoginFlows(t)
	raw, err := handleStartLogin(nil)
	if err != nil {
		t.Fatalf("start login: %v", err)
	}
	resp := decodeStart(t, raw)
	if resp.Provider != providerName {
		t.Errorf("provider=%q want %q", resp.Provider, providerName)
	}
	if resp.State == "" {
		t.Error("state must be non-empty")
	}
	if !strings.HasPrefix(resp.URL, ConsoleHost+"/authorization?") {
		t.Errorf("unexpected url: %s", resp.URL)
	}
	for _, want := range []string{"client_id=" + ClientID, "machine_id=", "device_id=", "auth_callback_url="} {
		if !strings.Contains(resp.URL, want) {
			t.Errorf("url missing %q: %s", want, resp.URL)
		}
	}
	if resp.ExpiresAt.Before(time.Now()) {
		t.Error("expiry must be in the future")
	}
	// 回调监听必须真的起来了
	loginFlows.Lock()
	flow := loginFlows.m[resp.State]
	loginFlows.Unlock()
	if flow == nil {
		t.Fatal("flow not registered")
	}
	if flow.srv == nil {
		t.Fatal("callback listener not started")
	}
}

// 回调捕获 → 异步兑换 → poll 返回 success + AuthData。
func TestLoginFlowCallbackThenSuccess(t *testing.T) {
	resetLoginFlows(t)
	stubLoginExchange(t, func(f *loginFlow) (*traeAuth, error) {
		f.mu.Lock()
		refresh := f.refreshToken
		f.mu.Unlock()
		if refresh == "" {
			return nil, fmt.Errorf("no refresh token captured")
		}
		return &traeAuth{UID: "12345", Nickname: "tester", AccessToken: "at-1", RefreshToken: refresh, ExpiresAt: time.Now().Add(time.Hour).Unix()}, nil
	})

	startRaw, err := handleStartLogin(nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	start := decodeStart(t, startRaw)

	// 先 poll：必须 pending
	pollRaw, err := handlePollLogin(mustMarshal(t, pluginapi.AuthLoginPollRequest{Provider: providerName, State: start.State}))
	if err != nil {
		t.Fatalf("poll pending: %v", err)
	}
	if got := decodePoll(t, pollRaw).Status; got != pluginapi.AuthLoginStatusPending {
		t.Fatalf("status=%q want pending", got)
	}

	// 模拟浏览器跳转到回调监听端口
	loginFlows.Lock()
	flow := loginFlows.m[start.State]
	loginFlows.Unlock()
	addr := flow.srv.Addr
	if addr == "" {
		// httptest 未用；从 URL 里解析端口
		addr = "127.0.0.1:0"
	}
	// 直接调用 ServeHTTP 模拟回调（避免依赖真实端口解析）
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, loginCallbackPath+"?refreshToken=rt-abc&userInfo=%7B%22UserID%22%3A%2212345%22%7D", nil)
	flow.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("callback code=%d", rec.Code)
	}

	// 等 finish 完成
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		flow.mu.Lock()
		done := flow.done
		flow.mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	pollRaw, err = handlePollLogin(mustMarshal(t, pluginapi.AuthLoginPollRequest{Provider: providerName, State: start.State}))
	if err != nil {
		t.Fatalf("poll done: %v", err)
	}
	resp := decodePoll(t, pollRaw)
	if resp.Status != pluginapi.AuthLoginStatusSuccess {
		t.Fatalf("status=%q want success (msg=%s)", resp.Status, resp.Message)
	}
	if resp.Auth.Provider != providerName || resp.Auth.ID != "12345" {
		t.Errorf("auth data wrong: %+v", resp.Auth)
	}
	// 成功后状态必须被清理
	loginFlows.Lock()
	_, still := loginFlows.m[start.State]
	loginFlows.Unlock()
	if still {
		t.Error("flow should be removed after success")
	}
}

// 兑换失败 → poll 报错并清理。
func TestLoginFlowExchangeError(t *testing.T) {
	resetLoginFlows(t)
	stubLoginExchange(t, func(f *loginFlow) (*traeAuth, error) {
		return nil, fmt.Errorf("ExchangeToken 失败: http 400")
	})
	startRaw, _ := handleStartLogin(nil)
	start := decodeStart(t, startRaw)

	loginFlows.Lock()
	flow := loginFlows.m[start.State]
	loginFlows.Unlock()
	rec := httptest.NewRecorder()
	flow.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, loginCallbackPath+"?refreshToken=rt-bad", nil))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		flow.mu.Lock()
		failed := flow.errMsg != ""
		flow.mu.Unlock()
		if failed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := handlePollLogin(mustMarshal(t, pluginapi.AuthLoginPollRequest{Provider: providerName, State: start.State})); err == nil {
		t.Fatal("poll should return error after failed exchange")
	}
}

func TestPollLoginUnknownState(t *testing.T) {
	resetLoginFlows(t)
	if _, err := handlePollLogin(mustMarshal(t, pluginapi.AuthLoginPollRequest{Provider: providerName, State: "nope"})); err == nil {
		t.Fatal("unknown state must error")
	}
}

func TestLoginCallbackWrongPath404(t *testing.T) {
	resetLoginFlows(t)
	startRaw, _ := handleStartLogin(nil)
	start := decodeStart(t, startRaw)
	loginFlows.Lock()
	flow := loginFlows.m[start.State]
	loginFlows.Unlock()
	rec := httptest.NewRecorder()
	flow.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("code=%d want 404", rec.Code)
	}
}

func TestParseJSONParam(t *testing.T) {
	if got := parseJSONParam(`{"UserID":"u1"}`); got["UserID"] != "u1" {
		t.Errorf("plain json: %#v", got)
	}
	if got := parseJSONParam(`%7B%22UserID%22%3A%22u2%22%7D`); got["UserID"] != "u2" {
		t.Errorf("url-encoded json: %#v", got)
	}
	if got := parseJSONParam(""); got != nil {
		t.Errorf("empty input should be nil, got %#v", got)
	}
}
