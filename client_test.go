package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   ErrKind
	}{
		{200, `{"code":1005,"message":"plan limit","extra":{"plan":2}}`, ErrPlanLimit},
		{200, `{"code":1005,"msg":"权益不足"}`, ErrPlanLimit},
		{429, ``, ErrSoftRate},
		{401, `{"code":1001,"msg":"login required"}`, ErrSessionDead},
		{401, ``, ErrSessionDead},
		{404, ``, ErrNotFound},
		{500, `boom`, ErrServer},
		{503, `unavailable`, ErrServer},
		{400, `{"code":11101,"msg":"bad param"}`, ErrClient},
		{200, `{"checked_in":false}`, ErrNone},
	}
	for _, c := range cases {
		if got := Classify(c.status, c.body); got != c.want {
			t.Errorf("Classify(%d,%q)=%v want %v", c.status, c.body, got, c.want)
		}
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testClient(fn rtFunc) *Client {
	return &Client{
		HTTP:      &http.Client{Transport: fn},
		AgentHost: "https://agent.example",
		UgHost:    "https://ug.example",
		OAuthHost: "https://oauth.example",
		ClientID:  ClientID,
	}
}

// fakeAuth 合成测试凭证（不含任何真实 token）。
func fakeAuth() *traeAuth {
	return &traeAuth{
		AccessToken:  "at-placeholder",
		RefreshToken: "rt-placeholder",
		ExpiresAt:    1,
		ApiHost:      "https://oauth.example",
		UID:          "1000000000000000",
	}
}

func TestRefreshTokenExchange(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, EpExchange) {
			return nil, errors.New("wrong path: " + r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			return nil, errors.New("missing content-type")
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"ClientID":"en1oxy7wnw8j9n"`)) || !bytes.Contains(body, []byte(`"RefreshToken":"rt-placeholder"`)) {
			return nil, errors.New("bad body: " + string(body))
		}
		return jsonResp(200, `{"Result":{"Token":"newat","RefreshToken":"newrt","TokenExpireAt":1786805537,"TokenExpireDuration":1209600}}`), nil
	})
	a := fakeAuth()
	if err := c.RefreshToken(a); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if a.AccessToken != "newat" || a.RefreshToken != "newrt" {
		t.Errorf("tokens not updated: %+v", a)
	}
	if a.ExpiresAt != 1786805537 {
		t.Errorf("expiresAt=%d", a.ExpiresAt)
	}
}

// TestRefreshTokenExchangeMilliseconds 覆盖上游 TokenExpireAt 返回毫秒的场景：
// 必须归一化为 Unix 秒后再写 ExpiresAt。
func TestRefreshTokenExchangeMilliseconds(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"Result":{"Token":"newat","RefreshToken":"newrt","TokenExpireAt":1786847930141,"TokenExpireDuration":1209600}}`), nil
	})
	a := fakeAuth()
	if err := c.RefreshToken(a); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if a.ExpiresAt != 1786847930 {
		t.Errorf("expiresAt=%d want 1786847930 (毫秒转秒)", a.ExpiresAt)
	}
}

func TestRefreshTokenIfNeededSkipsFresh(t *testing.T) {
	calls := 0
	c := testClient(func(r *http.Request) (*http.Response, error) {
		calls++
		return jsonResp(200, `{"Result":{"Token":"newat","RefreshToken":"newrt","TokenExpireAt":1786847930141}}`), nil
	})
	a := fakeAuth()
	a.ExpiresAt = 9999999999
	refreshed, err := c.RefreshTokenIfNeeded(a, 24*3600*1e9)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed {
		t.Error("fresh token should not refresh")
	}
	if calls != 0 {
		t.Errorf("ExchangeToken should not be called, calls=%d", calls)
	}
	if a.AccessToken != "at-placeholder" {
		t.Error("token should remain unchanged")
	}
}

func TestRefreshTokenIfNeededRefreshesExpired(t *testing.T) {
	calls := 0
	c := testClient(func(r *http.Request) (*http.Response, error) {
		calls++
		return jsonResp(200, `{"Result":{"Token":"newat","RefreshToken":"newrt","TokenExpireAt":1786847930141}}`), nil
	})
	a := fakeAuth()
	refreshed, err := c.RefreshTokenIfNeeded(a, 24*3600*1e9)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed || calls != 1 {
		t.Errorf("expired token should refresh once, refreshed=%v calls=%d", refreshed, calls)
	}
	if a.AccessToken != "newat" || a.RefreshToken != "newrt" {
		t.Errorf("tokens not updated: %+v", a)
	}
}

func TestRefreshTokenUsesAuthApiHost(t *testing.T) {
	var gotHost string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotHost = r.URL.Scheme + "://" + r.URL.Host
		return jsonResp(200, `{"Result":{"Token":"newat"}}`), nil
	})
	a := fakeAuth()
	a.ApiHost = "https://custom.example"
	if err := c.RefreshToken(a); err != nil {
		t.Fatal(err)
	}
	if gotHost != "https://custom.example" {
		t.Errorf("host=%s want auth.apiHost", gotHost)
	}
}

func TestChatStreamSendsHeadersAndRewritesBody(t *testing.T) {
	var gotAuth, gotUID, gotAppID, gotIdeVer, gotMachine, gotDevID string
	var gotBody []byte
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		gotUID = r.Header.Get("X-Uid")
		gotAppID = r.Header.Get("X-App-Id")
		gotIdeVer = r.Header.Get("X-Ide-Version")
		gotMachine = r.Header.Get("X-Machine-Id")
		gotDevID = r.Header.Get("X-Device-Id")
		gotBody, _ = io.ReadAll(r.Body)
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader("event:done\ndata:{\"finish_reason\":\"stop\"}\n\n")),
		}, nil
	})
	a := fakeAuth()
	a.MachineID = "m1"
	a.DeviceID = "d1"
	rc, status, respBody, err := c.ChatStream(a, []byte(`{"model":"glm-5.2","messages":[]}`))
	if err != nil || status != 200 {
		t.Fatalf("chat: status=%d err=%v", status, err)
	}
	if respBody != nil {
		t.Errorf("200 response should carry nil body, got %q", respBody)
	}
	rc.Close()
	if gotAuth != "Cloud-IDE-JWT at-placeholder" || gotUID != "1000000000000000" {
		t.Errorf("headers: auth=%q uid=%q", gotAuth, gotUID)
	}
	if gotAppID != AppID || gotIdeVer != IdeVersion {
		t.Errorf("app headers: appid=%q idever=%q", gotAppID, gotIdeVer)
	}
	if gotMachine != "m1" || gotDevID != "d1" {
		t.Errorf("device headers: machine=%q dev=%q", gotMachine, gotDevID)
	}
	if !bytes.Contains(gotBody, []byte(`"stream":true`)) || !bytes.Contains(gotBody, []byte(`"function":"solo_work_lite"`)) {
		t.Errorf("body not rewritten: %s", gotBody)
	}
}

func TestChatStreamUsesDedicatedStreamClient(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader("event:done\ndata:{\"finish_reason\":\"stop\"}\n\n")),
		}, nil
	})
	c.StreamHTTP = &http.Client{Transport: c.HTTP.Transport} // 无 Timeout
	rc, status, _, err := c.ChatStream(fakeAuth(), []byte(`{"model":"glm-5.2","messages":[]}`))
	if err != nil || status != 200 {
		t.Fatalf("chat: status=%d err=%v", status, err)
	}
	rc.Close()
	if c.StreamHTTP.Timeout != 0 {
		t.Errorf("stream client should have no total timeout, got %v", c.StreamHTTP.Timeout)
	}
}

func TestChatStreamHTTPError(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(429, `rate limited`), nil
	})
	_, status, respBody, err := c.ChatStream(fakeAuth(), []byte(`{}`))
	if status != 429 {
		t.Errorf("status=%d", status)
	}
	if err != nil {
		t.Fatalf("429 should come via status, err=%v", err)
	}
	if Classify(status, string(respBody)) != ErrSoftRate {
		t.Errorf("not classified soft rate: %q", respBody)
	}
}

func TestUserEntUsageAggregation(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, EpEntUsage) {
			return nil, errors.New("wrong path: " + r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Cloud-IDE-JWT at-placeholder" {
			return nil, errors.New("missing auth header")
		}
		return jsonResp(200, `{"is_credits_billing":true,"user_entitlement_pack_list":[
			{"entitlement_base_info":{"quota":{"credits_limit":2000}},"usage":{"credits_amount":0}},
			{"entitlement_base_info":{"quota":{"credits_limit":500}},"usage":{"credits_amount":0}}
		]}`), nil
	})
	remain, err := c.UserEntUsage(fakeAuth())
	if err != nil {
		t.Fatalf("ent usage: %v", err)
	}
	if remain != 2500 {
		t.Errorf("remain=%d want 2500", remain)
	}
}

// TestUserEntUsageSubtractsUsed 剩余 = Σ(limit) − Σ(used)，不能把总额度当剩余。
func TestUserEntUsageSubtractsUsed(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"user_entitlement_pack_list":[
			{"entitlement_base_info":{"quota":{"credits_limit":2000}},"usage":{"credits_amount":750.5}},
			{"entitlement_base_info":{"quota":{"credits_limit":500}},"usage":{"credits_amount":100}}
		]}`), nil
	})
	remain, err := c.UserEntUsage(fakeAuth())
	if err != nil {
		t.Fatalf("ent usage: %v", err)
	}
	if remain != 1649 { // 2500 - 850.5 → 1649.5 → 截断为 1649
		t.Errorf("remain=%d want 1649", remain)
	}
}

func TestUserEntUsageClampsNegative(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"user_entitlement_pack_list":[
			{"entitlement_base_info":{"quota":{"credits_limit":100}},"usage":{"credits_amount":900}}
		]}`), nil
	})
	remain, err := c.UserEntUsage(fakeAuth())
	if err != nil {
		t.Fatalf("ent usage: %v", err)
	}
	if remain != 0 {
		t.Errorf("remain=%d want 0 (clamped)", remain)
	}
}

func TestCheckinStatusSendsOfficialDeviceHeaders(t *testing.T) {
	var path string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		path = r.URL.Path
		if r.Header.Get("X-Device-Id") != "1111222233334444" ||
			r.Header.Get("X-Device-Brand") != "Mac16,10" ||
			r.Header.Get("X-Device-Type") != "mac" {
			return nil, errors.New("missing official device headers")
		}
		return jsonResp(200, `{"checked_in":false,"credits":200,"enable":true}`), nil
	})
	a := fakeAuth()
	a.DeviceID = "model-device"
	a.CheckinDeviceID = "1111222233334444"
	a.CheckinDeviceBrand = "Mac16,10"
	a.CheckinDeviceType = "mac"
	checkedIn, credits, enable, err := c.CheckinStatus(a)
	if err != nil {
		t.Fatal(err)
	}
	if checkedIn || !enable || credits != 200 {
		t.Errorf("status: checked=%v enable=%v credits=%d", checkedIn, enable, credits)
	}
	if path != EpCheckinStatus {
		t.Errorf("path=%s", path)
	}
}

func TestCheckinStatusRejectsBusinessFailure(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":1002,"message":"device unavailable"}`), nil
	})
	_, _, _, err := c.CheckinStatus(fakeAuth())
	if err == nil || !strings.Contains(err.Error(), "device unavailable") {
		t.Fatalf("error=%v want status business failure", err)
	}
}

func TestCheckinStatusRejectsMissingStateFields(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":0,"message":"success"}`), nil
	})
	_, _, _, err := c.CheckinStatus(fakeAuth())
	if err == nil || !strings.Contains(err.Error(), "missing checked_in or enable") {
		t.Fatalf("error=%v want protocol failure", err)
	}
}

func TestCheckinClaimRejectsBusinessFailure(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":1001,"message":"device rejected"}`), nil
	})
	err := c.CheckinClaim(fakeAuth())
	if err == nil || !strings.Contains(err.Error(), "device rejected") {
		t.Fatalf("error=%v want business failure", err)
	}
}

func TestCheckinClaimAcceptsBusinessSuccess(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != EpCheckinClaim {
			return nil, errors.New("wrong path: " + r.URL.Path)
		}
		return jsonResp(200, `{"code":0,"message":"success"}`), nil
	})
	if err := c.CheckinClaim(fakeAuth()); err != nil {
		t.Fatal(err)
	}
}

func TestFetchModelsParsesUpstream(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, EpModels) {
			return nil, errors.New("wrong path: " + r.URL.Path)
		}
		return jsonResp(200, `{"config_info_list":[
			{"config_name":"glm-5.2","display_config":{"display_name":"GLM-5.2","max_mode":true,"multimodal":false},
			 "display_contact_config":"{\"consumption_rate\":{\"data\":{\"rate\":1.5}}}",
			 "context_window_tokens":{"dev":131072,"max":200000},
			 "reasoning_effort_config":{"support_thinking":true,"default_level":"medium","options":["low","medium","high"]},
			 "model_detail_list":[{"model_name":"glm-5.2__dev","prompt_max_tokens":100000,"max_tokens":8192},
			                      {"model_name":"glm-5.2__max","prompt_max_tokens":180000,"max_tokens":16384}]},
			{"config_name":"","display_config":{}}
		]}`), nil
	})
	infos, err := c.FetchModels(fakeAuth())
	if err != nil {
		t.Fatalf("fetch models: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("empty config_name should be skipped, got %d", len(infos))
	}
	m := infos[0]
	if m.ID != "glm-5.2" || m.Name != "GLM-5.2" || !m.MaxMode {
		t.Errorf("model=%+v", m)
	}
	if m.ContextWindow != 131072 || m.MaxContextWindow != 200000 {
		t.Errorf("context: dev=%d max=%d", m.ContextWindow, m.MaxContextWindow)
	}
	if m.InputTokens != 100000 || m.MaxTokens != 8192 {
		t.Errorf("dev tokens: in=%d out=%d", m.InputTokens, m.MaxTokens)
	}
	if m.MaxModeInputTokens != 180000 || m.MaxModeOutputTokens != 16384 {
		t.Errorf("max tokens: in=%d out=%d", m.MaxModeInputTokens, m.MaxModeOutputTokens)
	}
	if m.ConsumptionRate != 1.5 {
		t.Errorf("consumption rate=%v", m.ConsumptionRate)
	}
	if len(m.ReasoningEfforts) != 3 || m.DefaultReasoningEffort != "medium" {
		t.Errorf("reasoning=%v default=%q", m.ReasoningEfforts, m.DefaultReasoningEffort)
	}
}

func TestFetchModelsRejectsEmptyList(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"config_info_list":[]}`), nil
	})
	if _, err := c.FetchModels(fakeAuth()); err == nil {
		t.Fatal("empty model list should be an error (so callers fall back to static)")
	}
}

func TestGetUserInfo(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(r.URL.Path, EpUserInfo) {
			return nil, errors.New("wrong path: " + r.URL.Path)
		}
		if r.Header.Get("X-Cloudide-Token") != "at-placeholder" {
			return nil, errors.New("missing X-Cloudide-Token")
		}
		return jsonResp(200, `{"Result":{"UserID":"u-1","ScreenName":"nick","EnterpriseID":"ent-1"}}`), nil
	})
	uid, nick, ent, err := c.GetUserInfo(fakeAuth())
	if err != nil {
		t.Fatal(err)
	}
	if uid != "u-1" || nick != "nick" || ent != "ent-1" {
		t.Errorf("got (%q,%q,%q)", uid, nick, ent)
	}
}
