package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"traework2api/internal/auth"
)

// TestRemoteSendMessageMaxStrategy P0：maxMode=true 时发消息 body 顶层
// model_selection_strategy 必须是 "max"；false 时必须保持模板原样 "manual"。
func TestRemoteSendMessageMaxStrategy(t *testing.T) {
	var gotStrategy string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("body: %v", err)
		}
		gotStrategy, _ = body["model_selection_strategy"].(string)
		return jsonResp(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	if _, err := c.RemoteSendMessage(a, "s1", "glm-5.3", "hi", true); err != nil {
		t.Fatal(err)
	}
	if gotStrategy != "max" {
		t.Errorf("maxMode=true: strategy=%q want max", gotStrategy)
	}
	if _, err := c.RemoteSendMessage(a, "s1", "glm-5.3", "hi", false); err != nil {
		t.Fatal(err)
	}
	if gotStrategy != "manual" {
		t.Errorf("maxMode=false: strategy=%q want manual（非 Max 请求行为不得改变）", gotStrategy)
	}
}

func TestSplitMaxSuffix(t *testing.T) {
	cases := []struct {
		in   string
		want string
		max  bool
	}{
		{"glm-5.3-max", "glm-5.3", true},
		{"DeepSeek-V4-Flash-Official-max", "DeepSeek-V4-Flash-Official", true},
		{"glm-5.3", "glm-5.3", false},
		{"", "", false},
		{"-max", "-max", false}, // 纯后缀不是合法模型名
		{"glm-max", "glm", true},
		{"max", "max", false},
	}
	for _, tc := range cases {
		got, isMax := SplitMaxSuffix(tc.in)
		if got != tc.want || isMax != tc.max {
			t.Errorf("SplitMaxSuffix(%q) = (%q,%v) want (%q,%v)", tc.in, got, isMax, tc.want, tc.max)
		}
	}
}

func TestRemoteSendMessageEncodesQueryAsJSON(t *testing.T) {
	const userText = "他说\"好\"\n路径\\tmp"
	var gotBody map[string]any
	c := testClient(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Fatalf("request body: %v", err)
		}
		return jsonResp(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
	})
	a := &auth.Auth{
		AccessToken: "at-2",
		UID:         "account-2",
		DeviceID:    "device-2",
		MachineID:   "machine-2",
	}

	if _, err := c.RemoteSendMessage(a, "session-1", "glm-5.3", userText, false); err != nil {
		t.Fatal(err)
	}

	var query []struct {
		Type string `json:"type"`
		Data struct {
			Content string `json:"content"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(gotBody["query"].(string)), &query); err != nil {
		t.Fatalf("inner query is not JSON: %v query=%q", err, gotBody["query"])
	}
	if len(query) != 1 || query[0].Data.Content != userText {
		t.Errorf("query=%+v", query)
	}
}

func TestRemoteSendMessageUsesSelectedAccountIdentity(t *testing.T) {
	var gotBody map[string]any
	var gotDeviceID, gotMachineID string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotDeviceID = r.Header.Get("X-Device-Id")
		gotMachineID = r.Header.Get("X-Machine-Id")
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Fatalf("request body: %v", err)
		}
		return jsonResp(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
	})
	a := &auth.Auth{
		AccessToken: "at-2",
		UID:         "account-2",
		DeviceID:    "device-2",
		MachineID:   "machine-2",
	}

	if _, err := c.RemoteSendMessage(a, "session-1", "glm-5.3", "OK", false); err != nil {
		t.Fatal(err)
	}

	commonParams, _ := gotBody["common_params"].(string)
	var common map[string]any
	if err := json.Unmarshal([]byte(commonParams), &common); err != nil {
		t.Fatalf("common_params: %v", err)
	}
	if common["icube_uid"] != "account-2" || common["device_id"] != "device-2" || common["machine_id"] != "machine-2" {
		t.Errorf("account identity=%v", common)
	}
	if gotDeviceID != "device-2" || gotMachineID != "machine-2" {
		t.Errorf("headers device=%q machine=%q", gotDeviceID, gotMachineID)
	}
}

func TestExtractAssistantTextPrefersFinishSummaryOverReasoning(t *testing.T) {
	content := `{"messages":[{"type":"plan_item","plan_item":{"reasoning_content":"内部思考","tool_call_info":{"name":"finish","params":{"summary":"OK"}}}}]}`

	if got := extractAssistantText(content); got != "OK" {
		t.Errorf("reply=%q want final summary only", got)
	}
}

func TestRemoteSendMessageParallelLimitIsRetryableBusy(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(429, `{"code":991502,"error":{"reason":"solo_agent_parallel_limit","limit":2,"running":2},"message":"solo agent parallel limit reached"}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	_, err := c.RemoteSendMessage(a, "s1", "glm-5.3", "hi", false)
	if !errors.Is(err, ErrRemoteBusy) {
		t.Fatalf("err=%v want ErrRemoteBusy", err)
	}
}

func TestRemoteDeleteSession(t *testing.T) {
	var gotMethod, gotPath string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotMethod, gotPath = r.Method, r.URL.Path
		return jsonResp(200, `{"code":0,"message":"success"}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	if err := c.RemoteDeleteSession(a, "s1"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/api/remote/v1/chat_sessions/s1" {
		t.Errorf("got %s %s", gotMethod, gotPath)
	}
}

func TestRemoteWaitAndReadReturnsOnContextCancel(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":0,"data":{"items":[{"role":"user","status":"in_progress"}]}}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.RemoteWaitAndRead(ctx, a, "s1", time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took %s, ctx cancel should return promptly", elapsed)
	}
}

// 看门狗回归：排队中（消息数 0）不算卡死——慢启动（重任务实测可达
// 13 分钟）只受总 timeout 约束，不触发 stall 误杀。
func TestRemoteWaitAndReadQueuedNotStallKilled(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":0,"data":{"items":[]}}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	origInterval, origLimit := remotePollInterval, remoteStallLimit
	remotePollInterval, remoteStallLimit = 20*time.Millisecond, 3
	defer func() { remotePollInterval, remoteStallLimit = origInterval, origLimit }()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	_, err := c.RemoteWaitAndRead(ctx, a, "s1", time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("排队中不应被 stall 误杀, err=%v", err)
	}
}

// 看门狗回归：消息已存在且完全静止（状态/内容长度不变）→ 阈值内判卡死。
func TestRemoteWaitAndReadStaticProgressStallKilled(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"in_progress","content":"partial"}]}}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	origInterval, origLimit := remotePollInterval, remoteStallLimit
	remotePollInterval, remoteStallLimit = 20*time.Millisecond, 3
	defer func() { remotePollInterval, remoteStallLimit = origInterval, origLimit }()

	_, err := c.RemoteWaitAndRead(context.Background(), a, "s1", time.Minute)
	if !errors.Is(err, ErrRemoteTimeout) || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("静止任务应被判卡死, err=%v", err)
	}
}

// 看门狗回归：内容长度持续增长（长生成中）→ 不算卡死。
func TestRemoteWaitAndReadGrowingContentNotStallKilled(t *testing.T) {
	var n int
	c := testClient(func(r *http.Request) (*http.Response, error) {
		n++
		return jsonResp(200, fmt.Sprintf(`{"code":0,"data":{"items":[{"role":"assistant","status":"in_progress","content":"%s"}]}}`, strings.Repeat("x", n))), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	origInterval, origLimit := remotePollInterval, remoteStallLimit
	remotePollInterval, remoteStallLimit = 20*time.Millisecond, 3
	defer func() { remotePollInterval, remoteStallLimit = origInterval, origLimit }()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	_, err := c.RemoteWaitAndRead(ctx, a, "s1", time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("内容增长中不应被 stall 误杀, err=%v", err)
	}
}

// 旧终稿防护回归（快照锚）：baseline=5（发送前快照的最大 message_index），
// 上一轮的 assistant completed 终稿(idx=3 ≤ 5)不得被当作本轮回复立即返回；
// 等 baseline 之后的新回复(idx=6)。
func TestRemoteWaitAndReadSkipsStaleReplyBeforeSnapshotAnchor(t *testing.T) {
	step := 0
	c := testClient(func(r *http.Request) (*http.Response, error) {
		step++
		switch step {
		case 1: // 首轮：旧终稿(idx=3) + 本次 user 发送(idx=5)，新回复未生成
			return jsonResp(200, `{"code":0,"data":{"items":[
				{"role":"assistant","status":"completed","content":"STALE","message_index":3},
				{"role":"user","status":"completed","content":"incremental","message_index":5}]}}`), nil
		default: // 后续轮：新回复出现（idx=6）
			return jsonResp(200, `{"code":0,"data":{"items":[
				{"role":"assistant","status":"completed","content":"STALE","message_index":3},
				{"role":"user","status":"completed","content":"incremental","message_index":5},
				{"role":"assistant","status":"completed","content":"FRESH","message_index":6}]}}`), nil
		}
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}
	orig := remotePollInterval
	remotePollInterval = 20 * time.Millisecond
	defer func() { remotePollInterval = orig }()

	got, err := c.RemoteWaitAndRead(context.Background(), a, "s1", 5*time.Second, 5)
	if err != nil {
		t.Fatal(err)
	}
	if got != "FRESH" {
		t.Fatalf("got %q want FRESH（旧终稿 STALE 不得立即返回）", got)
	}
}

// 快照锚缺省（-1）：不设锚，最后一条 assistant completed 直接认领
//（新建会话/等待既有任务完成的路径语义不变）。
func TestRemoteWaitAndReadNoAnchorKeepsLegacyBehavior(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":0,"data":{"items":[
			{"role":"user","status":"completed","content":"q","message_index":1},
			{"role":"assistant","status":"completed","content":"ANSWER","message_index":2}]}}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}
	got, err := c.RemoteWaitAndRead(context.Background(), a, "s1", 2*time.Second)
	if err != nil || got != "ANSWER" {
		t.Fatalf("got %q err=%v want ANSWER", got, err)
	}
}
