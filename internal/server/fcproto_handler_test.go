// fcproto_handler_test.go — 协议模式 handler 级 E2E（fake upstream 全链路）：
// 覆盖日常使用形态——首腿 tool_calls、工具结果回传续答、云端自跑检测重试、
// 非流式 tool_calls 响应。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

// readAllBody / parseJSONBody 测试小助手。
func readAllBody(r *http.Request) []byte {
	raw, _ := io.ReadAll(r.Body)
	return raw
}

func parseJSONBody(raw []byte, v any) error {
	return json.Unmarshal(raw, v)
}

// fcFakeUp 可编程 fake upstream：按 session 记录发送/轮询序列。
type fcFakeUp struct {
	mu      sync.Mutex
	creates int
	sends   map[string]string // sessionID -> 最后一次发送文本
	sendsN  map[string]int
	deletes []string
	polls   map[string]int    // sessionID -> poll 次数
	replies map[string]string // sessionID -> poll 回复（content 纯文本）
	snapMax int               // snapshot（page_size=5）返回的 max message_index
}

func newFcFakeUp(snapshotMax int) *fcFakeUp {
	return &fcFakeUp{
		sends: map[string]string{}, sendsN: map[string]int{},
		polls: map[string]int{}, replies: map[string]string{},
		snapMax: snapshotMax,
	}
}

func (f *fcFakeUp) client() *upstream.Client {
	return &upstream.Client{HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
			f.creates++
			return jsonHTTPResponse(200, fmt.Sprintf(`{"code":0,"data":{"chat_session_id":"cs%d"}}`, f.creates)), nil
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages"):
			sid := pathSessionID(r.URL.Path)
			raw := readAllBody(r)
			var body map[string]any
			_ = parseJSONBody(raw, &body)
			q, _ := body["query"].(string)
			var query []struct {
				Data struct {
					Content string `json:"content"`
				} `json:"data"`
			}
			_ = parseJSONBody([]byte(q), &query)
			text := ""
			if len(query) > 0 {
				text = query[0].Data.Content
			}
			f.sends[sid] = text
			f.sendsN[sid]++
			return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/messages"):
			sid := pathSessionID(r.URL.Path)
			if strings.Contains(r.URL.RawQuery, "page_size=5") {
				// 快照锚探测：只回 user 项（max index）
				return jsonHTTPResponse(200, fmt.Sprintf(`{"code":0,"data":{"items":[{"role":"user","status":"completed","content":"x","message_index":%d}]}}`, f.snapMax)), nil
			}
			f.polls[sid]++
			return jsonHTTPResponse(200, fmt.Sprintf(`{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":%q,"message_index":%d}]}}`,
				f.replies[sid], f.snapMax+2)), nil
		case r.Method == http.MethodDelete:
			f.deletes = append(f.deletes, pathSessionID(r.URL.Path))
			return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
		case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
			return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"DeepSeek-V4-Flash-Official"}]}`), nil
		default:
			return jsonHTTPResponse(404, `{"error":"unexpected"}`), nil
		}
	})},
}
}

func fcToolsJSONField() string {
	return `"tools":[{"type":"function","function":{"name":"Read","description":"Read a file","parameters":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}}]`
}

func fcReqBody(msgsJSON string) string {
	return `{"model":"DeepSeek-V4-Flash-Official","stream":true,` + fcToolsJSONField() + `,"messages":` + msgsJSON + `}`
}

func fcPost(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body)))
	return rec
}

const fcReplyToolCall = "<｜DSML｜tool_call>{\"name\": \"Read\", \"arguments\": {\"file_path\": \"/etc/hosts\"}}</｜DSML｜tool_call>"
const fcReplyFinal = "The file has 9 lines."

// fcSSEFrames 提取 SSE data 帧列表。
func fcSSEFrames(t *testing.T, body string) []map[string]any {
	t.Helper()
	var frames []map[string]any
	for _, ln := range strings.Split(body, "\n") {
		ln = strings.TrimSpace(ln)
		if !strings.HasPrefix(ln, "data: ") || ln == "data: [DONE]" {
			continue
		}
		var m map[string]any
		if err := parseJSONBody([]byte(strings.TrimPrefix(ln, "data: ")), &m); err == nil {
			frames = append(frames, m)
		}
	}
	return frames
}

// TestServeRemoteFCEndToEnd 首腿：带 tools 请求 → 协议头发云端 → 云端
// DSML tool_call 回复 → 原生 tool_calls 帧。
func TestServeRemoteFCEndToEnd(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	fake := newFcFakeUp(0)
	fake.replies["cs1"] = fcReplyToolCall
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      fake.client(),
		ConvStorePath: isolatedConvStore(),
	})

	rec := fcPost(t, h, fcReqBody(`[{"role":"system","content":"You are a coding agent."},{"role":"user","content":"How many lines does /etc/hosts have?"}]`))
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	// 发往云端的文本：协议头 + 工具 JSON + 用户消息，且不含桥工具痕迹
	sent := fake.sends["cs1"]
	for _, want := range []string{"API GATEWAY SIMULATION", `"name":"Read"`, "How many lines does /etc/hosts have?", "<system>", "<user_message>"} {
		if !strings.Contains(sent, want) {
			t.Errorf("发送文本缺少 %q:\n---\n%s\n---", want, sent)
		}
	}

	// 响应：原生 tool_calls 帧 + finish tool_calls
	frames := fcSSEFrames(t, rec.Body.String())
	var sawToolCall, sawFinish bool
	for _, fr := range frames {
		chs, _ := fr["choices"].([]any)
		if len(chs) == 0 {
			continue
		}
		ch := chs[0].(map[string]any)
		delta, _ := ch["delta"].(map[string]any)
		if tcs, ok := delta["tool_calls"].([]any); ok && len(tcs) > 0 {
			sawToolCall = true
			tc := tcs[0].(map[string]any)
			fn := tc["function"].(map[string]any)
			if fn["name"] != "Read" {
				t.Errorf("工具名=%v want Read", fn["name"])
			}
			if tc["id"] == "" {
				t.Error("tool_call 缺 id")
			}
		}
		if ch["finish_reason"] == "tool_calls" {
			sawFinish = true
		}
	}
	if !sawToolCall || !sawFinish {
		t.Errorf("sawToolCall=%v sawFinish=%v\n%s", sawToolCall, sawFinish, rec.Body.String())
	}
	if fake.creates != 1 {
		t.Errorf("creates=%d want 1", fake.creates)
	}
}

// TestServeRemoteFCToolResultReuse 第二腿：工具结果回传 → 复用会话增量
// append（<tool_call> 回显 + <tool_result>）→ 干净终稿（无注记/协议残留）。
func TestServeRemoteFCToolResultReuse(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	fake := newFcFakeUp(2) // turn2 快照锚：max index=2
	fake.replies["cs1"] = fcReplyToolCall
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      fake.client(),
		ConvStorePath: isolatedConvStore(),
	})

	// turn1
	rec1 := fcPost(t, h, fcReqBody(`[{"role":"user","content":"count lines"}]`))
	if rec1.Code != 200 {
		t.Fatalf("turn1 code=%d", rec1.Code)
	}
	// turn2：assistant tool_calls 回显 + tool 结果
	turn2 := `[
		{"role":"user","content":"count lines"},
		{"role":"assistant","content":"","tool_calls":[{"id":"call_fc1_0","type":"function","function":{"name":"Read","arguments":"{\"file_path\": \"/etc/hosts\"}"}}]},
		{"role":"tool","tool_call_id":"call_fc1_0","content":"line1\nline2"}
	]`
	fake.replies["cs1"] = fcReplyFinal
	rec2 := fcPost(t, h, fcReqBody(turn2))
	if rec2.Code != 200 {
		t.Fatalf("turn2 code=%d body=%s", rec2.Code, rec2.Body.String())
	}

	if fake.creates != 1 {
		t.Errorf("creates=%d want 1（第二腿必须复用会话）", fake.creates)
	}
	// 第二次发送：协议块回显 + 结果
	sent2 := fake.sends["cs1"]
	for _, want := range []string{
		`<tool_call>{"name": "Read", "arguments": {"file_path": "/etc/hosts"}}</tool_call>`,
		`<tool_result tool_call_id="call_fc1_0">`,
		"line1\nline2",
	} {
		if !strings.Contains(sent2, want) {
			t.Errorf("turn2 发送文本缺少 %q:\n---\n%s\n---", want, sent2)
		}
	}
	// 响应：干净终稿
	frames := fcSSEFrames(t, rec2.Body.String())
	var content strings.Builder
	sawToolCalls := false
	for _, fr := range frames {
		chs, _ := fr["choices"].([]any)
		if len(chs) == 0 {
			continue
		}
		ch := chs[0].(map[string]any)
		if delta, ok := ch["delta"].(map[string]any); ok {
			if c, ok := delta["content"].(string); ok {
				content.WriteString(c)
			}
			if _, ok := delta["tool_calls"]; ok {
				sawToolCalls = true
			}
		}
	}
	got := content.String()
	if got != fcReplyFinal {
		t.Errorf("终稿=%q want %q", got, fcReplyFinal)
	}
	if sawToolCalls {
		t.Error("终稿轮不应有 tool_calls 帧")
	}
	if strings.Contains(got, "tool_call") || strings.Contains(got, "🔧") || strings.Contains(got, "---") {
		t.Errorf("终稿被污染: %q", got)
	}
}

// TestServeRemoteFCSelfRunRetry 云端自跑沙盒工具（协议失败形态）→ 检测后
// 自动重试一次（新会话），旧会话删除释放并发槽。
func TestServeRemoteFCSelfRunRetry(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	fake := newFcFakeUp(0)
	// cs1 = 自跑回复（沙盒 Read 自答）；cs2 = 遵循协议
	fake.replies["cs1"] = "\n\n> 🔧 [沙盒] EnvironmentSetup\n\n\n> ✅ [EnvironmentSetup] MCP Servers: ✓\n\n> 🔧 [沙盒] Read\n8"
	fake.replies["cs2"] = fcReplyToolCall
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      fake.client(),
		ConvStorePath: isolatedConvStore(),
	})

	rec := fcPost(t, h, fcReqBody(`[{"role":"user","content":"How many lines does /etc/hosts have?"}]`))
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if fake.creates != 2 {
		t.Errorf("creates=%d want 2（自跑后应重试一次）", fake.creates)
	}
	if len(fake.deletes) != 1 || fake.deletes[0] != "cs1" {
		t.Errorf("deletes=%v want [cs1]（旧会话须删除释放并发槽）", fake.deletes)
	}
	frames := fcSSEFrames(t, rec.Body.String())
	var sawToolCall bool
	for _, fr := range frames {
		chs, _ := fr["choices"].([]any)
		if len(chs) == 0 {
			continue
		}
		if delta, ok := chs[0].(map[string]any)["delta"].(map[string]any); ok {
			if _, ok := delta["tool_calls"]; ok {
				sawToolCall = true
			}
		}
	}
	if !sawToolCall {
		t.Errorf("重试后应输出 tool_calls:\n%s", rec.Body.String())
	}
}

// TestServeRemoteFCSelfRunRetryDisabled env 关闭重试 → 保持单次行为。
func TestServeRemoteFCSelfRunRetryDisabled(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	t.Setenv("TW2API_DISABLE_FC_RETRY", "1")
	fake := newFcFakeUp(0)
	fake.replies["cs1"] = "\n\n> 🔧 [沙盒] Read\n8"
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      fake.client(),
		ConvStorePath: isolatedConvStore(),
	})

	rec := fcPost(t, h, fcReqBody(`[{"role":"user","content":"q"}]`))
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if fake.creates != 1 {
		t.Errorf("creates=%d want 1（重试已禁用）", fake.creates)
	}
}

// TestServeRemoteFCNonStreamToolCalls 非流式 + tools → 完整 tool_calls 响应。
func TestServeRemoteFCNonStreamToolCalls(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	fake := newFcFakeUp(0)
	fake.replies["cs1"] = fcReplyToolCall
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      fake.client(),
		ConvStorePath: isolatedConvStore(),
	})

	body := strings.Replace(fcReqBody(`[{"role":"user","content":"q"}]`), `"stream":true`, `"stream":false`, 1)
	rec := fcPost(t, h, body)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := parseJSONBody(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应非 JSON: %v\n%s", err, rec.Body.String())
	}
	ch := resp["choices"].([]any)[0].(map[string]any)
	if ch["finish_reason"] != "tool_calls" {
		t.Errorf("finish_reason=%v", ch["finish_reason"])
	}
	msg := ch["message"].(map[string]any)
	tcs := msg["tool_calls"].([]any)
	if len(tcs) != 1 {
		t.Fatalf("tool_calls=%v", tcs)
	}
	fn := tcs[0].(map[string]any)["function"].(map[string]any)
	if fn["name"] != "Read" {
		t.Errorf("name=%v", fn["name"])
	}
}
