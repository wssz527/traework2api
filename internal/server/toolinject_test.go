// toolinject_test.go — tool_calls 闭环单测：pending 注册表生命周期、
// /internal/inject-result 端点、订阅分支 pending→tool_calls 帧序列、
// role:tool 第二轮回灌短路。
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

// ---------------------------------------------------------------------------
// pendingRegistry 单元
// ---------------------------------------------------------------------------

func TestPendingRegistryLifecycle(t *testing.T) {
	r := newPendingRegistry()
	if r.get("p1") != nil {
		t.Fatal("空注册表不应命中")
	}
	ch := r.register("p1", "s1", "run_shell")
	if ch == nil {
		t.Fatal("register 应返回 channel")
	}
	// 重复登记幂等：返回同一 channel，不覆盖条目。
	if again := r.register("p1", "s1", "run_shell"); again != ch {
		t.Fatalf("重复登记应幂等: %p != %p", again, ch)
	}
	e := r.get("p1")
	if e == nil || e.sessID != "s1" || e.tool != "run_shell" {
		t.Fatalf("get=%+v", e)
	}
	// 按会话反查（双向）。
	if got := r.bySession("s1"); len(got) != 1 || got[0] != "p1" {
		t.Fatalf("bySession=%v", got)
	}
	if got := r.bySession("s-other"); len(got) != 0 {
		t.Fatalf("无关会话不应命中: %v", got)
	}
	// 回注塞渠道。
	ok := true
	if !r.inject("p1", McpEvent{Event: "tool_result_injected", PendingID: "p1", OK: &ok}) {
		t.Fatal("inject 应成功")
	}
	select {
	case ev := <-ch:
		if ev.PendingID != "p1" {
			t.Fatalf("渠道事件=%+v", ev)
		}
	default:
		t.Fatal("channel 里应有回注事件")
	}
	r.drop("p1")
	if r.get("p1") != nil {
		t.Fatal("drop 后不应命中")
	}
	if r.inject("p1", McpEvent{}) {
		t.Fatal("drop 后 inject 应失败")
	}
}

func TestPendingRegistryTTLExpiry(t *testing.T) {
	old := pendingTTL
	pendingTTL = 30 * time.Millisecond
	defer func() { pendingTTL = old }()
	r := newPendingRegistry()
	r.register("p1", "s1", "t")
	time.Sleep(60 * time.Millisecond)
	// TTL 过后任意入口都应把条目清掉（桥同窗口已本地兜底）。
	if r.get("p1") != nil {
		t.Fatal("过期条目应被惰性清理")
	}
	if got := r.bySession("s1"); len(got) != 0 {
		t.Fatalf("过期条目不应出现在 bySession: %v", got)
	}
}

// ---------------------------------------------------------------------------
// /internal/inject-result 端点
// ---------------------------------------------------------------------------

func TestInjectResultEndpoint(t *testing.T) {
	cases := []struct {
		name    string
		apiKey  string
		authHdr string
		preReg  bool // 是否预登记 pending p1
		payload string
		want    int
	}{
		{"无鉴权头-有key", "k", "", true, `{"pending_id":"p1","ok":true,"result_text":"R"}`, 401},
		{"错key", "k", "Bearer wrong", true, `{"pending_id":"p1"}`, 401},
		{"对key-未知pending", "k", "Bearer k", false, `{"pending_id":"nope","ok":true,"result_text":"R"}`, 404},
		{"对key-已登记", "k", "Bearer k", true, `{"pending_id":"p1","ok":true,"result_text":"结果文本"}`, 204},
		{"对key-坏JSON", "k", "Bearer k", true, "{not json", 400},
		{"对key-缺pending_id", "k", "Bearer k", true, `{"ok":true}`, 400},
		{"无key配置-放行-未知pending", "", "", false, `{"pending_id":"nope"}`, 404},
		{"无key配置-放行-命中", "", "", true, `{"pending_id":"p1","result_text":"R"}`, 204},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := NewHandler(Config{APIKey: c.apiKey, Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})})
			var ch chan McpEvent
			if c.preReg {
				ch = h.pendings.register("p1", "s1", "run_shell")
			}
			req := httptest.NewRequest("POST", "/internal/inject-result", strings.NewReader(c.payload))
			if c.authHdr != "" {
				req.Header.Set("Authorization", c.authHdr)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Fatalf("code=%d want=%d body=%q", rec.Code, c.want, rec.Body.String())
			}
			if c.want == 204 && ch != nil {
				select {
				case ev := <-ch:
					// 事件应回填注册表里的工具名；ok 缺省视为成功。
					if ev.Tool != "run_shell" || ev.PendingID != "p1" {
						t.Fatalf("channel 事件=%+v", ev)
					}
					if ev.OK == nil || !*ev.OK {
						t.Fatalf("ok 缺省应视为成功: %+v", ev)
					}
					if strings.Contains(c.payload, "结果文本") && ev.ResultFull != "结果文本" {
						t.Fatalf("ResultFull=%q", ev.ResultFull)
					}
				default:
					t.Fatal("回注事件应进 channel")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 第一腿集成：tool_pending 事件 → tool_calls 终帧结束本轮
// ---------------------------------------------------------------------------

// pendingFakeUpstream 带「方法+路径」记录的 remote fake：轮询阻塞在 gate
// （由测试控制放行），用于观察 pending 打断路径上的云端调用序列。
func pendingFakeUpstream(t *testing.T, gate chan struct{}) (*upstream.Client, func() []string) {
	t.Helper()
	snapshotDynamicModelsCache(t)
	var mu sync.Mutex
	var calls []string
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), calls...)
	}
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			calls = append(calls, r.Method+" "+r.URL.Path)
			mu.Unlock()
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"s1"}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				select {
				case <-gate:
					return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":"最终回复文本"}]}}`), nil
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			case r.Method == http.MethodDelete && r.URL.Path == "/api/remote/v1/chat_sessions/s1":
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
	}
	return up, snapshot
}

// TestPendingToolCallEndsStreamWithToolCalls 完整第一腿：流式请求进行中，
// 桥推来 tool_pending → 输出原生 tool_calls + finish_reason=tool_calls 结束
// 本轮；pending 已登记；云端会话不被删除（等第二轮回灌）；轮询被打断，
// 云端最终回复不出现在本轮流里。
func TestPendingToolCallEndsStreamWithToolCalls(t *testing.T) {
	restore := remoteTestHook()
	defer restore()

	gate := make(chan struct{}) // 保持关闭：轮询阻塞，只能被 pending 打断
	up, snapshot := pendingFakeUpstream(t, gate)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: t.TempDir() + "/convs.json",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req = req.WithContext(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(rec, req) }()

	waitForSubscriber(t, h)
	h.mcpEvents.Publish(McpEvent{
		Event: "tool_pending", Tool: "run_shell", PendingID: "pid-1",
		ArgsFull: `{"command":"ls -la"}`, ChatSessionID: "s1", TS: 1, RecvAt: time.Now(),
	})

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("请求超时：pending 未打断轮询")
	}

	body := rec.Body.String()
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%q", rec.Code, body)
	}
	// tool_calls 帧完整携带 id（=pending_id）/name/arguments。
	if !strings.Contains(body, `"tool_calls"`) || !strings.Contains(body, `"pid-1"`) ||
		!strings.Contains(body, `"run_shell"`) || !strings.Contains(body, `{\"command\":\"ls -la\"}`) {
		t.Fatalf("tool_calls 帧不完整: %q", body)
	}
	if !strings.Contains(body, `:"tool_calls"`) || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("缺 finish_reason=tool_calls 终帧或 DONE: %q", body)
	}
	// pending 已登记（供第二腿配对）。
	if e := h.pendings.get("pid-1"); e == nil || e.sessID != "s1" || e.tool != "run_shell" {
		t.Fatalf("pending 未登记: %+v", e)
	}
	// 云端会话不得删除（第二腿回灌后还要继续用）。
	for _, c := range snapshot() {
		if strings.HasPrefix(c, "DELETE ") {
			t.Fatalf("pending 打断不得删除云端会话: %v", snapshot())
		}
	}
	// 轮询被打断：云端最终回复与旧文本流水都不应出现。
	if strings.Contains(body, "最终回复文本") {
		t.Fatalf("云端终稿不应出现在 pending 打断轮: %q", body)
	}
	if strings.Contains(body, "[本地工具]") {
		t.Fatalf("tool_pending 不应渲染 🔧 文本行: %q", body)
	}
	// 帧序：tool_calls 帧先于 finish 帧先于 [DONE]。
	iTC := strings.Index(body, `"tool_calls"`)
	iFin := strings.Index(body, `:"tool_calls"`)
	iDone := strings.Index(body, "data: [DONE]")
	if !(iTC < iFin && iFin < iDone) {
		t.Fatalf("帧序错误 tc=%d fin=%d done=%d", iTC, iFin, iDone)
	}
}

// TestToolTimeoutFallbackDropsPending 桥 120s 兜底事件到达后，订阅分支应
// 作废对应 pending 且本轮照常走完云端终稿（不输出 tool_calls）。
func TestToolTimeoutFallbackDropsPending(t *testing.T) {
	restore := remoteTestHook()
	defer restore()

	gate := make(chan struct{})
	up, _ := pendingFakeUpstream(t, gate)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: t.TempDir() + "/convs.json",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req = req.WithContext(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(rec, req) }()

	waitForSubscriber(t, h)
	// 先登记（模拟上一条 tool_pending 已处理——drop 语义只关心条目存在）。
	h.pendings.register("pid-9", "s1", "run_shell")
	h.mcpEvents.Publish(McpEvent{
		Event: "tool_timeout_fallback", Tool: "run_shell", PendingID: "pid-9",
		ChatSessionID: "s1", TS: 1, RecvAt: time.Now(),
	})
	time.Sleep(100 * time.Millisecond) // 确保 pump 消费到事件
	close(gate)                        // 放行云端终稿

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("请求超时")
	}
	body := rec.Body.String()
	if e := h.pendings.get("pid-9"); e != nil {
		t.Fatalf("timeout_fallback 应作废 pending: %+v", e)
	}
	if strings.Contains(body, `"tool_calls"`) {
		t.Fatalf("兜底轮不应输出 tool_calls: %q", body)
	}
	if !strings.Contains(body, "最终回复文本") || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("兜底轮应照常拿到云端终稿: %q", body)
	}
}

// ---------------------------------------------------------------------------
// 第二腿集成：role:tool 增量 → 回注桥 + 短路回复
// ---------------------------------------------------------------------------

// fakeBridge 假 MCP 桥：记录 /internal/inject-result 请求（鉴权头 + 载荷）。
func fakeBridge(t *testing.T) (*httptest.Server, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var got []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/inject-result" {
			t.Errorf("unexpected bridge request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		body["__auth"] = r.Header.Get("Authorization")
		mu.Lock()
		got = append(got, body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	snapshot := func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), got...)
	}
	return ts, snapshot
}

// withBridgeURL 临时把回注目标指向 ts（用完恢复）。假桥不校验鉴权只记录
// 头，断言按 mcpBridgeToken() 当前取值对比即可，无需改 env。
func withBridgeURL(ts *httptest.Server) (restore func()) {
	old := mcpBridgeBaseURL
	mcpBridgeBaseURL = ts.URL
	return func() { mcpBridgeBaseURL = old }
}

func TestInjectToolResultTurnShortCircuit(t *testing.T) {
	ts, snapshot := fakeBridge(t)
	defer ts.Close()
	restore := withBridgeURL(ts)
	defer restore()

	h := NewHandler(Config{
		Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		// Upstream 故意留 nil：短路轮绝不能触云端（触了会 nil panic，测试即失败）。
	})
	h.pendings.register("pid-1", "s1", "run_shell")

	body := `{"model":"glm-5.3","stream":true,"messages":[
		{"role":"user","content":"列出目录"},
		{"role":"assistant","content":null,"tool_calls":[{"id":"pid-1","type":"function","function":{"name":"run_shell","arguments":"{\"command\":\"ls\"}"}}]},
		{"role":"tool","tool_call_id":"pid-1","content":"file_a.txt file_b.txt"}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	err := h.serveRemoteWith(rec, req, &auth.Auth{UID: "u1", AccessToken: "at1"}, "glm-5.3", []byte(body), true, false, openaiRenderer{})
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	out := rec.Body.String()
	// 短路回复（非云端终稿、非 tool_calls）。
	if !strings.Contains(out, injectShortReply) || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("缺短路回复: %q", out)
	}
	if strings.Contains(out, "---") {
		t.Fatalf("不应出现 streamFinish 分隔线: %q", out)
	}
	// 假桥收到回注：Bearer + pending_id + 结果文本。
	inj := snapshot()
	if len(inj) != 1 {
		t.Fatalf("回注次数=%d want 1: %v", len(inj), inj)
	}
	if inj[0]["__auth"] != "Bearer "+mcpBridgeToken() {
		t.Fatalf("回注鉴权头=%v", inj[0]["__auth"])
	}
	if inj[0]["pending_id"] != "pid-1" || inj[0]["ok"] != true || inj[0]["result_text"] != "file_a.txt file_b.txt" {
		t.Fatalf("回注载荷=%v", inj[0])
	}
	// 配对成功的 pending 已注销。
	if h.pendings.get("pid-1") != nil {
		t.Fatal("回注成功后 pending 应被 drop")
	}
}

// TestInjectToolResultTurnNonStreamTextContent 非流式 + 数组形态 content 的
// role:tool 消息也应能提取文本并短路。
func TestInjectToolResultTurnNonStreamTextContent(t *testing.T) {
	ts, snapshot := fakeBridge(t)
	defer ts.Close()
	restore := withBridgeURL(ts)
	defer restore()

	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})})
	h.pendings.register("pid-2", "s1", "read_file")

	body := `{"model":"glm-5.3","stream":false,"messages":[
		{"role":"user","content":"读文件"},
		{"role":"tool","tool_call_id":"pid-2","content":[{"type":"text","text":"文件内容 XYZ"}]}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	if err := h.serveRemoteWith(rec, req, &auth.Auth{UID: "u1", AccessToken: "at1"}, "glm-5.3", []byte(body), false, false, openaiRenderer{}); err != nil {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(rec.Body.String(), injectShortReply) {
		t.Fatalf("缺短路回复: %q", rec.Body.String())
	}
	if inj := snapshot(); len(inj) != 1 || inj[0]["result_text"] != "文件内容 XYZ" {
		t.Fatalf("回注=%v", inj)
	}
}

// TestInjectToolResultTurnNoPendingFallsThrough 负面：无未决 pending 的
// role:tool 轮（普通工具回填，与 defer 机制无关）不得短路，照常走云端往返。
func TestInjectToolResultTurnNoPendingFallsThrough(t *testing.T) {
	ts, snapshot := fakeBridge(t)
	defer ts.Close()
	restore := withBridgeURL(ts)
	defer restore()
	restoreHook := remoteTestHook()
	defer restoreHook()

	gate := make(chan struct{})
	up, snapshotCalls := pendingFakeUpstream(t, gate)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: t.TempDir() + "/convs.json",
	})

	body := `{"model":"glm-5.3","stream":true,"messages":[
		{"role":"user","content":"hi"},
		{"role":"tool","tool_call_id":"call_other","content":"别的来源的工具结果"}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	done := make(chan error, 1)
	go func() {
		done <- h.serveRemoteWith(rec, req, &auth.Auth{UID: "u1", AccessToken: "at1"}, "glm-5.3", []byte(body), true, false, openaiRenderer{})
	}()

	// 等消息发进 fake 会话后放行终稿（gate 关着时轮询阻塞在等待，终稿
	// 只有放行后才会出现，不能拿它当等待条件）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sent := false
		for _, c := range snapshotCalls() {
			if c == "POST /api/remote/v1/chat_sessions/s1/messages" {
				sent = true
				break
			}
		}
		if sent {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("err=%v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "最终回复文本") {
		t.Fatalf("无 pending 的 role:tool 轮应走正常云端往返: %q", out)
	}
	if inj := snapshot(); len(inj) != 0 {
		t.Fatalf("不应回注假桥: %v", inj)
	}
}

// TestInjectToolResultTurnViaChatCompletions 全链路：带 role:tool 的请求
// 经 POST /v1/chat/completions 必须穿透 mockprobe（无魔术标记不得劫持）、
// 完成回灌并短路 —— 这是闭环第二腿在真实入口上的回归。
func TestInjectToolResultTurnViaChatCompletions(t *testing.T) {
	ts, snapshot := fakeBridge(t)
	defer ts.Close()
	restore := withBridgeURL(ts)
	defer restore()
	snapshotDynamicModelsCache(t)

	// mapModel 会经 Upstream 拉动态模型表（短路发生在 mapModel 之后），
	// 给一个只有 get_detail_param 的 fake 即可（gate 永不关闭也无妨，
	// 回灌短路不会触轮询）。
	up, _ := pendingFakeUpstream(t, make(chan struct{}))
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	h.pendings.register("pid-1", "s1", "run_shell")

	body := `{"model":"glm-5.3","stream":true,"messages":[
		{"role":"user","content":"列出目录"},
		{"role":"assistant","content":null,"tool_calls":[{"id":"pid-1","type":"function","function":{"name":"run_shell","arguments":"{\"command\":\"ls\"}"}}]},
		{"role":"tool","tool_call_id":"pid-1","content":"file_a.txt"}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	h.ServeHTTP(rec, req)

	out := rec.Body.String()
	if strings.Contains(out, "MOCK_PROBE") {
		t.Fatalf("无魔术标记的 role:tool 轮被 mockprobe 劫持: %q", out)
	}
	if !strings.Contains(out, injectShortReply) || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("应穿透到回灌短路: %q", out)
	}
	if inj := snapshot(); len(inj) != 1 || inj[0]["pending_id"] != "pid-1" {
		t.Fatalf("回注=%v", inj)
	}
}

// TestMockProbeStillGatedByMarker mockprobe 门控回归：会话里出现过魔术
// 标记时，role:tool 第二轮仍被探针劫持（保留原有探针能力）。
func TestMockProbeStillGatedByMarker(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})})
	body := `{"model":"glm-5.3","stream":true,"messages":[
		{"role":"user","content":"请执行 TW2API_MOCK_PROBE_BASH"},
		{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"Bash","arguments":"{}"}}]},
		{"role":"tool","tool_call_id":"c1","content":"TW2API_MOCK_PROBE_OK\n"}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	h.ServeHTTP(rec, req)
	// mapModel 会拉动态模型表：真实拨号失败回退静态表，glm-5.3 在列。
	if !strings.Contains(rec.Body.String(), "MOCK_PROBE 第二轮") {
		t.Fatalf("含标记的探针第二轮应被劫持: %q", rec.Body.String())
	}
}
