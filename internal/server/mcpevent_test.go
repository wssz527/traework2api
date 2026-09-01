// mcpevent_test.go — /internal/mcp-event 接收端点 + 事件总线 + serveRemote 流式
// 工具事件转发的单测。
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

// waitForSubscriber 轮询等待有订阅者挂上事件总线（消除发布/订阅时序竞争）。
func waitForSubscriber(t *testing.T, h *Handler) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.mcpEvents.mu.Lock()
		n := len(h.mcpEvents.subs)
		h.mcpEvents.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("等待事件订阅者超时")
}

func TestMcpEventEndpointPublishes(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})})
	req := httptest.NewRequest("POST", "/internal/mcp-event", strings.NewReader(
		`{"event":"tool_call_end","tool":"read_file","ok":true,"result_preview":"abc","duration_ms":42,"ts":123}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}
	recent := h.mcpEvents.recent(10)
	if len(recent) != 1 || recent[0].Tool != "read_file" || recent[0].DurationMS != 42 {
		t.Fatalf("recent=%+v", recent)
	}
}

func TestMcpEventEndpointRejectsGarbage(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})})
	for name, payload := range map[string]string{
		"非JSON":     "{not json",
		"缺event字段": `{"tool":"read_file"}`,
	} {
		req := httptest.NewRequest("POST", "/internal/mcp-event", strings.NewReader(payload))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: code=%d want 400", name, rec.Code)
		}
	}
}

// remoteStreamTestUpstream 构造 remote 全链路 fake：建会话/发消息立即成功，
// 删除会话照常应答。gate 非 nil 时，消息轮询 GET 会阻塞直到 gate 关闭才返回
// completed（用事件推流后再放行，消除轮询时序竞争）；gate 为 nil 时直接返回
// in_progress（配合短 remoteWaitTotal 走超时路径）。
func remoteStreamTestUpstream(t *testing.T, completedContent string, gate chan struct{}) *upstream.Client {
	t.Helper()
	snapshotDynamicModelsCache(t) // 本 fake 的 get_detail_param mock 会写全局模型缓存，用完恢复
	return &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"s1"}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				// mapModel → fetchDynamicModels 会先拉动态模型表；自包含 fake：
				// 返回含 glm-5.3 的列表，避免依赖套件内其他测试填充全局缓存。
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				if gate != nil {
					select {
					case <-gate:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				}
				if gate != nil {
					// completedContent 为测试常量（无引号），直接内插即可。
					return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":"`+completedContent+`"}]}}`), nil
				}
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"in_progress"}]}}`), nil
			case r.Method == http.MethodDelete && r.URL.Path == "/api/remote/v1/chat_sessions/s1":
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
	}
}

// chunkDelta 提取 OpenAI chunk 的 delta.content。
func chunkDelta(t *testing.T, frame string) map[string]any {
	t.Helper()
	var chunk map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(frame, "data: ")), &chunk); err != nil {
		t.Fatalf("chunk 非法: %q err=%v", frame, err)
	}
	choices := chunk["choices"].([]any)
	return choices[0].(map[string]any)["delta"].(map[string]any)
}

func TestChatRemoteStreamForwardsMcpToolEvents(t *testing.T) {
	restore := remoteTestHook()
	defer restore()

	// content 为纯文本（extractAssistantText 原样返回）；gate 控制：事件推完并
	// 确认可见后才放行 completed，消除「事件还没写进响应、轮询先返回」的竞争。
	gate := make(chan struct{})
	up := remoteStreamTestUpstream(t, "最终回复文本", gate)
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up, ConvStorePath: "-"})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req = req.WithContext(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(rec, req) }()

	// 等订阅者挂上后连发三个事件：start / end(ok) / end(失败)
	waitForSubscriber(t, h)
	okFlag := true
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_start", Tool: "read_file", ArgsPreview: `{"path":"/tmp/x"}`, TS: 1, RecvAt: time.Now()})
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_end", Tool: "read_file", OK: &okFlag, ResultPreview: "package.json 内容", DurationMS: 123, TS: 2, RecvAt: time.Now()})
	badFlag := false
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_end", Tool: "grep", OK: &badFlag, ResultPreview: "拒绝访问", DurationMS: 5, TS: 3, RecvAt: time.Now()})
	// 三个事件均已送达订阅方（写出或缓存在 64 缓冲里）后再放行轮询 completed：
	// stopEventPump 的 drain 会把存量全部写出，事件必然落在终帧之前。
	time.Sleep(100 * time.Millisecond)
	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("请求超时")
	}

	if rec.Code != 200 {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()

	// 首帧 role assistant
	firstFrame := strings.Split(body, "\n\n")[0]
	delta := chunkDelta(t, firstFrame)
	if delta["role"] != "assistant" {
		t.Errorf("首帧 delta=%v 缺 role assistant", delta)
	}

	// 工具事件出现在最终回复之前
	iStart := strings.Index(body, "[本地工具] read_file")
	iEndOK := strings.Index(body, "✅ 完成 123ms")
	iEndBad := strings.Index(body, "❌ 完成 5ms")
	iFinal := strings.Index(body, "最终回复文本")
	// 分隔线在 JSON chunk 内是转义形式（字面 \n 字符），按转义后的字节查找。
	iSep := strings.Index(body, `\n\n---\n\n`)
	iDone := strings.Index(body, "data: [DONE]")
	if iStart < 0 || iEndOK < 0 || iEndBad < 0 || iFinal < 0 || iSep < 0 || iDone < 0 {
		t.Fatalf("关键片段缺失 start=%d endOK=%d endBad=%d final=%d sep=%d done=%d body=%q",
			iStart, iEndOK, iEndBad, iFinal, iSep, iDone, body)
	}
	if !(iStart < iEndOK && iEndOK < iEndBad && iEndBad < iSep && iSep < iFinal && iFinal < iDone) {
		t.Errorf("流内顺序错误：工具事件应先于分隔线/最终回复/DONE")
	}
	if !strings.Contains(body, `:"stop"`) {
		t.Errorf("缺 finish_reason stop: %q", body)
	}
	// 所有数据帧都是合法 JSON chunk 或 [DONE]；心跳注释帧（: keepalive）
	// 在等待窗口内出现属正常行为，SSE 规范允许。
	for _, frame := range strings.Split(strings.TrimSpace(body), "\n\n") {
		if frame == "data: [DONE]" || strings.HasPrefix(frame, "data: {") || frame == ": keepalive" {
			continue
		}
		t.Errorf("存在非标 SSE 数据帧: %q", frame)
	}
}

func TestChatRemoteStreamTimeoutStopsPumpBeforeErrorFrame(t *testing.T) {
	// 用长一点的 remoteWaitTotal，确保「发布事件 → drain → 超时收尾」的顺序
	// 不受发布时序竞争影响（remoteTestHook 的 200ms 太短，事件可能晚于超时）。
	oldWait, oldKeep, oldBusy := remoteWaitTotal, remoteKeepaliveInterval, remoteBusyRetryWait
	remoteWaitTotal, remoteKeepaliveInterval, remoteBusyRetryWait = 1500*time.Millisecond, 50*time.Millisecond, 10*time.Millisecond
	defer func() { remoteWaitTotal, remoteKeepaliveInterval, remoteBusyRetryWait = oldWait, oldKeep, oldBusy }()

	up := remoteStreamTestUpstream(t, "x", nil) // gate=nil：永不 completed → 超时路径
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), Upstream: up, ConvStorePath: "-"})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))

	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(rec, req) }()

	waitForSubscriber(t, h)
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_start", Tool: "read_file", ArgsPreview: `{"path":"/tmp/x"}`, TS: 1, RecvAt: time.Now()})
	// stopEventPump 的 drain 保证：在超时错误帧之前已送达订阅方的事件，
	// 一定会被写到错误帧之前（先断流、写完存量、再收尾）。
	late := McpEvent{Event: "tool_call_start", Tool: "late_tool", TS: 2, RecvAt: time.Now()}
	defer h.mcpEvents.Publish(late) // 无人订阅时 Publish 只是入环形缓冲，无害

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("请求超时")
	}
	body := rec.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Errorf("超时场景应包含 keepalive 心跳")
	}
	if !strings.Contains(body, "remote_task_failed") || !strings.Contains(body, "data: [DONE]\n\n") {
		t.Errorf("应以错误帧 + DONE 收尾: %q", body)
	}
	iTool := strings.Index(body, "[本地工具] read_file")
	iErr := strings.Index(body, "remote_task_failed")
	if iTool < 0 || iErr < 0 || iTool > iErr {
		t.Errorf("已送达的工具事件必须出现在错误帧之前（drain 语义）: tool=%d err=%d", iTool, iErr)
	}
	if strings.Contains(body, "late_tool") {
		t.Errorf("迟到事件不得出现在终帧之后: %q", body)
	}
}

// TestMcpEventDeltaRendering 校验旧版（200 字符 preview）渲染。
func TestMcpEventDeltaRendering(t *testing.T) {
	okFlag, badFlag := true, false
	if got := mcpEventDelta(McpEvent{Event: "tool_call_start", Tool: "read_file", ArgsPreview: `{"path":"x"}`}); got != "\n\n> 🔧 [本地工具] read_file · {\"path\":\"x\"}\n" {
		t.Errorf("start 渲染=%q", got)
	}
	if got := mcpEventDelta(McpEvent{Event: "tool_call_end", Tool: "read_file", OK: &okFlag, DurationMS: 123, ResultPreview: "abc"}); got != "> ✅ 完成 123ms\n>\n> abc\n" {
		t.Errorf("end ok 渲染=%q", got)
	}
	if got := mcpEventDelta(McpEvent{Event: "tool_call_end", Tool: "grep", OK: &badFlag, DurationMS: 5, ResultPreview: "boom"}); got != "> ❌ 完成 5ms\n>\n> boom\n" {
		t.Errorf("end fail 渲染=%q", got)
	}
	if got := mcpEventDelta(McpEvent{Event: "unknown"}); got != "" {
		t.Errorf("未知事件应渲染为空: %q", got)
	}
}

// TestMcpEventDeltaFullPayload 二期 P2：完整 args/result 必须出现在流式行里，
// 且不再被截到 200 字符；超长由上报方截断并标注。
func TestMcpEventDeltaFullPayload(t *testing.T) {
	okFlag := true
	long := strings.Repeat("x", 900) // 超过一期 200 字符上限
	start := mcpEventDelta(McpEvent{
		Event: "tool_call_start", Tool: "read_file",
		ArgsFull: `{"path":"/Users/a/very/long/path/to/some/file.json"}`,
	})
	if !strings.Contains(start, "/Users/a/very/long/path/to/some/file.json") {
		t.Errorf("start 应带完整参数: %q", start)
	}

	end := mcpEventDelta(McpEvent{
		Event: "tool_call_end", Tool: "read_file", OK: &okFlag, DurationMS: 7,
		ResultFull: long,
	})
	if strings.Count(end, "x") < 500 {
		t.Errorf("end 应带完整结果（未被 200 字符截断）: len=%d", len(end))
	}
	if !strings.Contains(end, "> ") {
		t.Errorf("多行结果应整体缩进为引用块: %q", end[:80])
	}

	// 截断标记必须透传到客户端。
	tr := mcpEventDelta(McpEvent{
		Event: "tool_call_end", Tool: "read_file", OK: &okFlag,
		ResultFull: "abc", ResultTruncated: true,
	})
	if !strings.Contains(tr, "已截断") {
		t.Errorf("截断应被标注: %q", tr)
	}

	// 旧版上报方（只有 preview 字段）仍可用。
	legacy := mcpEventDelta(McpEvent{
		Event: "tool_call_start", Tool: "t", ArgsPreview: `{"a":1}`,
	})
	if !strings.Contains(legacy, `{"a":1}`) {
		t.Errorf("旧版 preview 应作为回退: %q", legacy)
	}
}

// TestMcpEventEndpointAcceptsFullPayload P2：4KB 完整载荷经 JSON 转义后
// 体积膨胀，/internal/mcp-event 不得因旧 8KB 上限把它 400 掉。
func TestMcpEventEndpointAcceptsFullPayload(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})})
	// 单边 4KB、含大量换行（JSON 转义后膨胀近一倍），总 body 超 8KB。
	big := strings.Repeat("line\n", 1700) // ~8.5KB，%q 转义后 ~17KB
	body := fmt.Sprintf(`{"event":"tool_call_end","tool":"read_file","ok":true,"result_full":%q,"ts":1}`, big)
	if len(body) < 8<<10 {
		t.Fatalf("测试前提不成立：body 应超过旧 8KB 上限, len=%d", len(body))
	}
	req := httptest.NewRequest("POST", "/internal/mcp-event", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d（完整载荷被拒）", rec.Code)
	}
	recent := h.mcpEvents.recent(1)
	if len(recent) != 1 || len(recent[0].ResultFull) < 4000 {
		t.Fatalf("完整结果应原样入库: len=%d", len(recent[0].ResultFull))
	}
}

func TestMcpEventSlowSubscriberDoesNotBlockPublish(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith()})
	id, ch := h.mcpEvents.Subscribe()
	defer h.mcpEvents.Unsubscribe(id)
	// 塞满缓冲（64）后继续发，Publish 必须不阻塞（丢帧而非卡死）
	for i := 0; i < mcpEventSubscriberBuffer+10; i++ {
		h.mcpEvents.Publish(McpEvent{Event: "tool_call_start", Tool: "t", TS: int64(i), RecvAt: time.Now()})
	}
	// 环形缓冲仍保留最近事件
	if got := h.mcpEvents.recent(1); len(got) != 1 || got[0].TS != int64(mcpEventSubscriberBuffer+9) {
		t.Fatalf("recent=%+v", got)
	}
	drained := 0
	for {
		select {
		case <-ch:
			drained++
			continue
		default:
		}
		break
	}
	if drained != mcpEventSubscriberBuffer {
		t.Errorf("drained=%d want %d（满后应丢帧）", drained, mcpEventSubscriberBuffer)
	}
	_ = io.Discard
}
