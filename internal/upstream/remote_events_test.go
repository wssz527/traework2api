// remote_events_test.go — 云端事件流（P0）的单测。
//
// 覆盖：SSE 帧解析、plan_item 累积值差分、SSE 服务端模拟、重连退避、
// 降级（非 200 / 禁用开关）、ctx 取消关流。
package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"traework2api/internal/auth"
)

// sseServer 起一个可控的 SSE 测试服务端。
type sseServer struct {
	srv      *httptest.Server
	dials    int32  // 拨号次数
	status   int    // 强制返回的状态码（0=200 正常流）
	body     string // 要推送的原始 SSE 文本
	lastEvID string // 收到的 Last-Event-ID
	holdFor  time.Duration
}

func newSSEServer(t *testing.T, body string) *sseServer {
	t.Helper()
	s := &sseServer{body: body}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&s.dials, 1)
		s.lastEvID = r.Header.Get("Last-Event-ID")
		if s.status != 0 {
			w.WriteHeader(s.status)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if s.holdFor > 0 {
			select {
			case <-r.Context().Done():
			case <-time.After(s.holdFor):
			}
		}
		_, _ = w.Write([]byte(s.body))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// withHost 把 remote 通道地址临时指向测试服务端，返回还原函数。
func withHost(t *testing.T, url string) {
	t.Helper()
	restore := SetRemoteHost(url)
	t.Cleanup(restore)
}

func TestRemoteEventStreamParsesPlanItem(t *testing.T) {
	body := strings.Join([]string{
		`id: turn1:1`,
		`event: plan_item`,
		`data: {"id":"p1","thought":"你好","reasoning_content":"让我想想"}`,
		``,
		`id: turn1:2`,
		`event: plan_item`,
		`data: {"id":"p1","thought":"你好世界","reasoning_content":"让我想清楚了"}`,
		``,
		`event: done`,
		`data: {"status":"completed"}`,
		``,
	}, "\n") + "\n"

	s := newSSEServer(t, body)
	withHost(t, s.srv.URL)
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1")
	var content, reasoning string
	for d := range ch {
		content += d.Content
		reasoning += d.Reasoning
	}
	// 累积值差分：第二帧只应贡献新增后缀。
	// 注意 reasoning 两帧不构成前缀关系（"让我想想" → "让我想清楚了"），
	// 差分器会保守回退为整值——这是刻意设计，宁可重复也不丢字。
	if content != "你好世界" {
		t.Errorf("content=%q want %q（差分别重复、别丢字）", content, "你好世界")
	}
	if !strings.HasSuffix(reasoning, "让我想清楚了") {
		t.Errorf("reasoning 应以最新累积值收尾: %q", reasoning)
	}
}

func TestRemoteEventStreamToolLineOnce(t *testing.T) {
	tc := `{"id":"tc1","name":"run_mcp","params":{"server_label":"本地工作区","tool_name":"read_file","args":{"path":"/a/b.json"}},"result":{}}`
	body := strings.Join([]string{
		`event: plan_item`,
		`data: {"id":"p1","thought":"","reasoning_content":"","tool_call_info":` + tc + `}`,
		``,
		`event: plan_item`,
		`data: {"id":"p1","thought":"","reasoning_content":"","tool_call_info":` + tc + `}`,
		``,
	}, "\n") + "\n"

	s := newSSEServer(t, body)
	withHost(t, s.srv.URL)
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	toolLines := 0
	for d := range c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1") {
		if d.ToolLine != "" {
			toolLines++
			if !strings.Contains(d.ToolLine, "read_file") || !strings.Contains(d.ToolLine, "/a/b.json") {
				t.Errorf("工具行应含工具名与参数: %q", d.ToolLine)
			}
		}
	}
	if toolLines != 1 {
		t.Errorf("同一 tool_call.id 只应输出一次工具行: got %d", toolLines)
	}
}

// TestRemoteEventStreamSkipsEmptyMcpShell 云端 run_mcp 的参数空壳帧不得产生
// 注记行：它会抢在本地 MCP 事件之前输出无信息的一行，导致带真实参数的
// 本地行被去重抑制（信息倒挂）。参数补齐后的帧才应输出。
func TestRemoteEventStreamSkipsEmptyMcpShell(t *testing.T) {
	shell := `{"id":"p1","thought":"","reasoning_content":"","tool_call_info":{"id":"tc1","name":"run_mcp","params":{"server_name":"","server_label":"","tool_name":"","args":{}}}}`
	filled := `{"id":"p1","thought":"","reasoning_content":"","tool_call_info":{"id":"tc1","name":"run_mcp","params":{"server_label":"本地工作区","tool_name":"read_file","args":{"path":"/a/b.json"}}}}`
	body := "event: plan_item\ndata: " + shell + "\n\n" +
		"event: plan_item\ndata: " + filled + "\n\n"

	s := newSSEServer(t, body)
	withHost(t, s.srv.URL)
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var lines []string
	for d := range c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1") {
		if d.ToolLine != "" {
			lines = append(lines, d.ToolLine)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("空壳帧应被跳过、只输出补齐帧: got %d %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "read_file") || !strings.Contains(lines[0], "/a/b.json") {
		t.Errorf("输出行应带真实工具名与参数: %q", lines[0])
	}
}

// TestRemoteEventStreamIgnoresNoise metadata/token_usage 等事件不得产生输出。
func TestRemoteEventStreamIgnoresNoise(t *testing.T) {
	body := strings.Join([]string{
		`event: heartbeat`,
		`data: {"status":1}`,
		``,
		`event: metadata`,
		`data: {"message_id":"m1","status":"in_progress"}`,
		``,
		`event: token_usage`,
		`data: {"total_tokens":123}`,
		``,
		`event: status_changed`,
		`data: {"new_status":3}`,
		``,
	}, "\n") + "\n"

	s := newSSEServer(t, body)
	withHost(t, s.srv.URL)
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n := 0
	for range c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1") {
		n++
	}
	if n != 0 {
		t.Errorf("非内容事件不应产生增量: got %d", n)
	}
}

// TestRemoteEventStreamNon200Degrades 非 200 应静默降级：channel 直接关闭。
func TestRemoteEventStreamNon200Degrades(t *testing.T) {
	s := newSSEServer(t, "")
	s.status = http.StatusForbidden
	withHost(t, s.srv.URL)
	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n := 0
	for range c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1") {
		n++
	}
	if n != 0 {
		t.Errorf("403 应静默降级不产增量: got %d", n)
	}
	if got := atomic.LoadInt32(&s.dials); got != 1 {
		t.Errorf("非 200 不应重试: dials=%d", got)
	}
}

// TestRemoteEventStreamDisabledEnv 降级开关生效时不应拨号。
func TestRemoteEventStreamDisabledEnv(t *testing.T) {
	s := newSSEServer(t, "")
	withHost(t, s.srv.URL)
	orig := RemoteEventsDisabled
	RemoteEventsDisabled = true
	defer func() { RemoteEventsDisabled = orig }()

	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1") {
	}
	if got := atomic.LoadInt32(&s.dials); got != 0 {
		t.Errorf("禁用时不应拨号: dials=%d", got)
	}
}

// TestRemoteEventStreamReconnect 断流后按退避重连，并带 Last-Event-ID 续传。
// 服务端第一次只发半帧后关闭连接，第二次才发完整帧 —— 验证：
//  1. 确实发生重连（dials==2）
//  2. 重连带上此前收到的 SSE id
func TestRemoteEventStreamReconnect(t *testing.T) {
	var s *sseServer
	// 第一次请求：立即断开（模拟网关掐连接）；第二次：正常发帧。
	first := true
	s = &sseServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&s.dials, 1)
		s.lastEvID = r.Header.Get("Last-Event-ID")
		if first {
			first = false
			// 先送一帧（带 id），随后掐断，触发重连。
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			_, _ = w.Write([]byte("id: turn1:7\nevent: plan_item\ndata: {\"id\":\"p1\",\"thought\":\"前半\"}\n\n"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("id: turn1:8\nevent: plan_item\ndata: {\"id\":\"p1\",\"thought\":\"前半后半\"}\n\n"))
	}))
	defer s.srv.Close()
	withHost(t, s.srv.URL)

	// 缩短退避，避免测试跑十几秒。
	orig := remoteEventReconnectDelays
	remoteEventReconnectDelays = []time.Duration{10 * time.Millisecond}
	defer func() { remoteEventReconnectDelays = orig }()

	c := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var content string
	for d := range c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1") {
		content += d.Content
	}
	if got := atomic.LoadInt32(&s.dials); got != 2 {
		t.Errorf("应重连一次: dials=%d", got)
	}
	if s.lastEvID != "turn1:7" {
		t.Errorf("重连应带 Last-Event-ID=turn1:7, got %q", s.lastEvID)
	}
	if content != "前半后半" {
		t.Errorf("content=%q want %q", content, "前半后半")
	}
}

// TestRemoteEventStreamCtxCancel ctx 取消应立即关流。
func TestRemoteEventStreamCtxCancel(t *testing.T) {
	s := newSSEServer(t, "")
	s.holdFor = 30 * time.Second // 服务端一直挂着不结束
	withHost(t, s.srv.URL)
	c := New()
	ctx, cancel := context.WithCancel(context.Background())

	ch := c.RemoteOpenEventStream(ctx, &auth.Auth{}, "sess1")
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("取消后不应再有增量")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ctx 取消后 channel 未关闭")
	}
}

// TestPrefixDelta 累积值差分的边界。
func TestPrefixDelta(t *testing.T) {
	cases := []struct{ prev, cur, want string }{
		{"", "abc", "abc"},
		{"abc", "abcdef", "def"},
		{"abc", "abc", ""},
		{"", "", ""},
		{"abcdef", "abc", ""}, // 变短：异常保护，跳过
		{"abc", "xyz", "xyz"}, // 非前缀：保守返回整值
	}
	for _, c := range cases {
		if got := prefixDelta(c.prev, c.cur); got != c.want {
			t.Errorf("prefixDelta(%q,%q)=%q want %q", c.prev, c.cur, got, c.want)
		}
	}
}

// TestFormatToolLine 工具行渲染：MCP 工具带参数，finish 不外显。
func TestFormatToolLine(t *testing.T) {
	mcp := &planToolCall{
		ID: "t1", Name: "run_mcp",
		Params: []byte(`{"server_label":"本地工作区","tool_name":"read_file","args":{"path":"/a"}}`),
	}
	line := formatToolLine(mcp)
	if !strings.Contains(line, "read_file") || !strings.Contains(line, "/a") {
		t.Errorf("MCP 工具行应含工具名与参数: %q", line)
	}
	if !strings.Contains(line, "🔧") {
		t.Errorf("工具行应保持一期的 🔧 风格: %q", line)
	}
	if got := formatToolLine(&planToolCall{ID: "t2", Name: "finish"}); got != "" {
		t.Errorf("finish 不应产生注记行（其 summary 已是正文）: %q", got)
	}
	if got := formatToolLine(&planToolCall{ID: "t3", Name: "Read"}); !strings.Contains(got, "Read") {
		t.Errorf("沙盒工具应简写注记: %q", got)
	}
}

// TestRemoteEpEventsFormat 端点路径拼接（防止 %s 顺序写错）。
func TestRemoteEpEventsFormat(t *testing.T) {
	got := fmt.Sprintf(RemoteEpEvents, "abc123")
	if got != "/api/remote/v1/chat_sessions/abc123/events" {
		t.Fatalf("端点=%q", got)
	}
}

// TestFormatToolResultMCPEnvelope 工具结果摘要：真实 TRAE 信封（嵌套
// data.content[].text）必须解包出可读文本，而非原样 dump 整个 JSON。
// 回归背景：run_shell 等本地 MCP 工具的结果到达 plan_item 时被 TRAE 包成
// {status,error_message,data:{content:[{type:"text",text:...}]}}，旧逻辑
// 只认顶层字符串字段，全部 miss 后走 raw-JSON 兜底，客户端刷出巨丑转义块。
func TestFormatToolResultMCPEnvelope(t *testing.T) {
	// ① 真实形状：TRAE 信封 + 标准 MCP content 数组
	env := &planToolCall{
		ID:   "t1",
		Name: "run_mcp",
		Params: []byte(`{"server_label":"本地工作区","tool_name":"run_shell","args":{"command":"ls"}}`),
		Result: []byte(`{"status":"success","error_message":"","data":{"content":[{"type":"text","text":"exit=0\nmain.go\nREADME.md\n"}]}}`),
	}
	line := formatToolResult(env)
	if !strings.Contains(line, "exit=0") || !strings.Contains(line, "main.go") {
		t.Errorf("应解包 data.content[].text 的真实输出: %q", line)
	}
	if strings.Contains(line, `"status"`) || strings.Contains(line, `error_message`) {
		t.Errorf("不得残留 JSON 信封字段: %q", line)
	}
	if !strings.Contains(line, "✅") || !strings.Contains(line, "run_shell") {
		t.Errorf("应保持 ✅ [工具名] 风格: %q", line)
	}

	// ② 失败结果：status!=success → ❌ + error_message
	fail := &planToolCall{
		ID:   "t2",
		Name: "run_mcp",
		Params: []byte(`{"tool_name":"run_shell"}`),
		Result: []byte(`{"status":"error","error_message":"permission denied","data":null}`),
	}
	fline := formatToolResult(fail)
	if !strings.Contains(fline, "❌") || !strings.Contains(fline, "permission denied") {
		t.Errorf("失败结果应 ❌ 且带 error_message: %q", fline)
	}

	// ③ 兼容：顶层 content 字符串（旧路径）仍可用
	legacy := &planToolCall{
		ID:     "t3",
		Name:   "run_mcp",
		Params: []byte(`{"tool_name":"read_file"}`),
		Result: []byte(`{"content":"package main"}`),
	}
	if lline := formatToolResult(legacy); !strings.Contains(lline, "package main") {
		t.Errorf("顶层 content 字符串应保留: %q", lline)
	}

	// ④ 非 JSON 纯文本结果原样保留
	plain := &planToolCall{
		ID:     "t4",
		Name:   "run_shell",
		Params: []byte(`{}`),
		Result: []byte(`"exit=0\nhello"`),
	}
	if pline := formatToolResult(plain); !strings.Contains(pline, "hello") {
		t.Errorf("纯文本结果应保留: %q", pline)
	}
}

// TestFormatToolResultEmptyShells 空壳结果帧不应外显（回归：finish /
// EnvironmentSetup 的收尾空结果被 raw-JSON 兜底刷成零信息行）。
func TestFormatToolResultEmptyShells(t *testing.T) {
	cases := []struct {
		note string
		raw  string
	}{
		{"finish 空收尾", `{"data":{"products":null,"summary":""},"error_message":"","images":null,"interrupt":null,"is_truncated":null,"render":null,"status":"success"}`},
		{"Read 空信封", `{"error_message":"","images":null,"interrupt":null,"is_truncated":false,"render":null,"status":"success"}`},
		{"EnvironmentSetup 空壳", `{"data":{"command":null,"command_id":"","cwd":null,"exit_code":0,"pid":null,"sandbox_status":null,"status":null},"error_message":"","status":"success"}`},
		{"失败但无信息", `{"status":"error","error_message":""}`},
	}
	for _, c := range cases {
		tc := &planToolCall{ID: "x", Name: "run_mcp", Params: []byte(`{"tool_name":"Read"}`), Result: []byte(c.raw)}
		if got := formatToolResult(tc); got != "" {
			t.Errorf("%s: 空壳结果应不外显, got %q", c.note, got)
		}
	}
}

// TestFormatToolResultSandboxShapes 沙盒工具真实形态（2026-09-01 探针帧）。
func TestFormatToolResultSandboxShapes(t *testing.T) {
	// EnvironmentSetup：文本藏在 data.stderr（MCP 状态输出）
	env := &planToolCall{
		ID: "e1", Name: "EnvironmentSetup", Params: []byte(`{}`),
		Result: []byte(`{"status":"success","error_message":"","data":{"terminal_id":0,"command_id":"","pid":null,"stdout":"","stderr":"MCP Servers:\n  ✓ 本地工作区 - running (14 tools)","exit_code":0,"command":null},"render":null}`),
	}
	if got := formatToolResult(env); !strings.Contains(got, "本地工作区 - running") {
		t.Errorf("EnvironmentSetup 应提取 data.stderr: %q", got)
	}
	// 空壳 {}（Read/run_mcp 首帧）不外显
	if got := formatToolResult(&planToolCall{ID: "e2", Name: "Read", Params: []byte(`{}`), Result: []byte(`{}`)}); got != "" {
		t.Errorf("空 result 应不外显: %q", got)
	}
}
