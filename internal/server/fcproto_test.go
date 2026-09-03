// fcproto_test.go — 协议模式单元测试（TDD：先于 handler 集成）。
package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func fcTestBody(tools string, msgs string) []byte {
	return []byte(`{"model":"DeepSeek-V4-Flash-Official","stream":true` +
		tools + `,` + msgs + `}`)
}

func TestFCProtoEnabled(t *testing.T) {
	if !fcProtoEnabled(fcTestBody(`,"tools":[{"type":"function","function":{"name":"Bash"}}]`, `"messages":[{"role":"user","content":"hi"}]`)) {
		t.Fatal("带 tools 的请求应启用协议模式")
	}
	if fcProtoEnabled(fcTestBody(``, `"messages":[{"role":"user","content":"hi"}]`)) {
		t.Fatal("不带 tools 的请求不应启用协议模式")
	}
	if fcProtoEnabled(fcTestBody(`,"tools":[]`, `"messages":[{"role":"user","content":"hi"}]`)) {
		t.Fatal("空 tools 不应启用协议模式")
	}
}

func TestFCFullPromptContainsHeaderAndHistory(t *testing.T) {
	body := fcTestBody(
		`,"tools":[{"type":"function","function":{"name":"Bash","description":"run shell","parameters":{"type":"object","properties":{"command":{"type":"string"}}}}}]`,
		`"messages":[
			{"role":"system","content":"You are a coding agent."},
			{"role":"user","content":"list /tmp"},
			{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"Bash","arguments":"{\"command\":\"ls /tmp\"}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"filea\nfileb"}
		]`)
	got := fcFullPrompt(body)
	for _, want := range []string{
		"API GATEWAY SIMULATION", "<tools>", `"name":"Bash"`, "</tools>",
		"<tool_call>", "<system>\nYou are a coding agent.\n</system>",
		"<user_message>\nlist /tmp\n</user_message>",
		`<tool_call>{"name": "Bash", "arguments": {"command":"ls /tmp"}}</tool_call>`,
		"<tool_result tool_call_id=\"call_1\">\nfilea\nfileb\n</tool_result>",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("fcFullPrompt 缺少 %q:\n---\n%s\n---", want, got)
		}
	}
}

func TestFCFullPromptFiltersSystemReminder(t *testing.T) {
	body := fcTestBody(`,"tools":[{"type":"function","function":{"name":"Read"}}]`,
		`"messages":[{"role":"user","content":"<system-reminder>date</system-reminder>real task"}]`)
	got := fcFullPrompt(body)
	if strings.Contains(got, "system-reminder>") {
		t.Error("system-reminder 注入块不应进入 prompt")
	}
	if !strings.Contains(got, "<user_message>\nreal task\n</user_message>") {
		t.Error("user 正文应保留")
	}
}

func TestFCIncrementRendersToolTurns(t *testing.T) {
	inc := fcIncrement([]map[string]any{
		{"role": "assistant", "content": "", "tool_calls": []any{map[string]any{
			"id": "call_9", "type": "function",
			"function": map[string]any{"name": "Read", "arguments": `{"file_path":"/tmp/x"}`},
		}}},
		{"role": "tool", "tool_call_id": "call_9", "content": "42"},
	})
	for _, want := range []string{
		`<tool_call>{"name": "Read", "arguments": {"file_path":"/tmp/x"}}</tool_call>`,
		"<tool_result tool_call_id=\"call_9\">\n42\n</tool_result>",
	} {
		if !strings.Contains(inc, want) {
			t.Errorf("fcIncrement 缺少 %q:\n%s", want, inc)
		}
	}
}

func TestFCIncrementSkipsEmpty(t *testing.T) {
	inc := fcIncrement([]map[string]any{
		{"role": "assistant", "content": ""}, // 无 tool_calls 无正文
		{"role": "user", "content": "next"},
	})
	if strings.Contains(inc, "assistant_reply>") {
		t.Errorf("空 assistant 消息应跳过: %q", inc)
	}
	if !strings.Contains(inc, "<user_message>\nnext\n</user_message>") {
		t.Error("user 增量应保留")
	}
}

func TestParseFCToolCallsBasic(t *testing.T) {
	text := "I will check the directory first.\n<tool_call>{\"name\": \"Bash\", \"arguments\": {\"command\": \"ls\"}}</tool_call>\n<tool_call>{\"name\": \"Read\", \"arguments\": {\"file_path\": \"/etc/hosts\"}}</tool_call>\nDone waiting."
	calls, content := parseFCToolCalls(text)
	if len(calls) != 2 {
		t.Fatalf("应解析出 2 个调用, got %d (%+v)", len(calls), calls)
	}
	if calls[0].Name != "Bash" || calls[0].ArgsJSON != `{"command": "ls"}` {
		t.Errorf("calls[0] 错误: %+v", calls[0])
	}
	if calls[1].Name != "Read" {
		t.Errorf("calls[1].Name 错误: %+v", calls[1])
	}
	if !strings.Contains(content, "I will check the directory first.") || !strings.Contains(content, "Done waiting.") {
		t.Errorf("content 应保留块外正文: %q", content)
	}
	if strings.Contains(content, "tool_call") {
		t.Errorf("content 不应残留协议块: %q", content)
	}
}

func TestParseFCToolCallsCodeFence(t *testing.T) {
	text := "<tool_call>```json\n{\"name\": \"Grep\", \"arguments\": {\"q\": \"foo\"}}\n```</tool_call>"
	calls, content := parseFCToolCalls(text)
	if len(calls) != 1 || calls[0].Name != "Grep" {
		t.Fatalf("code fence 包裹应解析成功: %+v", calls)
	}
	if strings.Contains(content, "tool_call") {
		t.Errorf("content 应无协议残留: %q", content)
	}
}

func TestParseFCToolCallsNone(t *testing.T) {
	calls, content := parseFCToolCalls("这是最终答案，无需工具。")
	if calls != nil {
		t.Errorf("无块时应返回 nil: %+v", calls)
	}
	if content != "这是最终答案，无需工具。" {
		t.Errorf("content 应原样: %q", content)
	}
}

func TestParseFCToolCallsMalformedSkipped(t *testing.T) {
	text := "<tool_call>{broken json</tool_call>\n<tool_call>{\"name\":\"Bash\",\"arguments\":{}}</tool_call>"
	calls, _ := parseFCToolCalls(text)
	if len(calls) != 1 || calls[0].Name != "Bash" {
		t.Errorf("坏块应跳过、好块保留: %+v", calls)
	}
}

func TestParseFCToolCallsStringArguments(t *testing.T) {
	// 模型把 arguments 序列化成字符串的容错
	calls, _ := parseFCToolCalls(`<tool_call>{"name": "Write", "arguments": "{\"path\": \"a\"}"}</tool_call>`)
	if len(calls) != 1 {
		t.Fatalf("应解析 1 个: %+v", calls)
	}
	var probe map[string]any
	if err := json.Unmarshal([]byte(calls[0].ArgsJSON), &probe); err != nil {
		t.Errorf("ArgsJSON 应是合法对象 JSON: %q (%v)", calls[0].ArgsJSON, err)
	}
	if probe["path"] != "a" {
		t.Errorf("解包后参数错误: %v", probe)
	}
}

// 渲染：多调用 tool_calls 帧的 JSON 形状（客户端可执行）
func TestFCToolCallsFrameShape(t *testing.T) {
	calls := []fcCall{
		{Name: "Bash", ArgsJSON: `{"command": "ls"}`},
		{Name: "Read", ArgsJSON: `{"file_path": "/tmp/x"}`},
	}
	var sb strings.Builder
	writeFCToolCallsFrame(&sb, "chatcmpl-1", "m", 123, calls, true)
	// 按行收集 data: 帧（tool_calls 帧、finish 帧、[DONE]）
	var frames []string
	for _, ln := range strings.Split(sb.String(), "\n") {
		if strings.HasPrefix(ln, "data: ") {
			frames = append(frames, strings.TrimPrefix(ln, "data: "))
		}
	}
	if len(frames) != 3 || frames[2] != "[DONE]" {
		t.Fatalf("应有 tool_calls+finish+[DONE] 三帧, got %d: %v", len(frames), frames)
	}
	var toolLine, finLine map[string]any
	if err := json.Unmarshal([]byte(frames[0]), &toolLine); err != nil {
		t.Fatalf("tool_calls 帧不是合法 JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(frames[1]), &finLine); err != nil {
		t.Fatalf("finish 帧不是合法 JSON: %v", err)
	}
	if finLine["choices"].([]any)[0].(map[string]any)["finish_reason"] != "tool_calls" {
		t.Errorf("终帧 finish_reason 应为 tool_calls: %v", finLine)
	}
	tcs := toolLine["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"].([]any)
	if len(tcs) != 2 {
		t.Fatalf("应有 2 个调用: %v", tcs)
	}
	first := tcs[0].(map[string]any)
	if first["id"] == "" || first["type"] != "function" {
		t.Errorf("调用缺 id/type: %v", first)
	}
	fn := first["function"].(map[string]any)
	if fn["name"] != "Bash" || fn["arguments"] != `{"command": "ls"}` {
		t.Errorf("function 字段错误: %v", fn)
	}
}

// 非流式：tool_calls 完整响应形状
func TestFCNonStreamResponseShape(t *testing.T) {
	resp := fcNonStreamToolCallsJSON("m", "hi", []fcCall{{Name: "Bash", ArgsJSON: `{}`}})
	var m map[string]any
	if err := json.Unmarshal([]byte(resp), &m); err != nil {
		t.Fatalf("非合法 JSON: %v", err)
	}
	ch := m["choices"].([]any)[0].(map[string]any)
	if ch["finish_reason"] != "tool_calls" {
		t.Errorf("finish_reason 错误: %v", ch)
	}
	msg := ch["message"].(map[string]any)
	if msg["content"] != "hi" {
		t.Errorf("content 错误: %v", msg)
	}
	tc := msg["tool_calls"].([]any)[0].(map[string]any)
	if tc["type"] != "function" {
		t.Errorf("tool_call.type 错误: %v", tc)
	}
}

// DSML 变体解析（云端 DeepSeek 实测输出形态，全角竖线 + 可能不闭合）
func TestParseFCToolCallsDSML(t *testing.T) {
	calls, content := parseFCToolCalls("<｜DSML｜tool_call>{\"name\": \"Read\", \"arguments\": {\"file_path\": \"/etc/hosts\"}}</｜DSML｜tool_call>")
	if len(calls) != 1 || calls[0].Name != "Read" {
		t.Fatalf("DSML 全角形态应解析: %+v", calls)
	}
	if strings.Contains(content, "DSML") {
		t.Errorf("content 应剥除 DSML 标记: %q", content)
	}
	// 半角形态 + 未闭合兜底
	calls2, _ := parseFCToolCalls(`prefix <|DSML|tool_call>{"name":"Bash","arguments":{"command":"ls"}}`)
	if len(calls2) != 1 || calls2[0].Name != "Bash" {
		t.Fatalf("DSML 半角未闭合应兜底解析: %+v", calls2)
	}
}
