package server

import (
	"net/http/httptest"
	"strings"
	"testing"

	"traework2api/internal/upstream"
)

// 验证 anthropicRenderer 的事件成对性：content_block_start → delta → stop
func TestAnthropicRendererBlockPairing(t *testing.T) {
	var sb strings.Builder
	r := &anthropicRenderer{}
	r.streamStart(&sb, "msg_1", "glm-5.3", 1)
	// 思考增量 → thinking block
	r.delta(&sb, "msg_1", "glm-5.3", 1, upstream.RemoteEventDelta{Kind: upstream.DeltaReasoning, Reasoning: "思考"})
	// 正文增量 → text block
	r.delta(&sb, "msg_1", "glm-5.3", 1, upstream.RemoteEventDelta{Kind: upstream.DeltaContent, Content: "正文"})
	// 工具调用 → tool_use block
	r.delta(&sb, "msg_1", "glm-5.3", 1, upstream.RemoteEventDelta{Kind: upstream.DeltaToolCall, ToolLine: "> 🔧 [本地工具] read_file"})
	// 工具结果 → tool_result
	r.delta(&sb, "msg_1", "glm-5.3", 1, upstream.RemoteEventDelta{Kind: upstream.DeltaToolResult, ToolResult: "> ✅ [read_file] 结果"})
	// 结束
	r.streamFinish(&sb, "msg_1", "glm-5.3", 1, "")

	out := sb.String()
	// 事件必须成对：start 数 = stop 数
	starts := strings.Count(out, "content_block_start")
	stops := strings.Count(out, "content_block_stop")
	if starts != stops {
		t.Errorf("content_block start=%d stop=%d 应成对", starts, stops)
	}
	// 必须含 message_start 和 message_stop
	if !strings.Contains(out, "message_start") || !strings.Contains(out, "message_stop") {
		t.Errorf("缺 message_start/stop: %q", out[:200])
	}
	// 工具调用应含 tool_use
	if !strings.Contains(out, "tool_use") {
		t.Errorf("工具调用应渲染 tool_use block")
	}
}

// 验证非流式 Anthropic 响应
func TestAnthropicRendererNonStream(t *testing.T) {
	rec := httptest.NewRecorder()
	r := &anthropicRenderer{}
	r.nonStream(rec, "glm-5.3", "回复内容")
	if !strings.Contains(rec.Body.String(), "回复内容") {
		t.Errorf("非流式响应缺正文: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "message") {
		t.Errorf("非流式响应缺 type=message: %q", rec.Body.String())
	}
}
