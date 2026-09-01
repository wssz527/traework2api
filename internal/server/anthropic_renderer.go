// anthropic_renderer.go — Anthropic Messages 协议渲染器。
// 把 remote 通道的增量（reasoning/content/tool_call/tool_result）渲染成
// Anthropic SSE 事件（content_block start→delta→stop 成对）。
// 对齐 codex-proxy 的 codex-to-anthropic.ts 翻译模式。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"traework2api/internal/upstream"
)

// anthropicRenderer 渲染 Anthropic Messages API 事件流。
type anthropicRenderer struct {
	// blockSeq 当前 content_block 序号（start→delta→stop 成对）。
	blockSeq int
	// blockType 当前打开的 block 类型（text/thinking/tool_use/tool_result）。
	blockType string
}

// sseEvent 输出一条 Anthropic SSE 事件。
func sseEvent(w io.Writer, evt any) {
	raw, _ := json.Marshal(evt)
	fmt.Fprintf(w, "event: message_delta\n")
	_ = raw
}

func (r *anthropicRenderer) streamStart(w io.Writer, id, model string, created int64) {
	// Anthropic 流式首帧：message_start（含 message 元数据）
	msg := map[string]any{
		"id": id, "type": "message", "role": "assistant",
		"model": model, "content": []any{},
		"stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0},
	}
	evt := map[string]any{"type": "message_start", "message": msg}
	raw, _ := json.Marshal(evt)
	fmt.Fprintf(w, "event: message_start\ndata: %s\n\n", raw)
	// 初始 block：thinking（推理）占位——实际按增量类型开 block
}

func (r *anthropicRenderer) beginBlock(w io.Writer, blockType string) {
	if r.blockType != "" && r.blockType == blockType {
		return // 同一类型 block 已打开
	}
	// 关掉旧 block
	if r.blockType != "" {
		r.endBlock(w)
	}
	r.blockType = blockType
	start := map[string]any{"type": "content_block_start", "index": r.blockSeq, "content_block": map[string]any{"type": blockType}}
	if blockType == "tool_use" {
		start["content_block"] = map[string]any{"type": "tool_use", "id": fmt.Sprintf("toolu_%d", r.blockSeq), "name": "", "input": map[string]any{}}
	}
	raw, _ := json.Marshal(start)
	fmt.Fprintf(w, "event: content_block_start\ndata: %s\n\n", raw)
}

func (r *anthropicRenderer) deltaBlock(w io.Writer, blockType, text string) {
	if r.blockType != blockType {
		r.beginBlock(w, blockType)
	}
	var delta map[string]any
	switch blockType {
	case "thinking":
		delta = map[string]any{"type": "thinking_delta", "thinking": text}
	case "tool_use":
		delta = map[string]any{"type": "input_json_delta", "partial_json": text}
	default:
		delta = map[string]any{"type": "text_delta", "text": text}
	}
	evt := map[string]any{"type": "content_block_delta", "index": r.blockSeq, "delta": delta}
	raw, _ := json.Marshal(evt)
	fmt.Fprintf(w, "event: content_block_delta\ndata: %s\n\n", raw)
}

func (r *anthropicRenderer) endBlock(w io.Writer) {
	if r.blockType == "" {
		return
	}
	done := map[string]any{"type": "content_block_stop", "index": r.blockSeq}
	raw, _ := json.Marshal(done)
	fmt.Fprintf(w, "event: content_block_stop\ndata: %s\n\n", raw)
	r.blockSeq++
	r.blockType = ""
}

func (r *anthropicRenderer) delta(w io.Writer, id, model string, created int64, d upstream.RemoteEventDelta) {
	switch d.Kind {
	case upstream.DeltaReasoning:
		r.deltaBlock(w, "thinking", d.Reasoning)
	case upstream.DeltaContent:
		r.deltaBlock(w, "text", d.Content)
	case upstream.DeltaToolCall:
		// 工具调用：tool_use block（name + input）
		name, args := parseToolLine(d.ToolLine)
		if r.blockType != "tool_use" {
			r.beginBlock(w, "tool_use")
		}
		var input map[string]any
		_ = json.Unmarshal([]byte(args), &input)
		evt := map[string]any{
			"type": "content_block_delta", "index": r.blockSeq,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": args},
		}
		_ = name
		_ = input
		raw, _ := json.Marshal(evt)
		fmt.Fprintf(w, "event: content_block_delta\ndata: %s\n\n", raw)
	case upstream.DeltaToolResult:
		// 工具结果：tool_result block
		r.endBlock(w) // 关 tool_use
		r.beginBlock(w, "tool_result")
		evt := map[string]any{
			"type": "content_block_delta", "index": r.blockSeq,
			"delta": map[string]any{"type": "text_delta", "text": d.ToolResult},
		}
		raw, _ := json.Marshal(evt)
		fmt.Fprintf(w, "event: content_block_delta\ndata: %s\n\n", raw)
	}
}

func (r *anthropicRenderer) text(w io.Writer, id, model string, created int64, text string) {
	r.deltaBlock(w, "text", text)
}

func (r *anthropicRenderer) streamErr(w io.Writer, status int, code, msg string) {
	r.endBlock(w)
	evt := map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": msg}}
	raw, _ := json.Marshal(evt)
	fmt.Fprintf(w, "event: error\ndata: %s\n\n", raw)
	fmt.Fprintf(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func (r *anthropicRenderer) streamFinish(w io.Writer, id, model string, created int64, replyText string) {
	r.endBlock(w)
	// 收尾正文
	if replyText != "" {
		r.deltaBlock(w, "text", "\n\n"+replyText)
		r.endBlock(w)
	}
	// message_delta（stop_reason）+ message_stop
	md := map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}, "usage": map[string]any{"output_tokens": 0}}
	raw, _ := json.Marshal(md)
	fmt.Fprintf(w, "event: message_delta\ndata: %s\n\n", raw)
	fmt.Fprintf(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func (r *anthropicRenderer) nonStream(w http.ResponseWriter, model, replyText string) {
	created := time.Now().Unix()
	id := fmt.Sprintf("msg_%d", created)
	resp := map[string]any{
		"id": id, "type": "message", "role": "assistant", "model": model,
		"content": []map[string]any{{"type": "text", "text": replyText}},
		"stop_reason": "end_turn", "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": 0, "output_tokens": 0},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
