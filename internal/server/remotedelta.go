// remotedelta.go — 云端事件流增量 → OpenAI 流式 chunk（二期 P0）+ 工具去重。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"traework2api/internal/upstream"
)

// writeRemoteDelta 把一个云端事件增量写成 OpenAI 流式 chunk。
// 同一增量至多含一种正文：reasoning_content / content / 工具注记行。
// 三者都为空时不写任何字节（调用方已过滤，这里再兜一层）。
func writeRemoteDelta(w io.Writer, id string, created int64, model string, d upstream.RemoteEventDelta) {
	switch {
	case d.Reasoning != "":
		writeChunkWithReasoning(w, id, created, model, d.Reasoning)
	case d.Content != "":
		writeChatChunk(w, id, created, model, "", d.Content, nil)
	case d.ToolLine != "":
		// 工具调用走 OpenAI 流式结构化字段 delta.tool_calls，
		// 客户端正确显示为「工具调用」而非普通文本。
		writeChunkWithToolCall(w, id, created, model, d.ToolLine)
	}
}

// writeChunkWithToolCall 输出带 tool_calls 的 chunk（工具调用结构化字段）。
// 解析注记行里的工具名与参数摘要，尽量填充 function.name / arguments。
func writeChunkWithToolCall(w io.Writer, id string, created int64, model, line string) {
	name, args := parseToolLine(line)
	delta := map[string]any{
		"tool_calls": []map[string]any{{
			"index": 0,
			"type":  "function",
			"function": map[string]any{
				"name":      name,
				"arguments": args,
			},
		}},
	}
	chunk := map[string]any{
		"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]any{{
			"index": 0, "delta": delta, "finish_reason": nil,
		}},
	}
	raw, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", raw)
}

// parseToolLine 从注记行提取工具名与参数 JSON。
// 行格式（remote_events.go 的 formatToolLine）："> 🔧 [本地工具] 标签/工具名 · 参数"
// 或 "> 🔧 [沙盒] 工具名"。
func parseToolLine(line string) (name, args string) {
	s := strings.TrimSpace(line)
	s = strings.TrimPrefix(s, ">")
	s = strings.TrimSpace(s)
	// 去 emoji 前缀
	for _, em := range []string{"🔧", "✅", "❌"} {
		s = strings.TrimPrefix(s, em)
	}
	s = strings.TrimSpace(s)
	// 去 [分类] 前缀
	if i := strings.Index(s, "]"); i >= 0 && strings.HasPrefix(s, "[") {
		s = s[i+1:]
	}
	s = strings.TrimSpace(s)
	// 分离 工具名 · 参数
	if i := strings.Index(s, "·"); i >= 0 {
		name = strings.TrimSpace(s[:i])
		args = strings.TrimSpace(s[i+1:])
	} else {
		name = strings.TrimSpace(s)
	}
	// 工具名取 / 后末段
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	name = strings.TrimSpace(name)
	// 参数若是 JSON 保持，否则当字符串参数
	if args != "" && !strings.HasPrefix(args, "{") {
		args = fmt.Sprintf(`{"input":%q}`, args)
	}
	return name, args
}

// writeChunkWithReasoning 输出带 reasoning_content 的 chunk（DeepSeek 风格字段）。
// 单独一个函数是因为 delta 结构与普通 content chunk 不同。
func writeChunkWithReasoning(w io.Writer, id string, created int64, model, reasoning string) {
	delta := map[string]any{"reasoning_content": reasoning}
	chunk := map[string]any{
		"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]any{{
			"index":         0,
			"delta":         delta,
			"finish_reason": nil,
		}},
	}
	raw, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", raw)
}

// toolDedup 抑制同一本地工具调用的双重呈现。
//
// 背景：同一个本地 MCP 调用会在两条路上各出现一次——
//
//	① 本地 MCP server 的旁路事件（/internal/mcp-event）：🔧/✅ 行，带完整参数与结果
//	② 云端事件流的 plan_item（tool_call_info.name=="run_mcp"）：🔧 行
//
// 两路到达顺序不保证（本地事件通常先到，但不绝对）。本结构按
// 「工具名 + 时间窗」匹配：任一路先输出即登记，另一路在窗口内命中则跳过。
type toolDedup struct {
	mu     sync.Mutex
	window time.Duration
	seenAt map[string]time.Time // key: 工具名
}

// newToolDedup 构造去重器，window 为同名工具的抑制窗口。
func newToolDedup(window time.Duration) *toolDedup {
	return &toolDedup{window: window, seenAt: make(map[string]time.Time)}
}

// mark 登记一个本地 MCP 工具调用（由本地事件侧调用）。
func (d *toolDedup) mark(tool string) {
	if tool == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seenAt[normalizeToolKey(tool)] = time.Now()
}

// seen 判断某条工具注记是否已被另一路呈现过。
//
// 语义是「查询并登记」：未命中则登记并返回 false（本次应输出）；
// 命中则消费该标记并返回 true（本次应跳过）。
// 用「待消费标记」而非纯时间窗，避免同一工具连续调用两次时第二次被误抑。
func (d *toolDedup) seen(toolLine string) bool {
	key := toolKeyFromLine(toolLine)
	if key == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	// 清理过期标记，避免长任务里 map 持续增长。
	for k, t := range d.seenAt {
		if now.Sub(t) > d.window {
			delete(d.seenAt, k)
		}
	}
	if t, ok := d.seenAt[key]; ok && now.Sub(t) <= d.window {
		delete(d.seenAt, key) // 消费：一次登记只抑制一次
		return true
	}
	d.seenAt[key] = now
	return false
}

// normalizeToolKey 归一化工具名（去空白、小写），消除两路命名差异。
func normalizeToolKey(tool string) string {
	return strings.ToLower(strings.TrimSpace(tool))
}

// toolKeyFromLine 从注记行里抽出工具名。
// 行格式（remote_events.go 的 formatToolLine）："\n\n> 🔧 [本地工具] 标签/工具名 · 参数\n"
// 取工具名末段（斜杠后），与本地 MCP 事件的 tool 字段对齐。
func toolKeyFromLine(line string) string {
	s := strings.TrimSpace(line)
	if s == "" {
		return ""
	}
	// 定位 "·" 之前的工具部分，再取 "/" 后的末段。
	head := s
	if i := strings.Index(s, "·"); i >= 0 {
		head = s[:i]
	}
	head = strings.TrimSpace(head)
	if i := strings.LastIndex(head, "/"); i >= 0 {
		head = head[i+1:]
	}
	head = strings.TrimSpace(head)
	// 去掉可能残留的引用块前缀。
	head = strings.TrimPrefix(head, ">")
	head = strings.TrimPrefix(head, "🔧")
	head = strings.TrimSpace(head)
	if i := strings.LastIndex(head, " "); i >= 0 {
		head = head[i+1:]
	}
	return normalizeToolKey(head)
}
