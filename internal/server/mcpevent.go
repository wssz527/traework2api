// mcpevent.go — 本地 MCP 工具调用事件总线 + /internal/mcp-event 接收端点 + SSE 渲染辅助。
//
// 链路：trae-local-mcp server.js（每次工具调用 start/end 时 fire-and-forget POST）
// → 本端点解析 → 进程内 mcpEventBus（环形缓冲 + 订阅者广播，无持久化）
// → serveRemote 的 SSE 订阅者转成 OpenAI delta chunk，
// 让 API 调用方在流式响应里实时看到工具调用流水。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// mcpEventSinkBodyLimit /internal/mcp-event 请求体上限。
// P2 完整载荷单边 ≤4KB，JSON 字符串转义最坏可膨胀 6 倍（换行→\n、
// 控制字符→\uXXXX），双边 + 字段开销后取 32KB 余量。
const mcpEventSinkBodyLimit = 32 << 10

// mcpEventSubscriberBuffer 单订阅者 channel 缓冲：消费不动时新事件直接丢弃（非阻塞广播），
// 绝不让事件接收端点被慢消费者拖住。
const mcpEventSubscriberBuffer = 64

// mcpEventRingSize 环形缓冲容量：仅保留近期事件用于排查，无持久化。
const mcpEventRingSize = 512

// McpEvent MCP server 上报的工具调用事件（/internal/mcp-event 的 JSON body）。
//
// 会话关联（二期 P1）：MCP server 会把它从 HTTP 请求里能取到的会话级标识
// 一并带上（chat_session_id / session_keys / client_info / remote_addr）。
// 云端经 ngrok 转发本地 MCP 时是否携带会话标识尚未确认——若都为空，
// tw2api 侧退化为「活动任务注册表 + 时间窗归属」，见 taskRegistry。
type McpEvent struct {
	Event string `json:"event"` // tool_call_start / tool_call_end
	Tool  string `json:"tool"`  // 工具名

	// ChatSessionID 云端请求头里带的会话标识（可为空）。
	ChatSessionID string `json:"chat_session_id,omitempty"`
	// SessionKeys 所有疑似业务标识的请求头（取证用，可能为空对象）。
	SessionKeys map[string]string `json:"session_keys,omitempty"`
	// ClientInfo MCP initialize 阶段的 clientInfo（name/version）。
	ClientInfo map[string]any `json:"client_info,omitempty"`
	// RemoteAddr / XForwardedFor 来源地址，用于时间窗归属的辅助判据。
	RemoteAddr    string `json:"remote_addr,omitempty"`
	XForwardedFor string `json:"x_forwarded_for,omitempty"`

	// PendingID defer 模式（桥 MCP_DEFER=1）下 tool_pending / tool_timeout_fallback
	// 事件携带的挂起 ID：桥把经 ngrok 来的云端工具调用挂起为 pending 等客户端
	// 原生执行，tw2api 以此 ID 与第二轮回传的 tool_call_id 配对。
	PendingID string `json:"pending_id,omitempty"`

	// 完整载荷（二期 P2）：单边 ≤4KB，超出由上报方截断并置 *Truncated。
	ArgsFull        string `json:"args_full,omitempty"`
	ArgsTruncated   bool   `json:"args_truncated,omitempty"`
	ResultFull      string `json:"result_full,omitempty"`
	ResultTruncated bool   `json:"result_truncated,omitempty"`

	// 兼容旧字段（一期 200 字符预览），保留以免破坏其它消费方。
	ArgsPreview   string `json:"args_preview,omitempty"`
	ResultPreview string `json:"result_preview,omitempty"`

	OK         *bool     `json:"ok,omitempty"`          // 仅 tool_call_end：是否成功
	DurationMS int64     `json:"duration_ms,omitempty"` // 仅 tool_call_end：耗时 ms
	TS         int64     `json:"ts"`                    // 上报方毫秒时间戳
	RecvAt     time.Time `json:"-"`                     // 进程内接收时间
}

// mcpEventBus 进程内事件总线：定长环形缓冲 + 订阅者 channel 广播。
// 用于会话隔离（P1）——每条流式连接只收自己那个 chat_session_id 的事件。
type mcpEventFilter func(e McpEvent) bool

// mcpSub 单个订阅者的注册项（channel + 可选过滤器）。
type mcpSub struct {
	ch chan McpEvent
	f  mcpEventFilter
}

type mcpEventBus struct {
	mu     sync.Mutex
	ring   []McpEvent
	next   int
	filled int
	subs   map[int64]mcpSub
	subSeq int64
}

func newMcpEventBus() *mcpEventBus {
	return &mcpEventBus{
		ring: make([]McpEvent, mcpEventRingSize),
		subs: make(map[int64]mcpSub),
	}
}

// Publish 存入环形缓冲并广播给所有订阅者（非阻塞：满订阅者直接丢帧）。
func (b *mcpEventBus) Publish(e McpEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ring[b.next] = e
	b.next = (b.next + 1) % len(b.ring)
	if b.filled < len(b.ring) {
		b.filled++
	}
	for _, s := range b.subs {
		if s.f != nil && !s.f(e) {
			continue // 会话过滤：不属于本订阅者的事件直接跳过
		}
		select {
		case s.ch <- e:
		default:
		}
	}
}

// Subscribe 注册订阅者，返回取消句柄与只读 channel。
func (b *mcpEventBus) Subscribe() (int64, <-chan McpEvent) {
	return b.SubscribeFiltered(nil)
}

// SubscribeFiltered 带过滤器注册订阅者（P1 会话隔离）。
// f 为 nil 表示不过滤（等价 Subscribe）。
func (b *mcpEventBus) SubscribeFiltered(f mcpEventFilter) (int64, <-chan McpEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subSeq++
	id := b.subSeq
	ch := make(chan McpEvent, mcpEventSubscriberBuffer)
	b.subs[id] = mcpSub{ch: ch, f: f}
	return id, ch
}

// Unsubscribe 注销并关闭订阅者 channel（收到 ok=false 即事件流终止）。
func (b *mcpEventBus) Unsubscribe(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(s.ch)
	}
}

// recent 返回环形缓冲中最新的事件（按时间序），仅用于排查。
func (b *mcpEventBus) recent(n int) []McpEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := b.filled
	if n > total {
		n = total
	}
	out := make([]McpEvent, 0, n)
	for i := 0; i < n; i++ {
		idx := (b.next - n + i + len(b.ring)) % len(b.ring)
		out = append(out, b.ring[idx])
	}
	return out
}

// serveMcpEvent 接收本地 MCP server 的工具调用事件，推入事件总线。
// 无鉴权：依赖监听地址只绑本地网卡（main.go 的 Listen 配置），
// 事件本身仅为工具调用预览，不携带敏感数据。
func (h *Handler) serveMcpEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, mcpEventSinkBodyLimit+1))
	if err != nil || len(body) > mcpEventSinkBodyLimit {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var e McpEvent
	if err := json.Unmarshal(body, &e); err != nil || e.Event == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if e.TS == 0 {
		e.TS = time.Now().UnixMilli()
	}
	e.RecvAt = time.Now()
	if h.mcpEvents != nil {
		h.mcpEvents.Publish(e)
	}
	w.WriteHeader(http.StatusNoContent)
}

// mcpEventDelta 把 MCP 事件渲染为 OpenAI delta.content 文本（markdown 引用块）。
// 非工具调用事件返回空串（跳过）。
//
// 二期 P2：优先用完整载荷（args_full / result_full，单边 ≤4KB，由 MCP server
// 截断并置 truncated 标记）；旧版上报方只给 200 字符预览时回退到 preview 字段。
func mcpEventDelta(e McpEvent) string {
	switch e.Event {
	case "tool_call_start":
		args := e.ArgsFull
		if args == "" {
			args = e.ArgsPreview
		}
		if e.ArgsTruncated {
			args += " …(已截断)"
		}
		if args == "" {
			args = "…"
		}
		return fmt.Sprintf("\n\n> 🔧 [本地工具] %s · %s\n", e.Tool, onelineBlock(args))
	case "tool_call_end":
		icon := "✅"
		if e.OK != nil && !*e.OK {
			icon = "❌"
		}
		result := e.ResultFull
		if result == "" {
			result = e.ResultPreview
		}
		if e.ResultTruncated {
			result += "\n…(已截断)"
		}
		if result == "" {
			result = "…"
		}
		// 结果可能是多行（文件全文）：整体缩进成引用块，保持 markdown 可渲染。
		return fmt.Sprintf("> %s 完成 %dms\n>\n%s\n", icon, e.DurationMS, quoteLines(result))
	default:
		return ""
	}
}

// onelineBlock 入参行内展示：压缩空白为单行，超长截断。
// 工具参数是结构化 JSON，行内足够；结果才需要保留多行。
func onelineBlock(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 500 {
		s = s[:500] + "…"
	}
	if s == "" {
		s = "…"
	}
	return s
}

// quoteLines 把多行文本每行加 "> " 前缀，渲染为 markdown 引用块。
func quoteLines(s string) string {
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	for i, l := range lines {
		lines[i] = "> " + l
	}
	return strings.Join(lines, "\n")
}

// writeChatChunk 输出一个 OpenAI 流式 chunk。role 非空时写入 delta.role
// （首帧），content 非空时写入 delta.content；两者可同时为空（finish 帧）。
func writeChatChunk(w io.Writer, id string, created int64, model, role, content string, finishReason any) {
	delta := map[string]any{}
	if role != "" {
		delta["role"] = role
	}
	if content != "" {
		delta["content"] = content
	}
	chunk := map[string]any{
		"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]any{{
			"index":         0,
			"delta":         delta,
			"finish_reason": finishReason,
		}},
	}
	raw, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", raw)
}
