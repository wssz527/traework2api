// remote_events.go — remote 通道实时事件流（二期 P0）。
//
// 通道：GET {RemoteHost}/api/remote/v1/chat_sessions/{id}/events，SSE 长流。
// 该端点逆向自 SOLO CN 0.1.56 的 solo-lite bundle 路由表（实测已验证）：
//
//	"chat.subscribe":{method:"GET",
//	  path:"/api/remote/v1/chat_sessions/:chat_session_id/events",
//	  type:"subscribe",transport:"api"}
//
// 注意：一期假设的 explorer WebSocket（wss://.../explorer/{id}）**不是**
// chat 事件通道。实测结论（/tmp/ws-probe.jsonl）：
//   - WS 握手 101 成功、system.ping / system.info 可用（沙盒内 VS Code server）
//   - 102 个候选订阅 method（chat.subscribe / lite.subscribe_events /
//     subscribe_events 等）全部返回 -32601 Method not found
//
// 因此本文件按 REST SSE 实现，WS 侧废弃。
//
// 真实事件载荷（2026-08-31 实跑确认）：
//
//	event: status_changed / platform_timing / metadata / plan_item /
//	       model_config / session_title_message / session_icon_message /
//	       timing_events / token_usage / context_usage / heartbeat / done
//	SSE 行：id: {turn_id}:{seq}  —— 可用于断线续传（Last-Event-ID）
//	plan_item 载荷：{id, task_id, thought, reasoning_content, timing,
//	                tool_call_info:{id,name,params,result,meta},
//	                agent_id, agent_status, __packet_seq__}
//
// 关键语义：thought / reasoning_content 是**累积整值**而非增量 delta，
// 同一 plan_item.id 的后续帧会重复携带已发内容。故本文件按 plan_item.id
// 做前缀差分，只把新增后缀作为 delta 输出，避免正文重复刷屏。
//
// 设计约束（与一期的分工）：
//   - 本流只做**增量**，不做终态裁决。终态仍由 RemoteWaitAndRead 的
//     status=completed + extractAssistantText 决定，保证最终全文一字不差。
//   - 连不上/非 200/读流出错 → 静默降级（返回 error，由调用方忽略），
//     功能不回退到一期之前。
package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"traework2api/internal/auth"
)

// RemoteEpEvents remote 会话实时事件端点（SSE）。
const RemoteEpEvents = "/api/remote/v1/chat_sessions/%s/events"

// RemoteEvent 已解析的一条远端事件，供 handler 映射成 OpenAI delta。
type RemoteEvent struct {
	// Type 为 SSE 的 event: 行（plan_item / text_message / done …）。
	Type string
	// ID 为 SSE 的 id: 行（{turn_id}:{seq}），断线续传用。
	ID string
	// Raw 为 data: 行的原始 JSON。
	Raw []byte
}

// RemoteEventDelta 事件转换后的增量片段。
// Content 与 Reasoning 二者至多一个有值（同一时刻只推一种）。
type RemoteEventDelta struct {
	// Content 正文增量 → delta.content。
	Content string
	// Reasoning 思考过程增量 → delta.reasoning_content。
	Reasoning string
	// ToolLine 工具调用注记行 → delta.content（与一期 🔧 行风格统一）。
	ToolLine string
}

// remoteEventStream 事件流客户端：负责拨流、断线重连、事件 → delta 映射。
// 零值不可用，请用 newRemoteEventStream 构造。
type remoteEventStream struct {
	client *Client
	auth   *auth.Auth
	sessID string

	// lastEventID 最近收到的 SSE id，重连时通过 Last-Event-ID 续传。
	lastEventID string
	// seen 每个 plan_item.id 已输出的累积文本，用于前缀差分取增量。
	seen map[string]seenItem
	// sawDone 是否已收到 done 终态帧（用于判定 EOF 是否为异常断流）。
	sawDone bool
	// emittedTools 已输出过的工具调用注记（tool_call_info.id），
	// 避免同一工具在多帧里重复刷屏（plan_item 会重复携带同一 tool_call）。
	emittedTools map[string]bool
	// emittedResults 已输出过结果回传的 tool_call.id（结果帧可能多帧，
	// 只输出第一次）。
	emittedResults map[string]bool
}

// seenItem 单个 plan_item 已输出的累积值。
type seenItem struct {
	thought   string
	reasoning string
}

// newRemoteEventStream 构造事件流客户端。
func newRemoteEventStream(c *Client, a *auth.Auth, sessionID string) *remoteEventStream {
	return &remoteEventStream{
		client:       c,
		auth:         a,
		sessID:       sessionID,
		seen:         make(map[string]seenItem),
		emittedTools: make(map[string]bool),
		emittedResults: make(map[string]bool),
	}
}

// remoteEventReconnectDelays 重连退避：3s → 6s → 12s，共 3 次后放弃。
// 与 GUI 客户端行为一致（bundle 内 reconnect:{maxAttempts:3,delay:3e3,
// backoffMultiplier:2}）。
var remoteEventReconnectDelays = []time.Duration{3 * time.Second, 6 * time.Second, 12 * time.Second}

// Stream 拨号并持续投递事件增量，直到 ctx 取消 / 收到 done / 重连耗尽。
// out 由调用方消费；本函数保证返回前关闭 out。任何失败都是静默降级：
// 调用方只需停止消费，不得把错误当作任务失败。
func (s *remoteEventStream) Stream(ctx context.Context, out chan<- RemoteEventDelta) {
	defer close(out)

	// 首次拨号失败直接降级：不重试，避免任务被事件流拖住。
	resp, err := s.dial(ctx)
	if err != nil {
		return
	}
	attempt := 0
	body := resp.Body
	for {
		// 读流：正常结束（done / ctx 取消 / 服务端关闭）返回 nil，
		// 需要重连时返回 errNeedReconnect。
		rerr := s.readLoop(ctx, body, out)
		body.Close()
		if rerr == nil {
			return
		}
		if !isNeedReconnect(rerr) {
			return
		}
		if attempt >= len(remoteEventReconnectDelays) {
			return // 重连上限后本轮放弃，静默降级
		}
		wait := remoteEventReconnectDelays[attempt]
		attempt++
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		resp2, err := s.dial(ctx)
		if err != nil {
			return
		}
		body = resp2.Body
	}
}

// errNeedReconnect 标记「需要重连」的内部错误。
type errNeedReconnect struct{ cause error }

func (e *errNeedReconnect) Error() string { return "reconnect: " + e.cause.Error() }
func (e *errNeedReconnect) Unwrap() error { return e.cause }

func isNeedReconnect(err error) bool {
	_, ok := err.(*errNeedReconnect)
	return ok
}

// RemoteEventsDisabled 置为非空时禁用云端事件流（静默降级）。
// 仅用于 e2e 验证降级路径，正常部署不设置。
var RemoteEventsDisabled = os.Getenv("TW2API_DISABLE_REMOTE_EVENTS") != ""

// dial 发起 SSE 请求，返回已就绪（200）的响应体。
func (s *remoteEventStream) dial(ctx context.Context) (*http.Response, error) {
	if RemoteEventsDisabled {
		return nil, errors.New("remote events disabled by env")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		remoteHostURL() + fmt.Sprintf(RemoteEpEvents, s.sessID), nil)
	if err != nil {
		return nil, err
	}
	// RemoteHeaders 会把 Accept 设成 application/json，必须在其后覆写。
	RemoteHeaders(req, s.auth)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if s.lastEventID != "" {
		req.Header.Set("Last-Event-ID", s.lastEventID)
	}
	// 无总超时的客户端：事件流可持续数分钟，超时由 ctx 控制。
	hc := &http.Client{}
	if s.client != nil && s.client.StreamHTTP != nil {
		// 复用主客户端的 Transport（连接池），但不继承其超时设置。
		hc = &http.Client{Transport: s.client.StreamHTTP.Transport}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &httpError{status: resp.StatusCode}
	}
	return resp, nil
}

// httpError 非 200 响应（401/403 等）：一律按降级处理。
type httpError struct{ status int }

func (e *httpError) Error() string { return "events: http " + strconv.Itoa(e.status) }

// readLoop 解析 SSE 帧并投递增量。返回 nil 表示正常结束；
// 返回 *errNeedReconnect 表示连接中断且可重连。
func (s *remoteEventStream) readLoop(ctx context.Context, body interface{ Read([]byte) (int, error) }, out chan<- RemoteEventDelta) error {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), 8<<20) // 单帧最大 8MB（plan_item 可携带大 result）

	// ctx 取消时关闭 body，让阻塞中的 Scan 立刻返回。
	done := make(chan struct{})
	defer close(done)
	if cc, ok := body.(interface{ Close() error }); ok {
		go func() {
			select {
			case <-ctx.Done():
				cc.Close()
			case <-done:
			}
		}()
	}

	var cur RemoteEvent
	dataLines := make([]string, 0, 1)
	flush := func() {
		if cur.Type == "" {
			dataLines = dataLines[:0]
			return
		}
		ev := cur
		if len(dataLines) > 0 {
			ev.Raw = []byte(strings.Join(dataLines, "\n"))
		}
		for _, d := range s.mapEvent(ev) {
			select {
			case out <- d:
			case <-ctx.Done():
				return
			}
		}
		cur = RemoteEvent{}
		dataLines = dataLines[:0]
	}

	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			flush() // 空行=帧结束
		case strings.HasPrefix(line, ":"):
			// 注释行（含心跳注释），忽略
		case strings.HasPrefix(line, "id: "):
			cur.ID = line[len("id: "):]
			if cur.ID != "" {
				s.lastEventID = cur.ID
			}
		case strings.HasPrefix(line, "event: "):
			cur.Type = line[len("event: "):]
		case strings.HasPrefix(line, "data: "):
			dataLines = append(dataLines, line[len("data: "):])
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, line[len("data:"):])
		}
	}
	if err := sc.Err(); err != nil {
		select {
		case <-ctx.Done():
			return nil // 调用方主动取消：正常结束
		default:
		}
		return &errNeedReconnect{cause: err}
	}
	// 流正常读到 EOF 但从未收到 done 帧：说明连接被中途掐断（网关超时、
	// 服务端重启等），按可重连处理，否则增量会静默缺失。
	if !s.sawDone {
		return &errNeedReconnect{cause: errUnexpectedEOF}
	}
	return nil
}

// errUnexpectedEOF 未收到 done 帧就到达流末尾。
var errUnexpectedEOF = errors.New("stream closed before done event")

// mapEvent 把一条远端事件映射成零或多个增量片段。
// 未识别 / 无需外显的事件返回 nil（静默忽略）。
func (s *remoteEventStream) mapEvent(ev RemoteEvent) []RemoteEventDelta {
	switch ev.Type {
	case "plan_item":
		return s.mapPlanItem(ev.Raw)
	case "done":
		// 终态帧：不做裁决（仍归轮询），仅标记本流正常收尾。
		s.sawDone = true
		return nil
	default:
		// metadata / token_usage / fee_usage / timing_events / heartbeat /
		// status_changed / model_config / session_title_message /
		// session_icon_message / platform_timing / context_usage 等：
		// 不映射到客户端，仅内部使用。
		return nil
	}
}

// planItem 是 plan_item 事件里我们关心的字段子集。
// 其余字段（agent_id/agent_status/meta/__packet_seq__ 等）当前不需要。
type planItem struct {
	ID               string          `json:"id"`
	Thought          string          `json:"thought"`
	ReasoningContent string          `json:"reasoning_content"`
	ToolCallInfo     *planToolCall   `json:"tool_call_info"`
	ReplyToMessageID string          `json:"reply_to_message_id"`
	Extra            json.RawMessage `json:"-"`
}

// planToolCall plan_item.tool_call_info。
type planToolCall struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Meta   json.RawMessage `json:"meta"`
}

// mapPlanItem 处理 plan_item：
//  1. reasoning_content 累积值的增量 → delta.reasoning_content
//  2. thought 累积值的增量 → delta.content（本模型把正文放这里）
//  3. 首次出现的工具调用 → 工具注记行（🔧/✅），同一 tool_call.id 只出一次
func (s *remoteEventStream) mapPlanItem(raw []byte) []RemoteEventDelta {
	var pi planItem
	if err := json.Unmarshal(raw, &pi); err != nil {
		return nil
	}
	key := pi.ID
	prev := s.seen[key]
	var out []RemoteEventDelta

	// 思考过程：累积整值差分。
	if inc := prefixDelta(prev.reasoning, pi.ReasoningContent); inc != "" {
		out = append(out, RemoteEventDelta{Reasoning: inc})
	}
	// 正文：累积整值差分。
	if inc := prefixDelta(prev.thought, pi.Thought); inc != "" {
		out = append(out, RemoteEventDelta{Content: inc})
	}
	s.seen[key] = seenItem{thought: pi.Thought, reasoning: pi.ReasoningContent}

	// 工具调用注记：同一 tool_call.id 只输出一次。
	// 先渲染再标记：若本帧渲染为空（如 run_mcp 的参数未补齐空壳帧），
	// 不占用配额，等参数完整的后续帧再输出。
	if tci := pi.ToolCallInfo; tci != nil && tci.ID != "" && tci.Name != "" {
		if !s.emittedTools[tci.ID] {
			if line := formatToolLine(tci); line != "" {
				s.emittedTools[tci.ID] = true
				out = append(out, RemoteEventDelta{ToolLine: line})
			}
		}
		// 工具结果回传（对齐 codex-proxy 的 result 转发）：同一工具
		// 首次携带非空 result 时输出 ✅ 结果行，让客户端看到 MCP 执行
		// 完成及结果摘要——否则工具执行过程完全不可见，文本全堆一起。
		if tci.Result != nil && len(tci.Result) > 2 && !s.emittedResults[tci.ID] {
			if line := formatToolResult(tci); line != "" {
				s.emittedResults[tci.ID] = true
				out = append(out, RemoteEventDelta{ToolLine: line})
			}
		}
	}
	return out
}

// formatToolResult 把工具 result 渲染成结果回传行。
// MCP 工具（run_mcp）：✅ 工具名 → 结果摘要（stdout/内容前 200 字）。
// 其余沙盒工具：✅ 结果摘要。返回空串表示无需外显。
func formatToolResult(t *planToolCall) string {
	name := t.Name
	var summary string
	switch name {
	case "run_mcp":
		var p struct {
			ToolName string `json:"tool_name"`
		}
		_ = json.Unmarshal(t.Params, &p)
		if p.ToolName != "" {
			name = p.ToolName
		}
	}
	// 结果摘要：优先取常见字段，否则截 JSON。
	raw := string(t.Result)
	var m map[string]any
	if json.Unmarshal(t.Result, &m) == nil {
		if v, ok := m["content"].(string); ok && v != "" {
			summary = v
		} else if v, ok := m["output"].(string); ok && v != "" {
			summary = v
		} else if v, ok := m["stdout"].(string); ok && v != "" {
			summary = v
		} else if v, ok := m["result"].(string); ok && v != "" {
			summary = v
		}
	}
	if summary == "" {
		summary = raw
	}
	summary = oneLine(summary, 200)
	if summary == "" {
		return ""
	}
	return "\n\n> ✅ [" + name + "] " + summary + "\n"
}

// formatToolLine 把工具调用渲染成与一期风格统一的注记行。
// MCP 工具（run_mcp）单列一行并带上工具名与参数；其余沙盒工具简写。
// 返回空串表示该工具无需外显（如 finish 的 summary 已是正文，不重复注记）。
func formatToolLine(t *planToolCall) string {
	name := t.Name
	if name == "finish" {
		return "" // finish 的 params.summary 已作为正文输出，无需注记
	}
	switch name {
	case "run_mcp":
		var p struct {
			ServerLabel string         `json:"server_label"`
			ToolName    string         `json:"tool_name"`
			Args        map[string]any `json:"args"`
		}
		_ = json.Unmarshal(t.Params, &p)
		// 云端会先发一帧空壳（tool_name/args 均为空），参数在后续帧才补齐。
		// 空壳帧不注记：否则它先输出一条无信息的行，反而让随后到达的、
		// 带真实参数的本地 MCP 行被去重抑制（信息倒挂）。
		if p.ToolName == "" && len(p.Args) == 0 {
			return ""
		}
		label := p.ToolName
		if label == "" {
			label = "mcp"
		}
		if p.ServerLabel != "" {
			label = p.ServerLabel + "/" + label
		}
		args := compactJSON(p.Args)
		if args == "" || args == "{}" {
			args = compactJSONRaw(t.Params)
		}
		return "\n\n> 🔧 [本地工具] " + label + " · " + oneLine(args, 300) + "\n"
	default:
		// 沙盒内工具（Read / EnvironmentSetup 等）：只给一行轻量提示，
		// 不展开参数，避免与本地 MCP 行混淆、也不刷屏。
		return "\n\n> 🔧 [沙盒] " + name + "\n"
	}
}

// prefixDelta 求累积整值相对已输出前缀的新增后缀。
// cur 不以 prev 为前缀时（服务端重放/乱序），返回 cur 本身保守处理。
func prefixDelta(prev, cur string) string {
	if cur == "" {
		return ""
	}
	if len(prev) > len(cur) {
		return "" // 累积值变短：异常，跳过本帧
	}
	if prev == cur {
		return ""
	}
	if strings.HasPrefix(cur, prev) {
		return cur[len(prev):]
	}
	return cur
}

// compactJSON 紧凑序列化（无空格），失败返回空串。
func compactJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// compactJSONRaw 紧凑化一段原始 JSON（去空白），失败原样返回。
func compactJSONRaw(raw json.RawMessage) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return compactJSON(v)
}

// oneLine 压缩换行与空白并截断，用于工具参数注记。
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(strings.ReplaceAll(s, "\n", " ")), " ")
	if len(s) > n {
		s = s[:n] + "…"
	}
	if s == "" {
		s = "…"
	}
	return s
}

// RemoteOpenEventStream 便捷入口：起 goroutine 拨流，返回增量 channel。
// ctx 取消即关流。失败时 channel 会直接关闭（消费者读到零值即降级）。
func (c *Client) RemoteOpenEventStream(ctx context.Context, a *auth.Auth, sessionID string) <-chan RemoteEventDelta {
	out := make(chan RemoteEventDelta, 64)
	go newRemoteEventStream(c, a, sessionID).Stream(ctx, out)
	return out
}
