// mcpsession.go — 本地 MCP 事件的会话归属（二期 P1）。
//
// 问题：本地 MCP server（:8790）被云端沙盒经 ngrok 回调时，事件到达
// /internal/mcp-event 只带工具名与参数，不带「属于哪个 chat_session」的标识。
// 一期是全局广播，多任务并发会串台。
//
// 方案（两级）：
//  1. 强关联键：若事件自带 chat_session_id（云端通过请求头传递），直接精确匹配。
//     是否真有此头取决于云端实现 —— 由 /tmp/mcp-headers.log 取证确认。
//  2. 弱关联（当前默认）：活动任务注册表。serveRemote 在建会话后把
//     (chat_session_id, 起始时间, 账号) 登记进来，事件按时间窗归属：
//       - 只有一个活动任务 → 归属它（天然正确，也是绝大多数情况）
//       - 多个活动任务重叠 → 归属最近开始且未结束的那个；这是启发式，
//         存在残余歧义（见文末「已知局限」）
//
// 为什么不用更硬的判据：ngrok 只暴露一个出口 IP，云端所有会话共用；
// MCP 的 initialize 由长连接复用，clientInfo 里也不含会话信息。
// 若后续取证发现云端带了会话头，把 eventSessionID 的取值改一下即可，
// 注册表自动退化为兜底。
package server

import (
	"strings"
	"sync"
	"time"

	"traework2api/internal/auth"
)

// taskRegistry 活动 remote 任务注册表：记录当前正在跑的 chat_session。
type taskRegistry struct {
	mu    sync.Mutex
	tasks map[string]*activeTask // key: chat_session_id
}

// activeTask 一个正在执行的 remote 任务。
type activeTask struct {
	SessionID string
	Account   string // UID，用于区分账号（不同账号的并发任务互不干扰）
	StartedAt time.Time
	EndedAt   time.Time // 零值表示仍在运行

	// SandboxIP 云端沙盒 pod 的来源 IP（取自 MCP 请求的 X-Forwarded-For，
	// 由 serveRemote 在建会话后从首条事件学习并登记）。同一 pod 的并发
	// 会话共用该 IP，故它只能缩小范围、不能唯一确定会话。
	SandboxIP string
}

// newTaskRegistry 构造注册表。
func newTaskRegistry() *taskRegistry {
	return &taskRegistry{tasks: make(map[string]*activeTask)}
}

// register 登记一个活动任务（serveRemote 建会话成功后调用）。
func (r *taskRegistry) register(sessionID string, a *auth.Auth) {
	if sessionID == "" {
		return
	}
	uid := ""
	if a != nil {
		uid = a.UID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[sessionID] = &activeTask{
		SessionID: sessionID,
		Account:   uid,
		StartedAt: time.Now(),
	}
}

// finish 标记任务结束。保留记录一段时间（由 reap 清理），
// 以便迟到事件（工具回调可能晚于任务结束）仍能找到归属。
func (r *taskRegistry) finish(sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[sessionID]; ok && t.EndedAt.IsZero() {
		t.EndedAt = time.Now()
	}
}

// reap 清理结束超过 keep 的任务记录。
func (r *taskRegistry) reap(keep time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cut := time.Now().Add(-keep)
	for id, t := range r.tasks {
		if !t.EndedAt.IsZero() && t.EndedAt.Before(cut) {
			delete(r.tasks, id)
		}
	}
}

// MatchXFF 按 X-Forwarded-For（云端沙盒 pod IP）做归属判定。
//
// 取证（/tmp/mcp-headers.log）：云端调本地 MCP 的请求头里没有任何会话级
// 标识，唯一随任务变化的是 x-forwarded-for（沙盒 pod 出口 IP）。同一任务
// 的所有 MCP 调用来自同一 pod，故「事件 XFF == 该会话已学到的 pod IP」
// 是强归属信号；会话尚未学到 IP（首条事件之前）或 XFF 为空时返回 false，
// 由调用方落入时间窗启发式。
func (r *taskRegistry) MatchXFF(e McpEvent, sessionID string) bool {
	if r == nil || sessionID == "" || e.XForwardedFor == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[sessionID]
	// 与 LearnXFF 同口径：都取 XFF 首段（mcpEventXFF）比较。
	ip := mcpEventXFF(e)
	return ok && t.SandboxIP != "" && ip != "" && t.SandboxIP == ip
}

// LearnXFF 从事件里为会话登记其沙盒 pod IP（首次学习后不再覆盖：
// 同一任务理论上固定在一个 pod 上跑，防晚到事件把 IP 改串）。
func (r *taskRegistry) LearnXFF(sessionID, xff string) {
	if sessionID == "" || xff == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[sessionID]; ok && t.SandboxIP == "" {
		t.SandboxIP = xff
	}
}

// mcpEventXFF 提取事件的沙盒 pod IP：X-Forwarded-For 首段（最接近云端
// 沙盒的跳）。取证（/tmp/mcp-headers.log）显示云端→ngrok→本地链路里
// XFF 恒为单值沙盒 IP（115.191.57.248），这里兼容多段写法取首段。
func mcpEventXFF(e McpEvent) string {
	xff := strings.TrimSpace(e.XForwardedFor)
	if xff == "" {
		return ""
	}
	if i := strings.Index(xff, ","); i >= 0 {
		xff = xff[:i]
	}
	return strings.TrimSpace(xff)
}

// eventSessionID 从事件里提取会话标识（强关联键）。
// 返回空串表示事件本身不带标识，需走注册表时间窗归属。
func eventSessionID(e McpEvent) string {
	if e.ChatSessionID != "" {
		return e.ChatSessionID
	}
	// 兼容：部分上报方可能把标识塞进 session_keys 的常见键名。
	for _, k := range []string{
		"x-chat-session-id", "x-chatsessionid", "x-trae-chat-session-id",
		"x-session-id", "x-trae-session-id", "x-task-id", "x-trae-task-id",
	} {
		if v, ok := e.SessionKeys[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

// belongsTo 判断事件是否属于会话 sessionID。
//
// 判定顺序：
//  1. 事件自带会话标识 → 精确比较
//  2. 事件 XFF 与该会话已学习的沙盒 pod IP 一致 → 归属（强信号，取证依据
//     见 MatchXFF：云端 MCP 请求无会话头，XFF 是唯一随任务变化的标识）
//  3. 注册表里存在该会话 → 时间窗归属：
//     - 唯一活动任务 → 归属
//     - 多任务重叠 → 归属「已开始且最晚开始」的那个（最近活跃启发式）
//  4. 会话已不在注册表（任务已结束/未登记）→ 不归属
//
// 注意 2 在先：多任务重叠但 pod IP 不同（不同沙盒）时 XFF 能无歧义区分；
// 同 pod 重叠才落到 3 的启发式（残余风险，见文末「已知局限」）。
func (r *taskRegistry) belongsTo(e McpEvent, sessionID string, now time.Time) bool {
	if r == nil || sessionID == "" {
		return false
	}
	// 1) 强关联键优先。
	if sid := eventSessionID(e); sid != "" {
		return sid == sessionID
	}
	// 2) XFF pod 匹配（已学习过该会话的 pod IP 时为强信号）。
	if r.MatchXFF(e, sessionID) {
		return true
	}
	// 3) 弱关联：活动时间窗。
	r.mu.Lock()
	defer r.mu.Unlock()
	target, ok := r.tasks[sessionID]
	if !ok {
		return false
	}
	// 收集与 target 时间窗重叠、且同一账号的其它任务。
	var overlap []*activeTask
	for _, t := range r.tasks {
		if t.SessionID == sessionID {
			continue
		}
		if t.Account != target.Account {
			continue // 不同账号的任务不会共用同一个本地 MCP 事件源
		}
		if now.Before(t.StartedAt) {
			continue
		}
		if !t.EndedAt.IsZero() && t.EndedAt.Before(now) {
			continue // 已结束且不覆盖 now
		}
		overlap = append(overlap, t)
	}
	if len(overlap) == 0 {
		return true // 唯一活动任务：天然归属
	}
	// 多任务重叠：归属「最晚开始且仍未结束」的那个。
	best := target
	for _, t := range overlap {
		if t.EndedAt.IsZero() && !best.EndedAt.IsZero() {
			best = t
			continue
		}
		if t.StartedAt.After(best.StartedAt) && (t.EndedAt.IsZero() || !best.EndedAt.IsZero()) {
			best = t
		}
	}
	return best.SessionID == sessionID
}

// 已知局限（写在这里以免后人口头传承丢失）：
//   - 弱关联是启发式。若同一账号的两个 remote 任务在同一 pod 上时间完全
//     重叠（XFF 相同），本地工具事件可能被归到后开始的那个任务上。
//   - XFF 学习是「首条事件即锁定」：极端情况下云端把任务迁移到另一 pod，
//     后续事件会因 IP 不匹配落入时间窗启发式，不至于丢失归属。
// 要彻底消除歧义只有两条路：
//   a) 云端在 MCP 请求头里带 chat_session_id（已取证：不带，见 /tmp/mcp-headers.log）
//   b) 每个 remote 任务独占一个本地 MCP 实例/端口（成本高，需改造 bridge.sh）
// 当前 e2e 与日常使用（单任务串行）不受影响。
