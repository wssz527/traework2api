// toolinject.go — tool_calls 闭环的 pending 注册表 + 回注端点 + 桥回灌。
//
// 链路（桥 defer 模式，MCP_DEFER=1）：
//
//	桥收到云端工具调用 → 不本地执行，挂起为 pending（120s 超时兜底）
//	→ 推 tool_pending 事件（带 pending_id）到 /internal/mcp-event
//	→ serveRemotePumpWith 订阅分支登记进本注册表，并向客户端输出
//	  原生 tool_calls 终帧结束本轮（callID 即 pending_id）
//	→ 客户端本地执行工具，下一轮请求回传 role:tool 消息
//	→ serveRemoteWith 检出增量里的 role:tool 且能配对到未决 pending
//	  → POST 桥 /internal/inject-result 解除挂起（云端 run_mcp 由此继续）
//	  → 本轮不触云端，直接短路回复
//
// 兜底：挂起超过 pendingTTL 无回注（客户端死了/回注失败）→ 注册表惰性
// 清理掉条目；桥侧同窗口本地执行照旧返回，云端永不卡死。桥超时兜底时
// 会推 tool_timeout_fallback 事件，订阅分支据此立即作废对应条目。
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// pendingTTL pending 条目生存期，与桥 MCP_PENDING_TIMEOUT（默认 120s）对齐：
// 超过这个窗口桥已本地兜底执行，客户端再回注只会被桥 404，条目留在
// tw2api 侧也没有意义。var 便于测试调短。
var pendingTTL = 120 * time.Second

// pendingEntry 一个挂起中的本地工具调用。
type pendingEntry struct {
	sessID    string        // 登记时所处的云端会话（serveRemotePumpWith 的 sessID）
	tool      string        // 工具名（回灌匹配与 /internal/inject-result 事件回填用）
	ch        chan McpEvent // 回注结果（缓冲 1；HTTP 端点写入，语义上供后续消费者取用）
	createdAt time.Time
}

// pendingRegistry 未决 pending 注册表：pendingID → 条目，另支持按 sessID 反查。
// Handler 级生命周期（第一腿登记与第二轮回灌分属两次 HTTP 请求），
// 靠 pendingTTL 惰性清理防止泄漏。
type pendingRegistry struct {
	mu sync.Mutex
	m  map[string]*pendingEntry
}

func newPendingRegistry() *pendingRegistry {
	return &pendingRegistry{m: make(map[string]*pendingEntry)}
}

// sweepLocked 清理超时条目（调用方须持锁）。
func (r *pendingRegistry) sweepLocked(now time.Time) {
	for id, e := range r.m {
		if now.Sub(e.createdAt) > pendingTTL {
			delete(r.m, id)
		}
	}
}

// register 登记一个 pending，返回结果 channel。重复登记（同 ID 的
// tool_pending 重发）幂等返回已有条目的 channel。
func (r *pendingRegistry) register(pendingID, sessID, tool string) chan McpEvent {
	if r == nil || pendingID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(time.Now())
	if e, ok := r.m[pendingID]; ok {
		return e.ch
	}
	ch := make(chan McpEvent, 1)
	r.m[pendingID] = &pendingEntry{sessID: sessID, tool: tool, ch: ch, createdAt: time.Now()}
	return ch
}

// get 查条目（含惰性清理）；不存在返回 nil。
func (r *pendingRegistry) get(pendingID string) *pendingEntry {
	if r == nil || pendingID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(time.Now())
	return r.m[pendingID]
}

// drop 删除条目（回注成功 / 桥超时兜底时调用）。
func (r *pendingRegistry) drop(pendingID string) {
	if r == nil || pendingID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, pendingID)
}

// bySession 反查某云端会话名下的全部未决 pendingID（第二腿兜底配对用）。
// 按登记时间升序返回：兜底按顺序配对时结果确定（map 遍历序随机会把
// A 的工具结果错回注给 B），且与 tool 消息到达序一致。
func (r *pendingRegistry) bySession(sessID string) []string {
	if r == nil || sessID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweepLocked(time.Now())
	type pair struct {
		id string
		at time.Time
	}
	var pairs []pair
	for id, e := range r.m {
		if e.sessID == sessID {
			pairs = append(pairs, pair{id: id, at: e.createdAt})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].at.Before(pairs[j].at) })
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.id
	}
	return out
}

// inject 把回注结果塞进对应 channel（非阻塞）。未知 pending 或已有结果
// 在渠道里时返回 false。
func (r *pendingRegistry) inject(pendingID string, e McpEvent) bool {
	entry := r.get(pendingID)
	if entry == nil {
		return false
	}
	select {
	case entry.ch <- e:
		return true
	default:
		return false // 已有结果占位：视为幂等成功由调用方裁决
	}
}

// serveInjectResult POST /internal/inject-result：接收外部回注的工具执行
// 结果，塞进注册表对应 pending 的 channel。body 与桥的同名端点同构：
// {pending_id, ok, result_text, error}。未知 pending 返回 404。
func (h *Handler) serveInjectResult(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, mcpEventSinkBodyLimit+1))
	if err != nil || len(body) > mcpEventSinkBodyLimit {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var req struct {
		PendingID  string `json:"pending_id"`
		OK         *bool  `json:"ok"`
		ResultText string `json:"result_text"`
		ErrorText  string `json:"error"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.PendingID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	entry := h.pendings.get(req.PendingID)
	if entry == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "unknown pending_id", "pending_id": req.PendingID,
		})
		return
	}
	ok := req.OK == nil || *req.OK
	result := req.ResultText
	if !ok && req.ErrorText != "" {
		result = req.ErrorText
	}
	ev := McpEvent{
		Event:      "tool_result_injected",
		Tool:       entry.tool,
		PendingID:  req.PendingID,
		ResultFull: result,
		OK:         &ok,
		TS:         time.Now().UnixMilli(),
		RecvAt:     time.Now(),
	}
	// 塞不进（已有结果）也按成功应答：结果已在渠道里，语义幂等。
	h.pendings.inject(req.PendingID, ev)
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// 第二腿：role:tool 回灌
// ---------------------------------------------------------------------------

// mcpBridgeBaseURL 本地 MCP 桥地址（回注目标）。env TW2A_MCP_BRIDGE 可
// 覆盖；var 便于测试指向 httptest 假桥。
var mcpBridgeBaseURL = func() string {
	if v := strings.TrimSpace(os.Getenv("TW2A_MCP_BRIDGE")); v != "" {
		return v
	}
	return "http://127.0.0.1:8790"
}()

// mcpBridgeToken 回注 Bearer。优先 env TW2A_MCP_TOKEN（launchd 注入），
// 缺省回退为桥 .env 里配置的 MCP_TOKEN 值（保证 plist 加不了 env 也能用）。
func mcpBridgeToken() string {
	if v := strings.TrimSpace(os.Getenv("TW2A_MCP_TOKEN")); v != "" {
		return v
	}
	return "a318e5ddfd2ed3d7d585d9db112120948868c117c9c8a2a1"
}

// bridgeInjectTimeout 回注请求超时：失败只记日志（云端侧有 120s 兜底），
// 不能拖住客户端的第二轮请求。
const bridgeInjectTimeout = 3 * time.Second

// postBridgeInjectResult 把客户端执行的工具结果回注桥，解除 pending 挂起。
// 载荷与桥 /internal/inject-result 约定一致：{pending_id, ok, result_text}。
func postBridgeInjectResult(pendingID, resultText string) error {
	body, _ := json.Marshal(map[string]any{
		"pending_id":  pendingID,
		"ok":          true,
		"result_text": resultText,
	})
	ctx, cancel := context.WithTimeout(context.Background(), bridgeInjectTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		mcpBridgeBaseURL+"/internal/inject-result", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+mcpBridgeToken())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		// 404 = 桥侧已超时兜底（pending 不在了），属预期内的落空，不算异常路径。
		return fmt.Errorf("bridge inject status %d", resp.StatusCode)
	}
	return nil
}

// injectShortReply 回灌轮的短路回复文本：本轮不触云端，客户端 agent 循环
// 拿到它继续发起下一轮（那时云端 run_mcp 已被回注解锁，正常轮询拿终稿）。
const injectShortReply = "（本地工具结果已回传云端，等待云端继续）"

// tryInjectToolResultTurn 检测「本轮 = 工具结果回传轮」：增量里含 role:tool
// 且能配对到未决 pending 时，逐条回注桥并写短路回复，返回 true（调用方
// 直接结束本轮，不建云端会话、不发消息、不轮询）。
//
// 配对顺序：第一腿输出 tool_calls 时 callID 就是 pending_id，所以客户端回传
// 的 tool_call_id 可直接命中；id 配不上时按 Lookup 命中的云端会话名下的
// pending 顺序兜底（客户端改写过 id 的极端情形）。都配不上则返回 false，
// 走原有流程（普通工具回填轮，与 defer 机制无关）。
func (h *Handler) tryInjectToolResultTurn(w http.ResponseWriter, stream bool, model string, maxMode bool, body []byte) bool {
	if h.pendings == nil {
		return false
	}
	msgs := bodyMessages(body)
	type toolMsg struct{ id, text string }
	var tools []toolMsg
	for _, m := range msgs {
		if role, _ := m["role"].(string); role == "tool" {
			id, _ := m["tool_call_id"].(string)
			tools = append(tools, toolMsg{id: id, text: messageText(m["content"])})
		}
	}
	if len(tools) == 0 {
		return false
	}
	var matchedIDs, matchedTexts []string
	for _, tm := range tools {
		if h.pendings.get(tm.id) != nil {
			matchedIDs = append(matchedIDs, tm.id)
			matchedTexts = append(matchedTexts, tm.text)
		}
	}
	if len(matchedIDs) == 0 && h.convs != nil {
		// 会话兜底：请求命中的云端会话名下还有未决 pending 时按顺序配对。
		if e, _ := h.convs.Lookup(model, maxMode, msgs); e != nil {
			for i, pid := range h.pendings.bySession(e.CloudSessionID) {
				if i >= len(tools) {
					break
				}
				matchedIDs = append(matchedIDs, pid)
				matchedTexts = append(matchedTexts, tools[i].text)
			}
		}
	}
	if len(matchedIDs) == 0 {
		return false
	}
	for i, pid := range matchedIDs {
		if err := postBridgeInjectResult(pid, matchedTexts[i]); err != nil {
			// 失败只记日志：桥侧 120s 兜底保证云端不卡死，本轮照常短路。
			log.Printf("toolinject: 回注 pending=%s 失败: %v", pid, err)
			continue
		}
		h.pendings.drop(pid)
		log.Printf("toolinject: pending=%s 结果已回注桥（tool=%d 字节）", pid, len(matchedTexts[i]))
	}
	if stream {
		startRemoteStream(w)
	}
	h.writeRemoteReply(w, stream, model, injectShortReply)
	return true
}
