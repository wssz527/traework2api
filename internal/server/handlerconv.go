// handlerconv.go — P1 会话连续性在 serveRemote 里的支撑函数。
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

// DisableConvReuse 关闭会话复用（降级/对比开关；env TW2API_DISABLE_CONV_REUSE=1）。
// 由 NewHandler 读取，运行期不再变更。
func disableConvReuse() bool {
	switch strings.TrimSpace(os.Getenv("TW2API_DISABLE_CONV_REUSE")) {
	case "1", "true", "TRUE", "True", "yes", "on":
		return true
	}
	return false
}

// bindConv 幂等登记/刷新注册项：以 terminalKey（本次请求全量前缀的链终值）
// 为键绑定会话。旧前缀键保留在注册表里（多轮链的中间断点也可被更短前缀
// 命中，如客户端丢掉最后一轮重问）；它们的 CloudSessionID 相同。
func (h *Handler) bindConv(sessID, uid, terminalKey string, consumed int, replyTail string) {
	if h.convs == nil || sessID == "" || terminalKey == "" {
		return
	}
	h.convs.Bind(sessID, uid, terminalKey, consumed, replyTail)
}

// sendRemoteWithBusyRetry 发消息；429 并发槽满（ErrRemoteBusy）短暂等待后重试，
// 至多 3 次（与二期行为一致），其他错误立即返回。
// remoteBusyRetryWaits 递增退避：15s → 30s → 60s（对齐 codex-proxy 的
// proxy-fallback-retry 递增重试计划）。超过则返回 ErrRemoteBusyAfterRetries
// 让外层轮转换号。
var remoteBusyRetryWaits = []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second}

func (h *Handler) sendRemoteWithBusyRetry(r *http.Request, a *auth.Auth, sessID, model, userText string, maxMode bool) error {
	var lastErr error
	// 首次发送不算重试；失败后最多 len(remoteBusyRetryWaits) 次等待重试。
	for attempt := 0; ; attempt++ {
		// 重试耗尽（已等过 len 次）→ 特殊错误让外层换账号
		if attempt >= len(remoteBusyRetryWaits) {
			return fmt.Errorf("%w: %s", upstream.ErrRemoteBusyAfterRetries, lastErr.Error())
		}
		_, err := h.cfg.Upstream.RemoteSendMessage(a, sessID, model, userText, maxMode)
		if err == nil {
			return nil
		}
		if !errors.Is(err, upstream.ErrRemoteBusy) {
			return err
		}
		lastErr = err
		wait := remoteBusyRetryWaits[attempt]
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		case <-time.After(wait):
		}
	}
}

// ---------------------------------------------------------------------------
// legacy 路径（convs==nil 时）：二期行为，每次新建会话 + 用完即删
// ---------------------------------------------------------------------------

// serveRemoteLegacy P1 之前的完整往返。会话复用禁用（convs==nil）时由
// serveRemote 调用，行为与二期完全一致。
// serveRemoteLegacy 是 OpenAI 通道的 legacy 渲染入口。
func (h *Handler) serveRemoteLegacy(w http.ResponseWriter, r *http.Request, a *auth.Auth, model string, body []byte, stream bool, maxMode bool) error {
	return h.serveRemoteLegacyWith(w, r, a, model, body, stream, maxMode, openaiRenderer{})
}

// serveRemoteLegacyWith P1 之前的完整往返。会话复用禁用（convs==nil）时由
// serveRemoteWith 调用，行为与二期完全一致，仅协议帧渲染交给 render。
func (h *Handler) serveRemoteLegacyWith(w http.ResponseWriter, r *http.Request, a *auth.Auth, model string, body []byte, stream bool, maxMode bool, render remoteRenderer) error {
	sessID, err := h.cfg.Upstream.RemoteCreateSession(a)
	if err != nil {
		return err
	}
	if h.tasks != nil {
		h.tasks.register(sessID, a)
		defer func() {
			h.tasks.finish(sessID)
			h.tasks.reap(5 * time.Minute)
		}()
	}
	defer func() { _ = h.cfg.Upstream.RemoteDeleteSession(a, sessID) }()

	userText := remotePrompt(body)
	if err := h.sendRemoteWithBusyRetry(r, a, sessID, model, userText, maxMode); err != nil {
		return err
	}
	return h.serveRemotePumpWith(w, r, a, sessID, model, stream, nil, render, false, -1)
}

// ---------------------------------------------------------------------------
// messages → 增量文本
// ---------------------------------------------------------------------------

// bodyMessages 提取请求体里的 messages 数组（map 形式，保序）。
func bodyMessages(body []byte) []map[string]any {
	msgs, _ := bodyUnmarshal(body)["messages"].([]any)
	out := make([]map[string]any, 0, len(msgs))
	for _, raw := range msgs {
		if m, ok := raw.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// formatIncrement 把增量 messages 渲染成发进云端会话的单段文本。
// 格式与 remotePrompt 一致（role:\n内容），保证云端看到的对话形态统一；
// 跳过无文本内容的消息（如纯 tool_calls 载荷）。
// 过滤 <system-reminder> 包裹的注入内容（CLI 内部机制，非用户对话，
// 发进云端会污染上下文——实测 CLI -r resume 会把 system-reminder
// 当 user 消息带进来）。
func formatIncrement(msgs []map[string]any) string {
	parts := make([]string, 0, len(msgs))
	for _, m := range msgs {
		role, _ := m["role"].(string)
		text := messageText(m["content"])
		if text == "" {
			continue
		}
		if strings.Contains(text, "<system-reminder>") {
			continue
		}
		parts = append(parts, role+":\n"+text)
	}
	return strings.Join(parts, "\n\n")
}

// replyTail 取 assistant 回复的尾部片段（回显校验锚点）。
func replyTail(reply string) string {
	const n = 120
	r := []rune(strings.TrimSpace(reply))
	if len(r) == 0 {
		return ""
	}
	if len(r) > n {
		r = r[len(r)-n:]
	}
	return string(r)
}

// trimCloudEcho 校验请求增量与云端已消费内容的一致性，剥离与尾迹匹配的
// assistant 回显，返回真实待发送消息。
//
// 背景：客户端每次发全量 messages；增量 msgs 里若含 assistant 消息，其文本
// 必须是上一轮云端真实产出的回复（注册表尾迹）。对不上说明客户端改写了
// 历史（edit/分支切换/regenerate）——此时必须全量重建而非继续 append，
// 否则云端上下文与客户端认知脱节。匹配的 assistant 回显则直接剥离：云端
// 会话里已经有这段回复（是它自己生成的），重发一遍只会污染上下文。
//
// 规则：增量中每条非空 assistant 文本都要与 lastReplyTail 匹配（互相包含，
// 容忍客户端对回复的空白/截断处理），匹配项不进入返回值。注册表无尾迹
//（旧条目/重启恢复）时无法校验 → 保守拒绝复用，走全量重建。
func trimCloudEcho(msgs []map[string]any, lastReplyTail string) ([]map[string]any, bool) {
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		role, _ := m["role"].(string)
		if role != "assistant" {
			out = append(out, m)
			continue
		}
		text := strings.TrimSpace(messageText(m["content"]))
		if text == "" {
			out = append(out, m)
			continue
		}
		tail := strings.TrimSpace(lastReplyTail)
		if tail == "" {
			// 注册表无尾迹（上一轮任务未完成/回复为空）：无法校验回显。
			// 但增量是从 consumed 之后开始的，其中的 assistant 消息在云端
			// 必然已存在（它是上一轮生成的），安全剥离，不拒绝复用。
			// 否则同一会话下一轮永远新建沙盒、丢失历史。
			continue
		}
		if !strings.Contains(tail, text) && !strings.Contains(text, tail) {
			return nil, false
		}
		// 匹配尾迹的 assistant 回显：云端已有，剥离。
	}
	return out, true
}

// ---------------------------------------------------------------------------
// TTL 清扫
// ---------------------------------------------------------------------------

// convTTL 会话空闲 TTL（默认 24h；env TW2API_CONV_TTL 可覆盖，如 "6h"）。
func convTTLDur() time.Duration {
	if v := strings.TrimSpace(os.Getenv("TW2API_CONV_TTL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 24 * time.Hour
}

// StartConvSweeper 启动会话 TTL 清扫 goroutine：先立即扫一次（回收上次进程
// 遗留的空闲会话），然后每 interval 扫一次（TTL/8，夹在 [1m, 1h]），
// 直到 ctx 取消。
func (h *Handler) StartConvSweeper(ctx context.Context) {
	if h.convs == nil {
		return
	}
	ttl := convTTLDur()
	interval := ttl / 8
	// TTL 很短时（如单测 50ms）interval=ttl/8 会远小于 1m，不能 clamp 到
	// 1m——否则短 TTL 的极小间隔测试永远等不到周期清扫。下限放宽到 20ms，
	// 只对「极小 TTL 的测试/调试场景」生效；生产 24h TTL 下 interval=3h，
	// 完全不受影响（仍落在上限 1h 内）。
	if interval < 20*time.Millisecond {
		interval = 20 * time.Millisecond
	}
	if interval > time.Hour {
		interval = time.Hour
	}
	go func() {
		sweepOnce := func() {
			h.convs.Sweep(ttl, func(sessionID, uid string) {
				a := h.cfg.Pool.AuthByUID(uid)
				if a == nil {
					// 账号已不在池里：注册项已摘除即可，云端会话留给云端侧回收。
					return
				}
				if err := h.cfg.Upstream.RemoteDeleteSession(a, sessionID); err != nil {
					// 删失败只记日志：注册项已摘除，云端会话最多多活一个 TTL 周期。
					fmt.Printf("conv sweeper: delete session %s: %v\n", sessionID, err)
				}
			})
		}
		sweepOnce() // 启动即扫一次
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sweepOnce()
			}
		}
	}()
}
