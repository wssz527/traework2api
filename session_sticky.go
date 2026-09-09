// session_sticky.go 实现 credits 模式下的会话亲和路由。
//
// 动机：Trae 免费账号有并发限制，内建调度是请求级轮换——同一会话的并发
// 请求会瞬间打满单账号并发位导致排队。这里改成会话级绑定：会话首请求选
// 「活跃绑定最少」的账号（并列时保持面板选中/宿主顺序），后续请求钉住该
// 账号，绑定的会话独占一个账号的并发位。Trae 缓存跨账号共享，粘性没有
// 缓存收益，纯属并发隔离。
//
// 绑定失效条件：账号离开候选集（禁用/冷却/重试排除）、会话空闲超
// stickyTTL。session key 取自宿主 Enrich 注入的 Options.Metadata
// （execution_session_id > derived_session_id）或显式会话头。
package main

import (
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	// stickyTTL 滑动空闲过期：每次 pick 刷新 lastSeen，闲置超时才会重绑。
	stickyTTL = 60 * time.Minute
	// stickyMaxEntries 绑定表上限，超出逐出最旧。
	stickyMaxEntries = 4096

	stickyMetadataKeyExecution = "execution_session_id"
	stickyMetadataKeyDerived   = "derived_session_id"
)

// stickyExplicitHeaders 对齐宿主 extractSessionIDs 的显式会话头优先级。
var stickyExplicitHeaders = []string{
	"X-Claude-Code-Session-Id",
	"Session-Id",
	"Session_id",
	"X-Session-ID",
	"X-Session-Affinity",
	"X-Client-Request-Id",
}

type sessionBinding struct {
	authID   string
	lastSeen time.Time
}

var stickySessions = struct {
	sync.Mutex
	m map[string]*sessionBinding
}{m: make(map[string]*sessionBinding)}

// stickySessionKey 从 pick 请求提取稳定会话标识。
// 优先级：execution_session_id > derived_session_id > 显式会话头。
// 无任何会话信号时返回 ""（该请求不参与粘性）。键带 provider 前缀，
// 宿主调度是全局单槽，跨插件不得共用绑定表。
func stickySessionKey(req *pluginapi.SchedulerPickRequest) string {
	if req == nil {
		return ""
	}
	if v, ok := req.Options.Metadata[stickyMetadataKeyExecution].(string); ok {
		if v = strings.TrimSpace(v); v != "" {
			return providerName + "|exec:" + v
		}
	}
	if v, ok := req.Options.Metadata[stickyMetadataKeyDerived].(string); ok {
		if v = strings.TrimSpace(v); v != "" {
			return providerName + "|derived:" + v
		}
	}
	for _, name := range stickyExplicitHeaders {
		if v := stickyHeaderValue(req.Options.Headers, name); v != "" {
			return providerName + "|hdr:" + strings.ToLower(name) + ":" + v
		}
	}
	return ""
}

// stickyHeaderValue 大小写不敏感地读 plain map 形式的 header。
func stickyHeaderValue(headers map[string][]string, name string) string {
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, raw := range values {
			if v := strings.TrimSpace(raw); v != "" {
				return v
			}
		}
	}
	return ""
}

// stickyValidBinding 返回 key 绑定的账号：绑定新鲜且账号仍在候选集、未冷却
// 时有效。过期或不可用的绑定会被删除；返回 "" 表示需要重新挑号。
func stickyValidBinding(key string, candidates []activeAuthCandidate, now time.Time) string {
	stickySessions.Lock()
	b, ok := stickySessions.m[key]
	if !ok {
		stickySessions.Unlock()
		return ""
	}
	if now.Sub(b.lastSeen) > stickyTTL {
		delete(stickySessions.m, key)
		stickySessions.Unlock()
		return ""
	}
	authID := b.authID
	stickySessions.Unlock()

	usable := false
	for _, c := range candidates {
		if c.ID != authID {
			continue
		}
		usable = !c.Disabled && !c.Exhausted
		break
	}
	if !usable {
		stickySessions.Lock()
		if cur, ok := stickySessions.m[key]; ok && cur.authID == authID {
			delete(stickySessions.m, key)
		}
		stickySessions.Unlock()
		return ""
	}

	stickySessions.Lock()
	if cur, ok := stickySessions.m[key]; ok && cur.authID == authID {
		cur.lastSeen = now
	}
	stickySessions.Unlock()
	return authID
}

// stickyBind 记录或刷新 key → authID，顺带清理过期项并在超限时逐出最旧。
func stickyBind(key, authID string, now time.Time) {
	if key == "" || authID == "" {
		return
	}
	stickySessions.Lock()
	defer stickySessions.Unlock()
	if b, ok := stickySessions.m[key]; ok {
		b.authID = authID
		b.lastSeen = now
		return
	}
	if len(stickySessions.m) >= stickyMaxEntries {
		for k, b := range stickySessions.m {
			if now.Sub(b.lastSeen) > stickyTTL {
				delete(stickySessions.m, k)
			}
		}
		for len(stickySessions.m) >= stickyMaxEntries {
			oldestKey, oldestSeen := "", now
			for k, b := range stickySessions.m {
				if oldestKey == "" || b.lastSeen.Before(oldestSeen) {
					oldestKey, oldestSeen = k, b.lastSeen
				}
			}
			delete(stickySessions.m, oldestKey)
		}
	}
	stickySessions.m[key] = &sessionBinding{authID: authID, lastSeen: now}
}

// stickyActiveBindingCounts 统计每个账号当前持有的新鲜（未过期）绑定数，
// 供新会话分摊：优先选绑定最少的账号，避免并发会话挤爆单账号并发位。
func stickyActiveBindingCounts(now time.Time) map[string]int {
	stickySessions.Lock()
	defer stickySessions.Unlock()
	counts := make(map[string]int)
	for _, b := range stickySessions.m {
		if now.Sub(b.lastSeen) > stickyTTL {
			continue
		}
		counts[b.authID]++
	}
	return counts
}

// pickSpreadAuth 在合格候选（未禁用未冷却）中选「活跃绑定最少」的子集，
// 子集内交给 pickActiveAuth（保持面板选中/宿主顺序的稳定性）。
func pickSpreadAuth(cands []activeAuthCandidate, bindingCounts map[string]int) string {
	eligible := make([]activeAuthCandidate, 0, len(cands))
	for _, c := range cands {
		if c.Disabled || c.Exhausted {
			continue
		}
		eligible = append(eligible, c)
	}
	if len(eligible) == 0 {
		return ""
	}
	minBindings := -1
	for _, c := range eligible {
		n := bindingCounts[c.ID]
		if minBindings == -1 || n < minBindings {
			minBindings = n
		}
	}
	subset := make([]activeAuthCandidate, 0, len(eligible))
	for _, c := range eligible {
		if bindingCounts[c.ID] == minBindings {
			subset = append(subset, c)
		}
	}
	return pickActiveAuth(subset)
}

// resetStickySessions 是测试辅助：清空绑定并返回恢复函数。
func resetStickySessions() func() {
	stickySessions.Lock()
	old := stickySessions.m
	stickySessions.m = make(map[string]*sessionBinding)
	stickySessions.Unlock()
	return func() {
		stickySessions.Lock()
		stickySessions.m = old
		stickySessions.Unlock()
	}
}
