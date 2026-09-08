// lifecycle.go 账号冷却/禁用状态机 + 执行结果回写。
//
// 迁移报告 §2.1：CPA 内建已覆盖大部分冷却（request-retry + .cds 持久化）。
// 本插件只保留 tw2api 里 CPA 无法表达的两种：
//   - CoolPlan（1005 权益不足 → 12h 长冷却）：写 disabled=true + note
//   - ErrSessionDead（401 session 死）：写 disabled=true（需人工重登）
//
// 其余（429/404/连续错误）交给 CPA 内建 request-retry + cooldown。
package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	// planCooldown 1005 plan 权益不足的冷却时长（与原 traework2api 一致）。
	planCooldown = 12 * time.Hour
	// errThreshold / errCooldown 连续错误冷却（CPA 内建 request-retry 覆盖
	// 大部分场景，这里只做兜底计数）。
	errThreshold = 3
	errCooldown  = 10 * time.Minute
)

// lifecycleState 每账号的可变运行态（纯内存；宿主重启后由 .cds 承担持久化）。
type lifecycleStateEntry struct {
	mu        sync.Mutex
	errCount  int
	coolUntil time.Time
	coolFor   string // plan_limit | session_dead | error_threshold
	disabled  bool
	note      string
}

var (
	lifecycleState  sync.Map // authID -> *lifecycleStateEntry
	lifecycleAuto   = true
	lifecycleAutoMu sync.RWMutex
)

func lifecycleEnabled() bool {
	lifecycleAutoMu.RLock()
	defer lifecycleAutoMu.RUnlock()
	return lifecycleAuto
}

func setLifecycleAuto(v bool) {
	lifecycleAutoMu.Lock()
	lifecycleAuto = v
	lifecycleAutoMu.Unlock()
}

func stateFor(authID string) *lifecycleStateEntry {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return nil
	}
	v, _ := lifecycleState.LoadOrStore(authID, &lifecycleStateEntry{})
	return v.(*lifecycleStateEntry)
}

// isCooling 报告账号当前是否处于插件级冷却中。
func isCooling(authID string) bool {
	st := stateFor(authID)
	if st == nil {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return !st.coolUntil.IsZero() && time.Now().Before(st.coolUntil)
}

// reconcileAfterUpstreamError 按 HTTP 状态码分类并冷却/禁用账号。
func reconcileAfterUpstreamError(authID string, sa *traeAuth, status int, body string) {
	if !lifecycleEnabled() {
		return
	}
	kind := Classify(status, body)
	switch kind {
	case ErrPlanLimit:
		markCooling(authID, sa, planCooldown, "plan_limit", "plan 权益不足")
	case ErrSessionDead:
		markDisabled(authID, sa, "session dead")
	case ErrSoftRate, ErrNotFound, ErrServer, ErrClient:
		// 交给 CPA 内建 request-retry / cooldown（迁移报告 §2.1）。
		noteAccountError(authID, sa, status, body)
	default:
	}
}

// reconcileAfterStreamError 处理 SSE 流内业务错误。
func reconcileAfterStreamError(authID string, sa *traeAuth, se *SOLOStreamError) {
	if !lifecycleEnabled() || se == nil {
		return
	}
	switch se.Kind() {
	case ErrPlanLimit:
		markCooling(authID, sa, planCooldown, "plan_limit", "plan 权益不足")
	default:
		noteAccountError(authID, sa, 0, se.Error())
	}
}

// noteAccountError 记录一次错误；达到阈值后冷却。
func noteAccountError(authID string, sa *traeAuth, status int, msg string) {
	if !lifecycleEnabled() {
		return
	}
	st := stateFor(authID)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.errCount++
	count := st.errCount
	if st.errCount >= errThreshold {
		st.errCount = 0
		st.coolUntil = time.Now().Add(errCooldown)
		st.coolFor = "error_threshold"
		st.note = truncateRedacted(msg, 120)
	}
	st.mu.Unlock()
	if count >= errThreshold {
		pluginLogf("auth %s cooled (%s) after %d errors: %s", authID, "error_threshold", count, truncateRedacted(msg, 120))
		_ = persistAuthDisabled(authID, sa, false)
	}
}

// noteAccountSuccess 成功请求重置错误计数。
func noteAccountSuccess(authID string, sa *traeAuth) {
	st := stateFor(authID)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.errCount = 0
	st.mu.Unlock()
}

// markCooling 冷却账号（写宿主 disabled 标记，CPA 内建会跳过 disabled 账号）。
func markCooling(authID string, sa *traeAuth, d time.Duration, reason, note string) {
	st := stateFor(authID)
	if st != nil {
		st.mu.Lock()
		st.coolUntil = time.Now().Add(d)
		st.coolFor = reason
		st.note = note
		st.errCount = 0
		st.mu.Unlock()
	}
	// CPA 内建冷却以 auth.disabled 为准，写盘才能跨重启生效。
	_ = persistAuthDisabled(authID, sa, true)
	pluginLogf("auth %s cooling %v (%s): %s", authID, d, reason, note)
}

// markDisabled 永久禁用（session 失效），需人工重登。
func markDisabled(authID string, sa *traeAuth, note string) {
	st := stateFor(authID)
	if st != nil {
		st.mu.Lock()
		st.disabled = true
		st.note = note
		st.mu.Unlock()
	}
	_ = persistAuthDisabled(authID, sa, true)
	pluginLogf("auth %s disabled: %s", authID, note)
}

// persistAuthDisabled 把 disabled 标记写入宿主凭证文件。
// authID 可能是 auth_index / 文件 id / UID；先解析出可写的凭证。
func persistAuthDisabled(authID string, sa *traeAuth, disabled bool) error {
	if sa == nil {
		var err error
		sa, err = resolveAuthByID(authID)
		if err != nil || sa == nil {
			return fmt.Errorf("resolve auth for %s: %v", authID, err)
		}
	}
	return persistAuth(sa, disabled)
}

// resolveAuthByID 按 authID（auth_index / 文件 id / UID）取凭证。
func resolveAuthByID(authID string) (*traeAuth, error) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return nil, fmt.Errorf("empty auth id")
	}
	files, err := hostAuthList()
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.AuthIndex == authID || f.ID == authID {
			return hostAuthGet(f.AuthIndex)
		}
	}
	// 回退：按 UID 匹配
	for _, f := range files {
		sa, err := hostAuthGet(f.AuthIndex)
		if err != nil {
			continue
		}
		if sa.UID != "" && sa.UID == authID {
			return sa, nil
		}
	}
	return nil, fmt.Errorf("auth not found: %s", authID)
}

// pruneLifecycleState 清理已删除账号的状态，防止内存单调增长。
func pruneLifecycleState() {
	files, err := hostAuthList()
	if err != nil {
		return
	}
	live := make(map[string]struct{}, len(files))
	for _, f := range files {
		live[f.ID] = struct{}{}
		live[f.AuthIndex] = struct{}{}
	}
	lifecycleState.Range(func(key, _ any) bool {
		id, _ := key.(string)
		if _, ok := live[id]; !ok {
			lifecycleState.Delete(key)
		}
		return true
	})
}

// isSessionDead 判断是否为 session 失效。
func isSessionDead(status int, body string) bool {
	return status == http.StatusUnauthorized || Classify(status, body) == ErrSessionDead
}
