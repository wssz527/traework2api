package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// resetSticky 清空会话绑定并切到 credits 模式，测试结束恢复。
func resetSticky(t *testing.T) {
	t.Helper()
	restoreSessions := resetStickySessions()
	restoreMode := setSchedulerMode(schedulerModeCredits)
	setActiveAuthID("")
	t.Cleanup(func() {
		restoreSessions()
		restoreMode()
		setActiveAuthID("")
	})
}

func pickWithSession(t *testing.T, meta map[string]any, ids ...string) pluginapi.SchedulerPickResponse {
	t.Helper()
	cands := make([]pluginapi.SchedulerAuthCandidate, 0, len(ids))
	for _, id := range ids {
		cands = append(cands, pluginapi.SchedulerAuthCandidate{ID: id, Provider: providerName})
	}
	body, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Provider:   providerName,
		Options:    pluginapi.SchedulerOptions{Metadata: meta},
		Candidates: cands,
	})
	raw, err := handleMethodGuarded("scheduler.pick", body)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	var resp pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	return resp
}

func clearCooling(t *testing.T, authID string) {
	t.Helper()
	st := stateFor(authID)
	st.mu.Lock()
	st.coolUntil = time.Time{}
	st.mu.Unlock()
	t.Cleanup(func() {
		st.mu.Lock()
		st.coolUntil = time.Time{}
		st.mu.Unlock()
	})
}

func TestStickySessionKeyPriority(t *testing.T) {
	req := &pluginapi.SchedulerPickRequest{
		Options: pluginapi.SchedulerOptions{
			Metadata: map[string]any{
				"execution_session_id": "exec-1",
				"derived_session_id":   "derived-1",
			},
			Headers: map[string][]string{"X-Session-ID": {"hdr-1"}},
		},
	}
	if got := stickySessionKey(req); got != providerName+"|exec:exec-1" {
		t.Fatalf("execution id 应优先, got %q", got)
	}
	delete(req.Options.Metadata, "execution_session_id")
	if got := stickySessionKey(req); got != providerName+"|derived:derived-1" {
		t.Fatalf("derived id 次之, got %q", got)
	}
	delete(req.Options.Metadata, "derived_session_id")
	if got := stickySessionKey(req); got != providerName+"|hdr:x-session-id:hdr-1" {
		t.Fatalf("header 兜底, got %q", got)
	}
	req.Options.Headers = nil
	if got := stickySessionKey(req); got != "" {
		t.Fatalf("无信号应为空, got %q", got)
	}
}

// 已绑定会话钉住账号；同会话后续请求不再换号。
func TestStickyPickStaysOnBoundAccount(t *testing.T) {
	resetSticky(t)
	sess := map[string]any{"derived_session_id": "s-1"}

	resp := pickWithSession(t, sess, "a", "b")
	if !resp.Handled {
		t.Fatalf("首请求未处理: %+v", resp)
	}
	first := resp.AuthID
	for i := 0; i < 3; i++ {
		if resp := pickWithSession(t, sess, "a", "b"); resp.AuthID != first {
			t.Fatalf("第 %d 次跳号到 %q，应钉住 %q", i, resp.AuthID, first)
		}
	}
	if getActiveAuthID() != first {
		t.Fatalf("面板选中态应跟随绑定 %q, got %q", first, getActiveAuthID())
	}
}

// 新会话分摊到活跃绑定最少的账号：四个会话各占一个账号。
func TestStickyPickSpreadsNewSessions(t *testing.T) {
	resetSticky(t)
	ids := []string{"a", "b", "c", "d"}
	seen := map[string]string{}
	for _, s := range []string{"s-1", "s-2", "s-3", "s-4"} {
		resp := pickWithSession(t, map[string]any{"derived_session_id": s}, ids...)
		if !resp.Handled {
			t.Fatalf("%s 未处理", s)
		}
		seen[s] = resp.AuthID
	}
	// 四个会话必须落在四个不同账号上
	used := map[string]bool{}
	for _, s := range []string{"s-1", "s-2", "s-3", "s-4"} {
		if used[seen[s]] {
			t.Fatalf("账号 %q 被重复绑定: %v", seen[s], seen)
		}
		used[seen[s]] = true
	}
	// 第五个会话：所有账号各 1 个绑定 → 子集为全量，pickActiveAuth 保持当前选中 d
	resp := pickWithSession(t, map[string]any{"derived_session_id": "s-5"}, ids...)
	if resp.AuthID != "d" {
		t.Fatalf("绑定数并列时应保持当前选中 d, got %q", resp.AuthID)
	}
}

// 绑定账号冷却 → 重绑到其他账号；冷却恢复后也不跳回（粘性）。
func TestStickyPickRebindsWhenCooling(t *testing.T) {
	resetSticky(t)
	clearCooling(t, "a")
	sess := map[string]any{"derived_session_id": "s-1"}

	resp := pickWithSession(t, sess, "a", "b")
	if resp.AuthID != "a" {
		t.Fatalf("首请求应选 a, got %q", resp.AuthID)
	}
	markCooling("a", nil, time.Hour, "plan_limit", "测试冷却")
	resp = pickWithSession(t, sess, "a", "b")
	if resp.AuthID != "b" {
		t.Fatalf("a 冷却应重绑到 b, got %q", resp.AuthID)
	}
	// 冷却结束后同会话仍钉在 b
	clearCooling(t, "a")
	st := stateFor("a")
	st.mu.Lock()
	st.coolUntil = time.Time{}
	st.mu.Unlock()
	resp = pickWithSession(t, sess, "a", "b")
	if resp.AuthID != "b" {
		t.Fatalf("冷却恢复后不应跳回 a, got %q", resp.AuthID)
	}
}

// 绑定账号不在候选集（被禁用/重试排除）→ 重绑。
func TestStickyPickRebindsWhenCandidateGone(t *testing.T) {
	resetSticky(t)
	sess := map[string]any{"derived_session_id": "s-1"}

	if resp := pickWithSession(t, sess, "a", "b"); resp.AuthID != "a" {
		t.Fatalf("首请求应选 a, got %q", resp.AuthID)
	}
	if resp := pickWithSession(t, sess, "b"); resp.AuthID != "b" {
		t.Fatalf("a 消失应重绑到 b, got %q", resp.AuthID)
	}
}

// 无会话信号 → 退回面板粘性（pickActiveAuth），且不产生绑定。
func TestStickyPickNoSessionKey(t *testing.T) {
	resetSticky(t)
	setActiveAuthID("b")
	resp := pickWithSession(t, nil, "a", "b")
	if !resp.Handled || resp.AuthID != "b" {
		t.Fatalf("无会话信号应跟随面板选中 b, got %+v", resp)
	}
	stickySessions.Lock()
	n := len(stickySessions.m)
	stickySessions.Unlock()
	if n != 0 {
		t.Fatalf("无会话信号不应产生绑定, 绑定数=%d", n)
	}
}

// 空闲超 TTL → 绑定过期，按新会话重新分摊。
func TestStickyPickExpiresAfterIdleTTL(t *testing.T) {
	resetSticky(t)
	sess := map[string]any{"derived_session_id": "s-1"}

	if resp := pickWithSession(t, sess, "a", "b"); resp.AuthID != "a" {
		t.Fatalf("首请求应选 a, got %q", resp.AuthID)
	}
	stickySessions.Lock()
	stickySessions.m[providerName+"|derived:s-1"].lastSeen = time.Now().Add(-stickyTTL - time.Minute)
	stickySessions.Unlock()
	// a 的绑定已过期 → 按绑定数重新分摊：a 0 个（已过期不计）、b 0 个 → 顺序选 a
	// 再绑一个会话占住 a，过期会话重选时应落到 b
	stickyBind(providerName+"|derived:s-2", "a", time.Now())
	resp := pickWithSession(t, sess, "a", "b")
	if resp.AuthID != "b" {
		t.Fatalf("过期绑定应重新分摊到 b, got %q", resp.AuthID)
	}
}

// 非 trae 请求（mixed 路由把候选集全量下发）必须 defer，不得返回 trae 账号。
func TestSchedulerPickDefersNonTraeRequest(t *testing.T) {
	resetSticky(t)
	cands := []pluginapi.SchedulerAuthCandidate{
		{ID: "trae-a", Provider: providerName},
		{ID: "wb-a", Provider: "workbuddy"},
	}
	// 宿主 mixed 路由：Provider 为空、候选集混合。
	body, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Provider:   "",
		Options:    pluginapi.SchedulerOptions{Metadata: map[string]any{"derived_session_id": "s-x"}},
		Candidates: cands,
	})
	raw, err := handleMethodGuarded("scheduler.pick", body)
	if err != nil {
		t.Fatal(err)
	}
	var resp pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.Handled {
		t.Fatalf("mixed 路由不应处理: %+v", resp)
	}

	// 请求 Provider 指向其他 provider 时同样 defer。
	body2, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Provider:   "workbuddy",
		Candidates: cands,
	})
	raw2, err := handleMethodGuarded("scheduler.pick", body2)
	if err != nil {
		t.Fatal(err)
	}
	var resp2 pluginapi.SchedulerPickResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw2), &resp2)
	if resp2.Handled {
		t.Fatalf("workbuddy 请求不应处理: %+v", resp2)
	}
}
