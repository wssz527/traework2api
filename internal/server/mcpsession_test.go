// mcpsession_test.go — 本地 MCP 事件会话归属（P1）的单测。
package server

import (
	"testing"
	"time"

	"traework2api/internal/auth"
)

func TestEventSessionIDFromEvent(t *testing.T) {
	if got := eventSessionID(McpEvent{ChatSessionID: "s1"}); got != "s1" {
		t.Fatalf("强关联键应优先: %q", got)
	}
	if got := eventSessionID(McpEvent{SessionKeys: map[string]string{"x-task-id": "t9"}}); got != "t9" {
		t.Fatalf("应从 session_keys 回退: %q", got)
	}
	if got := eventSessionID(McpEvent{}); got != "" {
		t.Fatalf("无标识应返回空: %q", got)
	}
}

// TestBelongsToStrongKey 事件自带会话标识时精确匹配，不受注册表影响。
func TestBelongsToStrongKey(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	r.register("s2", &auth.Auth{UID: "u1"})

	e := McpEvent{ChatSessionID: "s2", RecvAt: time.Now()}
	if !r.belongsTo(e, "s2", e.RecvAt) {
		t.Error("带 s2 标识的事件应归属 s2")
	}
	if r.belongsTo(e, "s1", e.RecvAt) {
		t.Error("带 s2 标识的事件不应归属 s1")
	}
}

// TestBelongsToSingleTask 无标识 + 唯一活动任务 → 天然归属（常见路径）。
func TestBelongsToSingleTask(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	e := McpEvent{Tool: "read_file", RecvAt: time.Now()}
	if !r.belongsTo(e, "s1", e.RecvAt) {
		t.Fatal("唯一活动任务时事件应归属它")
	}
}

// TestBelongsToOverlapPicksLatest 多任务重叠且无标识 → 归属最晚开始的任务。
// 这是启发式，测试锁定当前语义，便于将来识别行为变化。
func TestBelongsToOverlapPicksLatest(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	time.Sleep(5 * time.Millisecond)
	r.register("s2", &auth.Auth{UID: "u1"})

	now := time.Now()
	e := McpEvent{Tool: "read_file", RecvAt: now}
	if r.belongsTo(e, "s1", now) {
		t.Error("重叠时 s1 不应收到事件（应归 s2）")
	}
	if !r.belongsTo(e, "s2", now) {
		t.Error("重叠时事件应归最晚开始的 s2")
	}
}

// TestBelongsToXFFPodMatch P1 XFF pod 归属：多任务重叠但沙盒 pod 不同时，
// 事件按 XFF 无歧义归到对应会话，优先于「最晚开始」启发式。
func TestBelongsToXFFPodMatch(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	time.Sleep(5 * time.Millisecond)
	r.register("s2", &auth.Auth{UID: "u1"})
	// 学习：s2 的事件来自 pod 1.1.1.1，s1 来自 2.2.2.2。
	r.LearnXFF("s2", "1.1.1.1")
	r.LearnXFF("s1", "2.2.2.2")

	now := time.Now()
	evPod1 := McpEvent{Tool: "read_file", XForwardedFor: "1.1.1.1", RecvAt: now}
	if r.belongsTo(evPod1, "s2", now) != true {
		t.Error("XFF 命中 pod 的事件应归属 s2")
	}
	if r.belongsTo(evPod1, "s1", now) != false {
		t.Error("XFF 命中 pod 的事件不应归属 s1（pod 不同）")
	}

	// 首段提取：多级 XFF 取第一段。
	if got := mcpEventXFF(McpEvent{XForwardedFor: "1.1.1.1, 10.0.0.1"}); got != "1.1.1.1" {
		t.Errorf("mcpEventXFF 多段应取首段: %q", got)
	}
	if got := mcpEventXFF(McpEvent{}); got != "" {
		t.Errorf("空 XFF 应返回空串: %q", got)
	}

	// 未学习过 IP 的会话：XFF 判据不生效，落回时间窗启发式。
	evUnknown := McpEvent{Tool: "read_file", XForwardedFor: "3.3.3.3", RecvAt: now}
	if r.belongsTo(evUnknown, "s1", now) {
		t.Error("未学习过 3.3.3.3 时 s1 不应凭 XFF 收到事件")
	}

	// LearnXFF 幂等（首条锁定）：后续不同 IP 不覆盖。
	r.LearnXFF("s2", "9.9.9.9")
	if r.tasks["s2"].SandboxIP != "1.1.1.1" {
		t.Errorf("LearnXFF 不应覆盖已学习的 IP: %q", r.tasks["s2"].SandboxIP)
	}
}

// TestBelongsToDifferentAccount 不同账号的并发任务互不干扰。
func TestBelongsToDifferentAccount(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	r.register("s2", &auth.Auth{UID: "u2"})

	now := time.Now()
	e := McpEvent{Tool: "read_file", RecvAt: now}
	// s1 与 s2 分属不同账号，互不视为重叠 → 各自都能收到（弱关联的已知放宽）。
	if !r.belongsTo(e, "s1", now) {
		t.Error("不同账号时 s1 应保留归属")
	}
	if !r.belongsTo(e, "s2", now) {
		t.Error("不同账号时 s2 应保留归属")
	}
}

// TestBelongsToUnregistered 未登记的会话不收事件。
func TestBelongsToUnregistered(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	e := McpEvent{Tool: "read_file", RecvAt: time.Now()}
	if r.belongsTo(e, "unknown", e.RecvAt) {
		t.Fatal("未登记会话不应收到事件")
	}
}

// TestTaskRegistryFinishAndReap 结束后仍可被迟到事件归属，超时后被清理。
func TestTaskRegistryFinishAndReap(t *testing.T) {
	r := newTaskRegistry()
	r.register("s1", &auth.Auth{UID: "u1"})
	r.finish("s1")
	// 刚结束：仍在表里，迟到事件可归属。
	if r.tasks["s1"].EndedAt.IsZero() {
		t.Fatal("finish 应写入 EndedAt")
	}
	r.reap(-1 * time.Second) // 负 keep = 立刻清掉所有已结束的
	if _, ok := r.tasks["s1"]; ok {
		t.Fatal("reap 应清理已结束任务")
	}
}

// TestSubscribeFiltered 订阅过滤器只放行匹配会话的事件。
func TestSubscribeFiltered(t *testing.T) {
	b := newMcpEventBus()
	id, ch := b.SubscribeFiltered(func(e McpEvent) bool {
		return e.ChatSessionID == "s1"
	})
	defer b.Unsubscribe(id)

	b.Publish(McpEvent{Event: "tool_call_start", Tool: "a", ChatSessionID: "s1"})
	b.Publish(McpEvent{Event: "tool_call_start", Tool: "b", ChatSessionID: "s2"})

	var got []string
	for i := 0; i < 2; i++ {
		select {
		case e, ok := <-ch:
			if !ok {
				i = 2
				continue
			}
			got = append(got, e.Tool)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("过滤器应只放行 s1 的事件: %v", got)
	}
}
