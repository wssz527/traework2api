// session_route_test.go — P1 会话隔离的端到端（集成）验证：
// 两个并发 remote 流式连接，各自只应收到自己会话的本地 MCP 工具事件。
package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

// remoteTwoSessionUpstream 支持两个会话（s1/s2）的 remote fake。
// 轮询分别在各自 gate 关闭后返回 completed。
func remoteTwoSessionUpstream(t *testing.T, gates map[string]chan struct{}) *upstream.Client {
	t.Helper()
	snapshotDynamicModelsCache(t) // 同上：隔离全局动态模型缓存
	return &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			p := r.URL.Path
			switch {
			case r.Method == http.MethodPost && p == "/api/remote/v1/chat_sessions":
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"__SID__"}}`), nil
			case r.Method == http.MethodPost && p == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && strings.HasSuffix(p, "/messages"):
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && strings.HasSuffix(p, "/messages"):
				// 路径形如 /api/remote/v1/chat_sessions/{sid}/messages：
				// 会话 ID 按段数定位（见 convReuseFakeUpstream 的 pathSessionID）。
				sid := pathSessionID(p)
				if g, ok := gates[sid]; ok {
					select {
					case <-g:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
					return jsonHTTPResponse(200,
						`{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":"回复-"+"`+sid+`"}]}}`), nil
				}
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[]}}`), nil
			case r.Method == http.MethodDelete && strings.HasPrefix(p, "/api/remote/v1/chat_sessions/"):
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
	}
}

// TestServeRemoteSessionIsolation 两个并发流式请求各自只收自己会话的事件。
//
// 这是 P1 的核心回归：改造前事件总线是全局广播，两个任务会互相串台。
// 由于 fake 的建会话固定返回同一路径，本测试直接驱动 serveRemote 的
// 注册表与过滤逻辑：分别以 s1 / s2 注册任务后发布带会话标识的事件，
// 验证两条流各自只收到自己的那一条。
func TestServeRemoteSessionIsolation(t *testing.T) {
	// 两个会话的 gate：先让两条流都挂上订阅，再放行轮询。
	g1, g2 := make(chan struct{}), make(chan struct{})
	up := remoteTwoSessionUpstream(t, map[string]chan struct{}{"s1": g1, "s2": g2})
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		ConvStorePath: "-",
		// 用 Upstream 字段注入 fake（serveRemote 经由 h.cfg.Upstream 调用）。
	})
	h.cfg.Upstream = up

	// 直接在注册表里登记两个活动任务（等价于两个 serveRemote 同时进行）。
	h.tasks.register("s1", &auth.Auth{UID: "u1"})
	h.tasks.register("s2", &auth.Auth{UID: "u1"})

	// 两条带会话过滤的订阅，模拟两条流式连接。
	id1, ch1 := h.mcpEvents.SubscribeFiltered(func(e McpEvent) bool {
		return h.tasks.belongsTo(e, "s1", e.RecvAt)
	})
	defer h.mcpEvents.Unsubscribe(id1)
	id2, ch2 := h.mcpEvents.SubscribeFiltered(func(e McpEvent) bool {
		return h.tasks.belongsTo(e, "s2", e.RecvAt)
	})
	defer h.mcpEvents.Unsubscribe(id2)

	now := time.Now()
	// 发布带强关联键的事件：应精确路由到各自订阅者。
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_start", Tool: "tool_for_s1",
		ChatSessionID: "s1", TS: 1, RecvAt: now})
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_start", Tool: "tool_for_s2",
		ChatSessionID: "s2", TS: 2, RecvAt: now})

	got1 := drainTools(t, ch1)
	got2 := drainTools(t, ch2)

	if len(got1) != 1 || got1[0] != "tool_for_s1" {
		t.Errorf("s1 的流只应收 tool_for_s1: %v", got1)
	}
	if len(got2) != 1 || got2[0] != "tool_for_s2" {
		t.Errorf("s2 的流只应收 tool_for_s2: %v", got2)
	}
}

// TestServeRemoteSessionIsolationWeakKey 无会话标识时（云端实际情形）：
// 两个任务重叠，事件归最晚开始的那个，先开始的那个不该收到。
func TestServeRemoteSessionIsolationWeakKey(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(), ConvStorePath: "-"})
	h.tasks.register("s1", &auth.Auth{UID: "u1"})
	time.Sleep(5 * time.Millisecond)
	h.tasks.register("s2", &auth.Auth{UID: "u1"})

	chans := map[string]<-chan McpEvent{}
	for _, sid := range []string{"s1", "s2"} {
		sid := sid
		_, ch := h.mcpEvents.SubscribeFiltered(func(e McpEvent) bool {
			return h.tasks.belongsTo(e, sid, e.RecvAt)
		})
		chans[sid] = ch
	}

	// 云端实际发出的事件：不带任何会话标识。
	h.mcpEvents.Publish(McpEvent{Event: "tool_call_start", Tool: "shared_tool", TS: 1, RecvAt: time.Now()})

	got1 := drainTools(t, chans["s1"])
	got2 := drainTools(t, chans["s2"])
	if len(got1) != 0 {
		t.Errorf("弱关联下重叠任务：s1 不应收到事件（应归 s2），got %v", got1)
	}
	if len(got2) != 1 {
		t.Errorf("弱关联下重叠任务：事件应归最晚开始的 s2，got %v", got2)
	}
}

// drainTools 排空订阅 channel 里的工具名（非阻塞，最多等 200ms）。
func drainTools(t *testing.T, ch <-chan McpEvent) []string {
	t.Helper()
	var out []string
	deadline := time.After(200 * time.Millisecond)
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e.Tool)
		case <-deadline:
			return out
		}
	}
}

// TestServeRemoteRegistersTask 完整链路：serveRemote 应在建会话后登记活动任务，
// 任务结束后标记完成（保证并发时的时间窗归属有数据）。
func TestServeRemoteRegistersTask(t *testing.T) {
	gate := make(chan struct{})
	up := remoteStreamTestUpstream(t, "最终回复", gate)
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}), ConvStorePath: "-"})
	h.cfg.Upstream = up

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req = req.WithContext(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); h.ServeHTTP(rec, req) }()

	// 等任务登记进注册表（fake 建会话返回 s1）。
	deadline := time.Now().Add(3 * time.Second)
	registered := false
	for time.Now().Before(deadline) {
		h.tasks.mu.Lock()
		_, ok := h.tasks.tasks["s1"]
		h.tasks.mu.Unlock()
		if ok {
			registered = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(gate)
	wg.Wait()

	if !registered {
		t.Fatal("serveRemote 应把 chat_session_id 登记进活动任务注册表")
	}
	h.tasks.mu.Lock()
	ended := h.tasks.tasks["s1"].EndedAt
	h.tasks.mu.Unlock()
	if ended.IsZero() {
		t.Fatal("serveRemote 结束后应标记任务完成（EndedAt）")
	}
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Errorf("响应应以 [DONE] 收尾: %q", rec.Body.String())
	}
}

// 确保 upstream 包被使用（fake 构造依赖它）。
var _ = upstream.Client{}
