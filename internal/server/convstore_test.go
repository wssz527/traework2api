// convstore_test.go — P1 会话连续性单测：
// 前缀链匹配、增量 append、复用失败重建、per-conversation 串行化、
// 粘性路由、持久化与 TTL 清扫。
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

// pathSessionID 从 /api/remote/v1/chat_sessions/{sid}[/messages|/events] 路径
// 里提取会话 ID。按「chat_sessions 段的下一位置」定位，避免 Split 前导空串
// 造成的下标错位（早期实现 [4] 取到的是 "chat_sessions" 本身）。
func pathSessionID(p string) string {
	const anchor = "/chat_sessions/"
	i := strings.Index(p, anchor)
	if i < 0 {
		return ""
	}
	rest := p[i+len(anchor):]
	if j := strings.Index(rest, "/"); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func msgsOf(pairs ...string) []map[string]any {
	out := make([]map[string]any, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, map[string]any{"role": pairs[i], "content": pairs[i+1]})
	}
	return out
}

// bodyWith 把 messages 组装成 OpenAI 请求体。
func bodyWith(msgs []map[string]any) []byte {
	raw, _ := json.Marshal(map[string]any{"model": "glm-5.3", "messages": msgs})
	return raw
}

func TestConvPrefixChainDeterministic(t *testing.T) {
	m1 := msgsOf("user", "hi", "assistant", "ok", "user", "more")
	m2 := msgsOf("user", "hi", "assistant", "ok", "user", "other")
	c1 := convPrefixChain("glm-5.3", false, m1)
	c2 := convPrefixChain("glm-5.3", false, m2)
	if len(c1) != 3 || len(c2) != 3 {
		t.Fatalf("chain len=%d/%d", len(c1), len(c2))
	}
	// 前两条相同 → 前缀哈希一致；第三条不同 → 分叉。
	if c1[0] != c2[0] || c1[1] != c2[1] {
		t.Error("相同前缀的链哈希应一致")
	}
	if c1[2] == c2[2] {
		t.Error("分叉后的链哈希应不同")
	}
	// 模型/maxMode 参与隔离。
	if convPrefixChain("glm-5.2", false, m1)[0] == c1[0] {
		t.Error("不同模型的链应隔离")
	}
	if convPrefixChain("glm-5.3", true, m1)[0] == c1[0] {
		t.Error("maxMode 不同的链应隔离")
	}
}

func TestConvStoreLookupLongestPrefix(t *testing.T) {
	s := newConvStore("")
	chain := convPrefixChain("glm-5.3", false, msgsOf("user", "hi", "assistant", "ok"))
	s.Bind("cloud-1", "u1", chain[0], 1, "ok")
	s.Bind("cloud-1", "u1", chain[1], 2, "ok")

	// 请求 = 前缀 + 新 user 消息 → 命中最长前缀 chain[1]，增量从下标 2 开始。
	req := msgsOf("user", "hi", "assistant", "ok", "user", "next")
	e, incFrom := s.Lookup("glm-5.3", false, req)
	if e == nil || e.CloudSessionID != "cloud-1" {
		t.Fatalf("lookup miss: %+v", e)
	}
	if incFrom != 2 {
		t.Errorf("incFrom=%d want 2（只 append 增量）", incFrom)
	}
	// 未知前缀 → 未命中。
	e2, inc2 := s.Lookup("glm-5.3", false, msgsOf("user", "different"))
	if e2 != nil || inc2 != 0 {
		t.Errorf("unexpected hit: %+v inc=%d", e2, inc2)
	}
}

func TestConvStoreUnbindAndSweep(t *testing.T) {
	s := newConvStore("")
	chain := convPrefixChain("glm-5.3", false, msgsOf("user", "hi"))
	s.Bind("cloud-1", "u1", chain[0], 1, "rep")
	s.Unbind("cloud-1")
	if e, _ := s.Lookup("glm-5.3", false, msgsOf("user", "hi")); e != nil {
		t.Error("Unbind 后不应再命中")
	}

	// TTL 清扫：把 LastActive 拨到过去。
	s.Bind("cloud-2", "u1", chain[0], 1, "rep")
	s.mu.Lock()
	s.m[chain[0]].LastActive = time.Now().Add(-2 * time.Hour)
	s.m[chain[0]].LastActiveUnix = s.m[chain[0]].LastActive.Unix()
	s.mu.Unlock()
	var deleted []string
	s.Sweep(time.Hour, func(sessionID, uid string) { deleted = append(deleted, sessionID) })
	if len(deleted) != 1 || deleted[0] != "cloud-2" {
		t.Errorf("sweep deleted=%v", deleted)
	}
	if e, _ := s.Lookup("glm-5.3", false, msgsOf("user", "hi")); e != nil {
		t.Error("TTL 过期项应被清扫")
	}
}

func TestConvStorePersistence(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "conversations.json")
	s := newConvStore(fp)
	chain := convPrefixChain("glm-5.3", false, msgsOf("user", "hi"))
	s.Bind("cloud-9", "u7", chain[0], 1, "回复尾部")
	// Bind 内部已落盘；新实例加载恢复。
	s2 := newConvStore(fp)
	e, incFrom := s2.Lookup("glm-5.3", false, msgsOf("user", "hi", "assistant", "next"))
	if e == nil || e.CloudSessionID != "cloud-9" || e.UID != "u7" || incFrom != 1 {
		t.Fatalf("restore failed: %+v inc=%d", e, incFrom)
	}
	if e.LastReplyTail != "回复尾部" {
		t.Errorf("tail=%q", e.LastReplyTail)
	}
}

func TestConvStoreLockForSerializes(t *testing.T) {
	s := newConvStore("")
	l1 := s.LockFor("cloud-1")
	l2 := s.LockFor("cloud-1")
	l3 := s.LockFor("cloud-2")
	if l1 != l2 {
		t.Error("同一会话应返回同一把锁")
	}
	if l1 == l3 {
		t.Error("不同会话不应共用锁")
	}
	// 串行化验证：持锁时另一 goroutine 不得进入。
	l1.Lock()
	entered := make(chan struct{})
	go func() {
		l2.Lock()
		close(entered)
	}()
	select {
	case <-entered:
		t.Fatal("per-conversation 锁未串行化")
	case <-time.After(50 * time.Millisecond):
	}
	l1.Unlock()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("解锁后另一 goroutine 应进入")
	}
}

// convReuseFakeUpstream 支持会话复用的 remote fake：
// 记录建会话次数、每个会话的 POST 次数与最后一次发送文本。
func convReuseFakeUpstream(t *testing.T, mu *sync.Mutex, creates *int, sends map[string]int, lastText map[string]string, pollFail bool) *upstream.Client {
	t.Helper()
	snapshotDynamicModelsCache(t) // 同上：隔离全局动态模型缓存
	createsN := 0
	return &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				createsN++
				*creates = createsN
				return jsonHTTPResponse(200, fmt.Sprintf(`{"code":0,"data":{"chat_session_id":"cs%d"}}`, createsN)), nil
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages"):
				// 路径形如 /api/remote/v1/chat_sessions/{sid}/messages：
				// Split 后 parts=[api remote v1 chat_sessions sid messages]，
				// 会话 ID 在下标 5（含前导空串则是 6）——按段数定位防错位。
				sid := pathSessionID(r.URL.Path)
				raw, _ := io.ReadAll(r.Body)
				var body map[string]any
				_ = json.Unmarshal(raw, &body)
				q, _ := body["query"].(string)
				var query []struct {
					Data struct {
						Content string `json:"content"`
					} `json:"data"`
				}
				_ = json.Unmarshal([]byte(q), &query)
				text := ""
				if len(query) > 0 {
					text = query[0].Data.Content
				}
				sends[sid]++
				lastText[sid] = text
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/messages"):
				if pollFail {
					return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"user","status":"in_progress"}]}}`), nil
				}
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":"云端回复A"}]}}`), nil
			case r.Method == http.MethodDelete:
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
	}
}

// TestServeRemoteConvReuse 两轮对话：turn1 新建会话；turn2 带 [turn1 问答 +
// 新问题] 再调 → 同一 chat_session_id 复用、只 append 增量消息。
func TestServeRemoteConvReuse(t *testing.T) {
	restore := remoteTestHook()
	defer restore()
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})
	// turn1：新建
	turn1 := msgsOf("user", "问题一")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 {
		t.Fatalf("turn1 code=%d body=%s", rec.Code, rec.Body)
	}
	if creates != 1 {
		t.Fatalf("turn1 creates=%d want 1", creates)
	}
	if got := lastText["cs1"]; got != "user:\n问题一" {
		t.Errorf("turn1 sent=%q", got)
	}

	// turn2：带 turn1 问答 + 新问题 → 复用 cs1，只发增量。
	turn2 := append(append([]map[string]any{}, turn1...),
		map[string]any{"role": "assistant", "content": "云端回复A"},
		map[string]any{"role": "user", "content": "问题二"})
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn2)))))
	if rec2.Code != 200 {
		t.Fatalf("turn2 code=%d body=%s", rec2.Code, rec2.Body)
	}
	if creates != 1 {
		t.Errorf("turn2 应复用会话：creates=%d want 1", creates)
	}
	if sends["cs1"] != 2 {
		t.Fatalf("sends[cs1]=%d want 2", sends["cs1"])
	}
	if got := lastText["cs1"]; got != "user:\n问题二" {
		t.Errorf("turn2 应只 append 增量，got=%q", got)
	}
}

// TestServeRemoteConvReuseFallsBackOnDeadSession 复用发消息失败 → 解绑并
// 降级新建会话发全量历史（不劣于现状）。
func TestServeRemoteConvReuseFallsBackOnDeadSession(t *testing.T) {
	restore := remoteTestHook()
	defer restore()
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	// 注入故障：往 cs1 发第二条消息返回 500。
	failOne := make(chan struct{})
	base := up.HTTP.Transport
	up.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages") && strings.Contains(r.URL.Path, "cs1") {
			select {
			case <-failOne:
				// 只失败一次：第一次发消息成功，让 turn1 正常绑定。
				failOne = nil
			default:
			}
			if failOne == nil {
				// 已消费过一次开关：不在此分支处理，透传
			}
		}
		return base.RoundTrip(r)
	})
	_ = failOne
	// 简化：直接构造「绑定存在但云端会话已死」的场景——手工往注册表
	// 绑一个 fake 会拒发的会话 ID。
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})
	up.HTTP.Transport = base // 还原，避免上面的注入干扰

	// turn1 正常新建绑定。
	turn1 := msgsOf("user", "问题一")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 || creates != 1 {
		t.Fatalf("turn1 code=%d creates=%d", rec.Code, creates)
	}
	// 把绑定指向一个不存在的会话 ID（模拟云端会话被删/已死）。
	chain := convPrefixChain("glm-5.3", false, turn1)
	h.convs.Bind("cs-dead", "u1", chain[len(chain)-1], 1, "云端回复A")

	// turn2：增量发往 cs-dead → 500 → 降级新建 cs2 全量。
	up.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "cs-dead") && strings.HasSuffix(r.URL.Path, "/messages") {
			return jsonHTTPResponse(500, `{"code":500,"message":"session gone"}`), nil
		}
		return base.RoundTrip(r)
	})
	turn2 := append(append([]map[string]any{}, turn1...),
		map[string]any{"role": "assistant", "content": "云端回复A"},
		map[string]any{"role": "user", "content": "问题二"})
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn2)))))
	if rec2.Code != 200 {
		t.Fatalf("turn2 code=%d body=%s", rec2.Code, rec2.Body)
	}
	mu.Lock()
	defer mu.Unlock()
	if creates != 2 {
		t.Errorf("死会话应降级新建：creates=%d want 2", creates)
	}
	if got := lastText["cs2"]; got != "user:\n问题一\n\nassistant:\n云端回复A\n\nuser:\n问题二" {
		t.Errorf("降级应发全量历史，got=%q", got)
	}
	// 死会话绑定应被摘除。
	if e, _ := h.convs.Lookup("glm-5.3", false, turn1); e != nil && e.CloudSessionID == "cs-dead" {
		t.Error("cs-dead 绑定应被 Unbind")
	}
}

// TestServeRemoteConvStickyReuse 回绑定创建账号（粘性）：即使池里另一个
// 账号积分更高，复用请求也应发往绑定账号。
func TestServeRemoteConvStickyReuse(t *testing.T) {
	restore := remoteTestHook()
	defer restore()
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	p := testPoolWith(
		&auth.Auth{UID: "u-low", AccessToken: "at-low", ExpiresAt: 9999999999},
		&auth.Auth{UID: "u-high", AccessToken: "at-high", ExpiresAt: 9999999999},
	)
	p.SetCredits("u-low", 100)
	p.SetCredits("u-high", 99000)
	h := NewHandler(Config{Pool: p, Upstream: up, ConvStorePath: isolatedConvStore()})

	turn1 := msgsOf("user", "问题一")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 {
		t.Fatalf("turn1 code=%d", rec.Code)
	}
	// turn1 由积分最高的 u-high 创建。
	if creates != 1 {
		t.Fatalf("creates=%d", creates)
	}
	turn2 := append(append([]map[string]any{}, turn1...),
		map[string]any{"role": "assistant", "content": "云端回复A"},
		map[string]any{"role": "user", "content": "问题二"})
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn2)))))
	if rec2.Code != 200 {
		t.Fatalf("turn2 code=%d", rec2.Code)
	}
	if creates != 1 {
		t.Errorf("粘性复用不应新建：creates=%d", creates)
	}
}

// TestServeRemoteConvRewrittenHistory 回显校验：客户端改写了 assistant
// 回复（与云端实际产出不符）→ 拒绝复用，走全量重建。
func TestServeRemoteConvRewrittenHistory(t *testing.T) {
	restore := remoteTestHook()
	defer restore()
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})
	turn1 := msgsOf("user", "问题一")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 {
		t.Fatalf("turn1 code=%d", rec.Code)
	}
	// turn2 携带与云端不一致的 assistant 回复 → 分叉重建。
	turn2 := append(append([]map[string]any{}, turn1...),
		map[string]any{"role": "assistant", "content": "被用户改写过的回复"},
		map[string]any{"role": "user", "content": "问题二"})
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn2)))))
	if rec2.Code != 200 {
		t.Fatalf("turn2 code=%d", rec2.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if creates != 2 {
		t.Errorf("改写历史应全量重建：creates=%d want 2", creates)
	}
}

// TestServeRemoteConvConcurrentSerialized 同一会话的两个并发请求应被
// per-conversation 锁串行化：两次发送不会交错（第二次在第一次完成后才开始）。
func TestServeRemoteConvConcurrentSerialized(t *testing.T) {
	restore := remoteTestHook()
	defer restore()

	var sendCount int32Mu
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	var inFlight, maxInFlight int
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	// 包装发送计数：记录并发峰值。
	base := up.HTTP.Transport
	up.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages") {
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			mu.Unlock()
			time.Sleep(30 * time.Millisecond) // 拉大窗口，暴露未串行化的并发
			resp, err := base.RoundTrip(r)
			mu.Lock()
			inFlight--
			mu.Unlock()
			return resp, err
		}
		return base.RoundTrip(r)
	})
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})
	// turn1 先建绑定。
	turn1 := msgsOf("user", "问题一")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 {
		t.Fatalf("turn1 code=%d", rec.Code)
	}
	// 同一前缀 + 同一新问题，两个并发请求（同一轮增量）。
	turn2 := append(append([]map[string]any{}, turn1...),
		map[string]any{"role": "assistant", "content": "云端回复A"},
		map[string]any{"role": "user", "content": "问题二"})
	rawBody := string(bodyWith(turn2))
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRecorder()
			h.ServeHTTP(r, httptest.NewRequest("POST", "/v1/chat/completions",
				strings.NewReader(rawBody)))
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight > 1 {
		t.Errorf("per-conversation 锁未串行化：并发峰值=%d", maxInFlight)
	}
	_ = sendCount
}

// int32Mu 占位类型（防止误用 atomic；计数已由 mu 保护）。
type int32Mu struct{}

// TestConvSweeperDeletesExpired 启动清扫器后（短 TTL），过期会话应被
// 删除云端会话并摘除注册项。
func TestConvSweeperDeletesExpired(t *testing.T) {
	restore := remoteTestHook()
	defer restore()
	t.Setenv("TW2API_CONV_TTL", "50ms")
	// convTTLDur 是即时读取的，Sweep 参数在 StartConvSweeper 内取。
	var deletes int
	up := convReuseFakeUpstream(t, &sync.Mutex{}, new(int), map[string]int{}, map[string]string{}, false)
	base := up.HTTP.Transport
	up.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			deletes++
		}
		return base.RoundTrip(r)
	})
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})
	turn1 := msgsOf("user", "问题一")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 {
		t.Fatalf("turn1 code=%d", rec.Code)
	}
	if len(h.convs.m) == 0 {
		t.Fatal("turn1 后注册表应有绑定")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.StartConvSweeper(ctx)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if deletes >= 1 && len(h.convs.m) == 0 {
			return // 清扫成功
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("清扫器未回收会话：deletes=%d remaining=%d", deletes, len(h.convs.m))
}

// TestConvReuseDisabledKeepsLegacy Env/Config 关闭复用 → 保持「用完即删」旧行为。
func TestConvReuseDisabledKeepsLegacy(t *testing.T) {
	snapshotDynamicModelsCache(t)
	restore := remoteTestHook()
	defer restore()
	var creates, deletes int
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/ide/v1/get_detail_param":
				return jsonHTTPResponse(200, `{"config_info_list":[{"config_name":"glm-5.3"}]}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions":
				creates++
				return jsonHTTPResponse(200, `{"code":0,"data":{"chat_session_id":"s1"}}`), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/remote/v1/chat_sessions/s1/messages":
				return jsonHTTPResponse(200, `{"code":0,"data":{"items":[{"role":"assistant","status":"completed","content":"OK"}]}}`), nil
			case r.Method == http.MethodDelete && r.URL.Path == "/api/remote/v1/chat_sessions/s1":
				deletes++
				return jsonHTTPResponse(200, `{"code":0,"message":"success"}`), nil
			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
				return nil, nil
			}
		})},
	}
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: "-",
	})
	if h.convs != nil {
		t.Fatal("ConvStorePath=- 应禁用会话注册表")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if creates != 1 || deletes != 1 {
		t.Errorf("禁用复用应保持旧路径：creates=%d deletes=%d", creates, deletes)
	}
}

var _ = os.Getenv // 保持 os import

// 回归：新会话第一条（纯 user 消息）不得复用旧会话（同提示词）。
func TestConvStoreNewSessionSameFirstPromptNoReuse(t *testing.T) {
	s := newConvStore("")
	// 旧会话：第一轮 user "hi" → assistant "ok"
	chain := convPrefixChain("glm-5.3", false, msgsOf("user", "hi", "assistant", "ok"))
	s.Bind("cloud-old", "u1", chain[1], 2, "ok")

	// 新会话第一条：只有 user "hi"，无历史 assistant 回显 → 必须新建，不命中旧会话
	req := msgsOf("user", "hi")
	e, incFrom := s.Lookup("glm-5.3", false, req)
	if e != nil {
		t.Fatalf("新会话第一条不得复用旧会话：got cloud=%s incFrom=%d", e.CloudSessionID, incFrom)
	}
	if incFrom != 0 {
		t.Errorf("incFrom=%d want 0（全量重建）", incFrom)
	}
}

// 回归：新会话首条（同提示词、无 assistant 回显）不得复用旧云端会话，
// 必须新建（此前 append=0B 空复用导致误连旧会话空等）。
func TestServeRemoteNewSessionSamePromptNoReuse(t *testing.T) {
	restore := remoteTestHook()
	defer restore()
	var mu sync.Mutex
	var creates int
	sends := map[string]int{}
	lastText := map[string]string{}
	up := convReuseFakeUpstream(t, &mu, &creates, sends, lastText, false)
	h := NewHandler(Config{
		Pool:          testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:      up,
		ConvStorePath: isolatedConvStore(),
	})

	// turn1：新建会话 cs1
	turn1 := msgsOf("user", "查一下磁盘")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec.Code != 200 {
		t.Fatalf("turn1 code=%d body=%s", rec.Code, rec.Body)
	}
	if creates != 1 {
		t.Fatalf("turn1 creates=%d want 1", creates)
	}

	// turn2：完全相同的提示词、无 assistant 历史 → 必须新建（不复用 cs1）
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(string(bodyWith(turn1)))))
	if rec2.Code != 200 {
		t.Fatalf("turn2 code=%d body=%s", rec2.Code, rec2.Body)
	}
	if creates != 2 {
		t.Errorf("新会话首条应新建会话：creates=%d want 2（误复用则仍是 1）", creates)
	}
}
