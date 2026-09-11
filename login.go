// login.go 实现宿主 AuthProvider 的浏览器登录流程（auth.login.start / poll）。
//
// 背景：CPA 管理页的「登录」按钮调 auth.login.start 拿登录 URL，用户浏览器
// 完成登录后宿主轮询 auth.login.poll 取回凭证。Trae 的登录没有服务端轮询
// 端点——refreshToken 只出现在「浏览器跳转到本地回调地址」的 URL 里（原
// login.sh 靠人工复制回调链接）。因此插件在 start 时临时监听 127.0.0.1
// 回调端口，浏览器跳转即被插件捕获并自动完成 ExchangeToken，poll 轮询到
// 结果即返回，用户全程只需在浏览器登录一次。
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	// loginFlowTTL 登录会话有效期：用户需在此时间内完成浏览器登录。
	loginFlowTTL = 5 * time.Minute
	// loginCallbackPort 首选回调端口（与原 login.sh 一致，上游流程已验证）。
	loginCallbackPort = 18080
	loginCallbackPath = "/authorize"
	// loginPluginVersion 登录页 plugin_version 参数（login.sh 实测值）。
	loginPluginVersion = "2.3.62834"
	// loginAppVersion 登录页 x_app_version 参数（login.sh 实测值）。
	loginAppVersion = "0.1.43"
)

// loginFlow 一次登录会话的完整状态。
type loginFlow struct {
	mu       sync.Mutex
	state    string
	machineID string
	deviceID  string
	srv      *http.Server
	expires  time.Time

	// 回调捕获（ServeHTTP 写入）
	callbackAt   time.Time
	refreshToken string
	userJWT      map[string]any

	// 兑换结果（finish 写入）
	done   bool
	sa     *traeAuth
	errMsg string
}

// loginExchange 执行「回调数据 → 凭证」的兑换。
// 声明为 var 作为测试缝：单测替换为 stub，不触网。
var loginExchange = exchangeLoginTokens

var (
	loginFlows = struct {
		sync.Mutex
		m map[string]*loginFlow
	}{m: make(map[string]*loginFlow)}
)

// handleStartLogin 生成登录 URL 并启动本地回调监听。
func handleStartLogin(raw []byte) ([]byte, error) {
	if len(raw) > 0 {
		var req pluginapi.AuthLoginStartRequest
		_ = json.Unmarshal(raw, &req)
	}
	sweepLoginFlows()

	machineID := randomHex(16)
	deviceID := randomHex(16)

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", loginCallbackPort))
	if err != nil {
		// 端口被占用（如另一登录流）→ 退回随机端口；回调地址同步使用实际端口。
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return nil, fmt.Errorf("start login: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	state := randomHex(16)
	flow := &loginFlow{
		state:     state,
		machineID: machineID,
		deviceID:  deviceID,
		expires:   time.Now().Add(loginFlowTTL),
		srv:       &http.Server{ReadHeaderTimeout: 10 * time.Second},
	}
	flow.srv.Handler = flow
	go func() {
		defer func() {
			if r := recover(); r != nil {
				pluginLogf("panic in login listener: %v", r)
			}
		}()
		_ = flow.srv.Serve(ln)
	}()

	loginFlows.Lock()
	loginFlows.m[state] = flow
	loginFlows.Unlock()

	loginURL := buildLoginURL(machineID, deviceID, port)
	pluginLogf("login flow started: state=%s callback_port=%d", state, port)
	return okEnvelope(pluginapi.AuthLoginStartResponse{
		Provider:  providerName,
		URL:       loginURL,
		State:     state,
		ExpiresAt: flow.expires.UTC(),
		Metadata:  map[string]any{"logo": pluginLogoURL},
	})
}

// handlePollLogin 供宿主轮询登录结果（单次快照，节奏由宿主驱动）。
func handlePollLogin(raw []byte) ([]byte, error) {
	var req pluginapi.AuthLoginPollRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	state := strings.TrimSpace(req.State)
	if state == "" {
		return nil, fmt.Errorf("poll: empty state")
	}
	loginFlows.Lock()
	flow := loginFlows.m[state]
	loginFlows.Unlock()
	if flow == nil {
		return nil, fmt.Errorf("poll: unknown state (restart login) — 登录会话已丢失，请重新发起登录")
	}
	if time.Now().After(flow.expires) {
		flow.cleanup()
		loginFlows.Lock()
		delete(loginFlows.m, state)
		loginFlows.Unlock()
		return nil, fmt.Errorf("poll: login expired (5 min timeout) — 请重新发起登录并在 5 分钟内完成")
	}

	flow.mu.Lock()
	done, errMsg, sa := flow.done, flow.errMsg, flow.sa
	flow.mu.Unlock()

	if errMsg != "" {
		flow.cleanup()
		loginFlows.Lock()
		delete(loginFlows.m, state)
		loginFlows.Unlock()
		return nil, fmt.Errorf("poll: %s", errMsg)
	}
	if !done {
		return okEnvelope(pluginapi.AuthLoginPollResponse{
			Status:  pluginapi.AuthLoginStatusPending,
			Message: "waiting for login",
		})
	}

	flow.cleanup()
	loginFlows.Lock()
	delete(loginFlows.m, state)
	loginFlows.Unlock()
	return okEnvelope(pluginapi.AuthLoginPollResponse{
		Status: pluginapi.AuthLoginStatusSuccess,
		Auth:   toAuthData(sa),
	})
}

// ServeHTTP 接收浏览器登录回调：捕获凭证参数并异步兑换。
func (f *loginFlow) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != loginCallbackPath {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	refresh := strings.TrimSpace(q.Get("refreshToken"))
	userJWT := parseJSONParam(q.Get("userJwt"))
	if refresh == "" {
		if v, _ := userJWT["RefreshToken"].(string); strings.TrimSpace(v) != "" {
			refresh = strings.TrimSpace(v)
		}
	}

	f.mu.Lock()
	f.callbackAt = time.Now()
	f.refreshToken = refresh
	f.userJWT = userJWT
	f.mu.Unlock()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, loginSuccessHTML)

	go f.finish()
}

// finish 在回调到达后执行兑换并落结果，供 poll 读取。
func (f *loginFlow) finish() {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in login exchange: %v", r)
			f.mu.Lock()
			f.errMsg = fmt.Sprintf("internal error: %v", r)
			f.mu.Unlock()
		}
	}()
	sa, err := loginExchange(f)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil {
		f.errMsg = redactSecrets(err.Error())
		pluginLogf("login exchange failed: %s", f.errMsg)
		return
	}
	f.sa = sa
	f.done = true
	pluginLogf("login exchange ok: uid=%s nickname=%s", maskUID(sa.UID), sa.Nickname)
}

// cleanup 停掉回调监听（幂等）。
func (f *loginFlow) cleanup() {
	if f.srv != nil {
		_ = f.srv.Close()
	}
}

// sweepLoginFlows 清理过期流（start 时顺带回收，防泄漏）。
func sweepLoginFlows() {
	now := time.Now()
	var stale []*loginFlow
	loginFlows.Lock()
	for state, f := range loginFlows.m {
		if now.After(f.expires) {
			stale = append(stale, f)
			delete(loginFlows.m, state)
		}
	}
	loginFlows.Unlock()
	for _, f := range stale {
		f.cleanup()
	}
}

// exchangeLoginTokens 把回调拿到的 refreshToken 兑换为完整凭证。
// 与 login.sh 的 Python 逻辑一致：ExchangeToken → GetUserInfo → 组装。
func exchangeLoginTokens(f *loginFlow) (*traeAuth, error) {
	f.mu.Lock()
	refresh := f.refreshToken
	userJWT := f.userJWT
	machineID, deviceID := f.machineID, f.deviceID
	f.mu.Unlock()

	client := currentClient()
	sa := &traeAuth{}
	jwtToken := ""
	if v, ok := userJWT["Token"].(string); ok {
		jwtToken = strings.TrimSpace(v)
	}

	switch {
	case refresh != "":
		sa.RefreshToken = refresh
		if err := client.RefreshToken(sa); err != nil {
			return nil, fmt.Errorf("ExchangeToken 失败: %w", err)
		}
	case jwtToken != "":
		// 兜底：回调无 refreshToken 时直接用 userJwt 的 Token（与原脚本一致）。
		sa.AccessToken = jwtToken
		if exp := int64FromAny(userJWT["TokenExpireAt"]); exp > 0 {
			sa.ExpiresAt = normalizeExpiresAt(exp)
		} else {
			sa.ExpiresAt = time.Now().Add(12 * time.Hour).Unix()
		}
	default:
		return nil, fmt.Errorf("回调链接缺少 refreshToken，且 userJwt 也没有 Token")
	}

	sa.MachineID = machineID
	sa.DeviceID = deviceID

	if uid, nickname, entID, err := client.GetUserInfo(sa); err == nil {
		sa.UID, sa.Nickname, sa.EnterpriseID = uid, nickname, entID
	}
	if sa.UID == "" {
		return nil, fmt.Errorf("登录成功但无法解析 uid，请重试")
	}
	// 缺签到设备 ID 的新凭证直接补上，保证登录后即可签到。
	EnsurePerAccountCheckinDevice(sa)
	return sa, nil
}

// buildLoginURL 构造 Trae SOLO 登录页 URL（参数集与 login.sh 一致）。
func buildLoginURL(machineID, deviceID string, callbackPort int) string {
	params := url.Values{}
	params.Set("login_version", "1")
	params.Set("auth_from", "solo")
	params.Set("login_channel", "native_ide")
	params.Set("plugin_version", loginPluginVersion)
	params.Set("auth_type", "local")
	params.Set("client_id", ClientID)
	params.Set("redirect", "0")
	params.Set("login_trace_id", randomHex(8))
	params.Set("auth_callback_url", fmt.Sprintf("http://127.0.0.1:%d%s", callbackPort, loginCallbackPath))
	params.Set("machine_id", machineID)
	params.Set("device_id", deviceID)
	params.Set("x_device_id", deviceID)
	params.Set("x_machine_id", machineID)
	params.Set("x_device_brand", "PC")
	params.Set("x_device_type", "PC")
	params.Set("x_os_version", "1.0")
	params.Set("x_app_version", loginAppVersion)
	params.Set("x_app_type", "stable")
	return ConsoleHost + "/authorization?" + params.Encode()
}

// parseJSONParam 解析回调里 URL 编码的 JSON 参数（parse_qs 已解一层，
// 再容错解一层 unquote——对齐 login.sh 的 parse_json_param）。
func parseJSONParam(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	for _, val := range []string{raw, strings.ReplaceAll(raw, "%22", `"`)} {
		decoded, err := url.QueryUnescape(val)
		if err != nil {
			decoded = val
		}
		for _, candidate := range []string{val, decoded} {
			var obj map[string]any
			if err := json.Unmarshal([]byte(candidate), &obj); err == nil && obj != nil {
				return obj
			}
		}
	}
	return nil
}

// randomHex 生成 n 字节的随机 hex 串（32 位十六进制用于设备 id）。
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// int64FromAny 容错读取 JSON number 的整数值（float64/json.Number 都可能）。
func int64FromAny(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case json.Number:
		n, _ := t.Int64()
		return n
	}
	return 0
}

const loginSuccessHTML = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"><title>登录成功</title>
<style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0f1115;color:#e6e6e6}
.card{text-align:center;padding:48px 56px;border-radius:16px;background:#171a21;box-shadow:0 8px 32px rgba(0,0,0,.4)}
h1{font-size:22px;margin:0 0 12px}p{color:#9aa4b2;margin:0;font-size:14px}</style></head>
<body><div class="card"><h1>✅ 登录成功</h1><p>凭证已捕获，请回到 CPA 管理面板完成导入。</p></div></body></html>`
