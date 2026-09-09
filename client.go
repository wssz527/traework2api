// client.go SOLO 上游客户端：llm_utils_chat / get_detail_param / ExchangeToken /
// checkin_credits / ide_user_ent_usage + 错误分类。
//
// 从 traework2api/internal/upstream/client.go 迁移。适配 CPA 插件的改动：
//   - 全部走插件内 net/http（macOS 禁用 host.http.do，见迁移报告 §5.4）
//   - log.Printf 改为 pluginLog（宿主 host.log，失败静默回落）
//   - 错误类型 *Error 的 Auth 依赖改为 *traeAuth
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// ErrKind 错误分类，pool 据此决定冷却时长（SPEC §4.3）。
type ErrKind int

const (
	ErrNone        ErrKind = iota // 成功
	ErrPlanLimit                  // 1005 + plan → 权益不足（硬冷却 12h）
	ErrSoftRate                   // 429 → 短冷却 60s
	ErrSessionDead                // 401 + Cloud-IDE-JWT 失效 → 禁用
	ErrNotFound                   // 404 → 短冷却 60s 不累计 errCount
	ErrServer                     // 5xx
	ErrClient                     // 其他 4xx
)

func (k ErrKind) String() string {
	switch k {
	case ErrPlanLimit:
		return "plan_limit"
	case ErrSoftRate:
		return "soft_rate"
	case ErrSessionDead:
		return "session_dead"
	case ErrNotFound:
		return "not_found"
	case ErrServer:
		return "server"
	case ErrClient:
		return "client"
	default:
		return "none"
	}
}

// Error 带分类的上游错误。
type Error struct {
	Kind   ErrKind
	Status int
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream %s (http %d): %s", e.Kind, e.Status, e.Msg)
}

var sessionDeadMarkers = []string{"login", "token 失效", "token invalid", "session", "unauthorized", "401"}

// Classify 按 HTTP 状态码 + body 判定错误类别（SPEC §4.3）。
func Classify(status int, body string) ErrKind {
	lower := strings.ToLower(body)
	if strings.Contains(body, `"code":1005`) || (strings.Contains(body, "1005") && strings.Contains(lower, "plan")) {
		return ErrPlanLimit
	}
	if status == http.StatusUnauthorized {
		for _, m := range sessionDeadMarkers {
			if strings.Contains(lower, strings.ToLower(m)) {
				return ErrSessionDead
			}
		}
		return ErrSessionDead
	}
	if status == http.StatusTooManyRequests {
		return ErrSoftRate
	}
	if status == http.StatusNotFound {
		return ErrNotFound
	}
	if status >= 500 {
		return ErrServer
	}
	if status >= 400 {
		return ErrClient
	}
	return ErrNone
}

// Client SOLO 上游 HTTP 客户端。
type Client struct {
	HTTP       *http.Client // 短 JSON 请求，有总超时兜底
	StreamHTTP *http.Client // SSE 流式对话：无总超时，靠 ResponseHeaderTimeout 兜底

	AgentHost string
	UgHost    string
	OAuthHost string
	ClientID  string
}

// New 生产默认值。
func New() *Client {
	tr := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 300 * time.Second, // 首字节兜底，不限制整流时长；长上下文（数十万 token）首包可能需 1-2 分钟
	}
	return &Client{
		HTTP:       &http.Client{Timeout: 120 * time.Second, Transport: tr},
		StreamHTTP: &http.Client{Transport: tr},
		AgentHost:  AgentHost,
		UgHost:     UgHost,
		OAuthHost:  OAuthHost,
		ClientID:   ClientID,
	}
}

// currentClient / setDefaultClient 管理插件共享的上游客户端（连接池复用）。
//
// 用 atomic.Pointer 而不是普通全局变量：ChatStream 会被后台 pump goroutine
// 读取（executor.execute_stream 的异步路径），而客户端理论上可被热替换
// （reconfigure / 测试）。普通全局变量在这两者并发时会数据竞争（-race 实测）。
var defaultClient atomic.Pointer[Client]

func currentClient() *Client {
	if c := defaultClient.Load(); c != nil {
		return c
	}
	c := New()
	defaultClient.CompareAndSwap(nil, c)
	return defaultClient.Load()
}

func setDefaultClient(c *Client) { defaultClient.Store(c) }

func init() { defaultClient.Store(New()) }

func (c *Client) agentBase() string { return c.AgentHost }
func (c *Client) ugBase() string    { return c.UgHost }
func (c *Client) oauthBase() string { return c.OAuthHost }

// doJSON 发请求并解 JSON；HTTP 非 2xx 时返回带 body 片段的 *Error。
func (c *Client) doJSON(req *http.Request) (json.RawMessage, error) {
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		kind := Classify(resp.StatusCode, string(raw))
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: truncate(redactSecrets(string(raw)), 200)}
	}
	return raw, nil
}

// RefreshToken 通过 ExchangeToken 强制刷新 access token（refreshToken 轮换）。
func (c *Client) RefreshToken(a *traeAuth) error {
	a.Lock()
	defer a.Unlock()
	return c.refreshLocked(a)
}

// RefreshTokenIfNeeded 仅当 token 在 skew 内即将过期（或已过期）时才刷新，
// 返回是否真正刷新。调用方仅在 true 时需要落盘。
func (c *Client) RefreshTokenIfNeeded(a *traeAuth, skew time.Duration) (bool, error) {
	a.Lock()
	defer a.Unlock()
	if !a.needsRefreshLocked(skew) {
		return false, nil
	}
	if err := c.refreshLocked(a); err != nil {
		return false, err
	}
	return true, nil
}

// refreshLocked 是 RefreshToken 的持锁内部实现；调用方必须已持有 a 写锁。
// 任何失败路径都不改写 a 字段，保证旧 refreshToken 可重试。
func (c *Client) refreshLocked(a *traeAuth) error {
	if strings.TrimSpace(a.RefreshToken) == "" {
		return fmt.Errorf("no refreshToken")
	}
	host := a.ApiHost
	if host == "" {
		host = c.oauthBase()
	}
	body := map[string]any{
		"ClientID":     c.ClientID,
		"RefreshToken": a.RefreshToken,
		"ClientSecret": "-",
		"UserID":       "",
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, host+EpExchange, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	OAuthHeaders(req)
	data, err := c.doJSON(req)
	if err != nil {
		return err
	}
	var resp struct {
		Result struct {
			Token               string `json:"Token"`
			TokenExpireAt       int64  `json:"TokenExpireAt"`
			TokenExpireDuration int64  `json:"TokenExpireDuration"`
			RefreshToken        string `json:"RefreshToken"`
			RefreshExpireAt     int64  `json:"RefreshExpireAt"`
		} `json:"Result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("exchange parse: %w", err)
	}
	if resp.Result.Token == "" {
		return fmt.Errorf("refresh_failed: no token in response — re-login required")
	}
	a.AccessToken = resp.Result.Token
	if resp.Result.RefreshToken != "" {
		a.RefreshToken = resp.Result.RefreshToken
	}
	if resp.Result.TokenExpireAt > 0 {
		a.ExpiresAt = normalizeExpiresAt(resp.Result.TokenExpireAt)
	} else if resp.Result.TokenExpireDuration > 0 {
		a.ExpiresAt = time.Now().Add(time.Duration(resp.Result.TokenExpireDuration) * time.Second).Unix()
	}
	return nil
}

// normalizeExpiresAt 把 ExchangeToken 的 TokenExpireAt 归一化为 Unix 秒。
// 上游返回毫秒（如 1786847930141），auth 文件用秒（1786847930）。
func normalizeExpiresAt(v int64) int64 {
	if v > 1e12 {
		return v / 1000
	}
	return v
}

// ChatStream 发 llm_utils_chat 请求并返回原始 SSE body 流（调用方负责 Close）。
// 非 2xx 时 rc 为 nil、respBody 为上游响应体（供调用方 Classify）、err 为 nil；
// 只有传输层失败才返回 err。
func (c *Client) ChatStream(a *traeAuth, body []byte) (rc io.ReadCloser, status int, respBody []byte, err error) {
	req, err := http.NewRequest(http.MethodPost, c.agentBase()+EpChat, bytes.NewReader(PrepareBody(body)))
	if err != nil {
		return nil, 0, nil, err
	}
	SOLOHeaders(req, a, true)
	hc := c.HTTP
	if c.StreamHTTP != nil {
		hc = c.StreamHTTP
	}
	resp, err := hc.Do(req)
	if err != nil {
		pluginLogf("chat_stream uid=%s: transport error: %v", a.UID, err)
		return nil, 0, nil, err
	}
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		kind := Classify(resp.StatusCode, string(raw))
		pluginLogf("chat_stream uid=%s: upstream %d %s body=%s", a.UID, resp.StatusCode, kind, redactSecrets(truncate(string(raw), 200)))
		return nil, resp.StatusCode, raw, nil
	}
	return resp.Body, resp.StatusCode, nil, nil
}

// ModelInfo 动态模型信息。
type ModelInfo struct {
	ID                     string
	Name                   string
	ContextWindow          int64
	InputTokens            int64
	MaxTokens              int64
	MaxMode                bool
	MaxContextWindow       int64
	MaxModeInputTokens     int64
	MaxModeOutputTokens    int64
	Multimodal             bool
	ReasoningEfforts       []string
	DefaultReasoningEffort string
	ConsumptionRate        float64
}

// FetchModels 拉 SOLO 模型表（get_detail_param）。
func (c *Client) FetchModels(a *traeAuth) ([]ModelInfo, error) {
	body := map[string]any{
		"function":            Function,
		"config_names":        nil,
		"need_prompt":         false,
		"current_config_info": nil,
		"poly_prompt":         true,
		"mode_type":           nil,
		"agent_type":          nil,
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, c.agentBase()+EpModels, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	SOLOHeaders(req, a, false)
	data, err := c.doJSON(req)
	if err != nil {
		return nil, err
	}
	var resp struct {
		ConfigInfoList []struct {
			ConfigName    string `json:"config_name"`
			DisplayConfig struct {
				DisplayName string `json:"display_name"`
				MaxMode     bool   `json:"max_mode"`
				Multimodal  bool   `json:"multimodal"`
			} `json:"display_config"`
			DisplayContactConfig  string           `json:"display_contact_config"`
			ContextWindowTokens   map[string]int64 `json:"context_window_tokens"`
			ReasoningEffortConfig struct {
				SupportThinking bool     `json:"support_thinking"`
				DefaultLevel    string   `json:"default_level"`
				Options         []string `json:"options"`
			} `json:"reasoning_effort_config"`
			ModelDetailList []struct {
				ModelName       string `json:"model_name"`
				PromptMaxTokens int64  `json:"prompt_max_tokens"`
				MaxTokens       int64  `json:"max_tokens"`
			} `json:"model_detail_list"`
		} `json:"config_info_list"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("models parse: %w", err)
	}
	out := make([]ModelInfo, 0, len(resp.ConfigInfoList))
	for _, cfg := range resp.ConfigInfoList {
		if cfg.ConfigName == "" {
			continue
		}
		info := ModelInfo{
			ID:               cfg.ConfigName,
			Name:             cfg.DisplayConfig.DisplayName,
			ContextWindow:    cfg.ContextWindowTokens["dev"],
			MaxContextWindow: cfg.ContextWindowTokens["max"],
			MaxMode:          cfg.DisplayConfig.MaxMode,
			Multimodal:       cfg.DisplayConfig.Multimodal,
		}
		for _, detail := range cfg.ModelDetailList {
			switch {
			case strings.HasSuffix(detail.ModelName, "__dev"):
				info.InputTokens, info.MaxTokens = detail.PromptMaxTokens, detail.MaxTokens
			case strings.HasSuffix(detail.ModelName, "__max"):
				info.MaxModeInputTokens, info.MaxModeOutputTokens = detail.PromptMaxTokens, detail.MaxTokens
			}
		}
		if cfg.ReasoningEffortConfig.SupportThinking {
			info.ReasoningEfforts = cfg.ReasoningEffortConfig.Options
			info.DefaultReasoningEffort = cfg.ReasoningEffortConfig.DefaultLevel
		}
		if cfg.DisplayContactConfig != "" {
			var dcc struct {
				ConsumptionRate *struct {
					Data *struct {
						Rate *float64 `json:"rate"`
					} `json:"data"`
				} `json:"consumption_rate"`
			}
			if err := json.Unmarshal([]byte(cfg.DisplayContactConfig), &dcc); err == nil &&
				dcc.ConsumptionRate != nil && dcc.ConsumptionRate.Data != nil && dcc.ConsumptionRate.Data.Rate != nil {
				info.ConsumptionRate = *dcc.ConsumptionRate.Data.Rate
			}
		}
		out = append(out, info)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("models api returned empty list")
	}
	return out, nil
}

// CheckinStatus 查询签到状态。
func (c *Client) CheckinStatus(a *traeAuth) (checkedIn bool, credits int64, enable bool, err error) {
	req, err := http.NewRequest(http.MethodPost, c.ugBase()+EpCheckinStatus, bytes.NewReader([]byte("{}")))
	if err != nil {
		return false, 0, false, err
	}
	UgHeaders(req, a)
	data, err := c.doJSON(req)
	if err != nil {
		return false, 0, false, err
	}
	var resp struct {
		CheckedIn *bool  `json:"checked_in"`
		Credits   int64  `json:"credits"`
		Enable    *bool  `json:"enable"`
		Code      *int   `json:"code"`
		Message   string `json:"message"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return false, 0, false, fmt.Errorf("checkin status parse: %w", err)
	}
	if resp.Code != nil && *resp.Code != 0 {
		return false, 0, false, fmt.Errorf("checkin status: code=%d message=%s", *resp.Code, resp.Message)
	}
	if resp.CheckedIn == nil || resp.Enable == nil {
		return false, 0, false, fmt.Errorf("checkin status: missing checked_in or enable")
	}
	return *resp.CheckedIn, resp.Credits, *resp.Enable, nil
}

// CheckinClaim 执行签到。
func (c *Client) CheckinClaim(a *traeAuth) error {
	req, err := http.NewRequest(http.MethodPost, c.ugBase()+EpCheckinClaim, bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	UgHeaders(req, a)
	data, err := c.doJSON(req)
	if err != nil {
		return err
	}
	var resp struct {
		Code    *int   `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("checkin claim parse: %w", err)
	}
	if resp.Code != nil && *resp.Code != 0 {
		return fmt.Errorf("checkin claim: code=%d message=%s", *resp.Code, resp.Message)
	}
	return nil
}

// UserEntUsage 聚合积分用量（ide_user_ent_usage）。
//
// 剩余 = Σ(credits_limit) − Σ(usage.credits_amount)，钳到 >= 0；
// used/limit/pack_count 一并返回，面板据此画用量进度条。
func (c *Client) UserEntUsage(a *traeAuth) (entUsage, error) {
	var zero entUsage
	req, err := http.NewRequest(http.MethodPost, c.ugBase()+EpEntUsage, bytes.NewReader([]byte("{}")))
	if err != nil {
		return zero, err
	}
	UgHeaders(req, a)
	data, err := c.doJSON(req)
	if err != nil {
		return zero, err
	}
	var resp struct {
		IsCreditsBilling        bool `json:"is_credits_billing"`
		UserEntitlementPackList []struct {
			EntitlementBaseInfo struct {
				Quota struct {
					CreditsLimit int64 `json:"credits_limit"`
				} `json:"quota"`
			} `json:"entitlement_base_info"`
			Usage struct {
				CreditsAmount float64 `json:"credits_amount"`
			} `json:"usage"`
		} `json:"user_entitlement_pack_list"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return zero, fmt.Errorf("ent usage parse: %w", err)
	}
	var limit, used float64
	for _, p := range resp.UserEntitlementPackList {
		limit += float64(p.EntitlementBaseInfo.Quota.CreditsLimit)
		used += p.Usage.CreditsAmount
	}
	out := entUsage{
		Used:      int64(used),
		Limit:     int64(limit),
		PackCount: len(resp.UserEntitlementPackList),
	}
	out.Remain = int64(limit - used)
	if out.Remain < 0 {
		out.Remain = 0
	}
	return out, nil
}

// GetUserInfo 查询账号信息（登录用）。
func (c *Client) GetUserInfo(a *traeAuth) (uid, nickname, enterpriseID string, err error) {
	host := a.ApiHost
	if host == "" {
		host = c.oauthBase()
	}
	body := map[string]any{"ReqSource": "IDE", "IDEVersion": ideVersion()}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, host+EpUserInfo, bytes.NewReader(raw))
	if err != nil {
		return "", "", "", err
	}
	OAuthHeaders(req)
	req.Header.Set("X-Cloudide-Token", a.JWT())
	data, err := c.doJSON(req)
	if err != nil {
		return "", "", "", err
	}
	var resp struct {
		Result struct {
			UserID       string `json:"UserID"`
			ScreenName   string `json:"ScreenName"`
			EnterpriseID string `json:"EnterpriseID"`
		} `json:"Result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", "", "", fmt.Errorf("userinfo parse: %w", err)
	}
	return resp.Result.UserID, resp.Result.ScreenName, resp.Result.EnterpriseID, nil
}
