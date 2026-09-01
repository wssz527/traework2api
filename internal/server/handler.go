// Package server 暴露 OpenAI 兼容 HTTP 接口，内部驱动 pool 挑号 + upstream 转发。
package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/pool"
	"traework2api/internal/upstream"
)

// Config handler 依赖。
type Config struct {
	Pool         *pool.Pool
	Upstream     *upstream.Client
	APIKey       string        // 空 = 不鉴权
	MaxRotate    int           // 单请求最多换号次数，默认 3
	PlanCooldown time.Duration // 1005 冷却，默认 12h
	SoftCooldown time.Duration // 429 冷却，默认 60s
	ErrThreshold int           // 连续错误阈值，默认 3
	ErrCooldown  time.Duration // 错误冷却，默认 10m
	RefreshSkew  time.Duration // token 预刷新窗口，默认 24h
	DefaultModel string        // 默认 glm-5.2
	// ConvStorePath 会话注册表落盘路径（P1）。空 = "data/conversations.json"；
	// "-" = 禁用会话复用（连同 env TW2API_DISABLE_CONV_REUSE=1）。
	ConvStorePath string
}

// maxBodyBytes 请求体大小上限（8MB），超过返回 413。
const maxBodyBytes = 8 << 20

// Handler 主路由。
type Handler struct {
	cfg Config
	mux *http.ServeMux
	// mcpEvents 本地 MCP 工具调用事件总线。由 NewHandler 创建；
	// 不经 NewHandler 构造的 Handler（如个别单测直接字面量构建）不支持
	// MCP 事件转发与 /internal/mcp-event。
	mcpEvents *mcpEventBus
	// tasks 活动 remote 任务注册表（P1 会话归属）。由 NewHandler 创建。
	tasks *taskRegistry
	// convs 会话连续性注册表（P1）。由 NewHandler 创建；nil = 禁用复用
	//（每次新建会话，行为回退二期）。
	convs *convStore
}

// NewHandler 构建 handler。
func NewHandler(cfg Config) *Handler {
	if cfg.MaxRotate <= 0 {
		cfg.MaxRotate = 3
	}
	if cfg.PlanCooldown <= 0 {
		cfg.PlanCooldown = 12 * time.Hour
	}
	if cfg.SoftCooldown <= 0 {
		cfg.SoftCooldown = 60 * time.Second
	}
	if cfg.ErrThreshold <= 0 {
		cfg.ErrThreshold = 3
	}
	if cfg.ErrCooldown <= 0 {
		cfg.ErrCooldown = 10 * time.Minute
	}
	if cfg.RefreshSkew <= 0 {
		cfg.RefreshSkew = 24 * time.Hour
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = upstream.DefaultConfigName
	}
	h := &Handler{
		cfg: cfg, mux: http.NewServeMux(),
		mcpEvents: newMcpEventBus(),
		tasks:     newTaskRegistry(),
	}
	// P1 会话连续性：默认启用；TW2API_DISABLE_CONV_REUSE=1 或 ConvStorePath
	// 为 "-" 时禁用（每次新建会话，回退二期行为）。
	if !disableConvReuse() && cfg.ConvStorePath != "-" {
		fp := cfg.ConvStorePath
		if fp == "" {
			fp = "data/conversations.json"
		}
		h.convs = newConvStore(fp)
	}
	h.mux.HandleFunc("POST /v1/chat/completions", h.withAuth(h.chatCompletions))
	h.mux.HandleFunc("GET /v1/models", h.withAuth(h.models))
	h.mux.HandleFunc("GET /status", h.withAuth(h.status))
	h.mux.HandleFunc("GET /healthz", h.healthz)
	// 本地 MCP 工具调用事件接收端点（事件总线在 NewHandler 时已建好）。
	h.mux.HandleFunc("POST /internal/mcp-event", h.serveMcpEvent)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.APIKey != "" {
			authz := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if len(authz) < len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
				writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
				return
			}
			key := authz[len(prefix):]
			// 常量时间比较，防时序攻击（本地代理但按规范）。
			if subtle.ConstantTimeCompare([]byte(key), []byte(h.cfg.APIKey)) != 1 {
				writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
				return
			}
		}
		next(w, r)
	}
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts": h.cfg.Pool.List(),
	})
}

// ---------------------------------------------------------------------------
// 模型映射
// ---------------------------------------------------------------------------

// mapModel 将客户端传入的 model 映射为 config_name（SPEC §4.5）：
//
//	"glm-5.2"（config_name）        → 直接转发
//	"glm-5.2__dev"（内部名）        → 去掉后缀映射回 config_name
//	"glm-5.3-max"（Max 长上下文）   → glm-5.3 + maxMode=true
//	"auto" / ""                     → 默认模型
//	其他未知                        → 400
//
// 返回值第二个为 maxMode：带 "-max" 后缀时为 true。
func (h *Handler) mapModel(model string) (string, bool, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return h.cfg.DefaultModel, false, nil
	}
	// "-max" 后缀 → Max 长上下文模式（P0）。剩余部分继续走原映射。
	base, maxMode := upstream.SplitMaxSuffix(model)
	// 去掉内部名后缀（__dev / __max 等）
	if i := strings.Index(base, "__"); i >= 0 {
		base = base[:i]
	}
	if h.knownModel(base) {
		return base, maxMode, nil
	}
	// 宽松匹配：下划线 → 横线，大小写不敏感（deepseek_v4_pro → DeepSeek-V4-Pro）
	norm := normalizeModelName(base)
	if h.knownModel(norm) {
		return norm, maxMode, nil
	}
	return "", false, fmt.Errorf("unknown model %q", model)
}

// normalizeModelName 将下划线命名的内部名归一化为 config_name 风格（横线分隔）。
func normalizeModelName(s string) string {
	parts := strings.Split(s, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + strings.ToLower(p[1:])
	}
	return strings.Join(parts, "-")
}

// knownModel 判断 model 是否在动态/静态模型表中。
func (h *Handler) knownModel(model string) bool {
	for _, m := range h.modelList() {
		if m["id"] == model {
			return true
		}
	}
	return false
}

// 静态 SOLO 模型表（SPEC P3：32 个 config_name，来自逆向报告；动态拉取失败时回退）。
var staticModels = []map[string]any{
	{"id": "Doubao-Seed-2.1-Pro", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "seed-code-pro-0430", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "Doubao-Seed-2.1-Turbo", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "Doubao-Seed-2.0-Code", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "DeepSeek-V4-Flash-Official", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "browser_use_subagent", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "glm-5.2", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "glm-5.3", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 168000},
	{"id": "glm-5-turbo", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "glm-5", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "DeepSeek-V4-Pro-Official", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "kimi-k3", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "kimi-k2.7-code", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "kimi-k2.6", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "minimax-m3", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "qwen-3.7-plus", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "sagitta", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "aquila", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_gemini", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_placeholder", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_1M_text", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_1M", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_kimi", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_claude", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_gpt-5", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_no-fc", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_deepseek_chat", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_deepseek_reasoner", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_deepseek_v4", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "explore_sub_agent_v13", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "explore_sub_agent_v2", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "summary", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
}

// dynamicModelsCache 动态模型缓存（成功 1h / 失败负缓存 5min）。
var dynamicModelsCache struct {
	sync.RWMutex
	ids      []upstream.ModelInfo
	fetched  time.Time
	lastFail time.Time
}

const (
	dynamicModelsTTL        = time.Hour
	modelsFetchFailCooldown = 5 * time.Minute
)

func (h *Handler) models(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   h.modelList(),
	})
}

// modelList 动态获取模型列表并包装成 OpenAI 格式；失败回退静态表。
// 已弃用的旧版模型（DeepSeek-V4-Pro / DeepSeek-V4-Flash）在动态列表中过滤掉，
// 只保留对应 Official 正式版，避免新旧混在一起。
var deprecatedModelIDs = map[string]bool{
	"DeepSeek-V4-Pro":   true,
	"DeepSeek-V4-Flash": true,
}

func (h *Handler) modelList() []map[string]any {
	if infos := h.fetchDynamicModels(); len(infos) > 0 {
		out := make([]map[string]any, 0, len(infos))
		for _, mi := range infos {
			if deprecatedModelIDs[mi.ID] {
				continue
			}
			entry := map[string]any{
				"id":             mi.ID,
				"object":         "model",
				"created":        1753600000,
				"owned_by":       "trae-solo",
				"context_length": mi.ContextWindow,
			}
			if entry["context_length"] == 0 {
				entry["context_length"] = 131072
			}
			out = append(out, entry)
			// P0：Max 长上下文变体。客户端模型选择代码只对 max_mode:true 的
			// 模型开 Max（逆向报告 §2①）；该标志位只在 get_detail_param 原始
			// 载荷里（FetchModels 未解析），本端拿不到 → 对全部模型暴露 "-max"
			// 变体并标注 max_mode 元数据。不支持 Max 的模型带 "-max" 请求时
			// 由云端报错，行为与客户端选错模型一致。
			out = append(out, map[string]any{
				"id": mi.ID + "-max", "object": "model", "created": 1753600000,
				"owned_by": "trae-solo", "context_length": entry["context_length"],
				"max_mode": true,
			})
		}
		return out
	}
	return staticModels
}

// fetchDynamicModels 从池中任一健康账号拉模型列表（get_detail_param），缓存 1h。
func (h *Handler) fetchDynamicModels() []upstream.ModelInfo {
	dynamicModelsCache.RLock()
	if len(dynamicModelsCache.ids) > 0 && time.Since(dynamicModelsCache.fetched) < dynamicModelsTTL {
		out := dynamicModelsCache.ids
		dynamicModelsCache.RUnlock()
		return out
	}
	if !dynamicModelsCache.lastFail.IsZero() && time.Since(dynamicModelsCache.lastFail) < modelsFetchFailCooldown {
		dynamicModelsCache.RUnlock()
		return nil
	}
	dynamicModelsCache.RUnlock()

	acct := h.cfg.Pool.Pick()
	if acct == nil {
		return nil
	}
	infos, err := h.cfg.Upstream.FetchModels(acct)
	if err != nil || len(infos) == 0 {
		dynamicModelsCache.Lock()
		dynamicModelsCache.lastFail = time.Now()
		dynamicModelsCache.Unlock()
		return nil
	}
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = infos
	dynamicModelsCache.fetched = time.Now()
	dynamicModelsCache.lastFail = time.Time{}
	dynamicModelsCache.Unlock()
	return infos
}

// ---------------------------------------------------------------------------
// chat
// ---------------------------------------------------------------------------

// setModelInBody 将 body 中 model 字段替换为 configName，并返回改写后的 body。
func setModelInBody(body []byte, configName string) []byte {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	obj["model"] = configName
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

func (h *Handler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}
	if len(body) > maxBodyBytes {
		writeOpenAIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 8MB limit")
		return
	}
	var peek struct {
		Stream bool   `json:"stream"`
		Model  string `json:"model"`
	}
	_ = json.Unmarshal(body, &peek)

	configName, maxMode, err := h.mapModel(peek.Model)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	body = setModelInBody(body, configName)
	// 双通道智能分配：
	//   useRemote=false（默认）→ 走老通道 llm_utils_chat（通用积分，每日恢复）
	//   老通道 4008（通用积分耗尽）→ useRemote=true 降级 remote 通道（Work 积分）
	//   glm-5.3 这类老通道拉不到模型的，直接走 remote。
	useRemote := isCloudAgentModel(configName) // 强制 remote 的模型（老通道模型表没有的）

	tried := map[string]bool{}
	var lastErr error
	for i := 0; i < h.cfg.MaxRotate; i++ {
		acct := h.cfg.Pool.PickExcluding(tried)
		if acct == nil {
			break
		}
		tried[acct.UID] = true

		// token 临近过期 → 先 refresh（持锁重查，避免并发重复轮换；失败冷却换号）。
		// 注意：remote 通道（本地任务）跳过 refresh——refresh 会换新 token，
		// 而 remote 需要与客户端一致的 token（刷新后 device 上下文不匹配导致 500）。
		if !useRemote {
			refreshed, err := h.cfg.Upstream.RefreshTokenIfNeeded(acct, h.cfg.RefreshSkew)
			if err != nil {
				lastErr = err
				var ue *upstream.Error
				if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
					h.cfg.Pool.Disable(acct.UID, "refresh session dead")
				} else {
					h.cfg.Pool.Cooldown(acct.UID, pool.CoolErr, h.cfg.ErrCooldown, "refresh: "+err.Error())
				}
				continue
			}
			if refreshed {
				_ = acct.SaveAtomic()
			}
		}

		// cloud_agent 模型（glm-5.3 等）→ remote 通道（Work 积分）；
		// 其余模型 → llm_utils_chat（通用积分）。
		var rc io.ReadCloser
		var status int
		var respBody []byte
		var terr error
		if useRemote {
			// remote 通道单账号完整往返（建会话→发消息→轮询→删会话）。
			// 任务级失败（超时/客户端断开）不换号重发：重发会在上游再占一个并发槽。
			rerr := h.serveRemote(w, r, acct, configName, body, peek.Stream, maxMode)
			if rerr == nil {
				return // 响应已写完（成功或流式内嵌错误）
			}
			lastErr = rerr
			if errors.Is(rerr, upstream.ErrRemoteTimeout) {
				writeOpenAIError(w, http.StatusGatewayTimeout, "remote_task_timeout", rerr.Error())
				return
			}
			if errors.Is(rerr, context.Canceled) {
				return // 客户端已断开
			}
			if errors.Is(rerr, upstream.ErrRemoteBusy) {
				h.cfg.Pool.Cooldown(acct.UID, pool.CoolSoft, h.cfg.SoftCooldown, "remote parallel limit")
			} else {
				h.cfg.Pool.NoteError(acct.UID, h.cfg.ErrThreshold, h.cfg.ErrCooldown)
			}
			continue
		}
		rc, status, respBody, terr = h.cfg.Upstream.ChatStream(acct, body)
		// 老通道配额耗尽（4008）→ 降级 remote 通道（Work 积分）
		if !useRemote && isQuotaExhausted(status, respBody) {
			useRemote = true
			tried[acct.UID] = false // 允许同账号走 remote（remote 不消耗通用积分）
			if rc != nil {
				rc.Close()
			}
			continue
		}
		if terr != nil {
			lastErr = terr
			h.cfg.Pool.NoteError(acct.UID, h.cfg.ErrThreshold, h.cfg.ErrCooldown)
			continue
		}
		if status >= 400 {
			kind := upstream.Classify(status, string(respBody))
			switch kind {
			case upstream.ErrPlanLimit:
				h.cfg.Pool.Cooldown(acct.UID, pool.CoolPlan, h.cfg.PlanCooldown, "plan 权益不足")
				lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody)}
				continue
			case upstream.ErrSoftRate:
				h.cfg.Pool.Cooldown(acct.UID, pool.CoolSoft, h.cfg.SoftCooldown, "429 rate limit")
				lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody)}
				continue
			case upstream.ErrSessionDead:
				h.cfg.Pool.Disable(acct.UID, "session dead")
				lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody)}
				continue
			case upstream.ErrNotFound:
				// 404 短冷却不累计 errCount（防雪崩）
				h.cfg.Pool.Cooldown(acct.UID, pool.CoolSoft, h.cfg.SoftCooldown, "upstream 404")
				lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody)}
				continue
			default:
				h.cfg.Pool.NoteError(acct.UID, h.cfg.ErrThreshold, h.cfg.ErrCooldown)
				lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody)}
				continue
			}
		}
		if peek.Stream {
			if useRemote {
				h.cfg.Pool.NoteSuccess(acct.UID)
				_ = upstream.CloudAgentStreamToOpenAI(w, rc)
				return
			}
			// 老通道流式：先聚合（缓冲）识别 4008，若配额耗尽则降级 remote 重试；
			// 否则把缓冲的 SOLO SSE 转成 OpenAI SSE。
			agg, aggErr := upstream.AggregateRaw(rc)
			if aggErr != nil {
				var se *upstream.SOLOStreamError
				if errors.As(aggErr, &se) && se.Code == 4008 {
					// 老通道配额耗尽 → 降级 remote（Work 积分）
					useRemote = true
					tried[acct.UID] = false
					continue
				}
				h.handleStreamError(acct.UID, se)
				// 输出错误
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "event: error\ndata: \"%s\"\n\n", aggErr.Error())
				w.Write([]byte("data: [DONE]\n\n"))
				return
			}
			h.cfg.Pool.NoteSuccess(acct.UID)
			_ = upstream.Stream(w, bytes.NewReader(agg))
			return
		}
		resp, err := upstream.Aggregate(rc)
		rc.Close() // 已完全消费，立即释放上游连接（防轮转 continue 泄漏 body）
		if err != nil {
			// 流内业务错误（如 1005 plan 权益不足）→ 冷却账号并轮转下一账号。
			var se *upstream.SOLOStreamError
			if errors.As(err, &se) {
				// 老通道 4008 配额耗尽 → 降级 remote 通道（Work 积分）
				if se.Code == 4008 && !useRemote {
					useRemote = true
					tried[acct.UID] = false
					continue
				}
				lastErr = err
				switch se.Kind() {
				case upstream.ErrPlanLimit:
					h.cfg.Pool.Cooldown(acct.UID, pool.CoolPlan, h.cfg.PlanCooldown, "plan 权益不足")
				default:
					h.cfg.Pool.NoteError(acct.UID, h.cfg.ErrThreshold, h.cfg.ErrCooldown)
				}
				continue
			}
			writeOpenAIError(w, http.StatusBadGateway, "upstream_parse", err.Error())
			return
		}
		h.cfg.Pool.NoteSuccess(acct.UID)
		writeJSON(w, http.StatusOK, resp)
		return
	}
	msg := "all accounts unavailable (cooling/disabled)"
	if lastErr != nil {
		msg += ": " + lastErr.Error()
	}
	writeOpenAIError(w, http.StatusServiceUnavailable, "no_healthy_account", msg)
}

// handleStreamError 流式响应中的上游业务错误 → pool 冷却状态机。
// 1005 plan 权益不足 → 长冷却；其余（5xx/参数错误等）→ 累计错误冷却。
func (h *Handler) handleStreamError(uid string, se *upstream.SOLOStreamError) {
	switch se.Kind() {
	case upstream.ErrPlanLimit:
		h.cfg.Pool.Cooldown(uid, pool.CoolPlan, h.cfg.PlanCooldown, "plan 权益不足")
	default:
		h.cfg.Pool.NoteError(uid, h.cfg.ErrThreshold, h.cfg.ErrCooldown)
	}
}

// ---------------------------------------------------------------------------
// cloud_agent 模型路由
// ---------------------------------------------------------------------------

// cloudAgentModels 需要走 create_agent_task（cloud_agent 通道，Work 专属积分）
// 的模型集合。当前已知 glm-5.3（模型表里 marked cloud_agent）；后续可扩展。
// 不在集合内的模型（DeepSeek 等）继续走 llm_utils_chat 老通道。
var cloudAgentModels = map[string]bool{
	"glm-5.3": true,
}

// isCloudAgentModel 判断 model（已 map 后的 config_name）是否走 cloud_agent 通道。
func isCloudAgentModel(model string) bool {
	return cloudAgentModels[model]
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    "api_error",
			"code":    code,
		},
	})
}

// bodyUnmarshal 解析请求体为 map（失败返回空 map）
func bodyUnmarshal(b []byte) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

// remotePrompt 将 OpenAI messages 转成 remote 任务的单段文本，保留角色、历史与文本内容。
func remotePrompt(body []byte) string {
	msgs, _ := bodyUnmarshal(body)["messages"].([]any)
	parts := make([]string, 0, len(msgs))
	for _, raw := range msgs {
		msg, _ := raw.(map[string]any)
		role, _ := msg["role"].(string)
		text := messageText(msg["content"])
		if text != "" {
			parts = append(parts, role+":\n"+text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func messageText(content any) string {
	if text, ok := content.(string); ok {
		return text
	}
	parts, _ := content.([]any)
	var texts []string
	for _, raw := range parts {
		part, _ := raw.(map[string]any)
		if part["type"] == "text" {
			if text, ok := part["text"].(string); ok {
				texts = append(texts, text)
			}
		}
	}
	return strings.Join(texts, "")
}

// writeRemoteReply 把 remote 通道的回复文本写成 OpenAI 格式响应体。
// 流式的响应头已由 startRemoteStream 提前发出（keepalive 需要），此处只写数据帧。
func (h *Handler) writeRemoteReply(w http.ResponseWriter, stream bool, model, text string) {
	created := time.Now().Unix()
	if stream {
		// 流式：一次输出全文 + 结束帧
		id := fmt.Sprintf("chatcmpl-%d", created)
		chunk := map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]any{{
				"index":         0,
				"delta":         map[string]any{"role": "assistant", "content": text},
				"finish_reason": nil,
			}},
		}
		raw, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", raw)
		// 结束 chunk
		done := map[string]any{
			"id": id, "object": "chat.completion.chunk", "created": created, "model": model,
			"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
		}
		raw, _ = json.Marshal(done)
		fmt.Fprintf(w, "data: %s\n\n", raw)
		w.Write([]byte("data: [DONE]\n\n"))
		sseFlush(w)
		return
	}
	// 非流式
	w.Header().Set("Content-Type", "application/json")
	id := fmt.Sprintf("chatcmpl-%d", created)
	resp := map[string]any{
		"id": id, "object": "chat.completion", "created": created, "model": model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": text},
			"finish_reason": "stop",
		}},
		"usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
	}
	json.NewEncoder(w).Encode(resp)
}

// ---------------------------------------------------------------------------
// remote 通道完整往返
// ---------------------------------------------------------------------------

// remoteWaitTotal 单次远程任务总等待上限。上游对重任务（长文/复杂编排）排队+执行
// 实测可达 13 分钟，120s 会让所有非平凡任务失败。
var remoteWaitTotal = 15 * time.Minute

// remoteKeepaliveInterval 流式等待远程任务期间的 SSE 心跳间隔（var 便于测试调短）。
var remoteKeepaliveInterval = 15 * time.Second

// remoteBusyRetryWait 发消息遇 429 并发槽满时的重试间隔（var 便于测试调短）。
var remoteBusyRetryWait = 15 * time.Second

func sseFlush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// startRemoteStream 立刻发出 SSE 响应头，让客户端在长轮询期间保持连接。
func startRemoteStream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	sseFlush(w)
}

// serveRemote 执行 remote 通道完整往返（P1 起支持会话复用）：
// 解析请求 messages 前缀哈希链 → 命中注册表则把增量消息 append 进已绑定的
// 云端会话（粘性账号），未命中/会话已死则新建会话发全量历史并重绑。
// 会话不再用完即删：由 TTL 清扫器（StartConvSweeper）回收空闲会话。
// 复用路径出现任何硬错误都降级为「新建会话 + 全量历史」，不劣于 P1 之前。
//
// 返回 nil 表示响应已完整写出（成功，或流式内嵌错误帧）；返回错误表示未写任何响应
// 字节，由调用方决定轮转下一账号或返回错误。
func (h *Handler) serveRemote(w http.ResponseWriter, r *http.Request, a *auth.Auth, model string, body []byte, stream bool, maxMode bool) error {
	// P1 兼容：未启用会话注册表（DisableConvReuse 或字面量构造的 Handler）
	// 时保持旧行为——每次新建、用完即删。
	if h.convs == nil {
		return h.serveRemoteLegacy(w, r, a, model, body, stream, maxMode)
	}

	msgs := bodyMessages(body)
	hasAssistant := false
	for _, m := range msgs {
		if role, _ := m["role"].(string); role == "assistant" {
			hasAssistant = true
			break
		}
	}
	chain := convPrefixChain(model, maxMode, msgs)
	var terminalKey string
	if n := len(chain); n > 0 {
		terminalKey = chain[n-1]
	}

	// ---- 会话复用解析（P1）----
	// 命中注册表 → 粘性回绑定账号，只 append 增量；绑定账号不健康或客户端
	// 改写了 assistant 历史（回显校验失败）→ 视为死会话，全量重建。
	var (
		reuseID    string
		appendText string
	)
	if len(msgs) > 0 {
		if e, incFrom := h.convs.Lookup(model, maxMode, msgs); e != nil {
			bound := h.cfg.Pool.AuthByUID(e.UID)
			st, _ := h.cfg.Pool.Status(e.UID)
			if bound != nil && !st.Disabled && !st.Cooling {
				if inc, ok := trimCloudEcho(msgs[incFrom:], e.LastReplyTail); ok {
					a = bound // 粘性：永远回创建它的账号
					reuseID = e.CloudSessionID
					appendText = formatIncrement(inc)
				}
			}
		}
	}

	// per-conversation 串行化：同一云端会话（或同一前缀终值的新建请求）排队，
	// 杜绝对同一云端会话并发 append。
	if key := reuseID; key == "" {
		key = terminalKey
		if key != "" {
			convMu := h.convs.LockFor(key)
			convMu.Lock()
			defer convMu.Unlock()
		}
	} else {
		convMu := h.convs.LockFor(key)
		convMu.Lock()
		defer convMu.Unlock()
	}

	// ---- 发送：复用增量 / 新建全量 ----
	var sessID string
	if reuseID != "" {
		sessID = reuseID
		log.Printf("remote: reuse session=%s model=%s uid=%s append=%dB", sessID, model, a.UID, len(appendText))
		if appendText == "" && !hasAssistant {
			// 增量剥离后为空且请求无 assistant 回显 = 新会话首条命中旧注册项。
			// 空复用只会空等旧会话已结束的回复，且让新会话误连旧云端会话。
			// 视为无效复用，降级新建（关键修复：会话隔离）。
			log.Printf("remote: reuse rejected (new session, empty append) session=%s -> rebuild", sessID)
			sessID = ""
		} else if appendText != "" {
			serr := h.sendRemoteWithBusyRetry(r, a, sessID, model, appendText, maxMode)
			if serr != nil {
				if errors.Is(serr, context.Canceled) {
					return serr // 客户端已断开，不做降级新建
				}
				// 云端会话已不可用（4xx/5xx）：解绑并降级为新建全量历史。
				h.convs.Unbind(sessID)
				sessID = ""
			}
		}
	}
	if sessID == "" {
		var err error
		sessID, err = h.cfg.Upstream.RemoteCreateSession(a)
		if err != nil {
			return err
		}
		log.Printf("remote: create session=%s model=%s uid=%s max=%v", sessID, model, a.UID, maxMode)
		userText := remotePrompt(body)
		if serr := h.sendRemoteWithBusyRetry(r, a, sessID, model, userText, maxMode); serr != nil {
			// 建了会话但消息没发出去：删会话释放并发槽，再交外层轮转。
			_ = h.cfg.Upstream.RemoteDeleteSession(a, sessID)
			return serr
		}
		// 发送已被云端接受即注册绑定（轮询超时也不丢）：客户端携全量历史
		// 重试时能命中同一会话直接等结果，避免重建会话重复计费。
		h.bindConv(sessID, a.UID, terminalKey, len(msgs), "")
	}

	// P1：登记活动任务，本地 MCP 事件按此做会话归属（并发任务不串台）。
	if h.tasks != nil {
		h.tasks.register(sessID, a)
		defer func() {
			h.tasks.finish(sessID)
			h.tasks.reap(5 * time.Minute)
		}()
	}

	// 响应泵：流式事件订阅 + keepalive + 轮询终态裁决（二期逻辑原样）。
	// 成功路径顺带把回复尾迹写回会话注册表（bindConv 的 replyTail 参数）。
	return h.serveRemotePump(w, r, a, sessID, model, stream, func(reply string) {
		h.bindConv(sessID, a.UID, terminalKey, len(msgs), replyTail(reply))
	})
}

// serveRemotePump 二期的响应泵主体：流式首帧/MCP 事件订阅/云端事件流/
// keepalive/轮询终态裁决。onReply 非 nil 时在成功拿到最终回复后回调一次
//（P1 会话注册表刷新用）。
func (h *Handler) serveRemotePump(w http.ResponseWriter, r *http.Request, a *auth.Auth, sessID, model string, stream bool, onReply func(string)) error {

	// 流式：先发头并周期发 SSE 注释心跳，防止客户端在分钟级轮询期间因空闲断连。
	var mu sync.Mutex
	var taskID string
	var taskCreated int64
	stopEventPump := func() {} // 非 stream 时空操作；stream 时替换为真正的收泵逻辑
	if stream {
		startRemoteStream(w)
		// 首帧：role assistant（任意 OpenAI 客户端都以它开始累积 assistant 消息）。
		taskID = fmt.Sprintf("chatcmpl-%d", time.Now().Unix())
		taskCreated = time.Now().Unix()
		mu.Lock()
		writeChatChunk(w, taskID, taskCreated, model, "assistant", "", nil)
		sseFlush(w)
		mu.Unlock()

		// 订阅本地 MCP 工具调用事件总线，转成 OpenAI delta chunk 推流。
		// P1：带会话过滤订阅 —— 只收属于本 chat_session 的事件。
		// 事件进入过滤前先学习其 XFF（沙盒 pod IP，首条锁定）：这是云端
		// MCP 请求里唯一随任务变化的标识（无会话头，见 /tmp/mcp-headers.log），
		// 后续多任务重叠时按它无歧义区分 pod。
		// 事件进入过滤前先学习其 XFF（沙盒 pod IP，首条锁定）：这是
		// 云端 MCP 请求里唯一随任务变化的标识（无会话头，见
		// /tmp/mcp-headers.log），后续多任务重叠时按它无歧义区分 pod。
		var subID int64
		var subCh <-chan McpEvent
		if h.mcpEvents != nil {
			subID, subCh = h.mcpEvents.SubscribeFiltered(func(e McpEvent) bool {
				h.tasks.LearnXFF(sessID, mcpEventXFF(e))
				return h.tasks.belongsTo(e, sessID, e.RecvAt)
			})
		}
		// P0：并行起云端事件流（GET /events SSE），把思考过程与正文增量推给客户端。
		// 终态裁决仍归轮询，本流只做增量；失败时静默降级（channel 直接关闭）。
		evCtx, stopEventStream := context.WithCancel(r.Context())
		defer stopEventStream()
		var evCh <-chan upstream.RemoteEventDelta
		if h.cfg.Upstream != nil {
			evCh = h.cfg.Upstream.RemoteOpenEventStream(evCtx, a, sessID)
		}

		// dedup 本地 MCP 工具调用的双重呈现去重（P0 与 P2 交叉点）：
		// 同一个本地 MCP 调用会同时出现在两条路上——
		//   ① 本地 MCP server 的旁路事件（🔧/✅ 行，带完整参数与结果）
		//   ② 云端事件流的 plan_item.run_mcp 工具卡（🔧 行，参数可能不完整）
		// 任一路先到即输出，另一路在时间窗内匹配到同工具名则跳过，避免刷屏。
		dedup := newToolDedup(30 * time.Second)

		drainReq := make(chan struct{})
		var pumpWG sync.WaitGroup
		pumpWG.Add(1)
		go func() {
			defer pumpWG.Done()
			for {
				select {
				case e, ok := <-subCh:
					if !ok {
						return
					}
					// 去重（两路都可能先到，任一路输出后抑制另一路）：
					// 本地事件是权威来源（带完整参数与结果），但若云端工具卡
					// 已先输出同名工具，则本行跳过，避免同一调用刷两次。
					if e.Event == "tool_call_start" && dedup.seen(e.Tool) {
						continue
					}
					text := mcpEventDelta(e)
					if text == "" {
						continue
					}
					mu.Lock()
					writeChatChunk(w, taskID, taskCreated, model, "", text, nil)
					sseFlush(w)
					mu.Unlock()
				case d, ok := <-evCh:
					if !ok {
						// 事件流结束（done 帧 / 重连耗尽 / 拨号失败）：
						// 把 channel 置 nil 使其永久阻塞，pump 继续只处理 MCP 事件。
						evCh = nil
						continue
					}
					// 工具卡去重：本地 MCP 事件已先报过同名工具则跳过云端版本。
					if d.ToolLine != "" && dedup.seen(d.ToolLine) {
						continue
					}
					mu.Lock()
					writeRemoteDelta(w, taskID, taskCreated, model, d)
					sseFlush(w)
					mu.Unlock()
				case <-drainReq:
					// 收尾：Unsubscribe 已断流（不再有新 MCP 事件），先停云端
					// 事件流再排空两个 channel 的存量。保证任何在终止帧之前
					// 送达的事件都落在终帧之前，且没有丢失。
					stopEventStream()
					for {
						select {
						case e, ok := <-subCh:
							if !ok {
								subCh = nil
								break
							}
							text := mcpEventDelta(e)
							if text == "" {
								continue
							}
							mu.Lock()
							writeChatChunk(w, taskID, taskCreated, model, "", text, nil)
							sseFlush(w)
							mu.Unlock()
						case d, ok := <-evCh:
							if !ok {
								evCh = nil
								break
							}
							if d.ToolLine != "" && dedup.seen(d.ToolLine) {
								continue
							}
							mu.Lock()
							writeRemoteDelta(w, taskID, taskCreated, model, d)
							sseFlush(w)
							mu.Unlock()
						}
						if subCh == nil && evCh == nil {
							return
						}
					}
				}
			}
		}()
		// 收泵：注销订阅（断流+关闭事件通道）→ 通知泵写完存量并退出 → 等待。
		// sync.Once 保证成功路径显式调用 + defer 兜底（ctx 取消等）不会双重收泵。
		var pumpOnce sync.Once
		stopEventPump = func() {
			pumpOnce.Do(func() {
				h.mcpEvents.Unsubscribe(subID)
				close(drainReq)
				pumpWG.Wait()
			})
		}
		defer stopEventPump()

		done := make(chan struct{})
		// 在启动 goroutine 前先读全局间隔：remoteKeepaliveInterval 是 var
		//（测试会临时改短），放到 goroutine 里读会与改写的测试构成数据竞争。
		keepalive := remoteKeepaliveInterval
		go func() {
			t := time.NewTicker(keepalive)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					mu.Lock()
					io.WriteString(w, ": keepalive\n\n")
					sseFlush(w)
					mu.Unlock()
				}
			}
		}()
		defer close(done)
	}

	replyText, werr := h.cfg.Upstream.RemoteWaitAndRead(r.Context(), a, sessID, remoteWaitTotal)
	if werr != nil {
		if errors.Is(werr, context.Canceled) {
			return werr // 客户端已断开，写响应无意义
		}
		if !stream {
			return werr
		}
		stopEventPump() // 先停事件泵，迟到工具事件不得出现在错误帧之后
		mu.Lock()
		errPayload, _ := json.Marshal(map[string]any{
			"error": map[string]any{"message": werr.Error(), "type": "api_error", "code": "remote_task_failed"},
		})
		fmt.Fprintf(w, "data: %s\n\n", errPayload)
		w.Write([]byte("data: [DONE]\n\n"))
		sseFlush(w)
		mu.Unlock()
		return nil
	}
	// 成功拿到回复：回调（P1 注册表刷新），再写响应。
	if onReply != nil {
		onReply(replyText)
	}
	if stream {
		// 终态裁决：先停事件泵（确保没有迟到工具事件穿插在终帧之后），
		// 再推分隔线 → 最终全文（单个 delta）→ finish_reason:stop → [DONE]。
		// 分隔线走 delta.content 而非裸 SSE 行：裸行会被客户端 SSE 解析器
		// 当注释丢弃，走 delta 才能在客户端累积文本里呈现为 markdown 分隔线。
		// 非流式路径的行为保持一字不差。
		stopEventPump()
		mu.Lock()
		writeChatChunk(w, taskID, taskCreated, model, "", "\n\n---\n\n", nil)
		writeChatChunk(w, taskID, taskCreated, model, "", replyText, nil)
		writeChatChunk(w, taskID, taskCreated, model, "", "", "stop")
		fmt.Fprint(w, "data: [DONE]\n\n")
		sseFlush(w)
		mu.Unlock()
		return nil
	}
	mu.Lock()
	h.writeRemoteReply(w, stream, model, replyText)
	mu.Unlock()
	return nil
}

// isQuotaExhausted 判断是否老通道配额耗尽（4008 quota exceeded）。
// 用于双通道降级：老通道 4008 → 切 remote 通道（Work 积分）。
func isQuotaExhausted(status int, respBody []byte) bool {
	body := string(respBody)
	if status >= 400 && strings.Contains(body, "4008") {
		return true
	}
	if strings.Contains(body, "quota") && strings.Contains(body, "exceeded") {
		return true
	}
	return false
}
