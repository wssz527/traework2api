// models.go 实现 ModelProvider：静态模型表 + 按账号动态发现（带缓存）。
//
// 静态表从 traework2api/internal/server/handler.go 的 staticModels 迁移
// （32 个 config_name），动态表走 get_detail_param（SOLO 上游）。
package main

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const modelCreatedFallback int64 = 1753600000

// staticModelIDs 静态 SOLO 模型表（32 个 config_name，来自逆向报告；
// 动态拉取失败时回退）。顺序与原实现一致。
var staticModelIDs = []string{
	"Doubao-Seed-2.1-Pro",
	"seed-code-pro-0430",
	"Doubao-Seed-2.1-Turbo",
	"Doubao-Seed-2.0-Code",
	"DeepSeek-V4-Flash-Official",
	"browser_use_subagent",
	"glm-5.2",
	"glm-5-turbo",
	"glm-5",
	"DeepSeek-V4-Pro",
	"DeepSeek-V4-Flash",
	"kimi-k3",
	"kimi-k2.7-code",
	"kimi-k2.6",
	"minimax-m3",
	"qwen-3.7-plus",
	"sagitta",
	"aquila",
	"custom_model_gemini",
	"custom_model_placeholder",
	"custom_model_1M_text",
	"custom_model_1M",
	"custom_model_kimi",
	"custom_model_claude",
	"custom_model_gpt-5",
	"custom_model_no-fc",
	"custom_model_deepseek_chat",
	"custom_model_deepseek_reasoner",
	"custom_model_deepseek_v4",
	"explore_sub_agent_v13",
	"explore_sub_agent_v2",
	"summary",
}

// deprecatedModelIDs 已弃用模型：动态列表中过滤掉，只保留 Official 正式版，
// 避免新旧混在一起。
var deprecatedModelIDs = map[string]bool{
	"DeepSeek-V4-Pro":   true,
	"DeepSeek-V4-Flash": true,
}

const defaultContextLength int64 = 131072

func traeStaticModels() []pluginapi.ModelInfo {
	out := make([]pluginapi.ModelInfo, 0, len(staticModelIDs))
	for _, id := range staticModelIDs {
		out = append(out, pluginapi.ModelInfo{
			ID:                         id,
			Object:                     "model",
			Created:                    modelCreatedFallback,
			OwnedBy:                    providerName,
			ContextLength:              defaultContextLength,
			SupportedGenerationMethods: []string{"chat"},
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// 动态模型表缓存（get_detail_param 上游源）。
//
// 模式参照 version.go：成功缓存 24h、失败负缓存 1h，后台刷新不阻塞请求链路。
// model.static / model.for_auth 会被 CPA 在每次 config reload 和每次 models
// 查询时重新调用——命中缓存直接返回，绝不扇出到每个账号一次上游调用。
// ---------------------------------------------------------------------------

const (
	dynamicModelsSuccessTTL = 24 * time.Hour
	dynamicModelsFailTTL    = 1 * time.Hour
	modelsFetchTO           = 20 * time.Second
)

type dynamicModelsEntry struct {
	models  []pluginapi.ModelInfo
	fetched time.Time
	ok      bool // false = 负缓存（上次拉取失败，占位用静态表）
}

// modelsAutoProbe 控制"无缓存时是否后台自动探测"。默认开启；单测关闭它以
// 获得确定性（后台 goroutine 会在测试断言之间写缓存）。
var modelsAutoProbe atomic.Bool

var (
	dynamicModelsCache atomic.Pointer[dynamicModelsEntry]
	modelsMu           sync.Mutex // 串行化后台刷新，避免并发重复请求上游
	dynamicModelsTrack atomic.Bool
)

func init() {
	dynamicModelsTrack.Store(true)
	modelsAutoProbe.Store(true)
}

func modelsTrackingEnabled() bool { return dynamicModelsTrack.Load() }

func setModelsTracking(v bool) { dynamicModelsTrack.Store(v) }

// cachedDynamicModels 返回有效缓存（命中即真）。负缓存未到期也视为"已尝试"
// 命中（返回 ok=false + 静态表），不再重复探测。
func cachedDynamicModels() ([]pluginapi.ModelInfo, bool) {
	e := dynamicModelsCache.Load()
	if e == nil {
		return nil, false
	}
	if e.ok && time.Since(e.fetched) < dynamicModelsSuccessTTL {
		return e.models, true
	}
	if !e.ok && time.Since(e.fetched) < dynamicModelsFailTTL {
		return nil, true
	}
	return nil, false
}

func storeDynamicModels(models []pluginapi.ModelInfo) {
	dynamicModelsCache.Store(&dynamicModelsEntry{
		models:  models,
		fetched: time.Now(),
		ok:      true,
	})
}

// storeDynamicModelsFail 记录一次失败（负缓存）。
func storeDynamicModelsFail() {
	dynamicModelsCache.Store(&dynamicModelsEntry{
		fetched: time.Now(),
		ok:      false,
	})
}

// fetchDynamicModels 从凭证拉模型表；优先返回动态缓存，内存态回退静态表。
// 负缓存命中（近期探测失败）时必须回退静态表——返回 nil 会让宿主把该账号的
// 模型全部注销 1h（empty list → UnregisterClient），所有 Trae 模型 400。
func fetchDynamicModels(sa *traeAuth) []pluginapi.ModelInfo {
	if models, ok := cachedDynamicModels(); ok {
		if len(models) > 0 {
			return models
		}
		return traeStaticModels()
	}
	if !modelsTrackingEnabled() {
		return traeStaticModels()
	}
	if modelsAutoProbe.Load() {
		go refreshDynamicModels(sa)
	}
	return traeStaticModels()
}

// refreshDynamicModels 后台探测并更新缓存；任何失败都静默回退（负缓存）。
// 与 version.go 的 refreshUpstreamVersion 同样：无有效缓存才拉，串行化并发。
func refreshDynamicModels(sa *traeAuth) {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in model refresh: %v", r)
		}
	}()
	if !modelsTrackingEnabled() || sa == nil {
		return
	}
	if _, ok := cachedDynamicModels(); ok {
		return
	}
	if !modelsMu.TryLock() {
		return
	}
	defer modelsMu.Unlock()
	infos, err := currentClient().FetchModels(sa)
	if err != nil || len(infos) == 0 {
		pluginLogf("models fetch failed (negative cache): %v", err)
		storeDynamicModelsFail()
		return
	}
	out := modelsFromUpstream(infos)
	if len(out) == 0 {
		pluginLogf("models list empty after filter — keeping static")
		storeDynamicModelsFail()
		return
	}
	// 动态表为准，但动态表缺了静态表里的某个模型时记一条 warn（人还在用的
	// 内部名不会凭空消失——已在用的人按动态表裁决，记录以便排查）。
	have := make(map[string]bool, len(out))
	for _, m := range out {
		have[m.ID] = true
	}
	for _, id := range staticModelIDs {
		if !have[id] && !deprecatedModelIDs[id] {
			pluginLogf("WARN: model %q present in static table but absent from upstream model list — dynamic table wins", id)
		}
	}
	storeDynamicModels(out)
	pluginLogf("models synced from upstream: %d entries", len(out))
}

// modelAliasCache 反向别名表：宿主应用 oauth-model-alias 后，客户端可能用
// 别名请求；上游只认真实 ID，必须反解。
var modelAliasCache struct {
	sync.RWMutex
	byAlias map[string]string
}

func cacheModelAliases(host pluginapi.HostConfigSummary) {
	entries := host.OAuthModelAlias[providerName]
	if len(entries) == 0 {
		for channel, list := range host.OAuthModelAlias {
			if strings.EqualFold(strings.TrimSpace(channel), providerName) {
				entries = list
				break
			}
		}
	}
	byAlias := make(map[string]string, len(entries))
	for _, e := range entries {
		name := strings.TrimSpace(e.Name)
		alias := strings.TrimSpace(e.Alias)
		if name == "" || alias == "" || strings.EqualFold(name, alias) {
			continue
		}
		byAlias[strings.ToLower(alias)] = name
	}
	modelAliasCache.Lock()
	modelAliasCache.byAlias = byAlias
	modelAliasCache.Unlock()
}

// resolveUpstreamModel 把客户端请求的模型（可能带 __dev/__max 后缀、下划线
// 命名或别名）映射为上游 config_name。
func resolveUpstreamModel(model string, attributes map[string]string) string {
	m := strings.TrimSpace(model)
	if m == "" {
		return DefaultConfigName
	}
	// 去掉内部名后缀（__dev / __max）
	base := m
	if i := strings.Index(m, "__"); i >= 0 {
		base = m[:i]
	}
	// 别名（per-auth 属性优先，其次全局表）
	key := strings.ToLower(base)
	if name, ok := parseModelAliasAttribute(attributes)[key]; ok {
		return name
	}
	modelAliasCache.RLock()
	name, ok := modelAliasCache.byAlias[key]
	modelAliasCache.RUnlock()
	if ok {
		return name
	}
	// 宽松匹配：下划线 → 横线，大小写不敏感（deepseek_v4_pro → DeepSeek-V4-Pro）
	if norm := normalizeModelName(base); norm != base && knownStaticModel(norm) {
		return norm
	}
	if knownStaticModel(base) {
		return base
	}
	return base
}

// knownStaticModel 判断 id 是否在静态表中。
func knownStaticModel(id string) bool {
	for _, s := range staticModelIDs {
		if s == id {
			return true
		}
	}
	return false
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

// parseModelAliasAttribute 解析 per-auth 别名覆盖（JSON 或 "alias=name" 对）。
func parseModelAliasAttribute(attributes map[string]string) map[string]string {
	if len(attributes) == 0 {
		return nil
	}
	raw := ""
	for _, k := range []string{"model_alias", "model-alias", "oauth-model-alias"} {
		if v := strings.TrimSpace(attributes[k]); v != "" {
			raw = v
			break
		}
	}
	if raw == "" {
		return nil
	}
	out := make(map[string]string)
	add := func(name, alias string) {
		name, alias = strings.TrimSpace(name), strings.TrimSpace(alias)
		if name != "" && alias != "" && !strings.EqualFold(name, alias) {
			out[strings.ToLower(alias)] = name
		}
	}
	if strings.HasPrefix(raw, "[") {
		var list []struct {
			Name  string `json:"name"`
			Alias string `json:"alias"`
		}
		if json.Unmarshal([]byte(raw), &list) == nil {
			for _, e := range list {
				add(e.Name, e.Alias)
			}
			return out
		}
	}
	if strings.HasPrefix(raw, "{") {
		var m map[string]string
		if json.Unmarshal([]byte(raw), &m) == nil {
			for alias, name := range m {
				add(name, alias)
			}
			return out
		}
	}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			add(kv[1], kv[0])
		}
	}
	return out
}

// filterExcludedModels 移除 oauth-excluded-models 中配置的模型。
func filterExcludedModels(models []pluginapi.ModelInfo, host pluginapi.HostConfigSummary) []pluginapi.ModelInfo {
	if len(host.ExcludedModels) == 0 {
		return models
	}
	excluded := host.ExcludedModels[providerName]
	if len(excluded) == 0 {
		for channel, list := range host.ExcludedModels {
			if strings.EqualFold(strings.TrimSpace(channel), providerName) {
				excluded = list
				break
			}
		}
	}
	if len(excluded) == 0 {
		return models
	}
	excludeSet := make(map[string]struct{}, len(excluded))
	for _, m := range excluded {
		excludeSet[strings.ToLower(strings.TrimSpace(m))] = struct{}{}
	}
	// 新切片——models[:0] 会别名到 dynamicModelsCache 的底层数组，
	// 就地过滤会污染缓存。
	out := make([]pluginapi.ModelInfo, 0, len(models))
	for _, m := range models {
		if _, skip := excludeSet[strings.ToLower(m.ID)]; skip {
			continue
		}
		out = append(out, m)
	}
	return out
}

// modelsFromUpstream 把上游 ModelInfo 转成宿主 ModelInfo（含 -max 变体）。
func modelsFromUpstream(infos []ModelInfo) []pluginapi.ModelInfo {
	out := make([]pluginapi.ModelInfo, 0, len(infos))
	for _, mi := range infos {
		if mi.ID == "" || deprecatedModelIDs[mi.ID] {
			continue
		}
		out = append(out, modelEntry(mi.ID, mi.Name, mi.ContextWindow, mi.InputTokens, mi.MaxTokens, mi.MaxMode, mi.Multimodal, mi.ReasoningEfforts))
		if mi.MaxMode {
			out = append(out, modelEntry(mi.ID+"-max", mi.Name+" Max", mi.MaxContextWindow, mi.MaxModeInputTokens, mi.MaxModeOutputTokens, true, mi.Multimodal, mi.ReasoningEfforts))
		}
	}
	return out
}

func modelEntry(id, name string, ctx, input, output int64, maxMode, multimodal bool, efforts []string) pluginapi.ModelInfo {
	m := pluginapi.ModelInfo{
		ID:                         id,
		Name:                       name,
		Object:                     "model",
		Created:                    modelCreatedFallback,
		OwnedBy:                    providerName,
		ContextLength:              ctx,
		InputTokenLimit:            input,
		OutputTokenLimit:           output,
		SupportedGenerationMethods: []string{"chat"},
		SupportedInputModalities:   []string{"text"},
		SupportedOutputModalities:  []string{"text"},
	}
	if m.ContextLength <= 0 {
		m.ContextLength = defaultContextLength
	}
	if multimodal {
		m.SupportedInputModalities = []string{"text", "image"}
	}
	if len(efforts) > 0 {
		m.Thinking = &pluginapi.ThinkingSupport{DynamicAllowed: true, Levels: efforts}
	}
	_ = maxMode
	return m
}

func handleModelStatic(raw []byte) ([]byte, error) {
	var req pluginapi.StaticModelRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cacheModelAliases(req.Host)
	models := filterExcludedModels(traeStaticModels(), req.Host)
	return okEnvelope(pluginapi.ModelResponse{Provider: providerName, Models: models})
}

func handleModelForAuth(raw []byte) ([]byte, error) {
	var req pluginapi.AuthModelRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cacheModelAliases(req.Host)
	var models []pluginapi.ModelInfo
	sa, err := parseStored(req.StorageJSON)
	if err != nil {
		models = traeStaticModels()
	} else {
		models = fetchDynamicModels(sa)
	}
	models = filterExcludedModels(models, req.Host)
	return okEnvelope(pluginapi.ModelResponse{Provider: providerName, Models: models})
}
