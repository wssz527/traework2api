// Package main implements the trae CLIProxyAPI dynamic plugin.
//
// trae wraps TRAE SOLO (trae-api-cn.mchost.guru) as a cliproxy provider: it
// parses trae-*.json credentials, refreshes Cloud-IDE-JWT access tokens,
// forwards OpenAI-compatible chat completions to the upstream SOLO SSE
// endpoint, and exposes daily check-in / credits management endpoints.
//
// Logic ported from traework2api (~/traework2api). Built with
// -buildmode=c-shared and exports the cliproxy C ABI entry points.
package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

static int trae_call_host(cliproxy_host_api* api, const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	return api->call(api->host_ctx, method, request, request_len, response);
}
static void trae_free_host_buffer(cliproxy_host_api* api, void* ptr, size_t len) {
	api->free_buffer(ptr, len);
}

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	providerName = "trae"
	authFileName = "trae.json"
	// pluginLogoURL 面板 logo（与 workbuddy 同图源）。
	pluginLogoURL = "https://raw.githubusercontent.com/DGZSbot/ai-icon/refs/heads/main/Trae.png"
)

// version is injected at build time via -ldflags "-X main.version=...".
var version = "0.2.1"

var hostAPI *C.cliproxy_host_api

func main() {}

// -----------------------------------------------------------------------------
// C ABI exports
// -----------------------------------------------------------------------------

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	hostAPI = host
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethodGuarded(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, len C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	// Intentionally a no-op. The host calls this on its own exit path (after
	// the host Go runtime has started tearing down) and dlclose()es this
	// library immediately afterwards. Touching any Go runtime state here —
	// mutexes, channel close, goroutine synchronization — risks a SIGSEGV in
	// cgo (observed in the workbuddy plugin on every docker restart).
	// The scheduler goroutine holds no resources that outlive the process.
}

// handleMethodGuarded 是所有 RPC 入口的统一 recover 包装。
// c-shared 插件没有进程隔离：任何 panic 都会拖垮整个 CPA 宿主进程
// （8317 上所有 provider 一起中断）。这里把 panic 转成 error envelope。
func handleMethodGuarded(method string, request []byte) (raw []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in %s: %v", method, r)
			raw, err = errorEnvelope("plugin_panic", fmt.Sprintf("%s panicked: %v", method, r)), nil
		}
	}()
	return handleMethod(method, request)
}

// -----------------------------------------------------------------------------
// Host calls
// -----------------------------------------------------------------------------

// hostCall invokes a host RPC method via the function-pointer table captured
// at init (host.auth.list / get / save, host.stream.emit / close, host.log).
func hostCall(method string, request []byte) ([]byte, error) {
	if hostAPI == nil || hostAPI.call == nil {
		return nil, fmt.Errorf("host API unavailable")
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var cReq unsafe.Pointer
	var reqLen C.size_t
	if len(request) > 0 {
		cReq = C.CBytes(request)
		defer C.free(cReq)
		reqLen = C.size_t(len(request))
	}
	var resp C.cliproxy_buffer
	rc := C.trae_call_host(hostAPI, cMethod, (*C.uint8_t)(cReq), reqLen, &resp)
	var out []byte
	if resp.ptr != nil && resp.len > 0 {
		out = C.GoBytes(resp.ptr, C.int(resp.len))
	}
	if resp.ptr != nil && hostAPI.free_buffer != nil {
		C.trae_free_host_buffer(hostAPI, resp.ptr, resp.len)
	}
	if rc != 0 {
		return out, fmt.Errorf("host call %s returned %d", method, int(rc))
	}
	return out, nil
}

// pluginLogf 通过 host.log 写宿主日志；宿主不可用时静默丢弃。
// 绝不打印 token —— 所有调用方传入的文本必须先 redactSecrets。
func pluginLogf(format string, args ...any) {
	if hostAPI == nil || hostAPI.call == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	body, _ := json.Marshal(map[string]any{"level": "info", "message": msg})
	_, _ = hostCall(pluginabi.MethodHostLog, body)
}

// -----------------------------------------------------------------------------

// traeRegistrationOrFallback 返回注册响应。自检失败时绝不发出会被宿主拒收的
// 空字段（宿主会静默丢弃整个插件），而是在插件侧记日志并降级处理。
func traeRegistrationOrFallback() registration {
	reg := traeRegistration()
	if !validPluginMetadata(reg.Metadata) {
		pluginLogf("registration metadata invalid (host would reject) — name=%q version=%q author=%q repo=%q",
			reg.Metadata.Name, reg.Metadata.Version, reg.Metadata.Author, reg.Metadata.GitHubRepository)
	}
	return reg
}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		configure(request)
		return okEnvelope(traeRegistrationOrFallback())
	case pluginabi.MethodModelStatic:
		return handleModelStatic(request)
	case pluginabi.MethodModelForAuth:
		return handleModelForAuth(request)
	case pluginabi.MethodAuthIdentifier:
		return okEnvelope(identifierResponse{Identifier: providerName})
	case pluginabi.MethodAuthParse:
		return handleParseAuth(request)
	case pluginabi.MethodAuthLoginStart:
		return handleStartLogin(request)
	case pluginabi.MethodAuthLoginPoll:
		return handlePollLogin(request)
	case pluginabi.MethodAuthRefresh:
		return handleRefreshAuth(request)
	case pluginabi.MethodExecutorIdentifier:
		return okEnvelope(identifierResponse{Identifier: providerName})
	case pluginabi.MethodExecutorExecute:
		return handleExecExecute(request)
	case pluginabi.MethodExecutorExecuteStream:
		return handleExecStream(request)
	case pluginabi.MethodExecutorCountTokens:
		// 上游无 count_tokens API；返回零值估计让客户端跳过/回退。
		return okEnvelope(pluginapi.ExecutorResponse{Payload: []byte(`{"input_tokens":0}`)})
	case pluginabi.MethodManagementRegister:
		var regReq pluginapi.ManagementRegistrationRequest
		if err := json.Unmarshal(request, &regReq); err == nil {
			if regReq.BasePath != "" {
				setManagementBasePath(regReq.BasePath)
			}
		}
		return okEnvelope(managementRegistration())
	case pluginabi.MethodManagementHandle:
		return handleManagement(request)
	case pluginabi.MethodSchedulerPick:
		return handleSchedulerPick(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type identifierResponse struct {
	Identifier string `json:"identifier"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type streamResponse struct {
	Headers http.Header                     `json:"headers,omitempty"`
	Chunks  []pluginapi.ExecutorStreamChunk `json:"chunks,omitempty"`
}

type registrationCapability struct {
	ModelProvider         bool                         `json:"model_provider"`
	AuthProvider          bool                         `json:"auth_provider"`
	FrontendAuthProvider  bool                         `json:"frontend_auth_provider"`
	Executor              bool                         `json:"executor"`
	ExecutorModelScope    pluginapi.ExecutorModelScope `json:"executor_model_scope"`
	ExecutorInputFormats  []string                     `json:"executor_input_formats,omitempty"`
	ExecutorOutputFormats []string                     `json:"executor_output_formats,omitempty"`
	Scheduler             bool                         `json:"scheduler"`
	ManagementAPI         bool                         `json:"management_api"`
	UsagePlugin           bool                         `json:"usage_plugin"`
}

// validPluginMetadata 复刻宿主 internal/pluginhost/host.go:validPlugin 的校验：
// Metadata 的 Name / Version / Author / GitHubRepository 四个字段都不能为空。
// 宿主在 plugin.register 返回后立刻用这个规则过滤，任一为空就打
// "pluginhost: plugin %s returned invalid metadata or no capabilities" 并拒绝注册。
// 这里自检一遍，保证永远不把一个会被宿主拒收的 registration 发出去。
func validPluginMetadata(m pluginapi.Metadata) bool {
	return strings.TrimSpace(m.Name) != "" &&
		strings.TrimSpace(m.Version) != "" &&
		strings.TrimSpace(m.Author) != "" &&
		strings.TrimSpace(m.GitHubRepository) != ""
}

func traeRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:    providerName,
			Version: version,
			Author:  "traework2api migration",
			// GitHubRepository 必须非空：宿主 validPlugin() 要求
			// Name/Version/Author/GitHubRepository 四个字段都非空，
			// 否则打 "invalid metadata or no capabilities" 并拒绝注册。
			GitHubRepository: "https://github.com/router-for-me/CLIProxyAPI",
			Logo:             pluginLogoURL,
			ConfigFields: []pluginapi.ConfigField{
				{Name: "checkin_auto", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Enable daily auto check-in (default true)."},
				{Name: "checkin_hour", Type: pluginapi.ConfigFieldTypeInteger, Description: "Local hour (0-23) for daily auto check-in (default 9, matching traework2api)."},
				{Name: "refresh_hours", Type: pluginapi.ConfigFieldTypeArray, Description: "Local hours for proactive token refresh (default [3], matching traework2api)."},
				{Name: "refresh_skew", Type: pluginapi.ConfigFieldTypeString, Description: "Pre-refresh window for access tokens, Go duration (default 24h)."},
				{Name: "lifecycle_auto", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Auto disable auths on session death / plan-limit cooldown (default true)."},
				{Name: "scheduler_mode", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{schedulerModeOff, schedulerModeCredits}, Description: "off (defer to CPA built-in, default) or credits (pick highest remaining credits)."},
				{Name: "version_track", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Auto-track upstream TRAE client version (default true; never downgrades below the built-in constant)."},
				{Name: "version_track_interval", Type: pluginapi.ConfigFieldTypeString, Description: "How often to re-probe the version, Go duration (default 24h)."},
				{Name: "models", Type: pluginapi.ConfigFieldTypeArray, Description: "Optional model list override."},
			},
		},
		Capabilities: registrationCapability{
			ModelProvider:         true,
			AuthProvider:          true,
			FrontendAuthProvider:  false,
			Executor:              true,
			ExecutorModelScope:    pluginapi.ExecutorModelScopeOAuth,
			ExecutorInputFormats:  []string{"chat-completions"},
			ExecutorOutputFormats: []string{"chat-completions"},
			ManagementAPI:         true,
			Scheduler:             true,
			UsagePlugin:           false,
		},
	}
}

// -----------------------------------------------------------------------------
// Auth handlers
// -----------------------------------------------------------------------------

// handleParseAuth claims trae-*.json credential files.
//
// 迁移报告 §3.3 的硬约束：AuthData.ID 必须留空，让宿主用 authIDForPath(path)
// 计算。若设成 ID=uid 而宿主 watcher 初次注册用 ID=filename，upsertAuthRecord
// 找不到已有记录 → 新建一条 → 同一文件出现重复 auth 条目。同时要回显 FileName。
func handleParseAuth(raw []byte) ([]byte, error) {
	var req pluginapi.AuthParseRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	// 归属判定：宿主按文件顶层 "type" 路由。type 显式声明且不是 trae → 不认领；
	// type 为空 → 回退到"宿主已路由给我"或"文件名前缀匹配 trae-"。
	var probeType struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(req.RawJSON, &probeType)
	declared := strings.ToLower(strings.TrimSpace(probeType.Type))
	if declared != "" && declared != providerName {
		return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
	}
	if declared == "" {
		routed := strings.EqualFold(strings.TrimSpace(req.Provider), providerName)
		prefixed := strings.HasPrefix(strings.ToLower(strings.TrimSpace(req.FileName)), providerName+"-")
		if !routed && !prefixed {
			return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
		}
	}
	sa, err := parseStored(req.RawJSON)
	if err != nil {
		return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
	}
	ad := toAuthData(sa)
	ad.ID = "" // 让宿主按 path 计算，防止重复 auth 条目
	if fn := strings.TrimSpace(req.FileName); fn != "" {
		ad.FileName = fn
	}
	return okEnvelope(pluginapi.AuthParseResponse{
		Handled: true,
		Auth:    ad,
	})
}

// handleRefreshAuth 通过 ExchangeToken 强制刷新 access token。
// 宿主在 token 临近过期时调用；成功时把新凭证回写（StorageJSON）。
func handleRefreshAuth(raw []byte) ([]byte, error) {
	var req pluginapi.AuthRefreshRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	sa, err := parseStored(req.StorageJSON)
	if err != nil {
		return nil, err
	}
	if err := currentClient().RefreshToken(sa); err != nil {
		return nil, err
	}
	storage, err := sa.marshalNested()
	if err != nil {
		return nil, err
	}
	// 宿主在 Refresh 返回后自己持久化凭证（conductor.go refreshAuth →
	// m.Update → persist）；插件再写一次会双写文件。ID/FileName 留空让宿主回填。
	ad := toAuthData(sa)
	ad.ID = ""
	ad.FileName = ""
	ad.StorageJSON = storage
	return okEnvelope(pluginapi.AuthRefreshResponse{Auth: ad})
}

// -----------------------------------------------------------------------------

func okEnvelope(v any) ([]byte, error) {
	result, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{OK: true, Result: result})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}
