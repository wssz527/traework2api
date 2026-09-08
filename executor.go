// executor.go 实现 CPA Executor：非流式聚合（executor.execute）与流式
// （executor.execute_stream）。
//
// 上游只有 SSE 一种形态，所以两条路径都是"发 stream=true 给上游"：
//   - 非流式：Aggregate 把 SSE 折叠成单个 chat.completion 对象
//   - 流式：collectOpenAISSEChunks 把 SOLO 事件转成 OpenAI SSE chunk
//
// 每个入口都 defer recover()：c-shared 无进程隔离，panic 会拖垮整个宿主。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// defaultRefreshSkew token 预刷新窗口（与原 traework2api 一致）。
const defaultRefreshSkew = 24 * time.Hour

// executorStreamRequest 宿主 executor.execute_stream 的入参：ExecutorRequest
// 加异步流 id。
type executorStreamRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

func handleExecExecute(raw []byte) (resp []byte, err error) {
	defer recoverTo(&err, "executor.execute")
	var req pluginapi.ExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	sa, err := parseStored(req.StorageJSON)
	if err != nil {
		return nil, err
	}
	upstreamModel := resolveUpstreamModel(req.Model, req.AuthAttributes)

	// token 临近过期 → 先 refresh（失败按分类冷却/禁用，不重试换号：
	// 换号由宿主 request-retry 负责）。
	refreshed, refreshErr := currentClient().RefreshTokenIfNeeded(sa, loadedRefreshSkew())
	if refreshErr != nil {
		reconcileAfterUpstreamError(req.AuthID, sa, statusOfError(refreshErr), refreshErr.Error())
		return nil, fmt.Errorf("token_refresh: %s", redactSecrets(refreshErr.Error()))
	}
	if refreshed {
		_ = persistAuth(sa, false)
	}

	body := requestPayload(req.Payload, req.OriginalRequest, upstreamModel)
	rc, status, respBody, terr := currentClient().ChatStream(sa, body)
	if terr != nil {
		noteAccountError(req.AuthID, sa, 0, terr.Error())
		return nil, fmt.Errorf("http_error: %s", truncateRedacted(terr.Error(), 200))
	}
	if status >= 400 {
		payload := string(respBody)
		reconcileAfterUpstreamError(req.AuthID, sa, status, payload)
		return nil, fmt.Errorf("upstream %d: %s", status, truncateRedacted(payload, 200))
	}
	defer rc.Close()

	completion, aggErr := Aggregate(rc)
	rc.Close()
	if aggErr != nil {
		// 流内业务错误（如 1005 plan 权益不足）→ 冷却账号，宿主换号重试。
		var se *SOLOStreamError
		if errors.As(aggErr, &se) {
			reconcileAfterStreamError(req.AuthID, sa, se)
			return nil, aggErr
		}
		noteAccountError(req.AuthID, sa, 0, aggErr.Error())
		return nil, aggErr
	}
	// 回填客户端请求的 model（上游返回的 model 字段为空）。
	if m, ok := completion["model"].(string); !ok || m == "" {
		completion["model"] = req.Model
	}
	out, err := json.Marshal(completion)
	if err != nil {
		return nil, err
	}
	noteAccountSuccess(req.AuthID, sa)
	return okEnvelope(pluginapi.ExecutorResponse{Payload: out})
}

func handleExecStream(raw []byte) (resp []byte, err error) {
	defer recoverTo(&err, "executor.execute_stream")
	var req executorStreamRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	sa, err := parseStored(req.StorageJSON)
	if err != nil {
		return nil, err
	}
	upstreamModel := resolveUpstreamModel(req.Model, req.AuthAttributes)

	refreshed, refreshErr := currentClient().RefreshTokenIfNeeded(sa, loadedRefreshSkew())
	if refreshErr != nil {
		reconcileAfterUpstreamError(req.AuthID, sa, statusOfError(refreshErr), refreshErr.Error())
		return nil, fmt.Errorf("token_refresh: %s", redactSecrets(refreshErr.Error()))
	}
	if refreshed {
		_ = persistAuth(sa, false)
	}

	body := requestPayload(req.Payload, req.OriginalRequest, upstreamModel)
	headers := streamHeaders()
	// 宿主 chat-completions 通道自己加 "data: " 前缀；跨格式转换器要求
	// payload 自带 SSE 帧。
	sseFramed := clientNeedsSSEFrame(req.Metadata)

	// 无异步流 id → 同步收集 chunk 后一次返回。
	if req.StreamID == "" {
		chunks, status, respBody, collErr := collectUpstreamChunks(sa, body, sseFramed)
		if collErr != nil {
			return nil, collErr
		}
		_ = status
		_ = respBody
		noteAccountSuccess(req.AuthID, sa)
		return okEnvelope(streamResponse{Headers: headers, Chunks: chunks})
	}

	// 异步：立即返回空 chunks，goroutine 泵上游并通过 host.stream.emit 下发。
	go pumpSoloStream(req, sa, body, sseFramed, upstreamModel)
	return okEnvelope(streamResponse{Headers: headers})
}

// pumpSoloStream 后台读取上游 SSE 并逐块 emit 给宿主。
// 自行 recover：goroutine 内 panic 同样会拖垮宿主。
func pumpSoloStream(req executorStreamRequest, sa *traeAuth, body []byte, sseFramed bool, upstreamModel string) {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in stream pump: %v", r)
		}
		streamClose(req.StreamID)
	}()

	rc, status, respBody, err := currentClient().ChatStream(sa, body)
	if err != nil {
		noteAccountError(req.AuthID, sa, 0, err.Error())
		streamEmitError(req.StreamID, "http_error: "+redactSecrets(err.Error()))
		return
	}
	defer rc.Close()
	if status >= 400 {
		payload := string(respBody)
		reconcileAfterUpstreamError(req.AuthID, sa, status, payload)
		streamEmitError(req.StreamID, fmt.Sprintf("upstream %d: %s", status, truncateRedacted(payload, 200)))
		return
	}
	chunks, streamErr := collectOpenAISSEChunks(rc, func(se *SOLOStreamError) {
		reconcileAfterStreamError(req.AuthID, sa, se)
	})
	rc.Close()
	for _, c := range chunks {
		payload := []byte(c)
		if !sseFramed {
			payload = []byte(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(c), "data: "), "\n\n"))
			payload = []byte(strings.TrimSpace(string(payload)))
		}
		if len(payload) == 0 {
			continue
		}
		if emitErr := streamEmit(req.StreamID, payload); emitErr != nil {
			// 客户端断开 → 停止读死上游。
			return
		}
	}
	if streamErr != nil {
		return
	}
	noteAccountSuccess(req.AuthID, sa)
}

// collectUpstreamChunks 同步收集上游 SSE → OpenAI chunk（无 stream id 路径）。
func collectUpstreamChunks(sa *traeAuth, body []byte, sseFramed bool) ([]pluginapi.ExecutorStreamChunk, int, []byte, error) {
	rc, status, respBody, err := currentClient().ChatStream(sa, body)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("http_error: %w", err)
	}
	defer rc.Close()
	if status >= 400 {
		return nil, status, respBody, fmt.Errorf("upstream %d: %s", status, truncateRedacted(string(respBody), 200))
	}
	frames, streamErr := collectOpenAISSEChunks(rc, nil)
	rc.Close()
	out := make([]pluginapi.ExecutorStreamChunk, 0, len(frames))
	for _, f := range frames {
		payload := strings.TrimSpace(f)
		if payload == "" {
			continue
		}
		if !sseFramed {
			payload = stripDataPrefix(payload)
			if payload == "[DONE]" {
				continue
			}
		}
		if payload == "" {
			continue
		}
		out = append(out, pluginapi.ExecutorStreamChunk{Payload: []byte(payload)})
	}
	if streamErr != nil {
		return out, status, nil, streamErr
	}
	return out, status, nil, nil
}

// requestPayload 取宿主给的 payload，缺失时回退原始请求体。
func requestPayload(payload, original []byte, upstreamModel string) []byte {
	body := payload
	if len(body) == 0 {
		body = original
	}
	return setModelInBody(body, upstreamModel)
}

// setModelInBody 把 body 的 model 字段替换为上游 config_name。
func setModelInBody(body []byte, configName string) []byte {
	if len(body) == 0 {
		return body
	}
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

func streamHeaders() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	return h
}

// clientNeedsSSEFrame 判断 chunk 是否必须自带 "data: " SSE 帧。
// CPA 的 chat-completions 直通会自己加前缀，但跨格式响应转换器只消费
// 已带 "data: " 的 payload。
func clientNeedsSSEFrame(metadata map[string]any) bool {
	path, _ := metadata["request_path"].(string)
	switch strings.ToLower(strings.TrimSpace(path)) {
	case "/v1/chat/completions", "/v1/completions":
		return false
	default:
		return true
	}
}

func stripDataPrefix(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "data:") {
		s = strings.TrimSpace(strings.TrimPrefix(s, "data:"))
	}
	return s
}

// statusOfError 从错误中提取 HTTP 状态码（无法提取返回 0）。
func statusOfError(err error) int {
	var ue *Error
	if errors.As(err, &ue) {
		return ue.Status
	}
	return 0
}

// -----------------------------------------------------------------------------
// Host stream RPC
// -----------------------------------------------------------------------------

func streamEmit(streamID string, payload []byte) error {
	if streamID == "" {
		return fmt.Errorf("no stream id")
	}
	body, _ := json.Marshal(map[string]any{"stream_id": streamID, "payload": payload})
	_, err := hostCall(pluginabi.MethodHostStreamEmit, body)
	return err
}

func streamEmitError(streamID, message string) {
	if streamID == "" {
		return
	}
	errJSON, _ := json.Marshal(map[string]any{"error": map[string]any{"message": redactSecrets(message)}})
	_ = streamEmit(streamID, errJSON)
}

func streamClose(streamID string) {
	if streamID == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{"stream_id": streamID})
	_, _ = hostCall(pluginabi.MethodHostStreamClose, body)
}

// recoverTo 把 panic 转成 error 返回（用于 RPC 入口的 defer）。
func recoverTo(err *error, where string) {
	if r := recover(); r != nil {
		pluginLogf("panic in %s: %v", where, r)
		*err = fmt.Errorf("%s panicked: %v", where, r)
	}
}
