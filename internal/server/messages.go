// messages.go — Anthropic Messages API 端点（/v1/messages）。
// 解析 Anthropic 请求（system + messages），转内部 messages 格式，
// 走 serveRemoteWith（anthropicRenderer 渲染 Anthropic 事件流）。
package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"traework2api/internal/upstream"
)

// anthropicMessage 一条 Anthropic 消息。
type anthropicMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"` // string 或 []block
}

// anthropicBlock Anthropic content block。
type anthropicBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// serveMessages POST /v1/messages（Anthropic Messages API）。
func (h *Handler) serveMessages(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}
	if len(body) > maxBodyBytes {
		writeOpenAIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 8MB limit")
		return
	}
	var req struct {
		Model     string             `json:"model"`
		System    any                `json:"system"` // string 或 []block
		Messages  []anthropicMessage `json:"messages"`
		Stream    bool               `json:"stream"`
		MaxTokens int                `json:"max_tokens"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "parse body: "+err.Error())
		return
	}
	if req.Model == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "missing model")
		return
	}

	// Anthropic 消息 → 内部统一 messages 格式
	msgs := make([]map[string]any, 0, len(req.Messages)+1)
	// system 参数 → 首条 system 消息
	sysText := anthropicSystemText(req.System)
	if sysText != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": sysText})
	}
	for _, m := range req.Messages {
		msgs = append(msgs, map[string]any{"role": m.Role, "content": anthropicContentText(m.Content)})
	}
	outBody, _ := json.Marshal(map[string]any{
		"model": req.Model, "messages": msgs, "stream": req.Stream,
	})
	// 强制 remote 通道（Anthropic 接入 trae 云端沙盒）
	configName, maxMode, err := h.mapModel(req.Model)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	outBody = setModelInBody(outBody, configName)
	a := h.cfg.Pool.Pick()
	if a == nil {
		writeOpenAIError(w, http.StatusServiceUnavailable, "no_healthy_account", "no healthy account")
		return
	}
	render := &anthropicRenderer{}
	if err := h.serveRemoteWith(w, r, a, configName, outBody, req.Stream, maxMode, render); err != nil {
		if !strings.Contains(err.Error(), "context canceled") {
			writeOpenAIError(w, http.StatusBadGateway, "remote_task_failed", err.Error())
		}
	}
}

// anthropicSystemText 提取 Anthropic system 参数文本（string 或 block 数组）。
func anthropicSystemText(sys any) string {
	switch v := sys.(type) {
	case string:
		return v
	case []any:
		var sb strings.Builder
		for _, item := range v {
			if b, ok := item.(map[string]any); ok {
				if t, ok := b["text"].(string); ok {
					sb.WriteString(t)
					sb.WriteString("\n")
				}
			}
		}
		return sb.String()
	}
	return ""
}

// anthropicContentText 提取 Anthropic content 文本（string 或 block 数组）。
func anthropicContentText(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		var sb strings.Builder
		for _, item := range v {
			if b, ok := item.(map[string]any); ok {
				if t, ok := b["text"].(string); ok {
					sb.WriteString(t)
					sb.WriteString("\n")
				}
			}
		}
		return sb.String()
	}
	return ""
}

var _ = upstream.RemoteEventDelta{} // 保持 upstream import
