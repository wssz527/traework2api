// cloudagent.go create_agent_task（cloud_agent）通道：OpenAI body 改写、
// 上游请求、SSE 事件 → OpenAI chat.completion.chunk 流式转换。
//
// 与 llm_utils_chat（老协议，走通用积分 endpoint=0）不同，cloud_agent
// 走 Work 专属积分（endpoint=1），是 glm-5.3 等新模型的唯一通道。
//
// 上游事件序列（SPEC，实测逆向）：
//
//	event:metadata                              ← 首个事件，任务信息（含 model_info.config_name）
//	event:message                               ← ×N，增量文本
//	data:{"type":"text","text":"<增量>", ...}（也可能直接是 JSON 字符串 "<增量>"）
//	event:toolcall / event:plan                 ← 忽略
//	event:done                                  ← 任务完成
//	event:error
//	data:{"code":...,"message":"..."}
package upstream

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"traework2api/internal/auth"
)

// CloudAgentVersionCode create_agent_task 请求的版本门槛（≥20260811 才下发
// glm-5.3；与 SOLOHeaders 的 X-Ide-Version-Code 保持一致，取最新 20260820）。
const CloudAgentVersionCode = "20260820"

// PrepareCloudAgentBody 把 OpenAI /v1/chat/completions 请求体转成
// create_agent_task 格式（SPEC body 结构）。无法解析时返回错误。
func PrepareCloudAgentBody(openAIBody []byte, a *auth.Auth) ([]byte, error) {
	var src map[string]any
	if err := json.Unmarshal(openAIBody, &src); err != nil {
		return openAIBody, fmt.Errorf("parse openai body: %w", err)
	}
	model, _ := src["model"].(string)
	model = strings.TrimSpace(model)
	if model == "" {
		model = DefaultConfigName
	}

	// 必填字段（probe 变体矩阵实测：conversation_id/session_id/user_id/
	// device_id/ide_version/user_input 缺一即返回参数错误 4001）。
	// 注：4001 是「参数非法」的通用码，错误体可能带 expr_path 指出具体字段；
	// 这里列出的字段集由 probe 逐字段剔除测出，不是从二进制常量读到的。
	uid := ""
	deviceID := "00000000-0000-0000-0000-000000000000"
	if a != nil {
		uid = a.UID
		if a.DeviceID != "" {
			deviceID = a.DeviceID
		}
	}
	convID := newUUID()
	sessionID := newUUID()
	messageID := newUUID()
	userInput := map[string]any{
		"id": messageID,
		"query": []any{
			map[string]any{
				"type": "text",
				"data": map[string]any{
					"content": "",
				},
			},
		},
	}

	// 对话文本：OpenAI messages 里最后一条 user 的纯文本 content
	// （当前为单轮转发；多模态数组内容拼接其中的 text 部分）。
	// query 内层是 JSON 数组字符串（SPEC 实测结构，勿改为对象）。
	var userText string
	if msgs, ok := src["messages"].([]any); ok && len(msgs) > 0 {
		last := msgs[len(msgs)-1]
		if m, ok := last.(map[string]any); ok {
			if role, _ := m["role"].(string); role == "user" {
				switch c := m["content"].(type) {
				case string:
					userText = c
				case []any:
					for _, part := range c {
						if p, ok := part.(map[string]any); ok {
							if typ, _ := p["type"].(string); typ == "text" {
								if t, ok := p["text"].(string); ok {
									userText += t
								}
							}
						}
					}
				}
			}
		}
	}
	query := "[{\"type\":\"text\",\"data\":{\"content\":\"" + jsonEscape(userText) + "\"}}]"
	userInput["query"] = []any{
		map[string]any{
			"type": "text",
			"data": map[string]any{
				"content": userText,
			},
		},
	}

	chatSessionID := newUUID()

	body := map[string]any{
		"agent_type":      Function,
		"agent_id":        Function,
		"function":        Function,
		"config_name":     model,
		"model_name":      model,
		"query":           query,
		"chat_session_id": chatSessionID,
		// 逆向确认的必填字段（缺一 4001）
		"conversation_id": convID,
		"session_id":      sessionID,
		"user_id":         uid,
		"device_id":       deviceID,
		"ide_version":     IdeVersion,
		"user_input":      userInput,
		// 逆向确认：顶层 model_config 必填（Go ModelConfig 结构），缺失报 "model config is empty"
		// 本地 Work 通道同构子树见 localwork.go 的 localWorkModelConfig。
		"model_config": map[string]any{
			"provider":                    "",
			"is_preset":                   true,
			"config_name":                 model,
			"config_source":               1,
			"model_name":                  model,
			"ak":                          "",
			"base_url":                    "",
			"use_remote_service":          true,
			"multimodal":                  false,
			"prompt_max_tokens":           168000,
			"toolcall_history_max_tokens": nil,
			"extra_config":                nil,
			"raw_chat_function":           nil,
			"prompt_set":                  nil,
			"context_window_sizes":        nil,
			"max_turn":                    500,
			"display_options":             nil,
			"max_tokens":                  32000,
			"application_config":          nil,
			"sk":                          "",
			"auth_type":                   0,
			"region":                      nil,
			"session_token":               nil,
			"custom_model_type":           nil,
		},
		"user_message_context": map[string]any{
			"model_info": map[string]any{
				"provider":                    "",
				"is_preset":                   true,
				"config_name":                 model,
				"config_source":               1,
				"model_name":                  model,
				"ak":                          "",
				"base_url":                    "",
				"use_remote_service":          true,
				"multimodal":                  false,
				"prompt_max_tokens":           168000,
				"toolcall_history_max_tokens": nil,
				"extra_config":                nil,
				"raw_chat_function":           nil,
				"prompt_set":                  nil,
				"context_window_sizes":        nil,
				"max_turn":                    500,
				"display_options":             nil,
				"max_tokens":                  32000,
				"application_config":          nil,
				"sk":                          "",
				"auth_type":                   0,
				"region":                      nil,
				"session_token":               nil,
				"custom_model_type":           nil,
			},
			"parsed_query": []any{userText},
			"query":        query,
		},
		"model_smart_selection_meta": map[string]any{
			"config_name": model,
			"mode":        "manual",
		},
		"client_info": map[string]any{
			"ppe_env_name":                "",
			"connect_session_id":          chatSessionID,
			"project_id":                  nil,
			"chat_session_id":             chatSessionID,
			"version_code":                20260820,
			"workspace_folder":            "",
			"icube_language":              "zh-CN",
			"confirm_config":              nil,
			"device_id":                   "00000000-0000-0000-0000-000000000000",
			"workspace_id":                "",
			"terminal_info":               nil,
			"is_solo_mode":                true,
			"client_type":                 "solo_lite",
			"git_repos":                   []any{},
			"security_rules":              []any{},
			"agent_task_service_strategy": "cloud_agent",
			"enable_llm_utils_cloud":      false,
			"is_evaluation":               false,
			"is_worktree":                 false,
			"is_workspace_folder_changed": false,
			"workspace_folders":           []any{},
			"runtime_environment_list":    nil,
			"agent_run_info":              nil,
			"web_fetch_strategy":          "auto",
			"web_fetch_ppe_env":           nil,
			"web_fetch_blocked_domains":   nil,
			"user_timezone":               "Asia/Shanghai",
			"authorized_services":         nil,
			"git_info":                    nil,
			"permission_profile_id":       nil,
		},
		"extra_chat_params": map[string]any{},
		"message_contents":  []any{},
		"use_inbox":         true,
	}
	out, err := json.Marshal(body)
	if err != nil {
		return openAIBody, fmt.Errorf("marshal agent body: %w", err)
	}
	return out, nil
}

// CreateAgentTask 发 create_agent_task 请求并返回原始 SSE body 流
// （调用方负责 Close）。签名/错误语义与 ChatStream 一致：
// 非 2xx 时 rc 为 nil、body 为上游响应体、err 为 nil；只有传输层失败才返回 err。
func (c *Client) CreateAgentTask(a *auth.Auth, body []byte) (rc io.ReadCloser, status int, respBody []byte, err error) {
	req, err := http.NewRequest(http.MethodPost, c.agentBase()+EpCreateAgentTask, bytes.NewReader(body))
	if err != nil {
		return nil, 0, nil, err
	}
	SOLOHeaders(req, a, true)
	// 用专用流客户端（无总超时），避免长 SSE 流被 HTTP.Timeout 截断。
	hc := c.HTTP
	if c.StreamHTTP != nil {
		hc = c.StreamHTTP
	}
	resp, err := hc.Do(req)
	if err != nil {
		log.Printf("create_agent_task uid=%s: transport error: %v", a.UID, err)
		return nil, 0, nil, err
	}
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		kind := Classify(resp.StatusCode, string(raw))
		log.Printf("create_agent_task uid=%s: upstream %d %s body=%s",
			a.UID, resp.StatusCode, kind, truncate(string(raw), 200))
		return nil, resp.StatusCode, raw, nil
	}
	return resp.Body, resp.StatusCode, nil, nil
}

// CloudAgentEvent 单条 create_agent_task SSE 事件（归一化）。
type CloudAgentEvent struct {
	Event        string // metadata | message | toolcall | plan | done | error
	Text         string // message: 增量文本
	FinishReason string // done
	ErrorCode    int64  // error
	ErrorMessage string // error
}

// parseCloudAgentLine 解析一条事件（eventName 为 event 行值，dataLine 为
// data 行值）。message 的 data 兼容两种形态：
//
//	{"type":"text","text":"<增量>", ...}    ← SPEC 结构
//	"<增量>"                               ← 可能被直接序列化为字符串
func parseCloudAgentLine(eventName, dataLine string) (*CloudAgentEvent, error) {
	ev := &CloudAgentEvent{Event: strings.TrimSpace(eventName)}
	if dataLine == "" {
		return ev, nil
	}
	switch ev.Event {
	case "message":
		var raw map[string]any
		if err := json.Unmarshal([]byte(dataLine), &raw); err == nil {
			if v, ok := raw["text"].(string); ok {
				ev.Text = v
			}
		} else if strings.HasPrefix(dataLine, `"`) {
			var s string
			if json.Unmarshal([]byte(dataLine), &s) == nil {
				ev.Text = s
			}
		}
	case "done":
		var s string
		if json.Unmarshal([]byte(dataLine), &s) == nil {
			ev.FinishReason = s
		}
	case "error":
		var raw map[string]any
		if err := json.Unmarshal([]byte(dataLine), &raw); err != nil {
			return nil, err
		}
		if v, ok := raw["code"].(float64); ok {
			ev.ErrorCode = int64(v)
		}
		if v, ok := raw["message"].(string); ok {
			ev.ErrorMessage = v
		}
	}
	return ev, nil
}

// CloudAgentStreamToOpenAI 把 create_agent_task 的 SSE 事件流转成 OpenAI
// chat.completion.chunk SSE 流（写出方式与 SOLO Stream 一致：先由调用方
// 设置 status 200，本函数自设 SSE headers、逐 chunk flush、保证 [DONE]）。
// rc 由本函数负责 Close。
func CloudAgentStreamToOpenAI(w http.ResponseWriter, rc io.ReadCloser) error {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	fl, _ := w.(http.Flusher)
	defer rc.Close()

	br := bufio.NewReaderSize(rc, 64*1024)
	id := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	sawDone := false

	writeChunk := func(delta map[string]any, finish string) error {
		chunk := map[string]any{
			"id":      id,
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   "",
			"choices": []any{
				map[string]any{
					"index": 0,
					"delta": delta,
				},
			},
		}
		if finish != "" {
			chunk["choices"].([]any)[0].(map[string]any)["finish_reason"] = finish
		}
		raw, _ := json.Marshal(chunk)
		if _, err := io.WriteString(w, "data: "+string(raw)+"\n\n"); err != nil {
			return err
		}
		if fl != nil {
			fl.Flush()
		}
		return nil
	}
	writeDONE := func() error {
		if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
			return err
		}
		if fl != nil {
			fl.Flush()
		}
		return nil
	}

	// cloud_agent 事件的状态机（与 SOLO 的 sseState 同构，事件名不同）。
	var (
		event string
		data  strings.Builder
	)
	reset := func() {
		event = ""
		data.Reset()
	}
	handleLine := func(line string) {
		switch {
		case line == "":
			if event == "" {
				reset()
				return
			}
			ev, err := parseCloudAgentLine(event, data.String())
			reset()
			if err != nil {
				return
			}
			switch ev.Event {
			case "message":
				if ev.Text != "" {
					if err := writeChunk(map[string]any{"content": ev.Text}, ""); err != nil {
						return
					}
				}
			case "done":
				if err := writeChunk(map[string]any{}, "stop"); err != nil {
					return
				}
				if err := writeDONE(); err != nil {
					return
				}
				sawDone = true
			case "error":
				msg := fmt.Sprintf("cloud_agent error code=%d msg=%s", ev.ErrorCode, ev.ErrorMessage)
				if _, err := io.WriteString(w, "event: error\ndata: "+jsonEscape(msg)+"\n\n"); err != nil {
					return
				}
				if err := writeDONE(); err != nil {
					return
				}
				sawDone = true
			}
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(line, "data:"))
		}
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		handleLine(strings.TrimRight(line, "\r\n"))
		if err == io.EOF {
			break
		}
	}
	if !sawDone {
		// 幂等兜底：上游中断（无 done）补发带 finish_reason 的完成 chunk + [DONE]。
		if err := writeChunk(map[string]any{}, "stop"); err != nil {
			return err
		}
		return writeDONE()
	}
	return nil
}

// newUUID 生成 v4 风格 UUID（无第三方依赖）。
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("chat-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// randomHex 生成 n 字节的随机十六进制字符串（用于 trace id / device id）。
func randomHex(n int) string {
	const hexdig = "0123456789abcdef"
	buf := make([]byte, n)
	rand.Read(buf)
	for i := range buf {
		buf[i] = hexdig[buf[i]&0x0f]
	}
	return string(buf)
}
