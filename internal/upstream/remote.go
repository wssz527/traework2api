package upstream

// remote.go — TRAE 本地任务（remote）通道：消耗 Work 专属积分（endpoint=1）。
// 逆向自客户端真实请求（2026-08-27 实测验证）：
//   1. POST {RemoteHost}/api/remote/v1/chat_sessions → 创建会话（带 agent_type=solo_work_lite + title）
//   2. POST {RemoteHost}/api/remote/v1/chat_sessions/{id}/messages → 发消息（agent_type=solo_work_remote）
//   3. 轮询 GET {RemoteHost}/api/remote/v1/chat_sessions/{id}/messages → 等 assistant status=completed
//   4. 读 assistant task content 的 plan_item.tool_call_info.params.summary → 最终回复
// 与 llm_utils_chat（消耗 ide_credits）不同，本通道消耗 work_credits。
// 实测：DeepSeek-V4-Flash-Official / glm-5.3 均可，明文 HTTP，无需 TTNet 加密。
// 注意：remote 接口的域名为 api5-normal.mchost.guru（与老通道 trae-api-cn.mchost.guru 不同），
// 且创建会话必须带 agent_type，否则任务不会真正启动（poll 永远 in_progress）。

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"traework2api/internal/auth"
)

// ErrRemoteBusy 上游 solo agent 并发槽已满（429 code=991502 solo_agent_parallel_limit，
// 每账号限 2 个并发任务）。槽被前序慢任务占用时会出现，等待后可重试。
var ErrRemoteBusy = errors.New("remote solo agent parallel limit reached")

// ErrRemoteTimeout 远程任务在总时限内未完成（上游重任务实测排队+执行可达 13 分钟）。
var ErrRemoteTimeout = errors.New("remote task timeout")

// remoteHostMu 保护 remoteHost 的并发读写：事件流 goroutine 会在任务
// 结束后仍存活片刻（drain / 重连退避），此时单测替换 host 变量会构成
// 数据竞争（-race 下已实际命中）。
var remoteHostMu sync.RWMutex

// remoteHost remote 通道专用域名（客户端实测，api5-normal）。
// 经 SetRemoteHost/remoteHostURL 读写而非直接赋值。
var remoteHost = "https://api5-normal.mchost.guru"

// SetRemoteHost 测试钩子：替换 remote 通道目标地址。返回还原函数。
func SetRemoteHost(url string) (restore func()) {
	remoteHostMu.Lock()
	defer remoteHostMu.Unlock()
	old := remoteHost
	remoteHost = url
	return func() {
		remoteHostMu.Lock()
		defer remoteHostMu.Unlock()
		remoteHost = old
	}
}

// remoteHostURL 当前 remote 通道地址（生产代码一律经此读取）。
func remoteHostURL() string {
	remoteHostMu.RLock()
	defer remoteHostMu.RUnlock()
	return remoteHost
}

// RemoteHostBase 对外（cmd 探针等外部包）导出的只读地址访问。
func RemoteHostBase() string { return remoteHostURL() }

// RemoteEpCreateSession 创建会话
const RemoteEpCreateSession = "/api/remote/v1/chat_sessions"

// maxModelSuffix 对外模型名的 Max 模式后缀（glm-5.3-max 等）。
// 客户端带该后缀请求 = 剥后缀得真实模型名 + 发消息时 model_selection_strategy=max。
const maxModelSuffix = "-max"

// SplitMaxSuffix 解析对外模型名：带 "-max" 后缀时返回 (真实模型名, true)，
// 否则原样返回 (model, false)。空串/纯 "-max" 不算 Max。
func SplitMaxSuffix(model string) (string, bool) {
	if len(model) > len(maxModelSuffix) && strings.HasSuffix(model, maxModelSuffix) {
		return model[:len(model)-len(maxModelSuffix)], true
	}
	return model, false
}

// RemoteEpMessages 发消息/查消息
const RemoteEpMessages = "/api/remote/v1/chat_sessions/%s/messages"

// RemoteCommonParams 客户端 common_params 结构（服务端校验用）。
// deviceID/machineID/UID 运行时从账号凭证注入（服务端按设备身份校验）。
// icube_main_uid / workspace_id 为客户端本地状态，无凭证来源，用占位值：
// 上游若严格校验需按抓包自行填入，否则保持占位即可正常工作。
func firstN(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

func RemoteCommonParams(a *auth.Auth) string {
	cp := map[string]any{
		"icube_uid": a.UID, "user_id": a.UID, "biz_user_id": a.UID,
		"user_is_login": true, "user_unique_id": a.DeviceID,
		"device_id":       a.DeviceID,
		"local_device_id": a.DeviceID,
		"is_special_uuid": false, "machine_id": a.MachineID,
		"arch": "arm64", "system": "darwin", "scope": "marscode", "organization": "",
		"build_version": "2.3.76922", "vscode_version": "1.107.1", "tenant": "marscode",
		"region": "CN", "aiRegion": "CN", "quality": "stable",
		"build_time":     "2026-08-24T08:19:28.286Z",
		"icube_main_uid": "YOUR_ICUBE_MAIN_UID",
		"window_id":      3, "workspace_id": "YOUR_WORKSPACE_ID",
		"app_version": "0.1.56", "os_name": "mac", "os_version": "macOS 26.5.2",
		"os_release": "26.5.2", "platform": "electron",
		"device_model": "Mac16,10", "device_manufacturer": "Apple Inc.",
		"cpu": "Apple", "cpu_brand": "Apple M4", "cpu_speed": 2.4,
		"memory": 17179869184, "is_ssh": false, "language": "zh-cn",
		"app_language": "zh-cn", "chat_mode": 1, "identity": "0",
		"identity_str": "Free", "is_freshman": "1", "channel_name": "common",
		"process_type": 2, "privacy_mode": "off",
		"aha_version":        "39.2.7-release.1.50.1",
		"store_country_code": "", "store_country_code_src": "", "store_region": "CN",
		"workspace_status": "unsaved_multi_root", "workspace_root_count": 1,
		"product_code": "SOLO_Lite", "ai_chat_version": "v1",
		"ai_chat_version_source": "default", "solo_lite_app_slim_enabled": "1",
		"app_is_solo_mode": "1", "icube_ab": `{"onboarding":"B"}`,
		"solo_chat_mode": "work", "app_active_workspace_vscdb_size": 12288,
		"app_global_vscdb_size": 761856, "app_window_count": 1,
		"app_system_disk_usage": 0.5836081522144555, "ai_database_db_size": 425984,
		"ai_snapshot_size": 0, "message_source": "manual",
	}
	b, _ := json.Marshal(cp)
	return string(b)
}

// RemoteCustomModel 模型配置（客户端 custom_model 结构）
func RemoteCustomModel(model string) map[string]any {
	return map[string]any{
		"provider": "", "is_preset": true, "config_name": model, "config_source": 1,
		"model_name": model, "display_model_name": "", "ak": "", "base_url": "",
		"use_remote_service": true, "multimodal": false, "prompt_max_tokens": 168000,
	}
}

// RemoteCreateSession 创建远程会话，返回 chat_session_id。
// 客户端真实请求：{"mode":"work","agent_type":"solo_work_lite","title":"test"}
// agent_type 必须携带，否则任务不会真正启动。
func (c *Client) RemoteCreateSession(a *auth.Auth) (string, error) {
	body, _ := json.Marshal(map[string]any{"mode": "work", "agent_type": "solo_work_lite", "title": "kimi-proxy"})
	req, err := http.NewRequest(http.MethodPost, remoteHostURL()+RemoteEpCreateSession, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	RemoteHeaders(req, a)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("create session: %d %s", resp.StatusCode, truncate(string(raw), 200))
	}
	var out struct {
		Code int `json:"code"`
		Data struct {
			ChatSessionID string `json:"chat_session_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("create session parse: %w", err)
	}
	if out.Data.ChatSessionID == "" {
		return "", fmt.Errorf("create session: empty id, body=%s", truncate(string(raw), 200))
	}
	return out.Data.ChatSessionID, nil
}

// RemoteSendMessage 发送消息，返回 message_id。
// 使用客户端真实模板（一字不差），只替换模型名和 query。
// maxMode 为 true 时顶层 model_selection_strategy 置 "max"（Trae SOLO 长上下文
// Max 模式，逆向结论 2026-08-31：Max 开关只改这一个字段），否则保持模板
// 原样 "manual"。
func (c *Client) RemoteSendMessage(a *auth.Auth, sessionID, model, userText string, maxMode bool) (string, error) {
	if os.Getenv("TW2API_DIAG_SEND") != "" {
		log.Printf("DIAG send sess=%s model=%s textLen=%d head=%q", sessionID, model, len(userText), firstN(userText, 200))
	}
	// base64 模板解码 + 替换模型名
	tplBytes, err := base64.StdEncoding.DecodeString(RemoteMsgTemplateB64)
	if err != nil {
		return "", fmt.Errorf("template decode: %w", err)
	}
	tpl := string(tplBytes)
	repl := strings.ReplaceAll(tpl, "DeepSeek-V4-Flash-Official", model)
	var body map[string]any
	if err := json.Unmarshal([]byte(repl), &body); err != nil {
		return "", fmt.Errorf("template parse: %w", err)
	}
	if maxMode {
		body["model_selection_strategy"] = "max"
	}
	query, _ := json.Marshal([]map[string]any{{
		"type": "text",
		"data": map[string]any{"content": userText},
	}})
	body["query"] = string(query)
	body["common_params"] = RemoteCommonParams(a)
	// 同步更新 user_input 和 common_params 里的 query（如有）
	if ui, ok := body["user_input"].(map[string]any); ok {
		ui["query"] = []any{map[string]any{"type": "text", "data": map[string]any{"content": userText}}}
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, remoteHostURL()+fmt.Sprintf(RemoteEpMessages, sessionID), bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	RemoteHeaders(req, a)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode >= 400 {
		// 429 并发槽满：可等待重试的临时状态，与真实错误区分开。
		if resp.StatusCode == http.StatusTooManyRequests && strings.Contains(string(data), "solo_agent_parallel_limit") {
			return "", fmt.Errorf("%w: %s", ErrRemoteBusy, truncate(string(data), 200))
		}
		return "", fmt.Errorf("send message: %d %s", resp.StatusCode, truncate(string(data), 200))
	}
	var out struct {
		Code int `json:"code"`
		Data struct {
			MessageID string `json:"message_id"`
			Accepted  bool   `json:"accepted"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", fmt.Errorf("send message parse: %w", err)
	}
	if !out.Data.Accepted || out.Data.MessageID == "" {
		return "", fmt.Errorf("send message not accepted: %s", truncate(string(data), 200))
	}
	return out.Data.MessageID, nil
}

// RemoteWaitAndRead 轮询消息直到 completed，返回 assistant 回复文本。
// timeout 为最大等待时长；ctx 取消（客户端断开）时立即返回 ctx.Err()。
func (c *Client) RemoteWaitAndRead(ctx context.Context, a *auth.Auth, sessionID string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	stalled := 0 // 连续无进展轮询次数（消息数/最新状态不变）
	var lastProg string
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			remoteHostURL()+fmt.Sprintf(RemoteEpMessages, sessionID)+"?page_size=20", nil)
		if err != nil {
			return "", err
		}
		RemoteHeaders(req, a)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			// 客户端已断开（ctx 取消）：立即返回，停止空轮询并释放并发槽。
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			time.Sleep(2 * time.Second)
			continue
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
		resp.Body.Close()
		var out struct {
			Code int `json:"code"`
			Data struct {
				Items []struct {
					Role    string `json:"role"`
					Status  string `json:"status"`
					Content string `json:"content"`
				} `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &out); err == nil {
			// 找到最后一条 assistant 且 completed 的消息（按 message_index 取最新）
			var best *struct {
				Role    string `json:"role"`
				Status  string `json:"status"`
				Content string `json:"content"`
			}
			for i := range out.Data.Items {
				it := &out.Data.Items[i]
				if it.Role == "assistant" && it.Status == "completed" && it.Content != "" {
					best = it
				}
			}
			if best != nil {
				return extractAssistantText(best.Content), nil
			}
			// 卡死检测：消息数 + 最新状态 连续 20 次无变化（约 1 分钟）→ 判定任务卡死，
			// 提前放弃而非傻等 15 分钟（实测 session_066c213e 云端卡 28 分钟）。
			prog := fmt.Sprintf("n=%d", len(out.Data.Items))
			if len(out.Data.Items) > 0 {
				last := out.Data.Items[len(out.Data.Items)-1]
				prog = fmt.Sprintf("n=%d last=%s/%s", len(out.Data.Items), last.Role, last.Status)
			}
			if prog == lastProg {
				stalled++
				if stalled >= 20 {
					return "", fmt.Errorf("%w: no progress after ~1min (%s)", ErrRemoteTimeout, prog)
				}
			} else {
				lastProg = prog
				stalled = 0
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return "", fmt.Errorf("%w after %s", ErrRemoteTimeout, timeout)
}

// RemoteDeleteSession 删除远程会话。任务完成或放弃后调用：
// 每账号只有 2 个 solo 并发槽，不删除会话会占住槽位，导致后续消息 429。
func (c *Client) RemoteDeleteSession(a *auth.Auth, sessionID string) error {
	req, err := http.NewRequest(http.MethodDelete, remoteHostURL()+RemoteEpCreateSession+"/"+sessionID, nil)
	if err != nil {
		return err
	}
	RemoteHeaders(req, a)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("delete session: %d %s", resp.StatusCode, truncate(string(raw), 200))
	}
	return nil
}

// extractAssistantText 从 assistant content（JSON task 结构）提取最终文本。
// 客户端实测结构：{"task_id":..., "messages":[{"type":"plan_item","plan_item":
//
//	{"tool_call_info":{"name":"finish","params":{"summary":"最终回复"},"result":{"data":{"summary":""}}},
//	 "reasoning_content":"思考过程", ...}}]}
func extractAssistantText(content string) string {
	// content 可能是 {"task_id":..., "messages":[...]} 或纯文本
	var obj map[string]any
	if err := json.Unmarshal([]byte(content), &obj); err != nil {
		return content // 纯文本
	}
	// 尝试从 messages 里找最后一条 text / finish summary / reasoning
	if msgs, ok := obj["messages"].([]any); ok && len(msgs) > 0 {
		var texts []string
		var reasoning []string
		for _, mi := range msgs {
			if m, ok := mi.(map[string]any); ok {
				if t, ok := m["type"].(string); ok && t == "text" {
					if txt, ok := m["text"].(string); ok && txt != "" {
						texts = append(texts, txt)
					}
				}
				// plan_item：提取 finish 工具的 summary（模型最终回复）
				if pi, ok := m["plan_item"].(map[string]any); ok {
					// reasoning_content（思考过程）
					if rc, ok := pi["reasoning_content"].(string); ok && rc != "" {
						reasoning = append(reasoning, rc)
					}
					// finish 工具的 params.summary（最终答案）
					if tci, ok := pi["tool_call_info"].(map[string]any); ok {
						if params, ok := tci["params"].(map[string]any); ok {
							if s, ok := params["summary"].(string); ok && s != "" {
								texts = append(texts, s)
							}
						}
						// result.data.summary
						if res, ok := tci["result"].(map[string]any); ok {
							if rd, ok := res["data"].(map[string]any); ok {
								if s, ok := rd["summary"].(string); ok && s != "" {
									texts = append(texts, s)
								}
							}
						}
					}
					// 直接 response
					if resp, ok := pi["response"].(string); ok && resp != "" {
						texts = append(texts, resp)
					}
				}
			}
		}
		if len(texts) > 0 {
			return joinTexts(texts)
		}
		if len(reasoning) > 0 {
			return joinTexts(reasoning)
		}
	}
	// 退化：返回原始 JSON（至少不丢信息）
	return content
}

func joinTexts(ts []string) string {
	out := ""
	for _, t := range ts {
		if out != "" {
			out += "\n"
		}
		out += t
	}
	return out
}
