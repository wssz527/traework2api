// mockprobe.go — tool_calls 闭环探针（魔术标记门控，不影响正常流量）。
//
// 目的：验证 kimi-code（OpenAI 协议客户端）在收到
// delta.tool_calls + finish_reason=tool_calls 时是否真的本地执行工具
// 并以 role:"tool" 消息回传第二轮请求。这是「云端工具卡 → 客户端原生
// 执行 → 结果回传云端」真 API 闭环的前提假设，先用最小探针实证。
//
// 门控：仅当最后一条 user 消息含魔术标记时触发：
//
//	TW2API_MOCK_PROBE_BASH      → 输出已声明工具 Bash 的 tool_call
//	TW2API_MOCK_PROBE_RUNSHELL  → 输出未声明工具 run_shell 的 tool_call
//	（任意标记会话中）消息里已含 role:"tool" → 视为第二轮，回纯文本总结
//
// 探针请求不建云端会话、不耗积分、不占账号池。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// mockProbeMarkers 探针魔术标记 → 模拟输出的工具名/参数。
var mockProbeMarkers = map[string][2]string{
	"TW2API_MOCK_PROBE_BASH":     {"Bash", `{"command":"echo TW2API_MOCK_PROBE_OK","description":"tw2api tool_calls 闭环探针"}`},
	"TW2API_MOCK_PROBE_RUNSHELL": {"run_shell", `{"command":"echo TW2API_MOCK_PROBE_OK"}`},
}

// mockProbeMarkerPrefix 探针标记公共前缀（会话门控用）。
const mockProbeMarkerPrefix = "TW2API_MOCK_PROBE_"

// containsMockMarker 判断会话消息文本里是否出现过探针魔术标记
// （第一轮 user 里的标记，或工具 echo 出的标记输出）。
func containsMockMarker(msgs []map[string]any) bool {
	for _, m := range msgs {
		if strings.Contains(messageText(m["content"]), mockProbeMarkerPrefix) {
			return true
		}
	}
	return false
}

// tryMockProbe 命中探针标记时完整写出响应并返回 true（请求被短路）。
// marker 检测基于全部 messages：含 role:"tool" → 第二轮回文本；
// 否则最后一条 user 含标记 → 输出 tool_calls 终帧。
func tryMockProbe(w http.ResponseWriter, body []byte, stream bool) bool {
	msgs := bodyMessages(body)
	hasToolMsg := false
	lastUser := ""
	for _, m := range msgs {
		role, _ := m["role"].(string)
		if role == "tool" {
			hasToolMsg = true
		}
		if role == "user" {
			lastUser = messageText(m["content"])
		}
	}

	// 第二轮：客户端已回传工具结果 → 纯文本确认，闭环在 CLI 输出里可见。
	// 门控收紧（tool_calls 闭环改造）：仅当会话消息里出现过魔术标记（探针
	// 第一轮的 user 标记 / echo 出的 TW2API_MOCK_PROBE_OK）时才劫持；
	// 真实 agent 的工具回填轮必须放行 —— 闭环第二腿（role:tool → 回灌桥）
	// 依赖这类请求穿透到 serveRemoteWith。旧世界里 tw2api 从不返回
	// tool_calls、请求不含 role:tool，故此前无标记判定也不影响正常流量。
	if hasToolMsg {
		if !containsMockMarker(msgs) {
			return false
		}
		text := "MOCK_PROBE 第二轮：已收到 role:tool 结果。闭环成立（客户端会执行 tool_calls 并回传）。"
		if stream {
			startMockStream(w)
			writeChatChunk(w, mockID(), time.Now().Unix(), "mock", "", text, nil)
			writeChatChunk(w, mockID(), time.Now().Unix(), "mock", "", "", "stop")
			io.WriteString(w, "data: [DONE]\n\n")
			sseFlush(w)
		} else {
			writeOpenAICompletion(w, "mock", text)
		}
		return true
	}

	var name, args string
	for marker, na := range mockProbeMarkers {
		if strings.Contains(lastUser, marker) {
			name, args = na[0], na[1]
			break
		}
	}
	if name == "" {
		return false
	}

	if stream {
		startMockStream(w)
		id := mockID()
		created := time.Now().Unix()
		writeChatChunk(w, id, created, "mock", "assistant", "", nil)
		writeRawToolCallChunk(w, id, created, "call_mock_probe", name, args, "tool_calls")
		io.WriteString(w, "data: [DONE]\n\n")
		sseFlush(w)
	} else {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"mock","object":"chat.completion","created":0,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_mock_probe","type":"function","function":{"name":%q,"arguments":%q}}]},"finish_reason":"tool_calls"}]}`, name, args)
	}
	return true
}

// mockID 探针响应 id。
func mockID() string { return fmt.Sprintf("chatcmpl-mock-%d", time.Now().UnixNano()) }

// startMockStream 探针专用 SSE 头。
func startMockStream(w io.Writer) {
	if hw, ok := w.(http.ResponseWriter); ok {
		startRemoteStream(hw)
		return
	}
}

// writeRawToolCallChunk 输出带完整 id/name/arguments 的 tool_calls chunk
// （探针用；正式实现见 remotedelta 的注记行解析路径）。
func writeRawToolCallChunk(w io.Writer, id string, created int64, callID, name, args, finish string) {
	delta := map[string]any{
		"tool_calls": []map[string]any{{
			"index": 0, "id": callID, "type": "function",
			"function": map[string]any{"name": name, "arguments": args},
		}},
	}
	chunk := map[string]any{
		"id": id, "object": "chat.completion.chunk", "created": created, "model": "mock",
		"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
	}
	raw, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", raw)
}
