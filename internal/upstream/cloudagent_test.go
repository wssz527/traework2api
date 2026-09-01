package upstream

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrepareCloudAgentBody(t *testing.T) {
	out, err := PrepareCloudAgentBody([]byte(`{
		"model":"glm-5.3",
		"messages":[{"role":"system","content":"sys"},{"role":"user","content":"你好，介绍一下北京"}]
	}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["function"] != "solo_work_lite" {
		t.Errorf("function=%v", m["function"])
	}
	if m["config_name"] != "glm-5.3" || m["model_name"] != "glm-5.3" {
		t.Errorf("model fields: %v / %v", m["config_name"], m["model_name"])
	}
	query, _ := m["query"].(string)
	if !strings.Contains(query, "你好，介绍一下北京") {
		t.Errorf("query=%q", query)
	}
	if !strings.Contains(query, `"type":"text"`) {
		t.Errorf("query should be inner JSON string: %q", query)
	}
	uc, _ := m["user_message_context"].(map[string]any)
	parsed, _ := uc["parsed_query"].([]any)
	if len(parsed) != 1 || parsed[0] != "你好，介绍一下北京" {
		t.Errorf("parsed_query=%v", parsed)
	}
	mi, _ := uc["model_info"].(map[string]any)
	if mi["config_name"] != "glm-5.3" || mi["is_preset"] != true {
		t.Errorf("model_info=%v", mi)
	}
	ci, _ := m["client_info"].(map[string]any)
	if ci["agent_task_service_strategy"] != "cloud_agent" {
		t.Errorf("strategy=%v", ci["agent_task_service_strategy"])
	}
	if ci["is_solo_mode"] != true || ci["client_type"] != "solo_lite" {
		t.Errorf("client_info=%v", ci)
	}
	if ci["version_code"] != float64(20260820) {
		t.Errorf("version_code=%v", ci["version_code"])
	}
	if m["use_inbox"] != true {
		t.Errorf("use_inbox=%v", m["use_inbox"])
	}
	if m["chat_session_id"] == "" {
		t.Error("chat_session_id missing")
	}
}

func TestPrepareCloudAgentBodyLastUserOnly(t *testing.T) {
	out, err := PrepareCloudAgentBody([]byte(`{
		"model":"glm-5.3",
		"messages":[{"role":"user","content":"前面的话"},{"role":"assistant","content":"回应"},{"role":"user","content":"最后的话"}]
	}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	if !strings.Contains(m["query"].(string), "最后的话") {
		t.Errorf("query=%v", m["query"])
	}
	if strings.Contains(m["query"].(string), "前面的话") {
		t.Errorf("query should only carry last user message: %v", m["query"])
	}
}

func TestPrepareCloudAgentBodyArrayContent(t *testing.T) {
	out, err := PrepareCloudAgentBody([]byte(`{
		"model":"glm-5.3",
		"messages":[{"role":"user","content":[{"type":"text","text":"看图说话"},{"type":"image_url","url":"x"}]}]
	}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	if !strings.Contains(m["query"].(string), "看图说话") {
		t.Errorf("query=%v", m["query"])
	}
}

func TestPrepareCloudAgentBodyInvalidJSON(t *testing.T) {
	in := []byte(`{broken`)
	out, err := PrepareCloudAgentBody(in, nil)
	if err == nil {
		t.Fatal("want error for invalid json")
	}
	if string(out) != string(in) {
		t.Error("invalid input should pass through unchanged")
	}
}

// 按 SPEC 事件序列构造的 create_agent_task SSE 样例。
const cloudAgentSSEFixture = "event:metadata\n" +
	"data:{\"chat_process_version\":\"v3\",\"agent_task_service_strategy\":\"cloud_agent\",\"agent_name\":\"SOLO MTC\",\"model_info\":{\"config_name\":\"glm-5.3\"}}\n\n" +
	"event:message\n" +
	"data:{\"type\":\"text\",\"text\":\"北京\"}\n\n" +
	"event:message\n" +
	"data:{\"type\":\"text\",\"text\":\"欢迎你\"}\n\n" +
	"event:message\n" +
	"data:{\"type\":\"plan\",\"content\":\"ignore me\"}\n\n" +
	"event:done\n" +
	"data:\"stop\"\n\n"

func TestCloudAgentStreamToOpenAI(t *testing.T) {
	rec := httptest.NewRecorder()
	err := CloudAgentStreamToOpenAI(rec, nopCloser(strings.NewReader(cloudAgentSSEFixture)))
	if err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"object":"chat.completion.chunk"`) {
		t.Errorf("missing chunk object: %q", body)
	}
	if !strings.Contains(body, `"content":"北京"`) || !strings.Contains(body, `"content":"欢迎你"`) {
		t.Errorf("missing content deltas: %q", body)
	}
	if strings.Contains(body, "ignore me") {
		t.Errorf("non-text message leaked: %q", body)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) {
		t.Errorf("missing finish chunk: %q", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Errorf("missing [DONE]: %q", body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content-type=%q", ct)
	}
}

func TestCloudAgentStreamToOpenAIStringData(t *testing.T) {
	// data 可能直接是 JSON 字符串形态。
	raw := "event:message\n" +
		"data:\"直接字符串增量\"\n\n" +
		"event:done\n" +
		"data:\"stop\"\n\n"
	rec := httptest.NewRecorder()
	if err := CloudAgentStreamToOpenAI(rec, nopCloser(strings.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Body.String(), "直接字符串增量") {
		t.Errorf("missing string delta: %q", rec.Body.String())
	}
}

func TestCloudAgentStreamToOpenAIError(t *testing.T) {
	raw := "event:error\n" +
		"data:{\"code\":1001,\"message\":\"auth failed\"}\n\n" +
		"event:done\n" +
		"data:\"stop\"\n\n"
	rec := httptest.NewRecorder()
	if err := CloudAgentStreamToOpenAI(rec, nopCloser(strings.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "cloud_agent error") || !strings.Contains(body, "1001") {
		t.Errorf("missing error event: %q", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Errorf("missing [DONE]: %q", body)
	}
}

func TestCloudAgentStreamToOpenAIInterrupted(t *testing.T) {
	// 上游中断（无 done）→ 补发 finish chunk + [DONE]。
	raw := "event:message\n" +
		"data:{\"type\":\"text\",\"text\":\"x\"}\n\n"
	rec := httptest.NewRecorder()
	if err := CloudAgentStreamToOpenAI(rec, nopCloser(strings.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Errorf("missing [DONE]: %q", rec.Body.String())
	}
}

func TestParseCloudAgentLine(t *testing.T) {
	ev, err := parseCloudAgentLine("message", `{"type":"text","text":"hi"}`)
	if err != nil || ev.Text != "hi" {
		t.Fatalf("message: %+v %v", ev, err)
	}
	ev, err = parseCloudAgentLine("message", `"raw string"`)
	if err != nil || ev.Text != "raw string" {
		t.Fatalf("string message: %+v %v", ev, err)
	}
	ev, err = parseCloudAgentLine("done", `"stop"`)
	if err != nil || ev.FinishReason != "stop" {
		t.Fatalf("done: %+v %v", ev, err)
	}
	ev, err = parseCloudAgentLine("error", `{"code":1001,"message":"auth failed"}`)
	if err != nil || ev.ErrorCode != 1001 || ev.ErrorMessage != "auth failed" {
		t.Fatalf("error: %+v %v", ev, err)
	}
}

// nopCloser 包装 io.Reader 为 io.ReadCloser（测试用）。
func nopCloser(r *strings.Reader) ioReadCloser {
	return ioReadCloser{r}
}

type ioReadCloser struct {
	*strings.Reader
}

func (ioReadCloser) Close() error { return nil }
