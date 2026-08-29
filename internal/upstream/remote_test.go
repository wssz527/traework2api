package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"traework2api/internal/auth"
)

func TestRemoteSendMessageEncodesQueryAsJSON(t *testing.T) {
	const userText = "他说\"好\"\n路径\\tmp"
	var gotBody map[string]any
	c := testClient(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Fatalf("request body: %v", err)
		}
		return jsonResp(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
	})
	a := &auth.Auth{
		AccessToken: "at-2",
		UID:         "account-2",
		DeviceID:    "device-2",
		MachineID:   "machine-2",
	}

	if _, err := c.RemoteSendMessage(a, "session-1", "glm-5.3", userText); err != nil {
		t.Fatal(err)
	}

	var query []struct {
		Type string `json:"type"`
		Data struct {
			Content string `json:"content"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(gotBody["query"].(string)), &query); err != nil {
		t.Fatalf("inner query is not JSON: %v query=%q", err, gotBody["query"])
	}
	if len(query) != 1 || query[0].Data.Content != userText {
		t.Errorf("query=%+v", query)
	}
}

func TestRemoteSendMessageUsesSelectedAccountIdentity(t *testing.T) {
	var gotBody map[string]any
	var gotDeviceID, gotMachineID string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotDeviceID = r.Header.Get("X-Device-Id")
		gotMachineID = r.Header.Get("X-Machine-Id")
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Fatalf("request body: %v", err)
		}
		return jsonResp(200, `{"code":0,"data":{"message_id":"m1","accepted":true}}`), nil
	})
	a := &auth.Auth{
		AccessToken: "at-2",
		UID:         "account-2",
		DeviceID:    "device-2",
		MachineID:   "machine-2",
	}

	if _, err := c.RemoteSendMessage(a, "session-1", "glm-5.3", "OK"); err != nil {
		t.Fatal(err)
	}

	commonParams, _ := gotBody["common_params"].(string)
	var common map[string]any
	if err := json.Unmarshal([]byte(commonParams), &common); err != nil {
		t.Fatalf("common_params: %v", err)
	}
	if common["icube_uid"] != "account-2" || common["device_id"] != "device-2" || common["machine_id"] != "machine-2" {
		t.Errorf("account identity=%v", common)
	}
	if gotDeviceID != "device-2" || gotMachineID != "machine-2" {
		t.Errorf("headers device=%q machine=%q", gotDeviceID, gotMachineID)
	}
}

func TestExtractAssistantTextPrefersFinishSummaryOverReasoning(t *testing.T) {
	content := `{"messages":[{"type":"plan_item","plan_item":{"reasoning_content":"内部思考","tool_call_info":{"name":"finish","params":{"summary":"OK"}}}}]}`

	if got := extractAssistantText(content); got != "OK" {
		t.Errorf("reply=%q want final summary only", got)
	}
}

func TestRemoteSendMessageParallelLimitIsRetryableBusy(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(429, `{"code":991502,"error":{"reason":"solo_agent_parallel_limit","limit":2,"running":2},"message":"solo agent parallel limit reached"}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	_, err := c.RemoteSendMessage(a, "s1", "glm-5.3", "hi")
	if !errors.Is(err, ErrRemoteBusy) {
		t.Fatalf("err=%v want ErrRemoteBusy", err)
	}
}

func TestRemoteDeleteSession(t *testing.T) {
	var gotMethod, gotPath string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotMethod, gotPath = r.Method, r.URL.Path
		return jsonResp(200, `{"code":0,"message":"success"}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	if err := c.RemoteDeleteSession(a, "s1"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/api/remote/v1/chat_sessions/s1" {
		t.Errorf("got %s %s", gotMethod, gotPath)
	}
}

func TestRemoteWaitAndReadReturnsOnContextCancel(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"code":0,"data":{"items":[{"role":"user","status":"in_progress"}]}}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", DeviceID: "d1", MachineID: "m1"}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.RemoteWaitAndRead(ctx, a, "s1", time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took %s, ctx cancel should return promptly", elapsed)
	}
}
