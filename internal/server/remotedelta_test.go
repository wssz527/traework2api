// remotedelta_test.go — 云端事件增量渲染 + 工具去重 + 会话路由的单测。
package server

import (
	"strings"
	"testing"
	"time"

	"traework2api/internal/upstream"
)

func TestWriteRemoteDeltaReasoning(t *testing.T) {
	var sb strings.Builder
	writeRemoteDelta(&sb, "id", 1, "m", upstream.RemoteEventDelta{Reasoning: "思考中"})
	got := sb.String()
	if !strings.Contains(got, `"reasoning_content":"思考中"`) {
		t.Fatalf("reasoning 应写入 delta.reasoning_content: %q", got)
	}
	if strings.Contains(got, `"content"`) {
		t.Fatalf("reasoning 帧不应带 content: %q", got)
	}
	if !strings.HasPrefix(got, "data: ") || !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("应为 SSE data 帧: %q", got)
	}
}

func TestWriteRemoteDeltaContent(t *testing.T) {
	var sb strings.Builder
	writeRemoteDelta(&sb, "id", 1, "m", upstream.RemoteEventDelta{Content: "正文"})
	if got := sb.String(); !strings.Contains(got, `"content":"正文"`) {
		t.Fatalf("content 应写入 delta.content: %q", got)
	}
}

func TestWriteRemoteDeltaToolLine(t *testing.T) {
	var sb strings.Builder
	writeRemoteDelta(&sb, "id", 1, "m", upstream.RemoteEventDelta{ToolLine: "\n\n> 🔧 [本地工具] 本地工作区/read_file · {\"path\":\"/tmp/x\"}\n"})
	got := sb.String()
	// 工具调用应走结构化 delta.tool_calls（客户端正确显示为工具调用而非文本）
	if !strings.Contains(got, "tool_calls") {
		t.Fatalf("工具注记应写入 delta.tool_calls: %q", got)
	}
	if !strings.Contains(got, "read_file") {
		t.Fatalf("tool_calls 应含工具名 read_file: %q", got)
	}
	if strings.Contains(got, "🔧") {
		t.Errorf("注记 emoji 不应出现在结构化字段里: %q", got)
	}
}

func TestWriteRemoteDeltaEmptyWritesNothing(t *testing.T) {
	var sb strings.Builder
	writeRemoteDelta(&sb, "id", 1, "m", upstream.RemoteEventDelta{})
	if sb.Len() != 0 {
		t.Fatalf("空增量不应写任何字节: %q", sb.String())
	}
}

// TestToolDedupSuppressesDuplicate 同一工具两路呈现必须只出一次。
func TestToolDedupSuppressesDuplicate(t *testing.T) {
	d := newToolDedup(30 * time.Second)
	d.mark("read_file") // 本地 MCP 事件先到
	line := "\n\n> 🔧 [本地工具] 本地工作区/read_file · {\"path\":\"x\"}\n"
	if !d.seen(line) {
		t.Fatal("云端工具卡应被本地事件抑制")
	}
	// 云端先到的反序情形：首次不抑制，本地随后到达应被抑制。
	d2 := newToolDedup(30 * time.Second)
	if d2.seen(line) {
		t.Fatal("首次出现不应被抑制")
	}
	if !d2.seen(line) {
		t.Fatal("重复出现应被抑制")
	}
}

// TestToolDedupDifferentTools 不同工具互不干扰。
func TestToolDedupDifferentTools(t *testing.T) {
	d := newToolDedup(30 * time.Second)
	d.mark("read_file")
	if d.seen("\n\n> 🔧 [本地工具] list_dir · {}\n") {
		t.Fatal("不同工具不应互相抑制")
	}
}

// TestToolDedupConsumesOnce 一次登记只抑制一次：同名工具连续调用两次时，
// 第二次必须正常输出（否则长任务里同一工具的后续调用会静默消失）。
func TestToolDedupConsumesOnce(t *testing.T) {
	d := newToolDedup(30 * time.Second)
	line := "\n\n> 🔧 [本地工具] read_file · {}\n"
	d.mark("read_file") // 云端工具卡先到
	if !d.seen(line) {
		t.Fatal("本地事件应被先到的云端工具卡抑制一次")
	}
	// 第二次调用：标记已被消费，应正常输出。
	if d.seen(line) {
		t.Fatal("标记已消费，第二次调用不应被抑制")
	}
}

// TestToolDedupWindowExpiry 超出时间窗后登记失效，不再抑制。
func TestToolDedupWindowExpiry(t *testing.T) {
	d := newToolDedup(10 * time.Millisecond)
	d.mark("read_file")
	time.Sleep(20 * time.Millisecond)
	if d.seen("\n\n> 🔧 [本地工具] read_file · {}\n") {
		t.Fatal("超出时间窗后不应再抑制")
	}
}

func TestToolKeyFromLine(t *testing.T) {
	cases := map[string]string{
		"\n\n> 🔧 [本地工具] 本地工作区/read_file · {\"path\":\"x\"}\n": "read_file",
		"\n\n> 🔧 [本地工具] 本地工作区/list_dir · {}\n":                "list_dir",
	}
	for line, want := range cases {
		if got := toolKeyFromLine(line); got != want {
			t.Errorf("toolKeyFromLine(%q)=%q want %q", line, got, want)
		}
	}
}

func TestWriteRemoteDeltaToolResult(t *testing.T) {
	var sb strings.Builder
	writeRemoteDelta(&sb, "id", 1, "m", upstream.RemoteEventDelta{ToolResult: "\n\n> ✅ [read_file] 文件内容摘要\n"})
	got := sb.String()
	if !strings.Contains(got, "read_file") {
		t.Fatalf("工具结果应写入 content: %q", got)
	}
	if strings.Contains(got, "tool_calls") {
		t.Errorf("工具结果不应走 tool_calls（应走 content 文本）: %q", got)
	}
}
