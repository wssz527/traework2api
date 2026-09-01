// eventsprobe — 验证 remote 通道实时事件端点：
//
//	GET {RemoteHost}/api/remote/v1/chat_sessions/{id}/events   (SSE, type=subscribe)
//
// 逆向来源：solo-lite bundle 的 remote 路由表
//
//	"chat.subscribe":{method:"GET",
//	  path:"/api/remote/v1/chat_sessions/:chat_session_id/events",
//	  type:"subscribe",transport:"api"}
//
// 用法：
//   eventsprobe              # 建会话（不发消息，零 Work 积分），只开 SSE 看是否有帧
//   SEND=1 eventsprobe       # 发一条真实任务（烧积分），完整看事件流
package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

func main() {
	accs, err := auth.LoadDir("/tmp/tw2api-e2e/auths")
	if err != nil || len(accs) == 0 {
		fmt.Println("FATAL: 账号加载失败:", err)
		os.Exit(1)
	}
	a := accs[0]
	c := upstream.New()
	if refreshed, err := c.RefreshTokenIfNeeded(a, 5*time.Minute); err == nil && refreshed {
		_ = a.SaveAtomic()
	}

	sid, err := c.RemoteCreateSession(a)
	if err != nil {
		fmt.Println("FATAL: 建会话失败:", err)
		os.Exit(1)
	}
	fmt.Printf("[session] id=%s\n", sid)
	defer func() {
		_ = c.RemoteDeleteSession(a, sid)
		fmt.Println("[cleanup] 会话已删除")
	}()

	if os.Getenv("SEND") == "1" {
		msg := os.Getenv("MSG")
		if msg == "" {
			msg = "先用一句话说你要做什么，然后调用你的本地 MCP 工具 read_file 读 /Users/wssz277/AgentProjects/trae-local-mcp/package.json 前 5 行，最后一句话总结"
		}
		mid, err := c.RemoteSendMessage(a, sid, "glm-5.3", msg, false)
		if err != nil {
			fmt.Println("FATAL: 发消息失败:", err)
			os.Exit(1)
		}
		fmt.Printf("[send] message_id=%s\n", mid)
	}

	// 开 SSE。GET 无 body，仅带 RemoteHeaders + Accept: text/event-stream。
	url := upstream.RemoteHostBase() + "/api/remote/v1/chat_sessions/" + sid + "/events"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fmt.Println("FATAL:", err)
		os.Exit(1)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	upstream.RemoteHeaders(req, a) // 会覆盖 Accept，故下面重设
	req.Header.Set("Accept", "text/event-stream")
	if v := os.Getenv("LAST_EVENT_ID"); v != "" {
		req.Header.Set("Last-Event-ID", v)
	}

	fmt.Printf("[sse] GET %s\n", url)
	client := &http.Client{} // 无总超时：长流
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("[sse] 请求失败:", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("[sse] status=%d content-type=%q\n", resp.StatusCode, resp.Header.Get("Content-Type"))
	for _, k := range []string{"X-Tt-Logid", "X-Request-Id", "Transfer-Encoding"} {
		if v := resp.Header.Get(k); v != "" {
			fmt.Printf("[sse] hdr %s=%s\n", k, v)
		}
	}
	if resp.StatusCode != http.StatusOK {
		buf := make([]byte, 4096)
		n, _ := resp.Body.Read(buf)
		fmt.Printf("[sse] body=%s\n", string(buf[:n]))
		return
	}

	// 读流：按 SSE 帧解析，落盘 /tmp/ws-probe.jsonl（沿用同一路径，便于报告引用）。
	f, _ := os.OpenFile("/tmp/ws-probe.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	defer f.Close()

	deadline := time.Now().Add(time.Duration(atoiDef(os.Getenv("SECS"), 90)) * time.Second)
	_ = deadline
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	go func() { // 超时兜底：关闭 body 让扫描结束
		time.Sleep(time.Duration(atoiDef(os.Getenv("SECS"), 90)) * time.Second)
		resp.Body.Close()
	}()
	n := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		n++
		ts := time.Now().Format("15:04:05.000")
		disp := line
		if len(disp) > 700 {
			disp = disp[:700] + fmt.Sprintf(" ...(%d bytes)", len(line))
		}
		fmt.Printf("[%s] %s\n", ts, disp)
		if f != nil {
			rec := fmt.Sprintf(`{"dir":"sse","ts":%d,"raw":%q}`+"\n",
				time.Now().UnixMilli(), line)
			_, _ = f.WriteString(rec)
		}
	}
	fmt.Printf("[sse] 流结束，共 %d 非空行 (err=%v)\n", n, sc.Err())
	if n == 0 {
		fmt.Println("[sse] 结论：端点可达但无任何事件（可能需先发消息，或需额外查询参数）")
	}
	_ = strings.TrimSpace("")
	_ = context.Background()
}

func atoiDef(s string, d int) int {
	if s == "" {
		return d
	}
	v := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return d
		}
		v = v*10 + int(ch-'0')
	}
	return v
}
