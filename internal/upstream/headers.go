// headers.go SOLO 三类请求头：对话（SOLOHeaders）/ ug（UgHeaders）/ oauth（OAuthHeaders）。
package upstream

import (
	"net/http"

	"traework2api/internal/auth"
)

const clientUA = "Trae/" + IdeVersion

// SOLOHeaders 设置 llm_utils_chat / get_detail_param 所需的 SOLO 专属头。
// 规则来自 SPEC §1 SOLO headers（实测必须）。
func SOLOHeaders(req *http.Request, a *auth.Auth, stream bool) {
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	req.Header.Set("User-Agent", clientUA)
	at := a.JWT() // 读锁快照，防与 RefreshToken 写并发竞态
	req.Header.Set("Authorization", "Cloud-IDE-JWT "+at)
	req.Header.Set("X-Cloudide-Token", at)
	req.Header.Set("X-Ide-Token", at)
	if a.UID != "" {
		req.Header.Set("X-Uid", a.UID)
	}
	req.Header.Set("X-App-Id", AppID)
	req.Header.Set("X-App-Version", "default")
	req.Header.Set("X-Ide-Version", IdeVersion)
	req.Header.Set("X-Ide-Version-Code", IdeVersionCode)
	req.Header.Set("X-App-Version-Code", IdeVersionCode)
	req.Header.Set("X-Ide-Version-Type", "stable")
	req.Header.Set("X-Device-Type", "windows")
	req.Header.Set("X-OS-Version", OSVersion)
	req.Header.Set("X-Device-Brand", DeviceBrand)
	req.Header.Set("Request-Traffic-Type", "prod")
	if a.MachineID != "" {
		req.Header.Set("X-Machine-Id", a.MachineID)
	}
	if a.DeviceID != "" {
		req.Header.Set("X-Device-Id", a.DeviceID)
	}
}

// UgHeaders 设置签到/积分（api.trae.cn）所需头。
func UgHeaders(req *http.Request, a *auth.Auth) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	req.Header.Set("Authorization", "Cloud-IDE-JWT "+a.JWT()) // 读锁快照
	req.Header.Set("X-User-Region", "CN")
	if a.CheckinDeviceID != "" {
		req.Header.Set("X-Device-Id", a.CheckinDeviceID)
	}
	if a.CheckinDeviceBrand != "" {
		req.Header.Set("X-Device-Brand", a.CheckinDeviceBrand)
	}
	if a.CheckinDeviceType != "" {
		req.Header.Set("X-Device-Type", a.CheckinDeviceType)
	}
}

// OAuthHeaders 设置 ExchangeToken / GetUserInfo 所需头（无签名，仅 UA）。
func OAuthHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
}

// RemoteHeaders 设置 remote 通道（chat_sessions/messages）请求头。
// 与 python 成功路径完全一致（2026-08-27 实测 26 头）。
func RemoteHeaders(req *http.Request, a *auth.Auth) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", "Cloud-IDE-JWT "+a.JWT())
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) TRAESOLOCN/1.107.1 Chrome/142.0.7444.235 Electron/39.2.7 Safari/537.36")
	req.Header.Set("X-Preferenced-Language", "zh-cn")
	req.Header.Set("X-Trae-Client-Type", "lite")
	req.Header.Set("X-Trae-User-Timezone", "Asia/Shanghai")
	req.Header.Set("X-User-Region", "CN")
	req.Header.Set("X-Lgw-Req-Sdk-Type", "3")
	req.Header.Set("Package-Type", "stable_cn")
	req.Header.Set("App-Version", "0.1.56")
	req.Header.Set("X-Request-Id", newUUID())
	req.Header.Set("X-Ss-Dp", "787976")
	req.Header.Set("X-Tt-Trace-Id", "00-"+randomHex(32)+"-0000000000000000-01")
	req.Header.Set("X-Ide-Token", a.JWT())
	req.Header.Set("X-Device-Id", a.DeviceID)
	req.Header.Set("X-App-Id", AppID)
	req.Header.Set("X-Ide-Version", "0.1.56")
	req.Header.Set("X-Ide-Version-Code", "20260820")
	req.Header.Set("X-Ide-Version-Type", "stable")
	req.Header.Set("X-Machine-Id", a.MachineID)
	req.Header.Set("X-Device-Type", "mac")
	req.Header.Set("X-Device-Brand", "Mac16,10")
	req.Header.Set("X-Device-Cpu", "Apple")
	req.Header.Set("Request-Traffic-Type", "prod")
	req.Header.Set("Origin", "vscode-file://vscode-app")
}
