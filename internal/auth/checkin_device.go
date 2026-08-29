// checkin_device.go 本机签到设备标识自动读取。
//
// 签到接口（/ug/checkin_credits/*）要求请求头带独立设备三件套
// （x-device-id / x-device-brand / x-device-type），且与模型通道的
// machineId/deviceId 不同。官方客户端（TRAE SOLO CN）把该设备 ID 以
// "user_unique_id":"<16位数字>" 明文保存在 Chromium Local Storage
// leveldb 里（mitm 抓包确认：leveldb 中的值与签到请求 x-device-id
// 完全一致）。本文件负责在凭证缺少 checkinDeviceId 时从本机官方客户端
// 自动读取，避免用户手工折腾；读取失败时由调用方明确报错并提示手动获取。
package auth

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

// userUniqueIDRe 匹配 leveldb 日志中的设备 ID 明文（10~20 位纯数字）。
var userUniqueIDRe = regexp.MustCompile(`"user_unique_id":"(\d{10,20})"`)

// scanFileMax 单个 leveldb 文件的读取上限（防御异常大文件，正常 .log/.ldb 远小于此）。
const scanFileMax = 64 << 20

// DetectLocalCheckinDevice 从本机已登录的 TRAE SOLO 官方客户端本地数据中
// 读取签到设备标识三件套。找不到（未安装/未登录官方客户端，或不支持的
// 平台）时 ok=false，由调用方决定跳过或要求手动填写。brand 为尽力探测，
// 个别环境可能为空（签到结果校验会兜底暴露问题）。
func DetectLocalCheckinDevice() (id, brand, typ string, ok bool) {
	// 环境变量兜底开关：禁止探测本机客户端数据（测试/隐私敏感环境用）。
	if os.Getenv("TW2A_DISABLE_CHECKIN_DETECT") != "" {
		return "", "", "", false
	}
	switch runtime.GOOS {
	case "darwin":
		typ = "mac"
	case "windows":
		typ = "windows"
	default:
		return "", "", "", false
	}
	for _, dir := range clientLeveldbDirs() {
		if id, ok = scanUserUniqueID(dir); ok {
			break
		}
	}
	if !ok {
		return "", "", "", false
	}
	if runtime.GOOS == "darwin" {
		brand = detectBrandDarwin()
	} else {
		brand = detectBrandWindows()
	}
	return id, brand, typ, true
}

// clientLeveldbDirs 返回官方客户端 Local Storage leveldb 候选目录
// （按优先级：SOLO CN 在前，兼容普通版 CN）。
func clientLeveldbDirs() []string {
	switch runtime.GOOS {
	case "darwin":
		home, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		base := filepath.Join(home, "Library", "Application Support")
		return []string{
			filepath.Join(base, "TRAE SOLO CN", "Local Storage", "leveldb"),
			filepath.Join(base, "TRAE CN", "Local Storage", "leveldb"),
		}
	case "windows":
		roaming := os.Getenv("APPDATA")
		if roaming == "" {
			return nil
		}
		return []string{
			filepath.Join(roaming, "TRAE SOLO CN", "Local Storage", "leveldb"),
			filepath.Join(roaming, "TRAE CN", "Local Storage", "leveldb"),
		}
	}
	return nil
}

// scanUserUniqueID 在 leveldb 目录中按修改时间从新到旧扫描 .log/.ldb
// 文件，返回第一个命中的 user_unique_id。读失败/未命中返回 false。
func scanUserUniqueID(dir string) (string, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}
	type f struct {
		path    string
		modTime int64
	}
	var files []f
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") && !strings.HasSuffix(name, ".ldb") {
			continue
		}
		info, err := e.Info()
		if err != nil || info.Size() > scanFileMax {
			continue
		}
		files = append(files, f{filepath.Join(dir, name), info.ModTime().UnixNano()})
	}
	// 新的在前：设备 ID 可能随客户端重装变化，以最新写入为准。
	sort.Slice(files, func(i, j int) bool { return files[i].modTime > files[j].modTime })
	for _, file := range files {
		raw, err := os.ReadFile(file.path)
		if err != nil {
			continue
		}
		if m := userUniqueIDRe.FindSubmatch(raw); m != nil {
			return string(m[1]), true
		}
	}
	return "", false
}

// detectBrandDarwin 读 Mac 机型（如 "Mac16,10"），与官方客户端
// x-device-brand 一致。失败返回空串。
func detectBrandDarwin() string {
	out, err := exec.Command("sysctl", "-n", "hw.model").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// detectBrandWindows 读主板型号（官方 Windows 客户端 x-device-brand
// 即主板 product 名）。wmic 已弃用，走 PowerShell CIM；失败返回空串。
func detectBrandWindows() string {
	out, err := exec.Command("powershell", "-NoProfile", "-Command",
		"(Get-CimInstance Win32_BaseBoard).Product").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
