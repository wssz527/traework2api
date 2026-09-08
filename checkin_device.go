// checkin_device.go 本机签到设备标识自动读取与每账号独立伪造。
//
// 从 traework2api/internal/auth/checkin_device.go 原样迁移（仅类型名改 traeAuth）。
//
// 签到接口（/ug/checkin_credits/*）要求请求头带独立设备三件套
// （x-device-id / x-device-brand / x-device-type），与模型通道的
// machineId/deviceId 不同。多账号共用同一设备 ID 会导致只有第一个账号能签成
// （上游报 9095），因此每个账号分配一个固定的 16 位数字 ID，生成一次写回长期复用。
package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

// userUniqueIDRe 匹配 leveldb 日志中的设备 ID 明文（10~20 位纯数字）。
var userUniqueIDRe = regexp.MustCompile(`"user_unique_id":"(\d{10,20})"`)

// scanFileMax 单个 leveldb 文件的读取上限。
const scanFileMax = 64 << 20

// DetectLocalCheckinDevice 从本机已登录的 TRAE SOLO 官方客户端本地数据中
// 读取签到设备标识三件套。找不到时 ok=false。
func DetectLocalCheckinDevice() (id, brand, typ string, ok bool) {
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

// clientLeveldbDirs 返回官方客户端 Local Storage leveldb 候选目录。
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

// scanUserUniqueID 在 leveldb 目录中按修改时间从新到旧扫描 .log/.ldb 文件。
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

func detectBrandDarwin() string {
	out, err := exec.Command("sysctl", "-n", "hw.model").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func detectBrandWindows() string {
	out, err := exec.Command("powershell", "-NoProfile", "-Command",
		"(Get-CimInstance Win32_BaseBoard).Product").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// fakeDeviceIDRe 校验既有签到设备 ID 是否为 16 位纯数字。
var fakeDeviceIDRe = regexp.MustCompile(`^\d{16}$`)

// newFakeCheckinID 生成一个 16 位纯数字设备 ID（与官方 user_unique_id 同构；
// hex 含字母的 ID 会触发上游风控 9074）。
func newFakeCheckinID() string {
	const digits = "0123456789"
	var b [16]byte
	rb := make([]byte, 16)
	if _, err := rand.Read(rb); err == nil {
		for i := range b {
			b[i] = digits[int(rb[i])%10]
		}
		return string(b[:])
	}
	return fmt.Sprintf("%016d", time.Now().UnixNano()%1e16)
}

// localBrandType 返回本机品牌与类型（尽力探测，与官方客户端一致）。
func localBrandType() (brand, typ string) {
	switch runtime.GOOS {
	case "darwin":
		typ = "mac"
		brand = detectBrandDarwin()
	case "windows":
		typ = "windows"
		brand = detectBrandWindows()
	default:
		typ = "unknown"
	}
	return brand, typ
}

// EnsurePerAccountCheckinDevice 为账号确保一个固定的、独立于其它账号的
// 签到设备三件套。返回是否写入了新设备标识（调用方据此落盘）。
func EnsurePerAccountCheckinDevice(a *traeAuth) (changed bool) {
	if a.CheckinDeviceID != "" {
		// 已有 ID：仅当它与本机探测的共用 ID 相同（多账号撞车）才迁移。
		if localID, _, _, ok := DetectLocalCheckinDevice(); ok && a.CheckinDeviceID == localID {
			a.CheckinDeviceID = newFakeCheckinID()
			if a.CheckinDeviceBrand == "" {
				if brand, _ := localBrandType(); brand != "" {
					a.CheckinDeviceBrand = brand
				}
			}
			if a.CheckinDeviceType == "" {
				_, typ := localBrandType()
				a.CheckinDeviceType = typ
			}
			return true
		}
		return false
	}
	brand, typ := localBrandType()
	a.CheckinDeviceID = newFakeCheckinID()
	a.CheckinDeviceBrand = brand
	a.CheckinDeviceType = typ
	return true
}
