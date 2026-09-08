// checkin_device.go 本机签到设备标识自动读取与每账号独立伪造。
//
// 签到接口（/ug/checkin_credits/*）要求请求头带独立设备三件套
// （x-device-id / x-device-brand / x-device-type），且与模型通道的
// machineId/deviceId 不同。官方客户端（TRAE SOLO CN）把该设备 ID 以
// "user_unique_id":"<16位数字>" 明文保存在 Chromium Local Storage
// leveldb 里（mitm 抓包确认：leveldb 中的值与签到请求 x-device-id
// 完全一致）。
//
// 多账号场景：若所有账号都从本机官方客户端读取到同一个设备 ID，
// 上游按（账号,设备）维度记录签到，共用 ID 会导致只有第一个账号能签成、
// 其余报 9095。因此本文件同时提供伪造设备三件套生成：
// 每个账号分配一个固定的 16 位数字 ID（与官方 user_unique_id 同构，
// brand/type 取本机真实机型），生成一次后写回凭证长期复用，不随重启变化。
package auth

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

// fakeDeviceIDRe 校验既有签到设备 ID 是否为"本文件生成的伪造 ID"：
// 16 位纯数字。官方 user_unique_id 也是 16 位数字，二者同构——该判断
// 仅用于决定是否迁移（共用本机探测 ID 的旧账号），不区分来源。
var fakeDeviceIDRe = regexp.MustCompile(`^\d{16}$`)

// newFakeCheckinID 生成一个 16 位纯数字设备 ID（与官方 user_unique_id
// 同构，实测纯数字 16 位可正常签到；hex 含字母的设备 ID 触发上游风控 9074）。
// 用 crypto/rand 逐字节取 0-9，避免 math/rand 种子可预测导致多账号 ID 相关。
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
	// crypto/rand 失败（极罕见）：退回时间戳派生，仍保证 16 位数字
	return fmt.Sprintf("%016d", time.Now().UnixNano()%1e16)
}

// localBrandType 返回本机品牌与类型（尽力探测，与官方客户端一致）。
// 失败时回退通用值：brand 为空串、type 按 OS。空 brand 实测不影响签到
// （上游以 device-id 为准），但尽量带上真实机型更接近官方形态。
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
// 签到设备三件套。规则：
//
//  1. 凭证已有签到设备 ID 且不是本机探测的共用 ID（见 DetectLocalCheckinDevice
//     的返回）→ 视为已固定，原样保留不动（历史账号维持稳定，避免频繁换设备
//     触发风控）。
//  2. 凭证缺 ID，或 ID 等于本机官方客户端探测到的共用 ID（多账号撞同一设备
//     → 上游只让第一个签成）→ 生成新的固定伪造 ID（16 位数字 + 本机
//     brand/type），写回凭证。
//
// 返回是否写入了新设备标识（调用方据此 SaveAtomic）。
func EnsurePerAccountCheckinDevice(a *Auth) (changed bool) {
	if a.CheckinDeviceID != "" {
		// 已有 ID：仅当它与本机探测的共用 ID 相同（多账号撞车）才需要迁移。
		if localID, _, _, ok := DetectLocalCheckinDevice(); ok && a.CheckinDeviceID == localID {
			// 撞车：迁移到独立伪造 ID（保留原 brand/type 风格）
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
		return false // 已有独立/真实 ID，保持不动
	}

	// 无 ID：生成固定伪造 ID（不再探测共用本机 ID）
	brand, typ := localBrandType()
	a.CheckinDeviceID = newFakeCheckinID()
	a.CheckinDeviceBrand = brand
	a.CheckinDeviceType = typ
	return true
}
