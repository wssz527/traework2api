// version.go 上游客户端版本自动跟踪。
//
// 目标：上游 / TRAE 客户端发新版时插件自己跟上，不需要手动改 constants.go。
//
// 版本源优先级（带"只进不退"保护）：
//  1. check_update API（主源）：官方公开无鉴权实时接口，返回 data.appVersion
//     （实测与客户端同步，官方 changelog 落后约 6 个补丁级，已弃用）
//  2. 本机客户端探测：/Applications/TRAE SOLO CN.app 的 CFBundleShortVersionString
//     （本地客户端存在时作为交叉校验；已卸载客户端则可选忽略）
//  3. constants.go 内置常量（最终回退 / 下限）
//
// 只进不退：内置常量作为**下限**，任何来源返回的版本低于内置值时忽略，
// 取所有可用来源中的最大值，跟踪只会向前，绝不降级。
//
// 失败静默回退，绝不阻塞请求链路：读取走缓存（无缓存时用内置常量），
// 刷新在后台 goroutine 里做，成功缓存 24h / 失败负缓存 1h。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 版本缓存时长。
const (
	versionSuccessTTL = 24 * time.Hour
	versionFailTTL    = 1 * time.Hour
	versionFetchTO    = 20 * time.Second
)

// checkUpdateAPI 是官方版本实时接口（主源）。声明为 var 而非 const：
// 测试可指向 httptest，避免单测依赖真实外网。
var checkUpdateAPI = "https://api.trae.com.cn/icube/api/v1/package/check_update"

// 本机客户端候选路径（可能未安装 / 已卸载）。
var localClientPaths = []string{
	"/Applications/TRAE SOLO CN.app/Contents/Info.plist",
	"/Applications/TRAE CN.app/Contents/Info.plist",
}

// checkUpdateParams 是 check_update 请求的固定参数子集（实测最小可返回正常结果）。
// mid 只需非空；uid/did/buildId 等客户端专用参数均可省略。
var checkUpdateParams = []string{
	"branch=release_solo_cn",
	"packageType=stable_cn",
	"productCode=SOLO_Lite",
	"platform=Mac",
	"arch=arm64",
	"userRegion=CN",
	"tenant=marscode",
}

// IdeBuildVersion 是内置的 tronBuildVersion（与 IdeVersion 配套；见
// trae-version-source.md 实测 2.3.81345）。check_update 用它作为当前已知值。
const IdeBuildVersion = "2.3.81345"

// upstreamVersion 一对版本标识。
type upstreamVersion struct {
	IdeVersion     string
	IdeVersionCode string
}

// builtinVersion 是内置常量，同时作为"只进不退"的下限。
func builtinVersion() upstreamVersion {
	return upstreamVersion{IdeVersion: IdeVersion, IdeVersionCode: IdeVersionCode}
}

// ---------------------------------------------------------------------------
// 缓存状态
// ---------------------------------------------------------------------------

type versionCacheEntry struct {
	v        upstreamVersion
	fetched  time.Time
	ok       bool // false = 负缓存（上次探测失败）
	sourceOf string
}

// versionAutoProbe 控制"无缓存时是否后台自动探测"。默认开启；
// 单测关闭它以获得确定性（后台 goroutine 会在测试断言之间写缓存）。
var versionAutoProbe atomic.Bool

var (
	versionCache atomic.Pointer[versionCacheEntry]
	versionMu    sync.Mutex // 串行化后台刷新，避免并发重复抓网页
	versionTrack atomic.Bool
)

func init() {
	versionTrack.Store(true)
	versionAutoProbe.Store(true)
}

func versionTrackingEnabled() bool { return versionTrack.Load() }

func setVersionTracking(v bool) { versionTrack.Store(v) }

// currentUpstreamVersion 返回当前应使用的版本对。
//
// 永不阻塞：缓存命中直接返回；缓存缺失或过期时用内置常量兜底，并在后台
// 触发一次刷新（下次调用即可看到新值）。
func currentUpstreamVersion() upstreamVersion {
	if !versionTrackingEnabled() {
		return builtinVersion()
	}
	if e := versionCache.Load(); e != nil {
		if e.ok {
			return e.v
		}
		// 负缓存：未到期就不再探测
		if time.Since(e.fetched) < versionFailTTL {
			return builtinVersion()
		}
		return builtinVersion()
	}
	// 首次调用：后台刷新，本次先用内置常量
	if versionAutoProbe.Load() {
		go refreshUpstreamVersion()
	}
	return builtinVersion()
}

// refreshUpstreamVersion 后台探测并更新缓存。任何失败都静默回退。
func refreshUpstreamVersion() {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in version refresh: %v", r)
		}
	}()
	if !versionTrackingEnabled() {
		return
	}
	// 已有有效缓存且未过期 → 不重复抓（防 config reload 打爆网页源）
	if e := versionCache.Load(); e != nil && e.ok && time.Since(e.fetched) < versionSuccessTTL {
		return
	}
	// 串行化：同一时刻只允许一个探测在跑
	if !versionMu.TryLock() {
		return
	}
	defer versionMu.Unlock()
	probeAndStoreVersion()
}

// probeAndStoreVersion 执行一次探测并把结果写入缓存（含负缓存）。
// 与 refreshUpstreamVersion 分开：后者负责加锁，便于测试直接驱动本函数。
func probeAndStoreVersion() {
	v, src, err := probeUpstreamVersion()
	if err != nil || !validVersion(v.IdeVersion) {
		pluginLogf("ide version probe failed (falling back): %v", err)
		versionCache.Store(&versionCacheEntry{v: builtinVersion(), fetched: time.Now(), ok: false})
		return
	}
	// 只进不退：取到的版本低于内置常量时沿用内置值。
	final := v
	if compareVersions(v.IdeVersion, IdeVersion) < 0 {
		pluginLogf("ide version %s from %s is older than built-in %s — keeping built-in",
			v.IdeVersion, src, IdeVersion)
		final = builtinVersion()
	}
	if !validVersionCode(final.IdeVersionCode) {
		final.IdeVersionCode = IdeVersionCode
	}
	versionCache.Store(&versionCacheEntry{v: final, fetched: time.Now(), ok: true, sourceOf: src})
	pluginLogf("ide version=%s code=%s (source=%s)", final.IdeVersion, final.IdeVersionCode, src)
}

// ---------------------------------------------------------------------------
// 版本源探测
// ---------------------------------------------------------------------------

// versionSources 返回当前版本源列表（按优先级，管理接口展示用）。
// 每次读取，便于测试覆盖。
func versionSources() []string {
	return []string{"api:" + checkUpdateAPI, "local:" + localClientPaths[0]}
}

// probeUpstreamVersion 依次尝试各来源，返回其中版本最大的一个。
// 单个来源失败不影响其它来源。
func probeUpstreamVersion() (upstreamVersion, string, error) {
	var best upstreamVersion
	var bestSrc string
	var firstErr error

	// 1) check_update API（主源，官方实时）
	v, src, err := probeCheckUpdateAPI()
	if err != nil {
		firstErr = fmt.Errorf("check_update: %w", err)
	} else {
		best, bestSrc = v, src
	}

	// 2) 本机客户端探测（交叉校验；客户端已卸载时可省略）
	for _, p := range localClientPaths {
		v, err := probeLocalClient(p)
		if err != nil {
			continue
		}
		if compareVersions(v.IdeVersion, best.IdeVersion) > 0 {
			best, bestSrc = v, "local:"+p
		}
		break
	}

	if !validVersion(best.IdeVersion) {
		if firstErr != nil {
			return upstreamVersion{}, "", firstErr
		}
		return upstreamVersion{}, "", fmt.Errorf("no source produced a usable version")
	}
	return best, bestSrc, nil
}

var versionHTTPClient = &http.Client{Timeout: versionFetchTO}

// checkUpdateResponse 是 /icube/api/v1/package/check_update 的响应形状（关注字段）。
// 实测：本地已是最新时响应只有 {"data":{"needUpdate":false}}，不含 appVersion。
type checkUpdateResponse struct {
	ErrCode int    `json:"err_code"`
	Data    struct {
		NeedUpdate bool   `json:"needUpdate"`
		AppVersion string `json:"appVersion"`
	} `json:"data"`
}

// checkUpdateURL 构造 check_update 请求 URL（appVersion/buildVersion 传当前
// 已知值，API 据此返回最新版；mid 非空即可，用内置固定值保证可复现）。
func checkUpdateURL() string {
	params := make([]string, 0, len(checkUpdateParams)+3)
	params = append(params, checkUpdateParams...)
	params = append(params,
		"appVersion="+IdeVersion,
		"buildVersion="+IdeBuildVersion,
		"mid=0123456789abcdef0123456789abcdef",
	)
	return checkUpdateAPI + "?" + strings.Join(params, "&")
}

// probeCheckUpdateAPI 请求官方实时接口并解析 data.appVersion。
// 成功返回该版本（src 记为 "api:"+checkUpdateAPI）。
func probeCheckUpdateAPI() (upstreamVersion, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), versionFetchTO)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkUpdateURL(), nil)
	if err != nil {
		return upstreamVersion{}, "", err
	}
	req.Header.Set("User-Agent", clientUAValue())
	req.Header.Set("Accept", "application/json")
	resp, err := versionHTTPClient.Do(req)
	if err != nil {
		return upstreamVersion{}, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return upstreamVersion{}, "", fmt.Errorf("http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return upstreamVersion{}, "", err
	}
	return parseCheckUpdateResponse(body)
}

// parseCheckUpdateResponse 解析 check_update 的 JSON 响应。
// 两种情况都视为成功：
//   - needUpdate==true 且 data.appVersion 是合法三段式版本号 → 采纳新版本
//   - needUpdate==false（本地已最新，响应不含 appVersion）→ 确认内置值仍最新
// 其余（err_code!=0、appVersion 非法、结构缺失）一律失败，绝不产出垃圾值。
func parseCheckUpdateResponse(body []byte) (upstreamVersion, string, error) {
	var resp checkUpdateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return upstreamVersion{}, "", fmt.Errorf("json: %w", err)
	}
	if resp.ErrCode != 0 {
		return upstreamVersion{}, "", fmt.Errorf("err_code=%d", resp.ErrCode)
	}
	var v string
	if resp.Data.NeedUpdate {
		norm, ok := normalizeVersion(strings.TrimSpace(resp.Data.AppVersion))
		if !ok {
			return upstreamVersion{}, "", fmt.Errorf("appVersion %q not a valid version", resp.Data.AppVersion)
		}
		v = norm
	} else {
		// 本地已是最新：接口未回传版本号，视为确认当前内置版本为最新。
		v = IdeVersion
	}
	// check_update 不返回日期型 version code（IdeVersionCode），沿用内置值。
	return upstreamVersion{IdeVersion: v, IdeVersionCode: IdeVersionCode}, "api:" + checkUpdateAPI, nil
}

// ---------------------------------------------------------------------------
// 解析：本机客户端 Info.plist
// ---------------------------------------------------------------------------

var rePlistVersion = regexp.MustCompile(`<key>CFBundleShortVersionString</key>\s*<string>([^<]+)</string>`)

func probeLocalClient(path string) (upstreamVersion, error) {
	if path == "" {
		return upstreamVersion{}, fmt.Errorf("empty path")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return upstreamVersion{}, err
	}
	// XML plist
	if m := rePlistVersion.FindSubmatch(raw); m != nil {
		if v, ok := normalizeVersion(strings.TrimSpace(string(m[1]))); ok {
			// 本机客户端没有日期型 version code，沿用内置值
			return upstreamVersion{IdeVersion: v, IdeVersionCode: IdeVersionCode}, nil
		}
	}
	// 二进制 plist / 其它形态 → 走 defaults
	if out, err := exec.Command("defaults", "read", strings.TrimSuffix(path, "/Contents/Info.plist"), "CFBundleShortVersionString").Output(); err == nil {
		if v, ok := normalizeVersion(strings.TrimSpace(string(out))); ok {
			return upstreamVersion{IdeVersion: v, IdeVersionCode: IdeVersionCode}, nil
		}
	}
	return upstreamVersion{}, fmt.Errorf("local client: no CFBundleShortVersionString in %s", filepath.Base(path))
}

// ---------------------------------------------------------------------------
// 版本工具
// ---------------------------------------------------------------------------

// reVersion 严格匹配 x.y.z（保守：不匹配就不接受）。
var reVersion = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// normalizeVersion 把 "0.1.49-52" / "v0.1.52" / "0.1.49 ~ 0.1.52" 归一化为 "x.y.z"。
// 无法解析出合法三段式版本号时返回 false。
func normalizeVersion(s string) (string, bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "v"), "V")
	m := reVersion.FindStringSubmatch(s)
	if m == nil {
		return "", false
	}
	return m[1] + "." + m[2] + "." + m[3], true
}

func validVersion(v string) bool {
	_, ok := normalizeVersion(v)
	return ok
}

// reVersionCode 日期型版本号：8 位 YYYYMMDD。
var reVersionCode = regexp.MustCompile(`^20\d{6}$`)

func validVersionCode(c string) bool {
	return reVersionCode.MatchString(strings.TrimSpace(c))
}

// compareVersions 语义化比较，返回 -1/0/1。非法版本视为最小。
func compareVersions(a, b string) int {
	pa, okA := versionParts(a)
	pb, okB := versionParts(b)
	switch {
	case !okA && !okB:
		return 0
	case !okA:
		return -1
	case !okB:
		return 1
	}
	for i := 0; i < 3; i++ {
		if pa[i] < pb[i] {
			return -1
		}
		if pa[i] > pb[i] {
			return 1
		}
	}
	return 0
}

func versionParts(v string) ([3]int, bool) {
	m := reVersion.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return [3]int{}, false
	}
	var out [3]int
	for i := 1; i <= 3; i++ {
		n, err := strconv.Atoi(m[i])
		if err != nil {
			return [3]int{}, false
		}
		out[i-1] = n
	}
	return out, true
}

// dateToCode "2026-08-21" → "20260821"
func dateToCode(iso string) string {
	iso = strings.TrimSpace(iso)
	if iso == "" {
		return ""
	}
	var y, m, d int
	if _, err := fmt.Sscanf(iso, "%d-%d-%d", &y, &m, &d); err != nil {
		return ""
	}
	if y < 2000 || m < 1 || m > 12 || d < 1 || d > 31 {
		return ""
	}
	return fmt.Sprintf("%04d%02d%02d", y, m, d)
}

// ---------------------------------------------------------------------------
// 供 headers/client 使用的访问器（替代直接读常量）
// ---------------------------------------------------------------------------

// ideVersion 返回当前应上报的 IdeVersion。
func ideVersion() string {
	v := currentUpstreamVersion().IdeVersion
	if !validVersion(v) {
		return IdeVersion
	}
	return v
}

// ideVersionCode 返回当前应上报的 IdeVersionCode。
func ideVersionCode() string {
	c := currentUpstreamVersion().IdeVersionCode
	if !validVersionCode(c) {
		return IdeVersionCode
	}
	return c
}

// clientUAValue 构造 User-Agent（版本来自跟踪结果）。
func clientUAValue() string { return "Trae/" + ideVersion() }

// versionStatus 面板/状态接口展示用（不含任何凭证）。
func versionStatus() map[string]any {
	e := versionCache.Load()
	out := map[string]any{
		"tracking":     versionTrackingEnabled(),
		"ide_version":  ideVersion(),
		"version_code": ideVersionCode(),
		"builtin":      map[string]any{"ide_version": IdeVersion, "version_code": IdeVersionCode},
		"sources":      versionSources(),
	}
	if e != nil {
		out["cache"] = map[string]any{
			"ok":       e.ok,
			"source":   e.sourceOf,
			"fetched":  e.fetched.UTC().Format(time.RFC3339),
			"age_secs": int64(time.Since(e.fetched).Seconds()),
		}
	} else {
		out["cache"] = map[string]any{"ok": false, "source": "", "fetched": "", "age_secs": 0}
	}
	return out
}

// ---------------------------------------------------------------------------
// 周期刷新（默认 24h，可配）
// ---------------------------------------------------------------------------

var (
	versionTrackInterval = versionSuccessTTL
	versionTrackIntMu    sync.RWMutex
	versionStop          chan struct{}
	versionStartMu       sync.Mutex
)

func loadedVersionInterval() time.Duration {
	versionTrackIntMu.RLock()
	defer versionTrackIntMu.RUnlock()
	if versionTrackInterval <= 0 {
		return versionSuccessTTL
	}
	return versionTrackInterval
}

func setVersionInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	versionTrackIntMu.Lock()
	versionTrackInterval = d
	versionTrackIntMu.Unlock()
}

// ensureVersionTracker 启动版本跟踪 goroutine（幂等）。
// 与 scheduler 的整点触发无关：版本刷新按固定间隔走。
//
// versionAutoProbe 为 false 时不启动（单测用）：否则后台 goroutine 会在
// 测试断言之间抢占探测锁并改写缓存，造成偶发失败。
func ensureVersionTracker() {
	if !versionAutoProbe.Load() {
		return
	}
	versionStartMu.Lock()
	defer versionStartMu.Unlock()
	if versionStop != nil {
		return
	}
	versionStop = make(chan struct{})
	go versionTrackerLoop(versionStop)
	// 启动后立刻探测一次（异步，不阻塞注册）。
	go refreshUpstreamVersion()
}

func versionTrackerLoop(stop chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in version tracker: %v", r)
		}
	}()
	ticker := time.NewTicker(loadedVersionInterval())
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			refreshUpstreamVersion()
		}
	}
}

// resetVersionCacheForTest 清空缓存（测试用）。
func resetVersionCacheForTest() { versionCache.Store(nil) }

// setVersionCacheForTest 注入缓存值（测试用）。
func setVersionCacheForTest(v upstreamVersion, ok bool, src string) {
	versionCache.Store(&versionCacheEntry{v: v, fetched: time.Now(), ok: ok, sourceOf: src})
}
