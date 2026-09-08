// version.go 上游客户端版本自动跟踪。
//
// 目标：上游 / TRAE 客户端发新版时插件自己跟上，不需要手动改 constants.go。
//
// 三级回退（按需求实现，但带"只进不退"保护）：
//  1. 公开网页源：抓 TRAE 官方更新日志页解析最新桌面端版本
//  2. 本机客户端探测：/Applications/TRAE SOLO CN.app 的 CFBundleShortVersionString
//  3. constants.go 内置常量（最终回退）
//
// 为什么不是"网页优先、取到就用"：实测（2026-09-08）两个网页源都停留在
// 0.1.49-52（2026-08-21），而内置常量与本机客户端都是 0.1.63。若网页优先
// 且取到就用，插件会把版本从 0.1.63 降到 0.1.49 —— 这是回退而不是跟踪，
// 上游可能据此拒绝请求或改变行为。因此这里把内置常量作为**下限**：
// 取所有可用来源中的最大值，跟踪只会向前。
//
// 失败静默回退，绝不阻塞请求链路：读取走缓存（无缓存时用内置常量），
// 刷新在后台 goroutine 里做，成功缓存 24h / 失败负缓存 1h。
package main

import (
	"context"
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

// 网页源候选（两个都试，取解析成功的结果）。
// 声明为 var 而非 const：测试可指向 httptest，避免单测依赖真实外网。
var (
	srcChangelog     = "https://www.trae.cn/changelog"
	srcWorkChangelog = "https://docs.trae.cn/work_changelog"
)

// 本机客户端候选路径（可能未安装 / 已卸载）。
var localClientPaths = []string{
	"/Applications/TRAE SOLO CN.app/Contents/Info.plist",
	"/Applications/TRAE CN.app/Contents/Info.plist",
}

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
// 三级探测
// ---------------------------------------------------------------------------

// webSourceURLs 返回当前网页源列表（每次读取，便于测试覆盖）。
func webSourceURLs() []string {
	return []string{srcChangelog, srcWorkChangelog}
}

// probeUpstreamVersion 依次尝试各来源，返回其中版本最大的一个。
// 单个来源失败不影响其它来源。
func probeUpstreamVersion() (upstreamVersion, string, error) {
	var best upstreamVersion
	var bestSrc string
	var firstErr error

	// 1) 公开网页源
	for _, u := range webSourceURLs() {
		body, err := fetchVersionPage(u)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", u, err)
			}
			continue
		}
		v, perr := parseVersionPage(u, body)
		if perr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", u, perr)
			}
			continue
		}
		if compareVersions(v.IdeVersion, best.IdeVersion) > 0 {
			best, bestSrc = v, u
		}
	}

	// 2) 本机客户端探测
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

func fetchVersionPage(url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), versionFetchTO)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", clientUAValue())
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	resp, err := versionHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}

// parseVersionPage 按来源选择解析，返回第一个（最新的）TraeWork 版本。
//
// 先按 URL 域名分派；域名不认识时（源地址变更 / 测试用 httptest）退回按
// 页面内容嗅探，避免换了域名就整个解析失效。两者都失败才算失败。
func parseVersionPage(url string, body []byte) (upstreamVersion, error) {
	switch {
	case strings.Contains(url, "docs.trae.cn"):
		return parseWorkChangelog(body)
	case strings.Contains(url, "www.trae.cn"), strings.Contains(url, "/changelog"):
		return parseChangelog(body)
	}
	// 内容嗅探：结构化日期/版本块 → changelog；markdown "年月日" → work changelog
	switch {
	case reCLItem.Match(body):
		return parseChangelog(body)
	case reWorkDate.Match(body):
		return parseWorkChangelog(body)
	}
	return parseChangelog(body)
}

// ---------------------------------------------------------------------------
// 解析：www.trae.cn/changelog
// ---------------------------------------------------------------------------
//
// 页面结构（实测 2026-09-08）：
//
//	<div class="metaItem-Mgnq7u date-FTRzTs">2026-08-21</div>
//	<div class="metaItem-Mgnq7u version-b47QNh">v<!-- -->0.1.49-52</div>
//	<span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span>
//
// 只取 type=TraeWork（TraeWork 是 0.1.x 系列，与constants.go 同一条产品线）；
// 页面结构变化 → 正则不命中 → 视为失败，绝不产出垃圾值。
var (
	reCLItem = regexp.MustCompile(`date-FTRzTs">(\d{4}-\d{2}-\d{2})</div>.*?version-b47QNh">v?(?:<!--\s*-->)?([0-9][0-9A-Za-z.\-]*?)</div><span class="metaItem-Mgnq7u type-jZDufH">([^<]+)<`)
	// 兜底：宽松匹配 "v1.2.3" 文本
	reCLFallback = regexp.MustCompile(`v?(\d+\.\d+\.\d+)(?:-(\d+))?`)
)

func parseChangelog(body []byte) (upstreamVersion, error) {
	matches := reCLItem.FindAllStringSubmatch(string(body), -1)
	for _, m := range matches {
		if len(m) < 4 {
			continue
		}
		date, rawVer, typ := m[1], m[2], strings.TrimSpace(m[3])
		if !strings.EqualFold(typ, "TraeWork") {
			continue
		}
		v, ok := normalizeVersion(rawVer)
		if !ok {
			continue
		}
		return upstreamVersion{IdeVersion: v, IdeVersionCode: dateToCode(date)}, nil
	}
	// 结构变化 → 宽松兜底：仍要求 0.1.x（TraeWork 产品线），避免抓到 TraeCode 的 3.x
	if m := reCLFallback.FindStringSubmatch(string(body)); m != nil {
		if strings.HasPrefix(m[1], "0.1.") || strings.HasPrefix(m[1], "0.0.") {
			return upstreamVersion{IdeVersion: m[1]}, nil
		}
	}
	return upstreamVersion{}, fmt.Errorf("changelog: no TraeWork version found")
}

// ---------------------------------------------------------------------------
// 解析：docs.trae.cn/work_changelog（markdown 渲染）
// ---------------------------------------------------------------------------
//
// 页面结构（实测 2026-09-08）：
//
//	<h2 id="...">2026 年 08 月 21 日</h2>
//	<p>TraeWork 桌面版 v0.1.49 ~ 0.1.52 版本正式发布，...</p>
var (
	reWorkDate = regexp.MustCompile(`(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日`)
	reWorkVer  = regexp.MustCompile(`v?(\d+\.\d+\.\d+)\s*[~～]\s*v?(\d+\.\d+\.\d+)`)
	// 单版本形态："v0.1.52 版本正式发布"
	reWorkSingle = regexp.MustCompile(`v?(\d+\.\d+\.\d+)\s*版本`)
)

func parseWorkChangelog(body []byte) (upstreamVersion, error) {
	s := string(body)
	// 第一个 <h2> 日期 + 紧随其后的版本区间
	dm := reWorkDate.FindStringSubmatch(s)
	vm := reWorkVer.FindStringSubmatch(s)
	if vm == nil {
		if sm := reWorkSingle.FindStringSubmatch(s); sm != nil {
			return upstreamVersion{IdeVersion: sm[1], IdeVersionCode: dateToCode(dateCNToISO(dm))}, nil
		}
		return upstreamVersion{}, fmt.Errorf("work_changelog: no version found")
	}
	// 区间取上界（如 "v0.1.49 ~ 0.1.52" → 0.1.52）
	hi := vm[2]
	var code string
	if dm != nil {
		code = dateToCode(dateCNToISO(dm))
	}
	return upstreamVersion{IdeVersion: hi, IdeVersionCode: code}, nil
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

// dateCNToISO "2026 年 08 月 21 日" 的 submatch → "2026-08-21"
func dateCNToISO(m []string) string {
	if len(m) < 4 {
		return ""
	}
	y, _ := strconv.Atoi(m[1])
	mo, _ := strconv.Atoi(m[2])
	d, _ := strconv.Atoi(m[3])
	if y < 2000 || mo < 1 || mo > 12 || d < 1 || d > 31 {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d", y, mo, d)
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
		"sources":      webSourceURLs(),
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
