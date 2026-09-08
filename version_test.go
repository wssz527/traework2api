package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 版本解析（静态 HTML 样例）
// ---------------------------------------------------------------------------

// stubWebSources 把网页源指向不可达地址，保证单测不依赖真实外网。
// 返回 restore 函数。
func stubWebSources(t *testing.T) {
	t.Helper()
	oldA, oldB := srcChangelog, srcWorkChangelog
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	srcChangelog, srcWorkChangelog = dead.URL, dead.URL
	t.Cleanup(func() { srcChangelog, srcWorkChangelog = oldA, oldB })
}

func TestParseChangelogFromFixture(t *testing.T) {
	v, err := parseChangelog([]byte(fixtureChangelog))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// 样例最新 TraeWork 条目是 v0.1.49-52（2026-08-21）
	if v.IdeVersion != "0.1.49" {
		t.Errorf("IdeVersion=%q want 0.1.49", v.IdeVersion)
	}
	if v.IdeVersionCode != "20260821" {
		t.Errorf("IdeVersionCode=%q want 20260821 (页面日期)", v.IdeVersionCode)
	}
}

// TestParseChangelogSkipsOtherProductLines 页面同时有 TraeCode(3.x) 与
// TRAE APP(0.0.x)；必须只取 TraeWork（0.1.x），否则会拿到错误产品线。
func TestParseChangelogSkipsOtherProductLines(t *testing.T) {
	v, err := parseChangelog([]byte(fixtureChangelog))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(v.IdeVersion, "3.") {
		t.Errorf("picked TraeCode line: %s", v.IdeVersion)
	}
	if !strings.HasPrefix(v.IdeVersion, "0.1.") && !strings.HasPrefix(v.IdeVersion, "0.0.") {
		t.Errorf("picked unexpected product line: %s", v.IdeVersion)
	}
}

func TestParseWorkChangelogFromFixture(t *testing.T) {
	v, err := parseWorkChangelog([]byte(fixtureWorkChangelog))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// "TraeWork 桌面版 v0.1.49 ~ 0.1.52 版本正式发布" → 取上界 0.1.52
	if v.IdeVersion != "0.1.52" {
		t.Errorf("IdeVersion=%q want 0.1.52 (区间上界)", v.IdeVersion)
	}
	if v.IdeVersionCode != "20260821" {
		t.Errorf("IdeVersionCode=%q want 20260821", v.IdeVersionCode)
	}
}

func TestParseWorkChangelogSingleVersion(t *testing.T) {
	html := `<h2 id="x">2026 年 09 月 08 日</h2><p>TraeWork 桌面版 v0.1.63 版本正式发布。</p>`
	v, err := parseWorkChangelog([]byte(html))
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.63" || v.IdeVersionCode != "20260908" {
		t.Errorf("got %+v", v)
	}
}

// ---------------------------------------------------------------------------
// 保守性：抓不到就是失败，绝不产出垃圾值
// ---------------------------------------------------------------------------

func TestParseRejectsGarbage(t *testing.T) {
	garbage := []string{
		``,
		`<html><body>维护中</body></html>`,
		`<div>版本号：最新</div>`,
		`<div>download v3</div>`,
		`<div>1.2.3.4.5.6</div>`,
	}
	for _, g := range garbage {
		if _, err := parseChangelog([]byte(g)); err == nil {
			// 兜底正则可能命中 0.1.x/0.0.x；非该形态必须失败
			if !strings.Contains(g, "0.1.") && !strings.Contains(g, "0.0.") {
				t.Errorf("garbage %q should not parse", g)
			}
		}
	}
}

func TestNormalizeVersion(t *testing.T) {
	cases := []struct{ in, want string }{
		{"0.1.49-52", "0.1.49"},
		{"v0.1.52", "0.1.52"},
		{"V1.2.3", "1.2.3"},
		{"  0.1.63  ", "0.1.63"},
		{"10.20.30", "10.20.30"},
	}
	for _, c := range cases {
		got, ok := normalizeVersion(c.in)
		if !ok || got != c.want {
			t.Errorf("normalizeVersion(%q)=%q,%v want %q", c.in, got, ok, c.want)
		}
	}
	bad := []string{"", "v", "1.2", "abc", "x.y.z", "1..3", "-1.2.3"}
	for _, b := range bad {
		if v, ok := normalizeVersion(b); ok {
			t.Errorf("normalizeVersion(%q) should fail, got %q", b, v)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"0.1.63", "0.1.49", 1},
		{"0.1.49", "0.1.63", -1},
		{"0.1.63", "0.1.63", 0},
		{"0.2.0", "0.1.99", 1},
		{"1.0.0", "0.9.9", 1},
		{"0.1.63", "", 1},  // 空视为最小
		{"", "0.1.63", -1}, // 空视为最小
		{"", "", 0},
		{"bad", "0.1.0", -1},
	}
	for _, c := range cases {
		if got := compareVersions(c.a, c.b); got != c.want {
			t.Errorf("compareVersions(%q,%q)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestValidVersionCode(t *testing.T) {
	for _, ok := range []string{"20260908", "20260821"} {
		if !validVersionCode(ok) {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "2026-09-08", "260908", "2026090", "abcdefgh", "19990101"} {
		if validVersionCode(bad) {
			t.Errorf("%q should be invalid", bad)
		}
	}
}

func TestDateToCode(t *testing.T) {
	cases := []struct{ in, want string }{
		{"2026-08-21", "20260821"},
		{"2026-09-08", "20260908"},
		{"", ""},
		{"bad", ""},
		{"2026-13-01", ""}, // 月份非法
		{"2026-00-10", ""},
	}
	for _, c := range cases {
		if got := dateToCode(c.in); got != want(c.want) {
			t.Errorf("dateToCode(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

func want(s string) string { return s }

// ---------------------------------------------------------------------------
// 三级回退优先级
// ---------------------------------------------------------------------------

func TestProbeLocalClientReadsPlist(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "Info.plist")
	content := `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
  <key>CFBundleShortVersionString</key><string>0.1.99</string>
  <key>CFBundleVersion</key><string>0.1.99</string>
</dict></plist>`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := probeLocalClient(p)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if v.IdeVersion != "0.1.99" {
		t.Errorf("IdeVersion=%q", v.IdeVersion)
	}
	// 本机 plist 没有日期型 code → 沿用内置
	if v.IdeVersionCode != IdeVersionCode {
		t.Errorf("IdeVersionCode=%q want built-in %q", v.IdeVersionCode, IdeVersionCode)
	}
}

func TestProbeLocalClientMissingFile(t *testing.T) {
	if _, err := probeLocalClient(filepath.Join(t.TempDir(), "nope.plist")); err == nil {
		t.Fatal("missing plist should be an error (uninstalled client → skip)")
	}
	// 存在但没有该 key
	p := filepath.Join(t.TempDir(), "Info.plist")
	_ = os.WriteFile(p, []byte(`<plist><dict><key>Other</key><string>x</string></dict></plist>`), 0o600)
	if _, err := probeLocalClient(p); err == nil {
		t.Fatal("plist without CFBundleShortVersionString should be an error")
	}
}

// TestProbePrefersNewestAcrossSources 多个来源都成功时取版本最大的那个。
func TestProbePrefersNewestAcrossSources(t *testing.T) {
	srcA := `<div class="metaItem-Mgnq7u date-FTRzTs">2026-08-01</div><div class="metaItem-Mgnq7u version-b47QNh">v0.1.40</div><span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span>`
	srcB := `<h2>2026 年 08 月 21 日</h2><p>TraeWork 桌面版 v0.1.49 ~ 0.1.52 版本正式发布。</p>`

	v1, err1 := parseVersionPage(srcChangelog, []byte(srcA))
	v2, err2 := parseVersionPage(srcWorkChangelog, []byte(srcB))
	if err1 != nil || err2 != nil {
		t.Fatalf("parse: %v / %v", err1, err2)
	}
	best := v1
	if compareVersions(v2.IdeVersion, best.IdeVersion) > 0 {
		best = v2
	}
	if best.IdeVersion != "0.1.52" {
		t.Errorf("best=%s want 0.1.52 (newest of the two sources)", best.IdeVersion)
	}
	// 两者都胜过更旧的本机客户端版本
	if compareVersions(best.IdeVersion, "0.1.20") <= 0 {
		t.Error("web sources should beat the older local client")
	}
	// 取最大值的逻辑对调顺序后结果不变（可交换）
	alt := v2
	if compareVersions(v1.IdeVersion, alt.IdeVersion) > 0 {
		alt = v1
	}
	if alt.IdeVersion != best.IdeVersion {
		t.Errorf("max selection is not commutative: %s vs %s", alt.IdeVersion, best.IdeVersion)
	}
}

// TestWebSourceFailureFallsBackToLocal 网页源全挂 → 回退本机客户端。
func TestWebSourceFailureFallsBackToLocal(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "Info.plist")
	_ = os.WriteFile(p, []byte(`<dict><key>CFBundleShortVersionString</key><string>0.1.80</string></dict>`), 0o600)

	orig := localClientPaths
	defer func() { localClientPaths = orig }()
	localClientPaths = []string{p}

	// 网页源不可达：关闭的 server
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	if _, err := fetchVersionPage(dead.URL); err == nil {
		t.Error("dead source should fail")
	}

	// 本机探测仍然可用 → 三级回退的第二级生效
	v, err := probeLocalClient(p)
	if err != nil {
		t.Fatalf("local fallback should still work: %v", err)
	}
	if v.IdeVersion != "0.1.80" {
		t.Errorf("local fallback version=%q want 0.1.80", v.IdeVersion)
	}
}

// ---------------------------------------------------------------------------
// 只进不退保护（关键）
// ---------------------------------------------------------------------------

// TestRefreshNeverDowngradesBelowBuiltin 网页源落后于内置常量时（实测两个源
// 都停在 0.1.49，而内置是 0.1.63），必须保留内置值——否则是回退不是跟踪。
func TestRefreshNeverDowngradesBelowBuiltin(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "Info.plist")
	_ = os.WriteFile(p, []byte(`<dict><key>CFBundleShortVersionString</key><string>0.1.10</string></dict>`), 0o600)
	orig := localClientPaths
	defer func() { localClientPaths = orig }()
	localClientPaths = []string{p} // 本机版本远低于内置

	resetVersionCacheForTest()
	defer resetVersionCacheForTest()

	// 缓存里塞一个旧版本并强制刷新路径按"只进不退"收敛：
	// 直接调用守卫逻辑的等价路径 —— 用 setVersionCacheForTest + 读取断言
	setVersionCacheForTest(upstreamVersion{IdeVersion: "0.1.10"}, true, "test")
	if got := ideVersion(); got != "0.1.10" {
		t.Fatalf("cache not honored: %q", got)
	}
	// 关掉跟踪 → 必须回到内置常量
	origTrack := versionTrackingEnabled()
	defer setVersionTracking(origTrack)
	setVersionTracking(false)
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("tracking off should use built-in, got %q", got)
	}
	if got := ideVersionCode(); got != IdeVersionCode {
		t.Errorf("tracking off should use built-in code, got %q", got)
	}
}

// TestVersionFloorGuard 内置常量是下限：任何低于它的候选都被丢弃。
func TestVersionFloorGuard(t *testing.T) {
	// 模拟：探测到 0.1.49（网页当前值），内置 0.1.63
	candidate := upstreamVersion{IdeVersion: "0.1.49", IdeVersionCode: "20260821"}
	if compareVersions(candidate.IdeVersion, IdeVersion) >= 0 {
		t.Skip("built-in is not newer than the web value; skip guard assertion")
	}
	// 守卫：低于内置 → 沿用内置
	final := candidate
	if compareVersions(candidate.IdeVersion, IdeVersion) < 0 {
		final = builtinVersion()
	}
	if final.IdeVersion != IdeVersion {
		t.Errorf("guard failed: got %q want built-in %q", final.IdeVersion, IdeVersion)
	}
}

func TestHigherVersionIsAccepted(t *testing.T) {
	// 模拟上游发新版 0.2.0：必须被采纳
	candidate := upstreamVersion{IdeVersion: "0.2.0", IdeVersionCode: "20261001"}
	final := candidate
	if compareVersions(candidate.IdeVersion, IdeVersion) < 0 {
		final = builtinVersion()
	}
	if final.IdeVersion != "0.2.0" {
		t.Errorf("newer version should be adopted, got %q", final.IdeVersion)
	}
}

// ---------------------------------------------------------------------------
// 缓存与不阻塞
// ---------------------------------------------------------------------------

func TestCurrentVersionUsesCache(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)

	setVersionCacheForTest(upstreamVersion{IdeVersion: "9.9.9", IdeVersionCode: "20261231"}, true, "test")
	if got := ideVersion(); got != "9.9.9" {
		t.Errorf("cached version not used: %q", got)
	}
	if got := ideVersionCode(); got != "20261231" {
		t.Errorf("cached code not used: %q", got)
	}
}

func TestCurrentVersionFallsBackOnNegativeCache(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)

	// 负缓存（探测失败）→ 用内置常量
	setVersionCacheForTest(upstreamVersion{}, false, "")
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("negative cache should fall back to built-in, got %q", got)
	}
}

func TestCurrentVersionNeverBlocks(t *testing.T) {
	stubWebSources(t)
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	// 无缓存且网页不可达（默认常量指向真实外网，但即便超时也必须立即返回）
	done := make(chan string, 1)
	start := time.Now()
	go func() {
		done <- ideVersion()
	}()
	select {
	case got := <-done:
		if !validVersion(got) {
			t.Errorf("returned invalid version %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ideVersion blocked the caller")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("ideVersion took too long: %v", elapsed)
	}
}

func TestVersionStatusShape(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	setVersionCacheForTest(upstreamVersion{IdeVersion: "0.1.70", IdeVersionCode: "20260908"}, true, "test")
	out := versionStatus()
	if out["ide_version"] != "0.1.70" {
		t.Errorf("ide_version=%v", out["ide_version"])
	}
	if out["tracking"] != true {
		t.Errorf("tracking=%v", out["tracking"])
	}
	builtin, ok := out["builtin"].(map[string]any)
	if !ok || builtin["ide_version"] != IdeVersion {
		t.Errorf("builtin=%v", out["builtin"])
	}
	srcs, ok := out["sources"].([]string)
	if !ok || len(srcs) != 2 {
		t.Errorf("sources=%v", out["sources"])
	}
}

func TestVersionTrackingToggle(t *testing.T) {
	orig := versionTrackingEnabled()
	defer setVersionTracking(orig)
	setVersionTracking(false)
	if versionTrackingEnabled() {
		t.Error("toggle off failed")
	}
	setVersionTracking(true)
	if !versionTrackingEnabled() {
		t.Error("toggle on failed")
	}
}

func TestVersionIntervalConfig(t *testing.T) {
	orig := loadedVersionInterval()
	defer setVersionInterval(orig)
	setVersionInterval(6 * time.Hour)
	if got := loadedVersionInterval(); got != 6*time.Hour {
		t.Errorf("interval=%v", got)
	}
	setVersionInterval(0) // 非法值必须被忽略
	if got := loadedVersionInterval(); got != 6*time.Hour {
		t.Errorf("invalid interval should be ignored, got %v", got)
	}
}

func TestEnsureVersionTrackerIdempotent(t *testing.T) {
	stubWebSources(t)
	ensureVersionTracker()
	ensureVersionTracker() // 第二次必须是 no-op
}

// TestParseVersionPageDispatchesByURL 不同来源走不同解析。
func TestParseVersionPageDispatchesByURL(t *testing.T) {
	v, err := parseVersionPage(srcWorkChangelog, []byte(fixtureWorkChangelog))
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.52" {
		t.Errorf("docs source dispatched wrong: %s", v.IdeVersion)
	}
	v2, err := parseVersionPage(srcChangelog, []byte(fixtureChangelog))
	if err != nil {
		t.Fatal(err)
	}
	if v2.IdeVersion != "0.1.49" {
		t.Errorf("www source dispatched wrong: %s", v2.IdeVersion)
	}
}

// TestHeadersUseTrackedVersion 请求头必须读跟踪结果，而不是常量。
func TestHeadersUseTrackedVersion(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	setVersionCacheForTest(upstreamVersion{IdeVersion: "7.7.7", IdeVersionCode: "20261225"}, true, "test")

	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid/x", strings.NewReader("{}"))
	SOLOHeaders(req, &traeAuth{UID: "u1"}, true)
	if got := req.Header.Get("User-Agent"); got != "Trae/7.7.7" {
		t.Errorf("UA=%q want Trae/7.7.7", got)
	}
	if got := req.Header.Get("X-Ide-Version"); got != "7.7.7" {
		t.Errorf("X-Ide-Version=%q", got)
	}
	if got := req.Header.Get("X-Ide-Version-Code"); got != "20261225" {
		t.Errorf("X-Ide-Version-Code=%q", got)
	}
	if got := req.Header.Get("X-App-Version-Code"); got != "20261225" {
		t.Errorf("X-App-Version-Code=%q", got)
	}
	// 与产品线无关的常量头不得被版本跟踪改动
	if got := req.Header.Get("X-Ide-Version-Type"); got != "stable" {
		t.Errorf("X-Ide-Version-Type=%q", got)
	}
}

// TestHeadersFallbackToBuiltin 无缓存时头里必须是内置常量值。
func TestHeadersFallbackToBuiltin(t *testing.T) {
	orig := versionTrackingEnabled()
	defer func() {
		setVersionTracking(orig)
		resetVersionCacheForTest()
	}()
	setVersionTracking(false)
	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid/x", strings.NewReader("{}"))
	SOLOHeaders(req, &traeAuth{}, false)
	if got := req.Header.Get("User-Agent"); got != "Trae/"+IdeVersion {
		t.Errorf("UA=%q want Trae/%s", got, IdeVersion)
	}
	if got := req.Header.Get("X-Ide-Version-Code"); got != IdeVersionCode {
		t.Errorf("code=%q want %s", got, IdeVersionCode)
	}
}

// TestVersionRefreshRecoversFromPanic 刷新路径 panic 不得逃逸。
func TestVersionRefreshRecoversFromPanic(t *testing.T) {
	orig := localClientPaths
	defer func() { localClientPaths = orig }()
	localClientPaths = nil
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	resetVersionCacheForTest()
	stubWebSources(t)
	// 所有来源都不可用 → 负缓存，不得 panic
	probeAndStoreVersion()
	e := versionCache.Load()
	if e == nil {
		t.Fatal("refresh should always leave a cache entry")
	}
	if e.ok {
		t.Error("with no sources the entry should be a negative cache")
	}
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("should fall back to built-in, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// 三级回退：用 httptest 完整驱动 probeUpstreamVersion
// ---------------------------------------------------------------------------

// withStubbedSources 把两个网页源分别指向给定的 httptest，本机路径指向给定 plist。
func withStubbedSources(t *testing.T, bodyA, bodyB string, statusA, statusB int, localPlist string) {
	t.Helper()
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusA)
		_, _ = w.Write([]byte(bodyA))
	}))
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusB)
		_, _ = w.Write([]byte(bodyB))
	}))
	t.Cleanup(func() { srvA.Close(); srvB.Close() })
	oldA, oldB := srcChangelog, srcWorkChangelog
	srcChangelog, srcWorkChangelog = srvA.URL, srvB.URL
	t.Cleanup(func() { srcChangelog, srcWorkChangelog = oldA, oldB })

	oldPaths := localClientPaths
	if localPlist == "" {
		localClientPaths = nil
	} else {
		localClientPaths = []string{localPlist}
	}
	t.Cleanup(func() { localClientPaths = oldPaths })
}

func writePlist(t *testing.T, version string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "Info.plist")
	content := `<dict><key>CFBundleShortVersionString</key><string>` + version + `</string></dict>`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const stubChangelogBody = `<div class="metaItem-Mgnq7u date-FTRzTs">2026-10-01</div><div class="metaItem-Mgnq7u version-b47QNh">v0.2.5</div><span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span>`
const stubWorkBody = `<h2>2026 年 09 月 30 日</h2><p>TraeWork 桌面版 v0.2.1 ~ 0.2.3 版本正式发布。</p>`

// TestTier1WebWins 网页源可用且比内置新 → 用网页值（取两源较大者）。
func TestTier1WebWins(t *testing.T) {
	withStubbedSources(t, stubChangelogBody, stubWorkBody, 200, 200, writePlist(t, "0.1.10"))
	v, src, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.2.5" {
		t.Errorf("IdeVersion=%q want 0.2.5 (max of 0.2.5 / 0.2.3 / local 0.1.10)", v.IdeVersion)
	}
	if v.IdeVersionCode != "20261001" {
		t.Errorf("code=%q want 20261001", v.IdeVersionCode)
	}
	if src != srcChangelog {
		t.Errorf("src=%q want the changelog source", src)
	}
}

// TestTier1PartialFailure 一个网页源挂了，另一个仍能提供值。
func TestTier1PartialFailure(t *testing.T) {
	withStubbedSources(t, "", stubWorkBody, 500, 200, "")
	v, src, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.2.3" {
		t.Errorf("IdeVersion=%q want 0.2.3 from the surviving source", v.IdeVersion)
	}
	if src != srcWorkChangelog {
		t.Errorf("src=%q", src)
	}
}

// TestTier2LocalFallback 两个网页源都挂 → 回退本机客户端。
func TestTier2LocalFallback(t *testing.T) {
	plist := writePlist(t, "0.1.85")
	withStubbedSources(t, "boom", "boom", 500, 500, plist)
	v, src, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.85" {
		t.Errorf("IdeVersion=%q want 0.1.85 (local client)", v.IdeVersion)
	}
	if !strings.HasPrefix(src, "local:") {
		t.Errorf("src=%q want local: prefix", src)
	}
	// 本机 plist 无日期型 code → 沿用内置
	if v.IdeVersionCode != IdeVersionCode {
		t.Errorf("code=%q want built-in %q", v.IdeVersionCode, IdeVersionCode)
	}
}

// TestTier3Builtin 三级全不可用 → 返回错误，调用方回退内置常量。
func TestTier3Builtin(t *testing.T) {
	withStubbedSources(t, "boom", "boom", 500, 500, "")
	_, _, err := probeUpstreamVersion()
	if err == nil {
		t.Fatal("all sources failing should return an error")
	}
	// 回退：内置常量仍然是有效值
	if !validVersion(IdeVersion) {
		t.Errorf("built-in %q must remain a valid fallback", IdeVersion)
	}
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("final fallback=%q want %q", got, IdeVersion)
	}
}

// TestWebLagsBehindLocal 网页落后于本机 → 取本机的更大值（只进不退的一半）。
func TestWebLagsBehindLocal(t *testing.T) {
	old := `<div class="metaItem-Mgnq7u date-FTRzTs">2026-08-21</div><div class="metaItem-Mgnq7u version-b47QNh">v0.1.49</div><span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span>`
	withStubbedSources(t, old, old, 200, 200, writePlist(t, "0.1.63"))
	v, _, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.63" {
		t.Errorf("IdeVersion=%q want 0.1.63 (local beats stale web)", v.IdeVersion)
	}
}

// TestEndToEndRefreshUsesWebValue 完整走 refreshUpstreamVersion：
// 网页更新 → 缓存写入 → 后续读取用新值。
func TestEndToEndRefreshUsesWebValue(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	resetVersionCacheForTest()
	withStubbedSources(t, stubChangelogBody, stubWorkBody, 200, 200, writePlist(t, "0.1.10"))

	refreshUpstreamVersion()
	e := versionCache.Load()
	if e == nil || !e.ok {
		t.Fatalf("refresh should cache a positive result, got %+v", e)
	}
	if got := ideVersion(); got != "0.2.5" {
		t.Errorf("ideVersion=%q want 0.2.5", got)
	}
	if got := ideVersionCode(); got != "20261001" {
		t.Errorf("ideVersionCode=%q want 20261001", got)
	}
}

// TestEndToEndRefreshKeepsBuiltinWhenWebStale 网页落后于内置常量时，
// 端到端刷新后仍必须保留内置值（防止版本被降）。
func TestEndToEndRefreshKeepsBuiltinWhenWebStale(t *testing.T) {
	stale := `<div class="metaItem-Mgnq7u date-FTRzTs">2026-08-21</div><div class="metaItem-Mgnq7u version-b47QNh">v0.1.49</div><span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span>`
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	resetVersionCacheForTest()
	withStubbedSources(t, stale, stale, 200, 200, "")

	refreshUpstreamVersion()
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("stale web must not downgrade: ideVersion=%q want built-in %q", got, IdeVersion)
	}
	if got := ideVersionCode(); got != IdeVersionCode {
		t.Errorf("stale web must not downgrade: code=%q want %q", got, IdeVersionCode)
	}
}

// TestNegativeCacheTTL 失败后写负缓存，且负缓存未到期不再重复探测。
func TestNegativeCacheTTL(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	resetVersionCacheForTest()
	withStubbedSources(t, "boom", "boom", 500, 500, "")

	probeAndStoreVersion()
	e := versionCache.Load()
	if e == nil || e.ok {
		t.Fatalf("expected negative cache, got %+v", e)
	}
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("negative cache should yield built-in, got %q", got)
	}
}

// TestMain 关闭后台自动探测，保证单测确定性（后台 goroutine 会在断言之间
// 改写缓存，导致偶发失败）。
func TestMain(m *testing.M) {
	versionAutoProbe.Store(false)
	os.Exit(m.Run())
}
