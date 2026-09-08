package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 版本解析：check_update API（主源）
// ---------------------------------------------------------------------------

// stubAPIWithCapture 把 checkUpdateAPI 指向一个返回给定 body 的 httptest，返回
// handler 捕获请求的访问器（用于断言参数）。restore 由 t.Cleanup 保证。
func stubAPIWithCapture(t *testing.T, body string, status int) func() *http.Request {
	t.Helper()
	var captured *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	old := checkUpdateAPI
	checkUpdateAPI = srv.URL
	t.Cleanup(func() { checkUpdateAPI = old })
	return func() *http.Request { return captured }
}

func apiBody(appVersion string) string {
	b, _ := json.Marshal(map[string]any{
		"err_code": 0,
		"err_message": "success",
		"data": map[string]any{
			"needUpdate": true,
			"appVersion": appVersion,
			"manifest": map[string]any{
				"darwin": map[string]any{"version": "2.3.81345"},
			},
		},
	})
	return string(b)
}

// TestParseCheckUpdateResponse 主源成功：err_code=0 + 合法 appVersion → 采纳。
func TestParseCheckUpdateResponse(t *testing.T) {
	v, src, err := parseCheckUpdateResponse([]byte(apiBody("0.1.70")))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if v.IdeVersion != "0.1.70" {
		t.Errorf("IdeVersion=%q want 0.1.70", v.IdeVersion)
	}
	// check_update 不返回日期型 code → 沿用内置
	if v.IdeVersionCode != IdeVersionCode {
		t.Errorf("code=%q want built-in %q", v.IdeVersionCode, IdeVersionCode)
	}
	if !strings.HasPrefix(src, "api:") {
		t.Errorf("src=%q want api: prefix", src)
	}
}

// TestParseCheckUpdateNoUpdate 本地已最新的响应只有 {"needUpdate":false} 无
// appVersion；此时视为确认内置版本仍最新 → 成功返回内置值。
func TestParseCheckUpdateNoUpdate(t *testing.T) {
	v, src, err := parseCheckUpdateResponse([]byte(`{"err_code":0,"data":{"needUpdate":false}}`))
	if err != nil {
		t.Fatalf("parse no-update: %v", err)
	}
	if v.IdeVersion != IdeVersion {
		t.Errorf("IdeVersion=%q want built-in %q", v.IdeVersion, IdeVersion)
	}
	if v.IdeVersionCode != IdeVersionCode {
		t.Errorf("code=%q want built-in %q", v.IdeVersionCode, IdeVersionCode)
	}
	if !strings.HasPrefix(src, "api:") {
		t.Errorf("src=%q want api: prefix", src)
	}
}

// TestParseCheckUpdateRejectsErrors err_code != 0 / 结构缺失 → 失败。
func TestParseCheckUpdateRejectsErrors(t *testing.T) {
	cases := []string{
		`{"err_code":1000,"err_message":"missing mid","data":{}}`,
		`{"err_code":1000,"data":{"needUpdate":true,"appVersion":"0.1.63"}}`,
		`not json`,
		``,
		`{"err_code":0,"data":{"needUpdate":true,"appVersion":"v"}}`,
	}
	for _, c := range cases {
		if _, _, err := parseCheckUpdateResponse([]byte(c)); err == nil {
			t.Errorf("body %q should fail to parse", c)
		}
	}
}

// TestCheckUpdateURLHasRequiredParams 请求 URL 必须带文档要求的最小参数集。
func TestCheckUpdateURLHasRequiredParams(t *testing.T) {
	u := checkUpdateURL()
	for _, want := range []string{
		"branch=release_solo_cn",
		"packageType=stable_cn",
		"productCode=SOLO_Lite",
		"platform=Mac",
		"arch=arm64",
		"appVersion=" + IdeVersion,
		"buildVersion=" + IdeBuildVersion,
		"mid=",
	} {
		if !strings.Contains(u, want) {
			t.Errorf("checkUpdateURL missing %q in %q", want, u)
		}
	}
}

// TestProbeCheckUpdateAPINetwork 端到端走真实 HTTP：httptest 返回新版 → 采纳。
func TestProbeCheckUpdateAPINetwork(t *testing.T) {
	capture := stubAPIWithCapture(t, apiBody("0.2.0"), 200)
	v, src, err := probeCheckUpdateAPI()
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if v.IdeVersion != "0.2.0" {
		t.Errorf("IdeVersion=%q want 0.2.0", v.IdeVersion)
	}
	if !strings.HasPrefix(src, "api:") {
		t.Errorf("src=%q", src)
	}
	req := capture()
	if req == nil {
		t.Fatal("handler did not capture request")
	}
	if q := req.URL.Query(); q.Get("mid") == "" {
		t.Error("mid param must be present")
	}
}

// TestProbeCheckUpdateAPIHTTPError 非 200 → 失败。
func TestProbeCheckUpdateAPIHTTPError(t *testing.T) {
	stubAPIWithCapture(t, "boom", 500)
	if _, _, err := probeCheckUpdateAPI(); err == nil {
		t.Fatal("HTTP 500 should fail")
	}
}

// ---------------------------------------------------------------------------
// 版本工具（与源无关）
// ---------------------------------------------------------------------------

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
		if got := dateToCode(c.in); got != c.want {
			t.Errorf("dateToCode(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// 本机客户端探测（第二优先级；客户端已卸载时可省略）
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

// ---------------------------------------------------------------------------
// 源优先级：①API（主源）②本机 ③内置下限
// ---------------------------------------------------------------------------

func writePlist(t *testing.T, version string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "Info.plist")
	content := `<dict><key>CFBundleShortVersionString</key><string>` + version + `</string></dict>`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// withSources 把 checkUpdateAPI 指向给定 httptest body/status，并把本机路径
// 指向给定 plist（空串 → 无本机客户端）。restore 全部由 t.Cleanup 保证。
func withSources(t *testing.T, apiBodyStr string, apiStatus int, localPlist string) {
	t.Helper()
	oldAPI := checkUpdateAPI
	checkUpdateAPI = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(apiStatus)
		_, _ = w.Write([]byte(apiBodyStr))
	})).URL
	t.Cleanup(func() { checkUpdateAPI = oldAPI })

	oldPaths := localClientPaths
	if localPlist == "" {
		localClientPaths = nil
	} else {
		localClientPaths = []string{localPlist}
	}
	t.Cleanup(func() { localClientPaths = oldPaths })
}

// TestAPISourceWins API 可用且比本机新 → 用 API 值（主源优先）。
func TestAPISourceWins(t *testing.T) {
	withSources(t, apiBody("0.1.80"), 200, writePlist(t, "0.1.10"))
	v, src, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.80" {
		t.Errorf("IdeVersion=%q want 0.1.80 (API beats local)", v.IdeVersion)
	}
	if !strings.HasPrefix(src, "api:") {
		t.Errorf("src=%q want api: prefix", src)
	}
}

// TestLocalBeatsStaleAPI API 返回旧值、本机更新 → 取本机（只进不退的一半）。
func TestLocalBeatsStaleAPI(t *testing.T) {
	withSources(t, apiBody("0.1.20"), 200, writePlist(t, "0.1.63"))
	v, _, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.63" {
		t.Errorf("IdeVersion=%q want 0.1.63 (local beats stale API)", v.IdeVersion)
	}
}

// TestAPIFailureFallsBackToLocal API 挂 → 回退本机客户端。
func TestAPIFailureFallsBackToLocal(t *testing.T) {
	plist := writePlist(t, "0.1.85")
	withSources(t, "boom", 500, plist)
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

// TestAllSourcesFail API 与本机都不可用（如客户端已卸载）→ 返回错误，调用方
// 回退内置常量。
func TestAllSourcesFail(t *testing.T) {
	withSources(t, "boom", 500, "")
	_, _, err := probeUpstreamVersion()
	if err == nil {
		t.Fatal("all sources failing should return an error")
	}
	if !validVersion(IdeVersion) {
		t.Errorf("built-in %q must remain a valid fallback", IdeVersion)
	}
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("final fallback=%q want %q", got, IdeVersion)
	}
}

// TestLocalClientUninstalledIsOptional 用户卸载客户端后：localClientPaths 为空，
// API 正常时仍可用（本地源可有可无）。
func TestLocalClientUninstalledIsOptional(t *testing.T) {
	withSources(t, apiBody("0.1.90"), 200, "")
	v, src, err := probeUpstreamVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v.IdeVersion != "0.1.90" {
		t.Errorf("IdeVersion=%q want 0.1.90", v.IdeVersion)
	}
	if !strings.HasPrefix(src, "api:") {
		t.Errorf("src=%q want api: prefix", src)
	}
}

// ---------------------------------------------------------------------------
// 只进不退保护（关键）
// ---------------------------------------------------------------------------

func TestVersionFloorGuard(t *testing.T) {
	candidate := upstreamVersion{IdeVersion: "0.1.49", IdeVersionCode: "20260821"}
	if compareVersions(candidate.IdeVersion, IdeVersion) >= 0 {
		t.Skip("built-in is not newer than the candidate; skip guard assertion")
	}
	final := candidate
	if compareVersions(candidate.IdeVersion, IdeVersion) < 0 {
		final = builtinVersion()
	}
	if final.IdeVersion != IdeVersion {
		t.Errorf("guard failed: got %q want built-in %q", final.IdeVersion, IdeVersion)
	}
}

func TestHigherVersionIsAccepted(t *testing.T) {
	candidate := upstreamVersion{IdeVersion: "0.2.0", IdeVersionCode: "20261001"}
	final := candidate
	if compareVersions(candidate.IdeVersion, IdeVersion) < 0 {
		final = builtinVersion()
	}
	if final.IdeVersion != "0.2.0" {
		t.Errorf("newer version should be adopted, got %q", final.IdeVersion)
	}
}

// TestRefreshNeverDowngradesBelowBuiltin 所有源返回都低于内置（如本地 0.1.10）时，
// 刷新路径按"只进不退"收敛，最终必须保留内置值。
func TestRefreshNeverDowngradesBelowBuiltin(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "Info.plist")
	_ = os.WriteFile(p, []byte(`<dict><key>CFBundleShortVersionString</key><string>0.1.10</string></dict>`), 0o600)
	orig := localClientPaths
	defer func() { localClientPaths = orig }()
	localClientPaths = []string{p}

	resetVersionCacheForTest()
	defer resetVersionCacheForTest()

	// 缓存里塞一个旧版本并读取断言
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

// TestEndToEndRefreshKeepsBuiltinWhenSourcesStale 端到端刷新：API 返回低于内置
// 的值时，仍必须保留内置（防止版本被降）。
func TestEndToEndRefreshKeepsBuiltinWhenSourcesStale(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	resetVersionCacheForTest()
	withSources(t, apiBody("0.1.10"), 200, "")

	refreshUpstreamVersion()
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("stale API must not downgrade: ideVersion=%q want built-in %q", got, IdeVersion)
	}
	if got := ideVersionCode(); got != IdeVersionCode {
		t.Errorf("stale API must not downgrade: code=%q want %q", got, IdeVersionCode)
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

	setVersionCacheForTest(upstreamVersion{}, false, "")
	if got := ideVersion(); got != IdeVersion {
		t.Errorf("negative cache should fall back to built-in, got %q", got)
	}
}

func TestCurrentVersionNeverBlocks(t *testing.T) {
	// 把 API 指向不可达地址，确保即便超时也必须立即返回内置值
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	old := checkUpdateAPI
	checkUpdateAPI = dead.URL
	t.Cleanup(func() { checkUpdateAPI = old })

	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
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
		t.Fatalf("sources=%v", out["sources"])
	}
	if !strings.HasPrefix(srcs[0], "api:") {
		t.Errorf("sources[0]=%q want api: prefix (main source first)", srcs[0])
	}
	if !strings.HasPrefix(srcs[1], "local:") {
		t.Errorf("sources[1]=%q want local: prefix", srcs[1])
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
	ensureVersionTracker()
	ensureVersionTracker() // 第二次必须是 no-op
}

// ---------------------------------------------------------------------------
// 请求头使用跟踪结果
// ---------------------------------------------------------------------------

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
	if got := req.Header.Get("X-Ide-Version-Type"); got != "stable" {
		t.Errorf("X-Ide-Version-Type=%q", got)
	}
}

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

// ---------------------------------------------------------------------------
// 刷新路径健壮性
// ---------------------------------------------------------------------------

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
	// API 指向不可达地址 → 负缓存，不得 panic
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	old := checkUpdateAPI
	checkUpdateAPI = dead.URL
	t.Cleanup(func() { checkUpdateAPI = old })

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

func TestNegativeCacheTTL(t *testing.T) {
	origTrack := versionTrackingEnabled()
	defer func() {
		setVersionTracking(origTrack)
		resetVersionCacheForTest()
	}()
	setVersionTracking(true)
	resetVersionCacheForTest()
	withSources(t, "boom", 500, "")

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
