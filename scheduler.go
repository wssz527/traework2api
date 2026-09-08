// scheduler.go 实现 CPA scheduler.pick + 每日签到/预刷新自起 goroutine。
//
// 迁移报告 §2：插件没有定时 RPC，只能自起 goroutine + ticker（照抄 workbuddy）。
// scheduler_mode 默认 off（交给 CPA 内建调度）；设为 credits 时插件按
// "面板选中 + 剩余积分最高"挑号。
package main

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	schedulerModeOff     = "off"
	schedulerModeCredits = "credits"
)

// 默认调度时刻（对齐原 traework2api）。
const (
	defaultCheckinHour = 9
)

var (
	schedulerMode   = schedulerModeOff
	schedulerModeMu sync.RWMutex

	checkinAuto   = true
	checkinAutoMu sync.RWMutex

	checkinHour   = defaultCheckinHour
	checkinHourMu sync.RWMutex

	refreshHours   = []int{3}
	refreshHoursMu sync.RWMutex
)

// setSchedulerMode is a test helper that returns a restore func.
func setSchedulerMode(mode string) func() {
	schedulerModeMu.Lock()
	old := schedulerMode
	schedulerMode = mode
	schedulerModeMu.Unlock()
	return func() {
		schedulerModeMu.Lock()
		schedulerMode = old
		schedulerModeMu.Unlock()
	}
}

func loadedSchedulerMode() string {
	schedulerModeMu.RLock()
	defer schedulerModeMu.RUnlock()
	return schedulerMode
}

func loadedCheckinAuto() bool {
	checkinAutoMu.RLock()
	defer checkinAutoMu.RUnlock()
	return checkinAuto
}

func setCheckinAuto(v bool) {
	checkinAutoMu.Lock()
	checkinAuto = v
	checkinAutoMu.Unlock()
}

func loadedCheckinHour() int {
	checkinHourMu.RLock()
	defer checkinHourMu.RUnlock()
	return checkinHour
}

func setCheckinHour(h int) {
	if h < 0 || h > 23 {
		return
	}
	checkinHourMu.Lock()
	checkinHour = h
	checkinHourMu.Unlock()
}

func loadedRefreshHours() []int {
	refreshHoursMu.RLock()
	defer refreshHoursMu.RUnlock()
	out := make([]int, len(refreshHours))
	copy(out, refreshHours)
	return out
}

func setRefreshHours(hours []int) {
	refreshHoursMu.Lock()
	refreshHours = hours
	refreshHoursMu.Unlock()
}

// handleSchedulerPick 挑号。mode=off 时全部 defer 给 CPA 内建。
func handleSchedulerPick(raw []byte) ([]byte, error) {
	var req pluginapi.SchedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if loadedSchedulerMode() != schedulerModeCredits {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}
	var cands []activeAuthCandidate
	for _, c := range req.Candidates {
		if !strings.EqualFold(strings.TrimSpace(c.Provider), providerName) {
			continue
		}
		if candidateDisabled(c) {
			continue
		}
		cands = append(cands, activeAuthCandidate{
			ID:        c.ID,
			Disabled:  false,
			Exhausted: isCooling(c.ID),
		})
	}
	if len(cands) == 0 {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}
	picked := pickActiveAuth(cands)
	if picked == "" {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}
	return okEnvelope(pluginapi.SchedulerPickResponse{AuthID: picked, Handled: true})
}

// candidateDisabled 从 Status/metadata 判断宿主侧禁用。
func candidateDisabled(c pluginapi.SchedulerAuthCandidate) bool {
	st := strings.ToLower(strings.TrimSpace(c.Status))
	if st == "disabled" {
		return true
	}
	if c.Metadata != nil {
		if v, ok := c.Metadata["disabled"]; ok {
			switch t := v.(type) {
			case bool:
				return t
			case string:
				return strings.EqualFold(strings.TrimSpace(t), "true")
			}
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// 自起 goroutine：每日签到 + token 预刷新
// -----------------------------------------------------------------------------

var (
	schedulerStop chan struct{}
	schedulerMu   sync.Mutex
)

// ensureScheduler 启动调度 goroutine（幂等）。在 plugin.register 时调用。
func ensureScheduler() {
	schedulerMu.Lock()
	defer schedulerMu.Unlock()
	if schedulerStop != nil {
		return
	}
	schedulerStop = make(chan struct{})
	go schedulerLoop(schedulerStop)
	// 版本跟踪用自己的固定间隔，不挂整点触发。
	ensureVersionTracker()
}

// 注意：故意不提供 stop 函数。宿主的 shutdown 导出是 no-op（见 main.go），
// 因为宿主在自己 runtime teardown 阶段调用它，此时触碰 Go 同步原语会 SIGSEGV。
// goroutine 靠进程退出回收。

// nextFire 返回 now 之后最近的一个整点触发时间。
func nextFire(now time.Time, hours []int) time.Time {
	var earliest time.Time
	for _, h := range hours {
		if h < 0 || h > 23 {
			continue
		}
		t := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, now.Location())
		if !t.After(now) {
			t = t.Add(24 * time.Hour)
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest
}

// allFireHours 合并签到与预刷新时刻，让 timer 在最早的那个唤醒。
func allFireHours() []int {
	hours := loadedRefreshHours()
	ch := loadedCheckinHour()
	all := make([]int, 0, len(hours)+1)
	all = append(all, hours...)
	all = append(all, ch)
	return all
}

func schedulerLoop(stop chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in scheduler loop: %v", r)
		}
	}()
	for {
		next := nextFire(time.Now(), allFireHours())
		timer := time.NewTimer(time.Until(next))
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
			runScheduledTick(time.Now().Hour())
		}
	}
}

// runScheduledTick 按当前小时决定跑签到还是预刷新（或两者）。
func runScheduledTick(hour int) {
	defer func() {
		if r := recover(); r != nil {
			pluginLogf("panic in scheduled tick: %v", r)
		}
	}()
	for _, h := range loadedRefreshHours() {
		if h == hour {
			runTokenRefreshNow()
			break
		}
	}
	if loadedCheckinHour() == hour {
		runCheckinNow()
	}
}

// runCheckinNow 对所有账号执行签到 + 积分刷新（对应原 scheduler.RunCheckinNow）。
// 每账号独立 goroutine（并发上限 4），失败不影响其它账号。
func runCheckinNow() {
	if !loadedCheckinAuto() {
		return
	}
	files, err := hostAuthList()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i := range files {
		wg.Add(1)
		go func(f pluginapi.HostAuthFileEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					pluginLogf("panic in checkin for %s: %v", f.AuthIndex, r)
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()
			checkinOneAccount(f)
		}(files[i])
	}
	wg.Wait()
	pruneLifecycleState()
}

// runTokenRefreshNow 对所有账号刷新 token（对应原 scheduler.RunRefreshNow）。
func runTokenRefreshNow() {
	files, err := hostAuthList()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i := range files {
		wg.Add(1)
		go func(f pluginapi.HostAuthFileEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					pluginLogf("panic in refresh for %s: %v", f.AuthIndex, r)
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()
			refreshOneAccount(f)
		}(files[i])
	}
	wg.Wait()
}

// refreshOneAccount 刷新单个账号的 token；session 失效的自动禁用。
func refreshOneAccount(f pluginapi.HostAuthFileEntry) map[string]any {
	out := map[string]any{"auth_index": f.AuthIndex}
	sa, err := hostAuthGet(f.AuthIndex)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	out["nickname"] = sa.Nickname
	if sa.UID != "" {
		out["uid"] = maskUID(sa.UID)
	}
	if sa.RefreshTokenValue() == "" {
		out["error"] = "no refreshToken"
		return out
	}
	refreshed, err := currentClient().RefreshTokenIfNeeded(sa, defaultRefreshSkew)
	if err != nil {
		if isSessionDead(statusOfError(err), err.Error()) {
			markDisabled(f.AuthIndex, sa, "session dead")
		}
		out["error"] = redactSecrets(err.Error())
		return out
	}
	if refreshed {
		if err := persistAuth(sa, false); err != nil {
			out["error"] = "persist: " + err.Error()
			return out
		}
	}
	out["refreshed"] = refreshed
	return out
}
