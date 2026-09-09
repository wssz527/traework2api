// checkin.go 签到执行（单账号）+ 每账号互斥锁。
//
// 对应原 traework2api 的 cmd/signin + internal/scheduler.RunCheckinNow。
package main

import (
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// checkinLocks 串行化每账号的手动签到，防浏览器多标签并发重复签到。
var checkinLocks sync.Map // auth_index -> *sync.Mutex

func checkinLockFor(authIndex string) *sync.Mutex {
	v, _ := checkinLocks.LoadOrStore(authIndex, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// checkinOneAccount 对单个账号执行签到 + 积分刷新。
//
// 流程（对应原 signin）：
//  1. 每账号独立固定设备 ID（缺失则生成并写回凭证）
//  2. CheckinStatus → 未签到且 enable 则 CheckinClaim → 复查
//  3. UserEntUsage 查积分（用于面板与冷却解冻）
func checkinOneAccount(f pluginapi.HostAuthFileEntry) map[string]any {
	out := map[string]any{"auth_index": f.AuthIndex}

	sa, err := hostAuthGet(f.AuthIndex)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	out["nickname"] = sa.Nickname
	out["uid"] = maskUID(sa.UID)

	mu := checkinLockFor(f.AuthIndex)
	mu.Lock()
	defer mu.Unlock()

	// 每账号独立固定设备 ID：新账号自动生成伪造 ID，已固定的保持不动。
	if EnsurePerAccountCheckinDevice(sa) {
		if err := persistAuth(sa, false); err != nil {
			out["device_id_error"] = err.Error()
		} else {
			out["device_id_generated"] = true
		}
	}
	if sa.CheckinDeviceID == "" {
		out["success"] = false
		out["error"] = "签到设备ID为空"
		return out
	}

	checkedIn, _, enable, err := currentClient().CheckinStatus(sa)
	if err != nil {
		// 明确"已签到"的业务错误不算失败。
		if isAlreadyCheckedIn(err.Error()) {
			out["success"] = true
			out["skipped"] = true
			out["reason"] = "already"
			out["message"] = "already checked in today"
		} else {
			out["success"] = false
			out["error"] = redactSecrets(err.Error())
		}
	} else if checkedIn {
		out["success"] = true
		out["skipped"] = true
		out["reason"] = "already"
		out["message"] = "already checked in today"
	} else if !enable {
		out["success"] = false
		out["skipped"] = true
		out["reason"] = "disabled"
		out["message"] = "checkin disabled by upstream"
	} else {
		if err := currentClient().CheckinClaim(sa); err != nil {
			if isAlreadyCheckedIn(err.Error()) {
				out["success"] = true
				out["skipped"] = true
				out["reason"] = "already"
				out["message"] = err.Error()
			} else {
				out["success"] = false
				out["error"] = redactSecrets(err.Error())
			}
		} else {
			verified, _, _, verr := currentClient().CheckinStatus(sa)
			switch {
			case verr != nil:
				out["success"] = false
				out["error"] = "verify: " + redactSecrets(verr.Error())
			case !verified:
				out["success"] = false
				out["error"] = "verify: status still unchecked"
			default:
				out["success"] = true
				out["message"] = "checked in"
			}
		}
	}

	// 查积分（签到就是为了补充积分；结果用于面板与冷却解冻）
	if u, qerr := currentClient().UserEntUsage(sa); qerr == nil {
		out["credits"] = u.Remain
		storeCreditsUsage(f.ID, u, time.Now())
		if u.Remain > 0 {
			unfreezeIfCooled(f.AuthIndex, sa)
		}
	} else if _, ok := out["credits"]; !ok {
		out["credits_error"] = redactSecrets(qerr.Error())
	}
	return out
}

// unfreezeIfCooled 签到后积分 > 0 → 解除插件级冷却（对应原 ReenableIfCredits）。
func unfreezeIfCooled(authID string, sa *traeAuth) {
	st := stateFor(authID)
	if st == nil {
		return
	}
	st.mu.Lock()
	if !st.disabled && !st.coolUntil.IsZero() {
		st.coolUntil = time.Time{}
		st.coolFor = ""
		st.note = ""
		st.errCount = 0
		st.mu.Unlock()
		_ = persistAuthDisabled(authID, sa, false)
		return
	}
	st.mu.Unlock()
}

// isAlreadyCheckedIn 已签判定：仅匹配明确表示"今日已签到"的业务错误。
// 只用无歧义标记，避免 429/5xx body 含 "checkin" 字样被误判为已签。
func isAlreadyCheckedIn(msg string) bool {
	s := strings.ToLower(msg)
	return strings.Contains(s, "已签到") ||
		strings.Contains(s, "already check") ||
		strings.Contains(s, "already checked")
}

// pruneCheckinLocks 清理已删除账号的锁条目。
func pruneCheckinLocks() {
	files, err := hostAuthList()
	if err != nil {
		return
	}
	live := make(map[string]struct{}, len(files))
	for _, f := range files {
		live[f.ID] = struct{}{}
		live[f.AuthIndex] = struct{}{}
	}
	checkinLocks.Range(func(key, _ any) bool {
		idx, _ := key.(string)
		if _, ok := live[idx]; !ok {
			checkinLocks.Delete(key)
		}
		return true
	})
}
