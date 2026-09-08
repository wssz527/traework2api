// credits.go 积分缓存 + 账号行结构（面板与管理接口共用）。
package main

import (
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// accountCacheTTL 积分缓存时长。
const accountCacheTTL = 5 * time.Minute

type creditsSummary struct {
	TotalRemain int64  `json:"total_remain"`
	FetchedAt   string `json:"fetched_at,omitempty"`
}

type accountCacheEntry struct {
	credits *creditsSummary
	fetched time.Time
}

var accountCache sync.Map // authID -> *accountCacheEntry

// storeCredits 写入积分快照（脱敏：只存数字，不含 token）。
func storeCredits(authID string, remain int64, at time.Time) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	accountCache.Store(authID, &accountCacheEntry{
		credits: &creditsSummary{TotalRemain: remain, FetchedAt: at.UTC().Format(time.RFC3339)},
		fetched: at,
	})
}

// cachedCredits 读取缓存的剩余积分；ok=false 表示未知。
func cachedCredits(authID string) (int64, bool) {
	v, ok := accountCache.Load(strings.TrimSpace(authID))
	if !ok {
		return 0, false
	}
	e, ok := v.(*accountCacheEntry)
	if !ok || e.credits == nil || time.Since(e.fetched) > accountCacheTTL {
		return 0, false
	}
	return e.credits.TotalRemain, true
}

// invalidateCredits 让某账号的积分缓存失效（聊天成功后调用）。
func invalidateCredits(authID string) {
	accountCache.Delete(strings.TrimSpace(authID))
}

// isCreditsExhausted 积分耗尽判定（<=0 视为耗尽）。
func isCreditsExhausted(cr *creditsSummary) bool {
	return cr == nil || cr.TotalRemain <= 0
}

// traeAccount 面板/账号列表的一行（全程脱敏）。
type traeAccount struct {
	AuthIndex string `json:"auth_index"`
	AuthID    string `json:"auth_id"`
	Name      string `json:"name,omitempty"`
	Label     string `json:"label,omitempty"`
	UID       string `json:"uid,omitempty"` // 已脱敏
	Nickname  string `json:"nickname,omitempty"`
	Status    string `json:"status,omitempty"`
	Disabled  bool   `json:"disabled"`
	Cooling   bool   `json:"cooling"`
	Credits   *int64 `json:"credits,omitempty"`
	Selected  bool   `json:"selected"`
	Error     string `json:"error,omitempty"`
}

// buildAccounts 组装账号列表（脱敏 + 积分缓存）。
func buildAccounts(force bool) []traeAccount {
	files, err := hostAuthList()
	if err != nil {
		return nil
	}
	pruneStaleCache(files)
	out := make([]traeAccount, 0, len(files))
	var wg sync.WaitGroup
	for i := range files {
		out = append(out, traeAccount{})
		f := files[i]
		wg.Add(1)
		go func(idx int, f pluginapi.HostAuthFileEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					out[idx].Error = "internal error"
					pluginLogf("panic building account %s: %v", f.AuthIndex, r)
				}
			}()
			out[idx] = buildOneAccount(f, force)
		}(i, f)
	}
	wg.Wait()
	active := ensureDefaultActiveAuth(out)
	for i := range out {
		out[i].Selected = out[i].AuthID == active
	}
	pruneLifecycleState()
	pruneCheckinLocks()
	return out
}

func buildOneAccount(f pluginapi.HostAuthFileEntry, force bool) traeAccount {
	a := traeAccount{
		AuthIndex: f.AuthIndex,
		AuthID:    f.ID,
		Name:      f.Name,
		Label:     f.Label,
		Status:    f.Status,
		Disabled:  f.Disabled,
	}
	sa, phys, err := hostAuthGetBundle(f.AuthIndex)
	if err != nil {
		a.Error = "load auth: " + redactSecrets(err.Error())
		return a
	}
	// 物理文件是 disabled 的真相来源（宿主 list 可能滞后）。
	if phys != nil {
		a.Disabled = phys.Disabled
		if phys.Name != "" {
			a.Name = phys.Name
		}
	}
	a.Nickname = sa.Nickname
	a.UID = maskUID(sa.UID)
	a.Cooling = isCooling(f.AuthIndex)
	if remain, ok := cachedCredits(f.ID); ok && !force {
		v := remain
		a.Credits = &v
		return a
	}
	if remain, err := currentClient().UserEntUsage(sa); err == nil {
		a.Credits = &remain
		storeCredits(f.ID, remain, time.Now())
	}
	return a
}

// pruneStaleCache 清理已删除账号的缓存条目，防止单调增长。
func pruneStaleCache(files []pluginapi.HostAuthFileEntry) {
	live := make(map[string]struct{}, len(files))
	for _, f := range files {
		live[f.ID] = struct{}{}
	}
	accountCache.Range(func(key, value any) bool {
		id, _ := key.(string)
		if _, ok := live[id]; !ok {
			accountCache.Delete(key)
			return true
		}
		if e, ok := value.(*accountCacheEntry); ok && time.Since(e.fetched) > 4*accountCacheTTL {
			accountCache.Delete(key)
		}
		return true
	})
}
