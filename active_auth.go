// active_auth.go 追踪面板选中的账号（scheduler.pick 用它做粘性路由）。
package main

import (
	"strings"
	"sync"
)

var (
	activeAuthID string
	activeAuthMu sync.RWMutex
)

func getActiveAuthID() string {
	activeAuthMu.RLock()
	defer activeAuthMu.RUnlock()
	return strings.TrimSpace(activeAuthID)
}

func setActiveAuthID(id string) {
	id = strings.TrimSpace(id)
	activeAuthMu.Lock()
	activeAuthID = id
	activeAuthMu.Unlock()
}

// activeAuthCandidate 是 pickActiveAuth 的瘦视图。
type activeAuthCandidate struct {
	ID        string
	Disabled  bool
	Exhausted bool
}

// pickActiveAuth 从宿主候选中选号。选中项是粘性的：只要它仍在候选列表里
// 且未禁用/未耗尽就保持；否则切到第一个未禁用的候选并记住选择。
func pickActiveAuth(candidates []activeAuthCandidate) string {
	if len(candidates) == 0 {
		return ""
	}
	byID := make(map[string]activeAuthCandidate, len(candidates))
	for _, c := range candidates {
		byID[c.ID] = c
	}

	cur := getActiveAuthID()
	if cur != "" {
		if c, ok := byID[cur]; ok && !c.Disabled && !c.Exhausted {
			return cur
		}
	}

	var next string
	for _, c := range candidates {
		if !c.Disabled && !c.Exhausted {
			next = c.ID
			break
		}
	}
	if next == "" {
		if cur != "" {
			if _, ok := byID[cur]; ok {
				return cur
			}
		}
		next = candidates[0].ID
	}
	if next != "" && next != cur {
		setActiveAuthID(next)
	}
	return next
}

// ensureDefaultActiveAuth 在 dashboard 加载时校正选中项，
// 保证面板显示的卡片与 scheduler.pick 实际路由的一致。
func ensureDefaultActiveAuth(accounts []traeAccount) string {
	cur := getActiveAuthID()
	live := make(map[string]traeAccount, len(accounts))
	for _, a := range accounts {
		live[a.AuthID] = a
	}
	if cur != "" {
		if a, ok := live[cur]; ok && !a.Disabled && !a.Cooling {
			return cur
		}
	}
	var firstAny, firstOK, firstReady string
	for _, a := range accounts {
		if firstAny == "" {
			firstAny = a.AuthID
		}
		if a.Disabled {
			continue
		}
		if firstOK == "" {
			firstOK = a.AuthID
		}
		if !a.Cooling && firstReady == "" {
			firstReady = a.AuthID
		}
	}
	next := firstReady
	if next == "" {
		if cur != "" {
			if a, ok := live[cur]; ok && !a.Disabled {
				return cur
			}
		}
		next = firstOK
	}
	if next == "" {
		next = firstAny
	}
	if next != "" && next != cur {
		setActiveAuthID(next)
	}
	return next
}
