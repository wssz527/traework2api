// signin 一次性批量签到工具：遍历 ./auths/trae-*.json 全部账号，
// 自动 RefreshToken（过期时），逐个签到，顺手查积分。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"traework2api/internal/auth"
	"traework2api/internal/upstream"
)

type row struct {
	file   string
	uid    string
	nick   string
	status string // OK | ALREADY | FAIL | AUTH_INVALID | LOAD_ERR
	detail string
	remain int64
	hasRem bool
}

func main() {
	dir := "auths"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	files, err := filepath.Glob(filepath.Join(dir, "trae-*.json"))
	if err != nil || len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no auth files in %s\n", dir)
		os.Exit(1)
	}
	sort.Strings(files)
	up := upstream.New()

	// 本机官方客户端签到设备标识，懒探测一次，全账号复用。
	var (
		detOnce               bool
		detID, detBrand, detT string
		detOK                 bool
	)
	detect := func() bool {
		if !detOnce {
			detID, detBrand, detT, detOK = auth.DetectLocalCheckinDevice()
			detOnce = true
		}
		return detOK
	}
	var notes []string
	idUsers := map[string][]string{} // 签到设备ID → 使用该ID的账号（查多账号共用）

	var rows []row
	okN, alreadyN, failN := 0, 0, 0
	for _, f := range files {
		r := row{file: filepath.Base(f)}
		raw, err := os.ReadFile(f)
		if err != nil {
			r.status, r.detail = "LOAD_ERR", err.Error()
			rows = append(rows, r)
			failN++
			continue
		}
		a, err := auth.Parse(raw)
		if err != nil {
			r.status, r.detail = "LOAD_ERR", err.Error()
			rows = append(rows, r)
			failN++
			continue
		}
		a.FilePath = f
		r.uid, r.nick = a.UID, a.Nickname

		// refresh 过期 token
		if a.NeedsRefresh(2 * time.Hour) {
			if err := up.RefreshToken(a); err != nil {
				if ue, ok := err.(*upstream.Error); ok && ue.Kind == upstream.ErrSessionDead {
					r.status = "AUTH_INVALID"
				} else {
					r.status = "FAIL"
				}
				r.detail = "refresh: " + short(err.Error())
				rows = append(rows, r)
				failN++
				continue
			}
			_ = a.SaveAtomic()
		}

		// 签到
		if a.CheckinDeviceID == "" && detect() {
			// 凭证缺独立签到设备标识时，从本机已登录的官方客户端自动读取，
			// 并写回凭证文件（容器内 scheduler 读不到本机客户端数据，靠落盘共享）。
			a.CheckinDeviceID, a.CheckinDeviceBrand, a.CheckinDeviceType = detID, detBrand, detT
			if err := a.SaveAtomic(); err != nil {
				notes = append(notes, fmt.Sprintf("%s: 自动读取签到设备ID成功但写回凭证失败: %v", r.uid, err))
			} else {
				notes = append(notes, fmt.Sprintf("%s: 已从本机官方客户端自动读取签到设备ID并写回凭证", r.uid))
			}
		}
		if a.CheckinDeviceID == "" {
			// 凭证缺少独立签到设备标识：签到请求必须带与模型通道不同的
			// 设备标识，且多账号之间必须各不相同（相同会被上游关联）。
			// 这里明确报 FAIL 并说明原因，不发无设备头的无效请求，
			// 也不生成随机假 ID（上游无法识别，签到永远不会生效）。
			r.status = "FAIL"
			r.detail = "缺签到设备ID且本机未检测到官方客户端，无法自动读取；手动获取见 README「签到设备ID」"
			failN++
		} else {
			idUsers[a.CheckinDeviceID] = append(idUsers[a.CheckinDeviceID], r.uid)
			checkedIn, _, enable, serr := up.CheckinStatus(a)
			switch {
			case serr != nil:
				if isAlready(serr.Error()) {
					r.status = "ALREADY"
					r.detail = short(serr.Error())
					alreadyN++
				} else {
					r.status = "FAIL"
					r.detail = short(serr.Error())
					failN++
				}
			case checkedIn:
				r.status = "ALREADY"
				r.detail = "already checked in"
				alreadyN++
			case !enable:
				r.status = "FAIL"
				r.detail = "checkin disabled"
				failN++
			default:
				if err := up.CheckinClaim(a); err != nil {
					r.status = "FAIL"
					r.detail = short(err.Error())
					failN++
				} else {
					verified, _, _, err := up.CheckinStatus(a)
					if err != nil {
						r.status = "FAIL"
						r.detail = "verify: " + short(err.Error())
						failN++
					} else if !verified {
						r.status = "FAIL"
						r.detail = "verify: status still unchecked"
						failN++
					} else {
						r.status = "OK"
						okN++
					}
				}
			}
		}
		// 查积分
		if remain, qerr := up.UserEntUsage(a); qerr == nil {
			r.remain, r.hasRem = remain, true
		}
		rows = append(rows, r)
	}

	// 报告
	fmt.Printf("uid                                  | nick        | status       | remain | detail\n")
	fmt.Printf("-------------------------------------+-------------+--------------+--------+------------------------------\n")
	for _, r := range rows {
		remain := "-"
		if r.hasRem {
			remain = fmt.Sprintf("%d", r.remain)
		}
		fmt.Printf("%-36s | %-11s | %-12s | %-6s | %s\n",
			trunc(r.uid, 36), trunc(r.nick, 11), r.status, remain, r.detail)
	}
	fmt.Printf("\ntotal=%d ok=%d already=%d fail=%d\n", len(rows), okN, alreadyN, failN)
	for _, n := range notes {
		fmt.Println("note:", n)
	}
	// 多账号共用同一签到设备ID 会被上游关联，明确警告。
	for _, uids := range idUsers {
		if len(uids) > 1 {
			fmt.Printf("WARN: 签到设备ID 被 %d 个账号共用（%s），多账号须各用不同设备ID，否则可能被上游关联\n",
				len(uids), strings.Join(uids, ", "))
		}
	}
}

// isAlready 已签判定：仅匹配明确表示"今日已签到"的业务错误。
// 只用无歧义标记，避免 429/5xx body 含 "checkin" 字样被误判为已签。
func isAlready(msg string) bool {
	s := strings.ToLower(msg)
	return strings.Contains(s, "已签到") ||
		strings.Contains(s, "already check") ||
		strings.Contains(s, "already checked")
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func short(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 60 {
		return s[:60]
	}
	return s
}
