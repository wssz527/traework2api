// authfile.go 凭证文件的宿主读写与序列化。
//
// 参照 workbuddy 插件：读走 host.auth.list / host.auth.get，写走 host.auth.save。
// 插件自己不直接碰磁盘（凭证目录由宿主 auth store 管理）。
package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// unsafeUIDChars 文件名不允许的字符。
var unsafeUIDChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func sanitizeUIDForFileName(uid string) string {
	uid = strings.TrimSpace(uid)
	uid = unsafeUIDChars.ReplaceAllString(uid, "_")
	if uid == "" || uid == "." || uid == ".." {
		return ""
	}
	if len(uid) > 64 {
		uid = uid[:64]
	}
	return uid
}

func authFileNameFor(sa *traeAuth) string {
	if sa != nil {
		if uid := sanitizeUIDForFileName(sa.UID); uid != "" {
			return providerName + "-" + uid + ".json"
		}
	}
	return authFileName
}

type hostAuthPhysical struct {
	AuthIndex string
	Name      string
	Path      string
	JSON      []byte
	Disabled  bool
}

// rpcHostAuthListResponse mirrors the host's host.auth.list envelope result.
type rpcHostAuthListResponse struct {
	Files []pluginapi.HostAuthFileEntry `json:"files"`
}

type rpcHostAuthGetResponse struct {
	AuthIndex string          `json:"auth_index"`
	Name      string          `json:"name"`
	Path      string          `json:"path"`
	JSON      json.RawMessage `json:"json"`
}

// hostAuthList 返回宿主已知的 trae 凭证。
// 按文件名前缀过滤（不依赖 type/provider 字段——存量文件可能没写）。
func hostAuthList() ([]pluginapi.HostAuthFileEntry, error) {
	raw, err := hostCall(pluginabi.MethodHostAuthList, nil)
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		return nil, fmt.Errorf("host.auth.list: bad envelope")
	}
	var resp rpcHostAuthListResponse
	if err := json.Unmarshal(env.Result, &resp); err != nil {
		return nil, err
	}
	out := make([]pluginapi.HostAuthFileEntry, 0, len(resp.Files))
	prefix := providerName + "-"
	for _, f := range resp.Files {
		if strings.HasPrefix(strings.ToLower(f.Name), prefix) {
			out = append(out, f)
		}
	}
	return out, nil
}

// hostAuthGet 取一个 auth index 的凭证。
func hostAuthGet(authIndex string) (*traeAuth, error) {
	phys, err := hostAuthGetPhysical(authIndex)
	if err != nil {
		return nil, err
	}
	return parseStored(phys.JSON)
}

// hostAuthGetBundle 一次 RPC 同时取凭证与物理元数据。
func hostAuthGetBundle(authIndex string) (*traeAuth, *hostAuthPhysical, error) {
	phys, err := hostAuthGetPhysical(authIndex)
	if err != nil {
		return nil, nil, err
	}
	sa, err := parseStored(phys.JSON)
	if err != nil {
		return nil, phys, err
	}
	return sa, phys, nil
}

func hostAuthGetPhysical(authIndex string) (*hostAuthPhysical, error) {
	body, _ := json.Marshal(map[string]string{"auth_index": authIndex})
	raw, err := hostCall(pluginabi.MethodHostAuthGet, body)
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		return nil, fmt.Errorf("host.auth.get: bad envelope")
	}
	var resp rpcHostAuthGetResponse
	if err := json.Unmarshal(env.Result, &resp); err != nil {
		return nil, err
	}
	return &hostAuthPhysical{
		AuthIndex: resp.AuthIndex,
		Name:      resp.Name,
		Path:      resp.Path,
		JSON:      resp.JSON,
		Disabled:  parseDisabledFromAuthJSON(resp.JSON),
	}, nil
}

func parseDisabledFromAuthJSON(raw []byte) bool {
	var m struct {
		Disabled bool `json:"disabled"`
	}
	_ = json.Unmarshal(raw, &m)
	return m.Disabled
}

// hostAuthSaveJSON 通过 host.auth.save 持久化凭证。
func hostAuthSaveJSON(name string, raw []byte) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("empty auth file name")
	}
	saveReq := pluginapi.HostAuthSaveRequest{Name: name, JSON: raw}
	saveBody, _ := json.Marshal(saveReq)
	rawResp, err := hostCall(pluginabi.MethodHostAuthSave, saveBody)
	if err != nil {
		return fmt.Errorf("host.auth.save: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(rawResp, &env); err != nil || !env.OK {
		msg := "host.auth.save failed"
		if env.Error != nil && env.Error.Message != "" {
			msg = truncateRedacted(env.Error.Message, 200)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// authFileJSON 把嵌套凭证包装成宿主落盘格式：顶层加 type/provider/logo/note/disabled。
// "type":"trae" 是必须的——宿主按顶层 type 把凭证路由给插件（迁移报告 §3.2）。
func authFileJSON(nestedRaw []byte, disabled bool) ([]byte, error) {
	var nested map[string]any
	if err := json.Unmarshal(nestedRaw, &nested); err != nil {
		return nil, err
	}
	note := ""
	if a, ok := nested["account"].(map[string]any); ok {
		if nick, _ := a["nickname"].(string); nick != "" {
			note = nick
		}
	}
	out := map[string]any{
		"type":     providerName,
		"provider": providerName,
		"logo":     pluginLogoURL,
		"disabled": disabled,
		"note":     note,
		"auth":     nested["auth"],
		"account":  nested["account"],
	}
	return json.Marshal(out)
}

// persistAuth 把凭证写回宿主（文件名按 UID 派生）。
// disabled 参数同时更新内存态（marshalNested 序列化 a.Disabled），
// 否则宿主凭证里的 disabled 标记会在下次任意写盘时被抹回 false。
func persistAuth(sa *traeAuth, disabled bool) error {
	if sa == nil {
		return fmt.Errorf("nil auth")
	}
	sa.mu.Lock()
	sa.Disabled = disabled
	sa.mu.Unlock()
	nested, err := sa.marshalNested()
	if err != nil {
		return err
	}
	return hostAuthSaveJSON(authFileNameFor(sa), nested)
}

// toAuthData 构造宿主侧 auth 记录。
func toAuthData(sa *traeAuth) pluginapi.AuthData {
	storage, _ := sa.marshalNested()
	id := providerName
	fileName := authFileName
	if uid := sanitizeUIDForFileName(sa.UID); uid != "" {
		id = uid
		fileName = providerName + "-" + uid + ".json"
	}
	label := sa.Nickname
	if label == "" {
		label = sa.UID
	}
	return pluginapi.AuthData{
		Provider:    providerName,
		ID:          id,
		FileName:    fileName,
		Label:       label,
		StorageJSON: storage,
		Metadata:    enrichAuthMetadata(sa, nil, false),
	}
}

// enrichAuthMetadata 标准 auth metadata：`type` 是宿主分类必需字段。
func enrichAuthMetadata(sa *traeAuth, cr *creditsSummary, disabled bool) map[string]any {
	note := displayNote(sa, cr, disabled)
	return map[string]any{
		"type":     providerName,
		"provider": providerName,
		"logo":     pluginLogoURL,
		"note":     note,
		"disabled": disabled,
	}
}

// displayNote 面板显示的备注（脱敏：不含 token）。
func displayNote(sa *traeAuth, cr *creditsSummary, disabled bool) string {
	if sa == nil {
		return ""
	}
	var b strings.Builder
	if sa.Nickname != "" {
		b.WriteString(sa.Nickname)
	} else if sa.UID != "" {
		b.WriteString("uid ")
		b.WriteString(maskUID(sa.UID))
	}
	if cr != nil {
		b.WriteString(fmt.Sprintf(" · 剩余 %d", cr.TotalRemain))
	}
	if disabled {
		b.WriteString(" · 已禁用")
	}
	return b.String()
}

// maskUID 脱敏 UID：保留首尾各 4 位，中间打码。账号列表接口用。
func maskUID(uid string) string {
	uid = strings.TrimSpace(uid)
	if len(uid) <= 8 {
		if len(uid) <= 2 {
			return strings.Repeat("*", len(uid))
		}
		return uid[:1] + strings.Repeat("*", len(uid)-2) + uid[len(uid)-1:]
	}
	return uid[:4] + strings.Repeat("*", len(uid)-8) + uid[len(uid)-4:]
}
