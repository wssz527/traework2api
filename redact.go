// redact.go 从任何可能进入日志、错误响应或 usage 上报的字符串中剥离凭证。
// 所有把上游错误文本暴露出去的代码路径都必须过 redactSecrets。
package main

import "regexp"

var (
	redactREBearer  = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9._\-+/=]{12,}`)
	redactREJWT     = regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b`)
	redactRETokenKV = regexp.MustCompile(`(?i)((?:access_token|refresh_token|id_token|accesstoken|refreshtoken)\s*[=:]\s*)([A-Za-z0-9._\-+/=]{12,})`)
	// TRAE 的凭证字段是嵌套 JSON 里的 accessToken/refreshToken 字符串值，
	// 与 Bearer/JWT 形态不同（自定义前缀），单独匹配。
	redactREJSONToken = regexp.MustCompile(`(?i)"(accessToken|refreshToken)"\s*:\s*"([^"]{8,})"`)
	redactREJWTLoose  = regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{8,}(?:\.[A-Za-z0-9_\-]{4,}){1,2}\b`)
)

// redactSecrets 剥离 bearer token / JWT / 凭证字段值。
func redactSecrets(s string) string {
	if s == "" {
		return s
	}
	s = redactREBearer.ReplaceAllString(s, "Bearer ***")
	s = redactREJWT.ReplaceAllString(s, "***jwt***")
	s = redactRETokenKV.ReplaceAllString(s, "${1}***")
	s = redactREJSONToken.ReplaceAllString(s, `"${1}":"***"`)
	s = redactREJWTLoose.ReplaceAllString(s, "***jwt***")
	return s
}

// truncateRedacted redacts secrets then truncates — 用于任何返回给客户端/日志的
// 错误体。
func truncateRedacted(s string, n int) string {
	return truncate(redactSecrets(s), n)
}

// truncate cuts s to at most n bytes.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
