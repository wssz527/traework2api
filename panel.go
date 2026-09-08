// panel.go 面板页面资源（/v0/resource/plugins/trae/panel）。
//
// 自包含 HTML：用宿主 management 接口取数据（同源 /v0/management/...），
// 不引入外部依赖。
package main

import (
	_ "embed"
	"strings"
	"time"
)

//go:embed panel.html
var panelHTML string

// servePanel 返回面板 HTML；sub 只接受 /panel（其它路径返回 404 文本）。
func servePanel(sub string) []byte {
	sub = strings.TrimRight(strings.TrimSpace(sub), "/")
	switch sub {
	case "", "/panel":
		return []byte(panelHTML)
	}
	return []byte("not found")
}

// nowUTC 单一时间源，便于测试替换。
func nowUTC() time.Time { return time.Now() }
