// index.go 面板静态资源与安全响应头。
//
// 资源经 go:embed 打进二进制（随服务部署，无外部构建步骤）：
//   - index.html  页面骨架
//   - app.js      全部前端逻辑（独立文件而非内联，为了启用无需 unsafe-inline 的严格 CSP）
//
// 安全头对"面板页面与全部 /panel/api/* 响应"统一生效：CSP 限制脚本只能来自本服务，
// 禁止被 iframe 嵌套（防点击劫持），禁 MIME 嗅探，并声明不泄露 Referer 出去。
package panel

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"net/http"
	"strings"
)

//go:embed index.html
var indexHTML []byte

//go:embed app.js
var appJS []byte

// appJSVer 由 app.js 内容派生的版本指纹（取 sha256 前 8 字节十六进制）。
//
// 为什么必须有：index.html 与 app.js 是**两个独立资源**，浏览器可能缓存了旧
// app.js 而拿到新 index.html（或反之）。曾出现的事故——index.html 已改成卡片
// 布局（删掉了 mdBody 表格），而浏览器里的旧 app.js 仍去 $('mdBody').innerHTML
// 赋值，抛错后整页空白，用户以为"模型没了"。
//
// 内容变了指纹就变 → index.html 里注入的 ?v= 变化 → 浏览器必然重新拉取。
// 这是唯一能保证两个资源版本一致的手段（Cache-Control 只是建议，不能强制
// 已缓存的资源失效）。
var appJSVer = func() string {
	sum := sha256.Sum256(appJS)
	return hex.EncodeToString(sum[:8])
}()

// cacheControl 静态资源一律不缓存（no-cache = 每次带 ETag 回源校验，
// 命中则 304，成本极低；而 max-age 会让用户停留在旧版本上看不到修复）。
const cacheControl = "no-cache, must-revalidate"

// csp 内容安全策略（严格版，无需 unsafe-inline）：
//   - default-src 'none'        默认全禁，逐个开口
//   - script-src 'self'         只跑同源脚本（app.js）；页面无内联事件处理器/内联脚本
//   - style-src 'self' 'unsafe-inline'
//     style 的内联是设计取舍：页面有少量 style="..." 属性（进度条宽度、表格列宽），
//     允许内联样式不会导致脚本执行；仍禁止外部样式域与 @import 外链。
//   - connect-src 'self'        前端 fetch 只能打本服务
//   - img-src 'self' data:      图标/内联图
//   - form-action 'none'        页面无表单提交目标（配置页是 JS 提交）
//   - frame-ancestors 'none'    禁止被任何站点 iframe 嵌套（点击劫持）
//   - base-uri 'none'          禁止注入 <base> 改写相对路径
const csp = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
	"connect-src 'self'; img-src 'self' data:; form-action 'none'; " +
	"frame-ancestors 'none'; base-uri 'none'"

// setSecurityHeaders 写入面板统一安全响应头（页面与 API 都要，API 也含 JSON 数据）。
func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy", csp)
	w.Header().Set("X-Content-Type-Options", "nosniff") // 禁 MIME 嗅探
	w.Header().Set("X-Frame-Options", "DENY")           // 老浏览器兜底（CSP frame-ancestors 的等价项）
	w.Header().Set("Referrer-Policy", "no-referrer")    // 不外泄面板地址给外部站点
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
}

// index 输出面板页面（静态无秘密；数据接口 /panel/api/* 才走鉴权）。
//
// 输出前把 <script src="app.js"> 改写为带内容指纹的 <script src="app.js?v=…">，
// 保证 HTML 与 JS 版本严格绑定（见 appJSVer 注释）。
func (p *Panel) index(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	w.Header().Set("Cache-Control", cacheControl)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	html := bytes.Replace(indexHTML,
		[]byte(`src="app.js"`),
		[]byte(`src="app.js?v=`+appJSVer+`"`), 1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(html)
}

// appScript 输出前端逻辑（同源脚本，供 CSP script-src 'self' 加载）。
// ETag = 内容指纹：no-cache 下浏览器每次回源，指纹一致直接 304，不重传 body。
func (p *Panel) appScript(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	w.Header().Set("Cache-Control", cacheControl)
	w.Header().Set("ETag", `"`+appJSVer+`"`)
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	if et := strings.TrimSpace(r.Header.Get("If-None-Match")); et != "" &&
		strings.Contains(et, appJSVer) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(appJS)
}
