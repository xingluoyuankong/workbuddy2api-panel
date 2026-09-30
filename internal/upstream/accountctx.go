// accountctx.go 出站 client 的「账号感知」选取（per-account 代理接入点）。
//
// 【为什么要绕 ctx 这一层】
// upstream 包里出站请求的构造点有十几处，doJSON/doStream 这类公共方法只拿得到
// *http.Request，拿不到是哪个账号在发。逐个改签名要把十几处调用链全部动一遍，
// 回归面太大。请求对象本身已经带 context，于是把账号塞进 ctx：构造请求的地方
// 顺手打标（reqWithAccount），公共方法从 req.Context() 读出来选 client。
// 不打标的请求读不到账号 → 回落既有 realm 逻辑，行为零变化。
package upstream

import (
	"context"
	"net/http"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// accountCtxKey 账号上下文键（私有类型，外部无法伪造同名 key）。
type accountCtxKey struct{}

// reqWithAccount 把账号绑定到请求上下文（原地改写，调用方无需接收返回值）。
// 重复打标是幂等的（已有账号则不覆盖）——同一条请求被多个头注入函数打标时
// 以第一次为准。
func reqWithAccount(req *http.Request, a *auth.Auth) {
	if req == nil || a == nil {
		return
	}
	if req.Context().Value(accountCtxKey{}) != nil {
		return
	}
	*req = *req.WithContext(context.WithValue(req.Context(), accountCtxKey{}, a))
}

// accountFromReq 从请求上下文取账号；无则返回 nil。
func accountFromReq(req *http.Request) *auth.Auth {
	if req == nil {
		return nil
	}
	a, _ := req.Context().Value(accountCtxKey{}).(*auth.Auth)
	return a
}

// httpForA 按账号取普通出站 client：账号代理（若配置且健康）优先，
// 否则回落 realm 默认出口（global 代理池 / 直连）。
func (c *Client) httpForA(a *auth.Auth) *http.Client {
	if c != nil && c.AccountProxy != nil && a != nil {
		if hc, ok := c.AccountProxy.HTTPFor(a.UID); ok {
			return hc
		}
	}
	return c.httpFor(a.Realm())
}

// chatHTTPForA 同上，取流式（无总超时）client。
func (c *Client) chatHTTPForA(a *auth.Auth) *http.Client {
	if c != nil && c.AccountProxy != nil && a != nil {
		if hc, ok := c.AccountProxy.ChatHTTPFor(a.UID); ok {
			return hc
		}
	}
	return c.chatHTTPFor(a.Realm())
}

// httpForReq 从请求上下文反查账号后选 client（doJSON 这类只拿到 req 的公共路径用）。
func (c *Client) httpForReq(req *http.Request) *http.Client {
	if a := accountFromReq(req); a != nil {
		return c.httpForA(a)
	}
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}
