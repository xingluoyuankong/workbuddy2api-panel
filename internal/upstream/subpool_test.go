package upstream

import "testing"

// TestIsDirectProxyLine 输入行分类：代理链接直接入池，订阅地址拉取。
// resin 形态回归：http://Link...:@host:port（带 @ 凭据）必须判为代理链接。
func TestIsDirectProxyLine(t *testing.T) {
	direct := []string{
		"http://Link.rl2.abc:@resin-proxy.x.com:2268",          // resin http 线路（空密码带@）
		"socks5h://Link.rl2.xyz:pw@resin-proxy.x.com:2269",     // resin socks5 线路
		"socks5://warp:1080",                                    // 无凭据 socks
		"https://user:pass@proxy.example.com:8080",              // https 代理
	}
	for _, s := range direct {
		if !isDirectProxyLine(s) {
			t.Errorf("should be direct proxy line: %q", s)
		}
	}
	subs := []string{
		"https://sub.example.com/clash/abc123",                  // 订阅地址（无 @）
		"http://sub.example.com/list",                            // http 订阅
		"https://sub.example.com/sub?token=abc&host=x",          // query 里的 @ 前无 userinfo
	}
	for _, s := range subs {
		if isDirectProxyLine(s) {
			t.Errorf("should be subscription URL, not proxy: %q", s)
		}
	}
}
