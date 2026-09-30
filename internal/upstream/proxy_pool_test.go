package upstream

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestProxySkipForBackoff(t *testing.T) {
	cases := []struct {
		fails int32
		want  time.Duration
	}{
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{4, 80 * time.Second},
		{5, 2 * time.Minute},
		{20, 2 * time.Minute},
	}
	for _, tc := range cases {
		if got := proxySkipFor(tc.fails); got != tc.want {
			t.Errorf("fails=%d: got %v, want %v", tc.fails, got, tc.want)
		}
	}
}

func TestProxyRTMarks403ButNot429Or503(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		fail bool
	}{
		{"waf", http.StatusForbidden, true},
		{"proxy_gateway", http.StatusBadGateway, true},
		{"upstream_rate_limit", http.StatusTooManyRequests, false},
		{"upstream_service_error", http.StatusServiceUnavailable, false},
		{"ok", http.StatusOK, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &proxyEntry{}
			rt := &proxyRT{entry: e, base: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.code, Body: io.NopCloser(strings.NewReader(""))}, nil
			})}
		resp, err := rt.RoundTrip(&http.Request{})
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if tc.fail != (e.fails.Load() == 1) {
			t.Errorf("code=%d: fails=%d, want fail=%v", tc.code, e.fails.Load(), tc.fail)
		}
	})
	}
}

func TestSetGlobalProxyMultipleAndRoundRobin(t *testing.T) {
	c := New()
	if err := c.SetGlobalProxy("socks5://warp:1080,socks5://resin:2269"); err != nil {
		t.Fatalf("SetGlobalProxy: %v", err)
	}
	if c.GlobalProxyCount() != 2 {
		t.Fatalf("proxy count=%d, want 2", c.GlobalProxyCount())
	}
	if c.HTTPGlobal != nil || c.ChatHTTPGlobal != nil {
		t.Error("multi-proxy mode should select from globalProxies, not fixed client")
	}
	first := c.pickProxy("global")
	second := c.pickProxy("global")
	if first == second {
		t.Error("two consecutive picks should rotate between exits")
	}
	if c.httpFor("cn") != c.HTTP || c.chatHTTPFor("cn") != c.chatHTTP() {
		t.Error("CN realm must remain direct")
	}
	if err := c.SetGlobalProxy(""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if c.GlobalProxyCount() != 0 {
		t.Error("clear should remove all exits")
	}
}

// TestPickGlobalProxyPrefersHealthyExit 回归 2026-09-19 出口整改。
//
// 现场：出口池是「一快一慢」（warp 20/20 成功 p50=0.23s；resin 2269 14/20
// 成功、p50=1.34s、max=16.3s），而原实现严格 50/50 轮询 → 约 15% 的 global
// 请求撞上 socks5 握手超时/拒连。整改后：坏出口一旦连续失败就让出流量，
// 只有当健康出口也出问题时才被重新探测。
func TestPickGlobalProxyPrefersHealthyExit(t *testing.T) {
	c := New()
	if err := c.SetGlobalProxy("socks5://warp:1080,socks5://resin:2269"); err != nil {
		t.Fatalf("SetGlobalProxy: %v", err)
	}
	// 全部健康时仍是轮询——新逻辑不得改变全健康下的既有行为。
	if a, b := c.pickProxy("global"), c.pickProxy("global"); a == b {
		t.Fatal("两个出口都健康时应继续轮询")
	}
	var good, bad *proxyEntry
	for _, e := range c.realmProxies["global"] {
		if strings.Contains(e.raw, "2269") {
			bad = e
		} else {
			good = e
		}
	}
	if good == nil || bad == nil {
		t.Fatal("出口识别失败")
	}

	// bad 连续失败 → 后续每次都必须落在 good（不再 50/50 平摊）。
	bad.noteFail()
	for i := 1; i <= 8; i++ {
		if got := c.pickProxy("global"); got != good {
			t.Fatalf("第 %d 次选到已连续失败的出口（应健康出口优先）", i)
		}
	}

	// good 也失败 → 无健康出口，回到全列表轮询（坏出口重新获得探测机会）。
	good.noteFail()
	seen := map[*proxyEntry]int{}
	for i := 0; i < 8; i++ {
		seen[c.pickProxy("global")]++
	}
	if seen[good] == 0 || seen[bad] == 0 {
		t.Errorf("无健康出口时应回到全列表轮询，实际 seen=%v", seen)
	}

	// bad 被一次成功洗白（fails 归零）→ 重新成为可轮询的健康出口。
	bad.noteSuccess()
	for i := 1; i <= 4; i++ {
		if got := c.pickProxy("global"); got != bad {
			t.Fatalf("洗白后第 %d 次未回到该出口（fails 应已归零）", i)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
