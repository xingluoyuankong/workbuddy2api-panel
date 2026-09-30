package upstream

import (
	"sync"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// authOf 构造一个最小 CN 账号（只用到 UID）。
func authOf(uid string) *auth.Auth {
	return &auth.Auth{UID: uid, Nickname: "t"}
}

// TestFirstIPIn 回显形态五花八门，抠 IP 必须稳。
func TestFirstIPIn(t *testing.T) {
	cases := []struct{ in, want string }{
		{"1.2.3.4", "1.2.3.4"},
		{"  1.2.3.4\n", "1.2.3.4"},
		{"Your IP: 8.8.8.8 </body>", "8.8.8.8"},
		{"2001:db8::1", "2001:db8::1"},
		{"no ip here", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := firstIPIn(c.in); got != c.want {
			t.Errorf("firstIPIn(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestExpectedIPOf 声明 IP 只认显式 expected_ip。
// host 一律不当声明：入口≠出口是聚合代理（resin 等）的常态，曾因把 host
// 自动当声明出口，把用户配置正确的代理误判成 mismatch。
func TestExpectedIPOf(t *testing.T) {
	if got := expectedIPOf(&AccountProxyEntry{Proxy: "socks5://user:pw@1.2.3.4:60000"}); got != "" {
		t.Errorf("host 不该被当声明: got %q, want empty", got)
	}
	if got := expectedIPOf(&AccountProxyEntry{Proxy: "socks5://warp:1080"}); got != "" {
		t.Errorf("host-is-name: got %q, want empty", got)
	}
	if got := expectedIPOf(&AccountProxyEntry{Proxy: "http://gate.example.com:8080", ExpectedIP: "9.9.9.9"}); got != "9.9.9.9" {
		t.Errorf("explicit: got %q, want 9.9.9.9", got)
	}
}

// TestAcceptLanguageForCountry 出口地 → 语言映射（非华语出口不该再发 zh-CN）。
func TestAcceptLanguageForCountry(t *testing.T) {
	cases := map[string]string{
		"US": "en-US", "JP": "ja-JP", "DE": "de-DE", "SG": "en-SG", "BR": "pt-BR",
		"ZZ": "en-US", // 未知国家回落到 en-US
	}
	for cc, want := range cases {
		if got := acceptLanguageForCountry(cc); got != want {
			t.Errorf("acceptLanguageForCountry(%q) = %q, want %q", cc, got, want)
		}
	}
}

// TestAcceptLanguageProxyGeo 绑了代理的账号按实测出口地对齐语言；未绑 / geo 未知
// 时必须回落既有行为（zh-CN），不得因为拿不到 geo 就改头。
func TestAcceptLanguageProxyGeo(t *testing.T) {
	base := New()
	if got := base.acceptLanguageFor(nil); got != "zh-CN" {
		t.Fatalf("无代理无账号: got %q, want zh-CN", got)
	}

	c := New()
	m := NewAccountProxy(AccountProxyOptions{File: "/tmp/px-test.json"})
	spec := &AccountProxyEntry{Proxy: "socks5://warp:1080", Enabled: true}
	b, err := newAccountBinding("uid-1", spec)
	if err != nil {
		t.Fatalf("newAccountBinding: %v", err)
	}
	m.bindings["uid-1"] = b
	c.AccountProxy = m

	// geo 未知 → 回落 zh-CN（保守，不猜）
	if got := c.acceptLanguageFor(authOf("uid-1")); got != "zh-CN" {
		t.Errorf("geo 未知: got %q, want zh-CN", got)
	}
	// 美国出口 → 不该再发 zh-CN
	b.countryC.Store("US")
	if got := c.acceptLanguageFor(authOf("uid-1")); got != "en-US" {
		t.Errorf("US 出口: got %q, want en-US", got)
	}
	// 香港出口 → zh-HK（华语区细分）
	b.countryC.Store("HK")
	if got := c.acceptLanguageFor(authOf("uid-1")); got != "zh-HK" {
		t.Errorf("HK 出口: got %q, want zh-HK", got)
	}
	// 国内出口 → zh-CN（与既有行为一致）
	b.countryC.Store("CN")
	if got := c.acceptLanguageFor(authOf("uid-1")); got != "zh-CN" {
		t.Errorf("CN 出口: got %q, want zh-CN", got)
	}
	// 未绑代理的账号不受影响
	if got := c.acceptLanguageFor(authOf("uid-unknown")); got != "zh-CN" {
		t.Errorf("未绑代理: got %q, want zh-CN", got)
	}
}

// TestUsableState 失配处置策略：fallback 下失配即不采用该代理；ignore 下仍采用。
func TestUsableState(t *testing.T) {
	fallback := NewAccountProxy(AccountProxyOptions{OnMismatch: MismatchFallback})
	if fallback.usableState(proxyStateMismatch) {
		t.Error("fallback: 失配仍被判可用")
	}
	if !fallback.usableState(proxyStateUnchecked) {
		t.Error("fallback: 未校验应放行（否则刚启动全部回落）")
	}
	if !fallback.usableState(proxyStateOK) {
		t.Error("fallback: 正常应可用")
	}
	ignore := NewAccountProxy(AccountProxyOptions{OnMismatch: MismatchIgnore})
	if !ignore.usableState(proxyStateMismatch) {
		t.Error("ignore: 失配应仍可用（只告警）")
	}
}

// TestProxyThrottle 每账号节流：相邻两次出站至少隔 MinInterval，且并发时不会堆成无限排队。
func TestProxyThrottle(t *testing.T) {
	const gap = 200 * time.Millisecond
	m := NewAccountProxy(AccountProxyOptions{MinInterval: gap})
	b, err := newAccountBinding("uid-t", &AccountProxyEntry{Proxy: "socks5://127.0.0.1:1", Enabled: true})
	if err != nil {
		t.Fatalf("newAccountBinding: %v", err)
	}
	m.bindings["uid-t"] = b

	// 串行 3 次：第一次立即，后两次各排一个间隔 → 总耗时 ≈ 2×gap。
	start := time.Now()
	for i := 0; i < 3; i++ {
		m.Wait("uid-t")
	}
	el := time.Since(start)
	if el < 2*gap-gap/4 {
		t.Errorf("节流未生效：3 次 Wait 仅耗时 %v，应 ≈ %v", el, 2*gap)
	}
	if el > 4*gap {
		t.Errorf("节流过度：3 次 Wait 耗时 %v（> %v）", el, 4*gap)
	}

	// 关闭节流必须是空操作（未配置的部署不能凭空变慢）。
	m2 := NewAccountProxy(AccountProxyOptions{})
	m2.bindings["uid-t"] = b
	start = time.Now()
	for i := 0; i < 50; i++ {
		m2.Wait("uid-t")
	}
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Errorf("MinInterval=0 时 Wait 仍有耗时 %v", el)
	}
}

// TestProxyThrottleConcurrent 并发调用 Wait 时排队必须有上限（maxWait=3×interval），
// 不能把并发请求无限往后堆——否则高并发下延迟全部转成超时。
func TestProxyThrottleConcurrent(t *testing.T) {
	const gap = 100 * time.Millisecond
	m := NewAccountProxy(AccountProxyOptions{MinInterval: gap})
	b, _ := newAccountBinding("uid-c", &AccountProxyEntry{Proxy: "socks5://127.0.0.1:1", Enabled: true})
	m.bindings["uid-c"] = b

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Wait("uid-c")
		}()
	}
	wg.Wait()
	el := time.Since(start)
	// 12 个并发若严格排队要 1.1s；有 maxWait 兜底后应显著更短。
	if el > 600*time.Millisecond {
		t.Errorf("并发排队未设上限：12 个并发 Wait 耗时 %v（应 ≤ 3×interval 量级）", el)
	}
}

// TestSharedIPQuarantine 同出口 IP 承载账号超限时的隔离：保留前 MaxPerIP 个（按 uid
// 排序，结果必须确定），其余的 Usable=false。warn 模式下不得隔离任何人。
func TestSharedIPQuarantine(t *testing.T) {
	mk := func(action string) *AccountProxy {
		m := NewAccountProxy(AccountProxyOptions{MaxPerIP: 1, MaxPerIPAction: action})
		for _, uid := range []string{"uid-b", "uid-a", "uid-c"} {
			b, _ := newAccountBinding(uid, &AccountProxyEntry{Proxy: "socks5://127.0.0.1:1", Enabled: true})
			b.probeIP.Store("9.9.9.9") // 三个账号共用同一出口
			m.bindings[uid] = b
		}
		return m
	}

	m := mk(MaxPerIPQuarantine)
	m.recomputeShared()
	if got := m.sharedCount("9.9.9.9"); got != 3 {
		t.Fatalf("sharedCount = %d, want 3", got)
	}
	// 保留排序第一的 uid-a，其余（uid-b / uid-c）隔离。
	if !m.Usable("uid-a") {
		t.Error("uid-a 应被保留（排序第一），却被隔离")
	}
	if m.Usable("uid-b") {
		t.Error("uid-b 应被隔离（排序靠后），却仍可用")
	}
	if m.Usable("uid-c") {
		t.Error("uid-c 应被隔离（排序靠后），却仍可用")
	}
	// 结果必须稳定：重复计算不该换人（否则表现为账号随机不可用）。
	for i := 0; i < 5; i++ {
		m.recomputeShared()
		if m.Usable("uid-b") || m.Usable("uid-c") || !m.Usable("uid-a") {
			t.Fatalf("第 %d 轮隔离结果不稳定", i)
		}
	}

	w := mk(MaxPerIPWarn)
	w.recomputeShared()
	for _, uid := range []string{"uid-a", "uid-b", "uid-c"} {
		if !w.Usable(uid) {
			t.Errorf("warn 模式下 %s 不应被隔离", uid)
		}
	}
}
