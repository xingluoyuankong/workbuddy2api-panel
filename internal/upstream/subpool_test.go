package upstream

import (
	"path/filepath"
	"testing"
	"time"
)

// TestIsDirectProxyLine 输入行分类：代理链接直接入池，订阅地址拉取。
// resin 形态回归：http://Link...:@host:port（带 @ 凭据）必须判为代理链接。
func TestIsDirectProxyLine(t *testing.T) {
	direct := []string{
		"http://Link.rl2.abc:@resin-proxy.x.com:2268",      // resin http 线路（空密码带@）
		"socks5h://Link.rl2.xyz:pw@resin-proxy.x.com:2269", // resin socks5 线路
		"socks5://warp:1080",                               // 无凭据 socks
		"https://user:pass@proxy.example.com:8080",         // https 代理
	}
	for _, s := range direct {
		if !isDirectProxyLine(s) {
			t.Errorf("should be direct proxy line: %q", s)
		}
	}
	subs := []string{
		"https://sub.example.com/clash/abc123",         // 订阅地址（无 @）
		"http://sub.example.com/list",                  // http 订阅
		"https://sub.example.com/sub?token=abc&host=x", // query 里的 @ 前无 userinfo
	}
	for _, s := range subs {
		if isDirectProxyLine(s) {
			t.Errorf("should be subscription URL, not proxy: %q", s)
		}
	}
}

// TestSubPoolApplySubs 三种写入模式的语义必须互不混淆——旧面板只有"保存"
// （=整体覆盖），用户想加一条链接会把整池冲掉，这正是要修的问题。
//
//   - replace：用给定列表整体覆盖
//   - append ：保序追加到尾部，已有条目一条不动，重复条目去重
//   - clear  ：列表与该 realm 出口池一起清空（onChange 收到空串 = 清池）
//
// 全部用"直接代理链接"（带 @ 凭据的 http）构造，不触发任何网络拉取。
func TestSubPoolApplySubs(t *testing.T) {
	file := filepath.Join(t.TempDir(), "subpool.json")
	var pushed []string
	m := NewSubPool(file, time.Hour, func(realm, joined string) error {
		pushed = append(pushed, realm+"="+joined)
		return nil
	})

	a := "http://u1:p1@h1.example.com:1080"
	b := "http://u2:p2@h2.example.com:1080"
	c := "http://u3:p3@h3.example.com:1080"

	if err := m.ApplySubs("cn", []string{a, b}, SubModeReplace); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if got := m.Subs("cn"); len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("replace 后应为 [a b]，got %v", got)
	}

	// append：只加不删，保序接到尾部
	if err := m.ApplySubs("cn", []string{c}, SubModeAppend); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := m.Subs("cn"); len(got) != 3 || got[0] != a || got[1] != b || got[2] != c {
		t.Fatalf("append 应保序追加到尾部 [a b c]，got %v", got)
	}

	// 全是重复条目 = 无操作：不改变条数、也不该触发刷新（onChange）
	before := len(pushed)
	if err := m.AppendSubs("cn", []string{a, c}); err != nil {
		t.Fatalf("append dup: %v", err)
	}
	if got := m.Subs("cn"); len(got) != 3 {
		t.Fatalf("重复追加不该改变条数，got %v", got)
	}
	if len(pushed) != before {
		t.Fatalf("全重复条目不该触发 onChange（无效刷新），pushed %d -> %d", before, len(pushed))
	}

	// 空 mode 回落 replace（旧客户端不带 mode 字段的向后兼容）
	if err := m.ApplySubs("cn", []string{a}, ""); err != nil {
		t.Fatalf("empty mode: %v", err)
	}
	if got := m.Subs("cn"); len(got) != 1 || got[0] != a {
		t.Fatalf("空 mode 应等同 replace，got %v", got)
	}

	// 两池不互通：cn 的增删不影响 global
	if err := m.ApplySubs("global", []string{b}, SubModeAppend); err != nil {
		t.Fatalf("global append: %v", err)
	}
	if got := m.Subs("cn"); len(got) != 1 {
		t.Fatalf("global 的操作污染了 cn 池，got %v", got)
	}

	// clear：列表与池一起清空
	if err := m.Clear("cn"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got := m.Subs("cn"); len(got) != 0 {
		t.Fatalf("clear 后列表应为空，got %v", got)
	}
	if got := m.Subs("global"); len(got) != 1 {
		t.Fatalf("clear cn 不该影响 global，got %v", got)
	}
	if last := pushed[len(pushed)-1]; last != "cn=" {
		t.Fatalf("clear 必须把出口池一并清空（onChange 收到空串），got %q", last)
	}
}
