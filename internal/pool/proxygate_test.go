package pool

import (
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// TestProxyGateBlocksPick 代理闸门必须真的把账号摘出选号池。
//
// 语义：闸门只在 quarantine 策略下对「出口不可信」的账号返回 false。这个测试守住
// 的是「闸门挂上了但选号没看它」这类接线错误——那种错误线上表现是：面板显示
// 账号已隔离，请求却照样打到它，隔离形同虚设。
func TestProxyGateBlocksPick(t *testing.T) {
	p := New("") // 空 state 文件 = 纯内存池
	p.Add(&auth.Auth{UID: "uid-a", Nickname: "A"})
	p.Add(&auth.Auth{UID: "uid-b", Nickname: "B"})

	p.SetProxyGate(func(uid string) bool { return uid != "uid-a" })

	picked := map[string]int{}
	for i := 0; i < 40; i++ {
		a := p.Pick()
		if a == nil {
			continue // 防撞号窗口内可能空手，不算失败
		}
		picked[a.UID]++
		if a.UID == "uid-a" {
			t.Fatalf("被闸门隔离的账号仍被选中（第 %d 次）", i)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if picked["uid-b"] == 0 {
		t.Fatalf("闸门误伤：另一个账号一次都没被选中（picked=%v）", picked)
	}

	// 粘性路径同样要过闸门（否则会话粘住一个被隔离的号就绕过去了）。
	if got := p.PickByUIDForModel("uid-a", ""); got != nil {
		t.Fatalf("PickByUIDForModel 未过闸门，返回了被隔离账号 %s", got.UID)
	}

	// 撤销闸门后账号必须能重新出池（代理修好即自动回池，不需要重启）。
	p.SetProxyGate(nil)
	back := 0
	for i := 0; i < 40; i++ {
		if a := p.Pick(); a != nil && a.UID == "uid-a" {
			back++
		}
		time.Sleep(5 * time.Millisecond)
	}
	if back == 0 {
		t.Error("闸门撤销后 uid-a 仍未被选中（闸门不可逆？）")
	}
}

// TestProxyGateNil 未注入闸门时必须恒放行（零回归：不配账号代理的部署行为不变）。
func TestProxyGateNil(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "uid-x"})
	if !p.proxyGateOK("uid-x") {
		t.Fatal("未注入闸门时 proxyGateOK 返回 false")
	}
	p.SetProxyGate(nil)
	if !p.proxyGateOK("uid-x") {
		t.Fatal("显式设 nil 后仍不放行")
	}
}

// TestEgressProvider 出口信息注入：未注入时 Status.Egress 恒 nil（零回归），
// 注入后按 uid 取值，未知 uid 返回 nil（不 panic、不编造）。
func TestEgressProvider(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "uid-e", Nickname: "E"})

	// 未注入：nil（面板显示「直连」）
	for _, st := range p.List() {
		if st.Egress != nil {
			t.Fatalf("未注入提供者时 %s 的 Egress 应为 nil", st.UID)
		}
	}

	p.SetEgressProvider(func(uid string) *EgressInfo {
		if uid != "uid-e" {
			return nil
		}
		return &EgressInfo{IP: "104.28.215.70", CountryCode: "HK", State: "ok", ProxyHost: "warp:1080"}
	})
	var got *EgressInfo
	for _, st := range p.List() {
		if st.UID == "uid-e" {
			got = st.Egress
		}
	}
	if got == nil {
		t.Fatal("注入后 Egress 仍为 nil")
	}
	if got.IP != "104.28.215.70" || got.CountryCode != "HK" || got.State != "ok" {
		t.Fatalf("Egress 字段不符: %+v", got)
	}

	// 提供者返回 nil（该号没绑代理）也必须能正常出列表
	p.SetEgressProvider(func(string) *EgressInfo { return nil })
	for _, st := range p.List() {
		if st.Egress != nil {
			t.Fatal("提供者返回 nil 时 Egress 应为 nil")
		}
	}
}
