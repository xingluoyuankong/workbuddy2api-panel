package server

import (
	"testing"
	"time"
)

// TestWafIPGateSingleUIDRepeat 单账号池必须能触发 IP 级 fail-fast（P0-WAF-IP-SINGLE）。
// 回归背景：原实现只数「不同 UID」，单号池 len(hits) 恒 ≤1 < 阈值 2 → 保护失效。
func TestWafIPGateSingleUIDRepeat(t *testing.T) {
	oldWin, oldRep := wafIPWindow, wafIPRepeatWindow
	defer func() { wafIPWindow, wafIPRepeatWindow = oldWin, oldRep }()
	wafIPWindow = 60 * time.Second
	wafIPRepeatWindow = 10 * time.Second

	var g wafIPGate
	const uid = "only-global-account"

	// 同一账号连续命中：前 wafIPRepeatThreshold-1 次不激活
	for i := 1; i < wafIPRepeatThreshold; i++ {
		if g.noteWaf(uid) {
			t.Fatalf("第 %d 次单号命中不应激活（阈值 %d）", i, wafIPRepeatThreshold)
		}
	}
	// 第 wafIPRepeatThreshold 次激活
	if !g.noteWaf(uid) {
		t.Fatalf("单号第 %d 次命中应激活 IP 级拦截", wafIPRepeatThreshold)
	}
	if !g.active() {
		t.Fatal("激活后 active() 应为 true")
	}
	// 激活期内继续命中仍为 true（不续期）
	if !g.noteWaf(uid) {
		t.Fatal("激活期内命中应返回 true")
	}
}

// TestWafIPGateSingleUIDSlowNoTrigger 单号慢速偶发（超过连击窗）不应误判为 IP 级。
func TestWafIPGateSingleUIDSlowNoTrigger(t *testing.T) {
	oldWin, oldRep := wafIPWindow, wafIPRepeatWindow
	defer func() { wafIPWindow, wafIPRepeatWindow = oldWin, oldRep }()
	wafIPWindow = 60 * time.Second
	wafIPRepeatWindow = 10 * time.Millisecond

	var g wafIPGate
	const uid = "slow-account"
	for i := 0; i < 5; i++ {
		g.noteWaf(uid)
		time.Sleep(20 * time.Millisecond) // 超出连击窗 → rep 每次重置为 1
	}
	if g.active() {
		t.Fatal("慢速偶发 403 不应激活 IP 级拦截")
	}
}

// TestWafIPGateDistinctUID 保留原有不同 UID 通道语义（回归保护）。
func TestWafIPGateDistinctUID(t *testing.T) {
	oldWin, oldRep := wafIPWindow, wafIPRepeatWindow
	defer func() { wafIPWindow, wafIPRepeatWindow = oldWin, oldRep }()
	wafIPWindow = 60 * time.Second
	wafIPRepeatWindow = 10 * time.Second

	var g wafIPGate
	if g.noteWaf("uid-a") {
		t.Fatal("首个账号不应激活")
	}
	if !g.noteWaf("uid-b") {
		t.Fatal("第二个不同账号应激活（原通道语义）")
	}
}

// TestWafIPGateExpiry 激活期过后自然解除，需全新命中重新判定。
func TestWafIPGateExpiry(t *testing.T) {
	// 激活时长已与判定窗分离（wafIPBaseCooldown，生产 5 分钟——见 wafip.go 注释：
	// 实测 60s 冷却到期后 24 秒内又被拦，冷却必须显著长于判定窗）。测试要一起调短。
	oldWin, oldRep, oldCool := wafIPWindow, wafIPRepeatWindow, wafIPBaseCooldown
	defer func() { wafIPWindow, wafIPRepeatWindow, wafIPBaseCooldown = oldWin, oldRep, oldCool }()
	wafIPWindow = 30 * time.Millisecond
	wafIPRepeatWindow = 10 * time.Second
	wafIPBaseCooldown = 30 * time.Millisecond

	var g wafIPGate
	const uid = "expiry-account"
	for i := 0; i < wafIPRepeatThreshold; i++ {
		g.noteWaf(uid)
	}
	if !g.active() {
		t.Fatal("应已激活")
	}
	time.Sleep(60 * time.Millisecond)
	if g.active() {
		t.Fatal("窗口过后应自然解除")
	}
	// 解除后计数已清空：单次命中不应再激活
	if g.noteWaf(uid) {
		t.Fatal("解除后首次命中不应激活（计数已清空）")
	}
}
