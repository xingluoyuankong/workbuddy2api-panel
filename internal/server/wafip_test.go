package server

import (
	"testing"
	"time"
)

// TestWAFIPCooldownEscalates 连续触发的冷却必须指数递增并封顶。
//
// 回归背景（2026-09-18 22:12–22:15 实测）：判定窗与冷却时长都是 60s 时，
// 冷却一到期立刻重试 → 24 秒内又被拦，连续三次。上游 WAF 的实际封禁窗口
// 明显长于 60s，冷却太短等于持续撞墙。现在基础 5 分钟、连续触发翻倍封顶 30 分钟。
func TestWAFIPCooldownEscalates(t *testing.T) {
	g := &wafIPGate{}
	if got := g.cooldownFor(1); got != time.Minute {
		t.Errorf("streak=1 → %v, want 1m", got)
	}
	if got := g.cooldownFor(2); got != 2*time.Minute {
		t.Errorf("streak=2 → %v, want 2m", got)
	}
	if got := g.cooldownFor(3); got != 4*time.Minute {
		t.Errorf("streak=3 → %v, want 4m", got)
	}
	if got := g.cooldownFor(4); got != 5*time.Minute {
		t.Errorf("streak=4 → %v, want 5m (capped)", got)
	}
	if got := g.cooldownFor(99); got != 5*time.Minute {
		t.Errorf("streak=99 → %v, want 5m (capped)", got)
	}
}

// TestWAFIPCooldownIndependentOfDetectWindow 冷却与判定窗是独立概念。
//
// 事故回顾：最初两者共用一个 60s 值，冷却一到期立刻重试、24 秒内又被拦。
// 现在冷却基础值仍取 60s（与判定窗同量级），但**必须能独立调整**——真正的
// 自愈靠 global 域多出口轮换换 IP，而不是靠把冷却拉长到用户干等。
func TestWAFIPCooldownIndependentOfDetectWindow(t *testing.T) {
	oldCool := wafIPBaseCooldown
	defer func() { wafIPBaseCooldown = oldCool }()
	wafIPBaseCooldown = 7 * time.Second
	if got := (&wafIPGate{}).cooldownFor(1); got != 7*time.Second {
		t.Fatalf("cooldown must follow wafIPBaseCooldown independently, got %v", got)
	}
}

// TestWAFIPStreakResetsOnSuccess 成功请求后连击计数清零（冷却回到基础值）。
func TestWAFIPStreakResetsOnSuccess(t *testing.T) {
	g := &wafIPGate{}
	g.mu.Lock()
	g.streak = 3
	g.mu.Unlock()
	g.noteSuccess()
	g.mu.Lock()
	got := g.streak
	g.mu.Unlock()
	if got != 0 {
		t.Errorf("streak = %d, want 0 after success", got)
	}
}
