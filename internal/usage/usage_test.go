package usage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 成功/失败尝试计数、total 的 pt+ct 兜底口径、按域/账号聚合。
func TestAddAndTotals(t *testing.T) {
	r := New("")
	now := time.Now()
	r.Add(now, "cn", "uid1", "glm-5.2", Delta{PromptTokens: 100, HasPromptTokens: true, CompletionTokens: 50, HasCompletion: true, LatencyMs: 200, HasLatency: true}, true)
	// 失败尝试：无 usage → 只计请求数与失败数，token 不加。
	r.Add(now, "global", "uid1", "claude-4.6", Delta{}, false)
	// 上游没给 total 时用 pt+ct 兜底，保证总量口径连续。
	r.Add(now, "cn", "uid1", "glm-5.2", Delta{PromptTokens: 10, HasPromptTokens: true, CompletionTokens: 5, HasCompletion: true}, true)

	s := r.Snapshot(24, nil)
	if s.Totals.Requests != 3 || s.Totals.Errors != 1 {
		t.Fatalf("requests/errors = %d/%d, want 3/1", s.Totals.Requests, s.Totals.Errors)
	}
	if s.Totals.PromptTokens != 110 || s.Totals.CompletionTok != 55 {
		t.Fatalf("pt/ct = %d/%d, want 110/55", s.Totals.PromptTokens, s.Totals.CompletionTok)
	}
	if s.Totals.TotalTokens != 165 {
		t.Fatalf("tt = %d, want 165（无 total 时按 pt+ct 兜底）", s.Totals.TotalTokens)
	}
	if s.Totals.AvgLatencyMs != 200 {
		t.Fatalf("avg latency = %v, want 200", s.Totals.AvgLatencyMs)
	}
	if len(s.ByRealm) != 2 {
		t.Fatalf("by_realm = %d 项, want 2", len(s.ByRealm))
	}
	if s.ByAccount[0].Realm == "" {
		t.Fatal("by_account 行缺 realm 标注")
	}
}

// Rollup 把超出 hourlyKeep 的小时桶折叠为日桶，且幂等：重复折叠不重复计数。
func TestRollupIdempotent(t *testing.T) {
	r := New("")
	old := time.Now().AddDate(0, 0, -100) // 100 天前，超出 90 天小时保留
	r.Add(old, "cn", "u", "m", Delta{PromptTokens: 7, HasPromptTokens: true}, true)
	r.Add(old, "cn", "u", "m", Delta{PromptTokens: 7, HasPromptTokens: true}, true)
	r.Add(time.Now(), "cn", "u", "m", Delta{PromptTokens: 1, HasPromptTokens: true}, true)

	r.Rollup(time.Now())
	// 用全量窗口（hours=0）验证折叠总量：Totals 现在按窗口过滤，
	// 100 天前的数据在 24h 窗口下本就不该计入——这是新语义，不是回归。
	after := r.Snapshot(0, nil)
	if after.Totals.Requests != 3 || after.Totals.PromptTokens != 15 {
		t.Fatalf("折叠后 totals = %d/%d, want 3/15", after.Totals.Requests, after.Totals.PromptTokens)
	}
	if len(after.Series) != 2 || after.Series[0].Scope != "day" || after.Series[1].Scope != "hour" {
		t.Fatalf("series = %+v, want 日点在前 + 小时点在后", after.Series)
	}

	r.Rollup(time.Now())
	again := r.Snapshot(0, nil)
	if again.Totals.Requests != 3 || again.Totals.PromptTokens != 15 {
		t.Fatalf("二次折叠后 totals = %d/%d, want 3/15（幂等被破坏）", again.Totals.Requests, again.Totals.PromptTokens)
	}
}

// 落盘→新实例恢复，数据不丢；落盘结构带版本号。
func TestFlushLoadRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.json")
	r1 := New(path)
	r1.Add(time.Now(), "cn", "u1", "glm-5.2", Delta{PromptTokens: 42, HasPromptTokens: true, TotalTokens: 42, HasTotal: true}, true)
	r1.Save()

	r2 := New(path)
	s := r2.Snapshot(24, nil)
	if s.Totals.Requests != 1 || s.Totals.TotalTokens != 42 {
		t.Fatalf("恢复后 totals = %d/%d, want 1/42", s.Totals.Requests, s.Totals.TotalTokens)
	}
	raw, _ := os.ReadFile(path)
	var f file
	if err := json.Unmarshal(raw, &f); err != nil || f.Version != 1 || len(f.Buckets) != 1 {
		t.Fatalf("落盘文件异常: err=%v buckets=%d", err, len(f.Buckets))
	}
}

// Snapshot 把小时窗口外的细粒度并入日点，时序不出现空洞。
func TestSnapshotStitching(t *testing.T) {
	r := New("")
	now := time.Now()
	// 改用 100 天前（超出 hourlyKeep=90d）+ 现在，才会折成日桶+小时桶
	r.Add(now.AddDate(0, 0, -100), "cn", "u", "m", Delta{PromptTokens: 5, HasPromptTokens: true}, true) // 窗口外 → 日点
	r.Add(now, "cn", "u", "m", Delta{PromptTokens: 3, HasPromptTokens: true}, true)                     // 窗口内 → 小时点
	r.Rollup(now) // 折叠，100d 前的进日桶
	// 全量：两点都在，时序含窗口外日点（拼接意图）。
	s := r.Snapshot(0, nil)
	if len(s.Series) != 2 || s.Series[0].Scope != "day" || s.Series[1].Scope != "hour" {
		t.Fatalf("series = %+v", s.Series)
	}
	if s.Series[0].PromptTokens != 5 || s.Series[1].PromptTokens != 3 {
		t.Fatalf("series tokens = %d/%d, want 5/3", s.Series[0].PromptTokens, s.Series[1].PromptTokens)
	}
	// 24h 窗口：Totals 只含窗口内的 3（窗口外的 5 被过滤）——新语义的核心。
	w := r.Snapshot(24, nil)
	if w.Totals.PromptTokens != 3 {
		t.Fatalf("24h 窗口 totals = %d, want 3（窗口外数据必须被过滤）", w.Totals.PromptTokens)
	}
	if w.Totals.Requests != 1 {
		t.Fatalf("24h 窗口 requests = %d, want 1", w.Totals.Requests)
	}
}

// TestWindowFiltersTotals 窗口切换必须反映到 Totals/ByAccount（原 bug：都不过滤，
// 24h/3天/7天完全相同）。
func TestWindowFiltersTotals(t *testing.T) {
	r := New("")
	now := time.Now()
	// 三天前 / 一天前 / 现在，各 10 token
	r.Add(now.AddDate(0, 0, -3), "cn", "u", "m", Delta{PromptTokens: 10, HasPromptTokens: true}, true)
	r.Add(now.AddDate(0, 0, -1), "cn", "u", "m", Delta{PromptTokens: 10, HasPromptTokens: true}, true)
	r.Add(now, "cn", "u", "m", Delta{PromptTokens: 10, HasPromptTokens: true}, true)

	// 24h：只含现在 → 10
	if got := r.Snapshot(24, nil).Totals.PromptTokens; got != 10 {
		t.Errorf("24h totals = %d, want 10", got)
	}
	// 3 天（72h）→ dayFrom = 2 天前，含一天前 + 现在 → 20
	if got := r.Snapshot(72, nil).Totals.PromptTokens; got != 20 {
		t.Errorf("72h totals = %d, want 20", got)
	}
	// 全量 → 30
	if got := r.Snapshot(0, nil).Totals.PromptTokens; got != 30 {
		t.Errorf("全量 totals = %d, want 30", got)
	}
	// ByAccount 也必须同步过滤（原 bug：也不过滤）
	if got := r.Snapshot(24, nil).ByAccount[0].PromptTokens; got != 10 {
		t.Errorf("24h by_account = %d, want 10", got)
	}
}

// Stop 触发最终落盘（Start 后未到防抖间隔也要落）。
func TestLifecycleFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.json")
	r := New(path)
	r.Start()
	r.Add(time.Now(), "cn", "u", "m", Delta{PromptTokens: 9, HasPromptTokens: true}, true)
	r.Stop()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stop 后应有落盘文件: %v", err)
	}
}
