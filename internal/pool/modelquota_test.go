package pool

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// TestModelDayUsageAndQuota 当日用量记数 + 6004 快照成实测额度。
//
// 上游没有"查询模型调用额度"的接口，6004 文案只带重置墙钟不带限额数字——
// 所以额度只能被动观测：平时记每模型当日用量，撞线瞬间快照。本测试锁死：
//   - 每次上游尝试记 1 次（失败尝试也占限流计数，漏记会让额度偏大）
//   - 同日重复触发 6004 不重复累计样本（额度没变）
//   - 跨日：当日用量清零重计，额度知识保留且样本 +1
func TestModelDayUsageAndQuota(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "uid-q"})
	for i := 0; i < 3; i++ {
		p.RecordTokenUsage("uid-q", TokenUsageDelta{Model: "glm-free", HasTotalTokens: true, TotalTokens: 100})
	}
	st, ok := p.Status("uid-q")
	if !ok {
		t.Fatal("取不到账号状态")
	}
	v := st.ModelDay["glm-free"]
	if v.Reqs != 3 || v.Tokens != 300 {
		t.Fatalf("当日用量应为 3 次/300 tok，got %+v", v)
	}

	// 6004 触发 → 快照为实测额度
	p.CooldownSoftForModel("uid-q", time.Hour, time.Now().Add(time.Hour), "glm-free", "6004 model rate limit")
	st, _ = p.Status("uid-q")
	v = st.ModelDay["glm-free"]
	if v.QuotaReqs != 3 || v.QuotaTokens != 300 || v.QuotaSamples != 1 {
		t.Fatalf("6004 快照应为 3 次/300 tok/样本 1，got %+v", v)
	}
	if !v.Limited {
		t.Fatal("6004 后 Limited 应为 true")
	}

	// 同日重复触发：样本不翻倍（额度没变）
	p.CooldownSoftForModel("uid-q", time.Hour, time.Now().Add(time.Hour), "glm-free", "6004 model rate limit")
	st, _ = p.Status("uid-q")
	if v := st.ModelDay["glm-free"]; v.QuotaSamples != 1 {
		t.Fatalf("同日重复触发不应累计样本，got %d", v.QuotaSamples)
	}

	// 跨日：当日用量清零重计，额度知识保留
	p.mu.Lock()
	e := p.byUID["uid-q"]
	e.modelDay["glm-free"].Day = "2000-01-01" // map 指针值可直接改
	q := e.modelQuota["glm-free"]
	q.Day = "2000-01-01"
	e.modelQuota["glm-free"] = q // map 存的是结构体值，须整体回写
	p.mu.Unlock()
	p.RecordTokenUsage("uid-q", TokenUsageDelta{Model: "glm-free", HasTotalTokens: true, TotalTokens: 50})
	st, _ = p.Status("uid-q")
	v = st.ModelDay["glm-free"]
	if v.Reqs != 1 || v.Tokens != 50 {
		t.Fatalf("跨日应清零重计，got %+v", v)
	}
	if v.QuotaReqs != 3 {
		t.Fatalf("跨天额度知识应保留（昨日观测 3 次），got %d", v.QuotaReqs)
	}

	// 跨天再触发：样本 +1，快照更新为今天的新计数
	p.CooldownSoftForModel("uid-q", time.Hour, time.Now().Add(time.Hour), "glm-free", "6004 model rate limit")
	st, _ = p.Status("uid-q")
	v = st.ModelDay["glm-free"]
	if v.QuotaSamples != 2 || v.QuotaReqs != 1 || v.QuotaTokens != 50 {
		t.Fatalf("跨天再观测应样本 +1 且快照更新，got %+v", v)
	}
}

// TestModelDaySkipEmptyModel 无模型名的增量不得计入任何模型的当日用量。
func TestModelDaySkipEmptyModel(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "uid-e"})
	p.RecordTokenUsage("uid-e", TokenUsageDelta{HasTotalTokens: true, TotalTokens: 10})
	st, _ := p.Status("uid-e")
	if len(st.ModelDay) != 0 {
		t.Fatalf("空模型名不应入账，got %+v", st.ModelDay)
	}
}

// TestModelQuotaPersistence 实测额度必须跨重启保留——观测到一次要等真撞一天线，
// 重启丢掉就得重等一天。当日用量也一并恢复（今天的），昨天的丢弃。
func TestModelQuotaPersistence(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "state.json")
	p := New(fp)
	p.Add(&auth.Auth{UID: "uid-p"})
	p.RecordTokenUsage("uid-p", TokenUsageDelta{Model: "m1", HasTotalTokens: true, TotalTokens: 10})
	p.CooldownSoftForModel("uid-p", time.Hour, time.Now().Add(time.Hour), "m1", "6004 model rate limit")
	p.Flush()

	p2 := New(fp)
	p2.Add(&auth.Auth{UID: "uid-p"})
	st, ok := p2.Status("uid-p")
	if !ok {
		t.Fatal("重载后取不到账号")
	}
	v := st.ModelDay["m1"]
	if v.QuotaReqs != 1 || v.QuotaSamples != 1 {
		t.Fatalf("实测额度应跨重启保留，got %+v", v)
	}
	if v.Reqs != 1 || v.Tokens != 10 {
		t.Fatalf("当日用量应跨重启保留（今天的），got %+v", v)
	}

	// 额度不按 Day 过期：把快照改成昨天，重载后仍保留（面板标注观测日期）
	p.mu.Lock()
	q2 := p.byUID["uid-p"].modelQuota["m1"]
	q2.Day = "2000-01-01"
	q2.ObservedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local)
	p.byUID["uid-p"].modelQuota["m1"] = q2
	p.byUID["uid-p"].modelDay["m1"].Day = "2000-01-01" // 用量是昨天的 → 应被丢弃
	p.mu.Unlock()
	// 直接改内存绕过了业务方法，dirty 不会自己置位；生产路径的变更都经业务方法置脏。
	p.dirty.Store(true)
	p.Flush()
	p3 := New(fp)
	p3.Add(&auth.Auth{UID: "uid-p"})
	st, _ = p3.Status("uid-p")
	v = st.ModelDay["m1"]
	if v.QuotaReqs != 1 {
		t.Fatalf("昨日观测的额度知识应保留，got %+v", v)
	}
	if v.Reqs != 0 {
		t.Fatalf("昨日用量应被丢弃（日额度按日清零），got %d", v.Reqs)
	}
}
