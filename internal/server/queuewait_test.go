package server

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

// 上游排队真实样本（2026-09-22 线上抓取）。
const queueBody6020 = `{"code":6020,"msg":"queue.waiting.title","requestId":"ab0b9c9e3fc8638820ec66bcddaaa726",` +
	`"data":{"queue_position":2159,"queue_size":"999+","retry_after":40,"estimated_wait":10795,"client_user_type":"free"}}`

// TestChat6020QueueIsModelLevel 端到端回归 2026-09-22 线上事故。
//
// 现场：用户的 agent 钉在 hy4-preview-f 上，该模型在上游排大队（queue_position
// 2000+）。网关不认识 code 6020，把它当**账号级** 429 处理 → 每个撞上的号都被打
// 600s 账号级冷却 → 池子清空 10 分钟 → **连别的模型一起 503**（实测 483 次 429
// + 146 次 uid=- 的 503，用户以为"账号全废了"而把号删了）。
//
// 本测试锁死正确行为：排队是**模型级**问题——账号保持健康、其它模型照常可用。
func TestChat6020QueueIsModelLevel(t *testing.T) {
	var calls int
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		calls++
		if authz == "Bearer at-bad" {
			return 429, queueBody6020, false
		}
		return 200, sseOK, true
	})
	p := testPoolWith(
		&auth.Auth{UID: "bad", AccessToken: "at-bad", ExpiresAt: 9999999999},
		&auth.Auth{UID: "good", AccessToken: "at-good", ExpiresAt: 9999999999},
	)
	p.SetCredits("bad", 2000, 0) // 让 bad 先被选中
	p.SetCredits("good", 1000, 0)
	// SoftCooldown 用 600s（事故当时的默认值）：修复前这条路径会给账号级 600s。
	h := NewHandler(Config{Pool: p, Upstream: up, SoftCooldown: 600 * time.Second})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"hy4-preview-f","messages":[]}`)))
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s (want 200 after rotate to good)", rec.Code, rec.Body)
	}

	st, _ := p.Status("bad")
	// 核心断言 1：账号级**不**冷却（修复前这里会是 600s 的 soft_rate 冷却）。
	if st.Cooling {
		t.Fatalf("6020 排队不得设置账号级冷却（这正是事故根因）: %+v", st)
	}
	// 核心断言 2：该模型被记入模型级台账，且时长取自 body 的 retry_after=40s。
	if len(st.RateLimitedModels) != 1 || st.RateLimitedModels[0].Model != "hy4-preview-f" {
		t.Fatalf("want 单行模型台账 hy4-preview-f: %+v", st.RateLimitedModels)
	}
	if d := time.Until(st.RateLimitedModels[0].Until); d < 30*time.Second || d > 50*time.Second {
		t.Errorf("模型冷却剩余 %v，want ≈40s（来自 body retry_after）", d)
	}

	// 核心断言 3（行为层）：同模型请求跳过 bad，换模型请求仍可选中 bad。
	p.SetRandomSource(func(n int64) int64 { return 0 })
	if same := p.PickExcludingForModel(nil, "hy4-preview-f"); same == nil || same.UID != "good" {
		t.Fatalf("同模型选号应跳过 bad，实际 %+v", same)
	}
	if diff := p.PickExcludingForModel(nil, "glm-5.3"); diff == nil || diff.UID != "bad" {
		t.Fatalf("换模型应豁免排队冷却、仍可选中 bad，实际 %+v", diff)
	}
}

// TestQueueWaitClamped 6020 的建议等待值必须夹到 [15s, 2min]：
// 缺失/0 → 下限（别把上游打爆），超大值（如 99999 秒）→ 上限（别把模型封死）。
func TestQueueWaitClamped(t *testing.T) {
	cases := []struct {
		name         string
		body         string
		wantLo, want int64 // 期望剩余秒数的下界/上界
	}{
		{"缺失 retry_after → 下限 15s", `{"code":6020,"msg":"queue.waiting.title"}`, 13, 17},
		{"retry_after=0 → 下限 15s", `{"code":6020,"msg":"queue.waiting.title","data":{"retry_after":0}}`, 13, 17},
		{"超大值 → 上限 2min", `{"code":6020,"msg":"queue.waiting.title","data":{"retry_after":99999}}`, 110, 121},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
			h := NewHandler(Config{Pool: p, SoftCooldown: 600 * time.Second})
			h.applyErrorPolicy("u1", upstream.ErrSoftRate, c.body, "hy4-preview-f",
				&upstream.Error{Kind: upstream.ErrSoftRate, Msg: c.body})

			st, _ := p.Status("u1")
			if st.Cooling {
				t.Fatalf("账号级不应冷却: %+v", st)
			}
			if len(st.RateLimitedModels) != 1 {
				t.Fatalf("want 1 行模型台账: %+v", st.RateLimitedModels)
			}
			sec := int64(time.Until(st.RateLimitedModels[0].Until).Seconds())
			if sec < c.wantLo || sec > c.want {
				t.Errorf("剩余 %ds，want [%d,%d]s", sec, c.wantLo, c.want)
			}
		})
	}
}

// TestQueueWaitDoesNotPoisonOtherModels 事故的核心破坏路径回归：
// 一个排队模型不得让**其它模型**的请求也被 503。
//
// 修复前的实际表现：4 个号依次被同一个排队模型打成账号级冷却 → 池空 →
// 随后任意模型的请求都拿 503 no_healthy_account。
func TestQueueWaitDoesNotPoisonOtherModels(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		if authz == "Bearer at-a" {
			return 429, queueBody6020, false
		}
		return 200, sseOK, true
	})
	p := testPoolWith(
		&auth.Auth{UID: "a", AccessToken: "at-a", ExpiresAt: 9999999999},
		&auth.Auth{UID: "b", AccessToken: "at-b", ExpiresAt: 9999999999},
	)
	p.SetCredits("a", 2000, 0)
	p.SetCredits("b", 1000, 0)
	h := NewHandler(Config{Pool: p, Upstream: up, SoftCooldown: 600 * time.Second})

	// 先让 a 撞一次排队。
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"hy4-preview-f","messages":[]}`)))
	if rec.Code != 200 {
		t.Fatalf("首次 code=%d body=%s", rec.Code, rec.Body)
	}

	// 换一个模型：两个号都必须仍可用（不能出现 no_healthy_account）。
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.3","messages":[]}`)))
	if rec2.Code != 200 {
		t.Fatalf("换模型后 code=%d body=%s（排队不得污染整个池子）", rec2.Code, rec2.Body)
	}
	if body := rec2.Body.String(); strings.Contains(body, "no_healthy_account") {
		t.Fatalf("换模型不该无号可用: %s", body)
	}
}
