package modelmeta

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- 静态知识库 ---

func TestLookupFactSgSuffix(t *testing.T) {
	// sg = Singapore 区域变体：global 可调用且归一化为基模型；cn 不可调用。
	g, ok := LookupFact("global", "deepseek-v4.1-flash-sg")
	if !ok {
		t.Fatal("global sg fact missing")
	}
	if g.Upstream != "deepseek-v4.1-flash" {
		t.Errorf("sg upstream = %q, want deepseek-v4.1-flash", g.Upstream)
	}
	if !strings.Contains(g.Note, "Singapore") {
		t.Errorf("sg note should explain Singapore, got %q", g.Note)
	}
	if g.Category != CatRegion {
		t.Errorf("sg category = %q, want %q", g.Category, CatRegion)
	}
	cn, ok := LookupFact("cn", "deepseek-v4.1-flash-sg")
	if !ok {
		t.Fatal("cn sg fact missing")
	}
	// CN 侧不得宣称可调用。
	if strings.Contains(cn.Note, "可调用") && !strings.Contains(cn.Note, "不可调用") {
		t.Errorf("cn sg note must not claim callable: %q", cn.Note)
	}
	if !strings.Contains(cn.Note, "不可调用") {
		t.Errorf("cn sg note should mark not callable, got %q", cn.Note)
	}
}

func TestLookupFactDeepModelAlias(t *testing.T) {
	cn, _ := LookupFact("cn", "deep-model")
	if cn.Upstream != "glm-5.3" {
		t.Errorf("cn deep-model upstream = %q, want glm-5.3", cn.Upstream)
	}
	if cn.Category != CatAlias {
		t.Errorf("cn deep-model category = %q, want %q", cn.Category, CatAlias)
	}
	gl, _ := LookupFact("global", "deep-model")
	if gl.Upstream != "deep-model" {
		t.Errorf("global deep-model upstream = %q, want deep-model (not alias)", gl.Upstream)
	}
}

func TestLookupFactFallsBackToBareID(t *testing.T) {
	// 无 realm 专属条目时回落到裸 id（如 default-1.2 两域同语义）。
	f, ok := LookupFact("global", "default-1.2")
	if !ok {
		t.Fatal("default-1.2 should fall back to bare id")
	}
	if !strings.Contains(f.Note, "下线") && !strings.Contains(f.Note, "不存在") {
		t.Errorf("default-1.2 note = %q, want 下线/不存在", f.Note)
	}
}

func TestLookupFactUnknown(t *testing.T) {
	if _, ok := LookupFact("cn", "totally-made-up-model"); ok {
		t.Error("unknown model should not have a fact")
	}
}

// --- 视图合成 ---

func TestAnnotateUsesFactWhenNoRecord(t *testing.T) {
	s := New("")
	v := s.Annotate("global", "deepseek-v4.1-flash-sg", "")
	if v.UpstreamModel != "deepseek-v4.1-flash" {
		t.Errorf("upstream_model = %q, want deepseek-v4.1-flash", v.UpstreamModel)
	}
	if !v.IsAlias {
		t.Error("sg should be flagged as alias")
	}
	if v.Status != StatusUnverified {
		t.Errorf("status = %q, want unverified (never claim verified without a probe)", v.Status)
	}
	if v.Verified {
		t.Error("unverified model must not report verified=true")
	}
	if v.FullID != "global:deepseek-v4.1-flash-sg" || v.CallName != v.FullID {
		t.Errorf("full_id=%q call_name=%q mismatch", v.FullID, v.CallName)
	}
	if v.Display == "" {
		t.Error("display should fall back to upstream name or id")
	}
}

func TestAnnotateUpstreamNameFallback(t *testing.T) {
	s := New("")
	v := s.Annotate("cn", "glm-5.3", "GLM-5.3")
	if v.Display != "GLM-5.3" {
		t.Errorf("display = %q, want upstream name GLM-5.3", v.Display)
	}
}

func TestAnnotateManualOverrideWins(t *testing.T) {
	s := New("")
	got := s.Update("cn", "deep-model", Record{Note: "人工订正", Display: "深度模型", Category: CatFlagship, Effort: "max"})
	if got.Note != "人工订正" || got.Display != "深度模型" || got.Category != CatFlagship {
		t.Fatalf("manual override not applied: %+v", got)
	}
	v := s.Annotate("cn", "deep-model", "Deep")
	if v.Note != "人工订正" || v.Display != "深度模型" {
		t.Errorf("override lost on re-annotate: %+v", v)
	}
	if v.Effort != "max" {
		t.Errorf("effort = %q, want max", v.Effort)
	}
	if got.UpstreamModel != "glm-5.3" {
		t.Errorf("fact upstream should persist: %q", got.UpstreamModel)
	}
}

// --- 验证引擎 ---

func TestVerifyRecordsSuccessAndAlias(t *testing.T) {
	s := New("")
	v := NewVerifier(s)
	views, err := v.Verify(context.Background(), func(ctx context.Context, full string) ProbeResult {
		if full == "cn:deep-model" {
			return ProbeResult{OK: true, UpstreamModel: "glm-5.3", Credit: 0.09, Latency: 120 * time.Millisecond}
		}
		return ProbeResult{OK: true, Credit: 0}
	}, VerifyOption{Only: []ModelRef{{Realm: "cn", ID: "deep-model", Full: "cn:deep-model"}}})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("views = %d, want 1", len(views))
	}
	got := views[0]
	if got.Status != StatusVerified || !got.Verified {
		t.Errorf("status = %q verified=%v, want verified", got.Status, got.Verified)
	}
	if got.UpstreamModel != "glm-5.3" || !got.IsAlias {
		t.Errorf("alias not recorded: upstream=%q is_alias=%v", got.UpstreamModel, got.IsAlias)
	}
	if got.Credit != 0.09 {
		t.Errorf("credit = %v, want 0.09", got.Credit)
	}
	if got.Samples != 1 {
		t.Errorf("samples = %d, want 1", got.Samples)
	}
}

func TestVerifyRecordsFailure(t *testing.T) {
	s := New("")
	v := NewVerifier(s)
	views, _ := v.Verify(context.Background(), func(ctx context.Context, full string) ProbeResult {
		return ProbeResult{OK: false, ErrCode: "no_healthy_account", ErrMsg: strings.Repeat("x", 500)}
	}, VerifyOption{Only: []ModelRef{{Realm: "cn", ID: "default-model", Full: "cn:default-model"}}})
	got := views[0]
	if got.Status != StatusFailed {
		t.Errorf("status = %q, want failed", got.Status)
	}
	if got.Verified {
		t.Error("failed model must not be verified")
	}
	if got.ErrCode != "no_healthy_account" {
		t.Errorf("err_code = %q", got.ErrCode)
	}
	if len(got.ErrMsg) != 300 {
		t.Errorf("err_msg len = %d, want 300 (truncated)", len(got.ErrMsg))
	}
}

func TestVerifyNilProbe(t *testing.T) {
	s := New("")
	if _, err := NewVerifier(s).Verify(context.Background(), nil, VerifyOption{}); err == nil {
		t.Fatal("nil probe should error")
	}
}

func TestVerifyRespectsContextCancel(t *testing.T) {
	s := New("")
	v := NewVerifier(s)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	views, err := v.Verify(ctx, func(ctx context.Context, full string) ProbeResult {
		return ProbeResult{OK: true}
	}, VerifyOption{Only: []ModelRef{{Realm: "cn", ID: "hy3", Full: "cn:hy3"}}})
	if err == nil {
		t.Error("cancelled context should surface an error")
	}
	if len(views) != 0 && views[0].Status != StatusUnverified {
		t.Errorf("cancelled probe must not mark verified, got %q", views[0].Status)
	}
}

func TestVerifyConcurrency(t *testing.T) {
	s := New("")
	v := NewVerifier(s)
	refs := make([]ModelRef, 0, 12)
	for i := 0; i < 12; i++ {
		id := "m" + string(rune('a'+i))
		refs = append(refs, ModelRef{Realm: "cn", ID: id, Full: "cn:" + id})
	}
	views, _ := v.Verify(context.Background(), func(ctx context.Context, full string) ProbeResult {
		return ProbeResult{OK: true, Credit: 0.01}
	}, VerifyOption{Only: refs, Concurrency: 4})
	if len(views) != 12 {
		t.Fatalf("views = %d, want 12", len(views))
	}
	for _, vv := range views {
		if !vv.Verified {
			t.Errorf("%s not verified", vv.ID)
		}
	}
	if n := s.Count()["verified"]; n != 12 {
		t.Errorf("verified count = %d, want 12", n)
	}
}

// --- 持久化 ---

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "model_meta.json")
	s := New(p)
	v := NewVerifier(s)
	_, _ = v.Verify(context.Background(), func(ctx context.Context, full string) ProbeResult {
		return ProbeResult{OK: true, UpstreamModel: "glm-5.3", Credit: 0.09}
	}, VerifyOption{Only: []ModelRef{{Realm: "cn", ID: "deep-model", Full: "cn:deep-model"}}})

	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("persist file missing: %v", err)
	}
	var raw []Record
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("persisted file is not valid json: %v", err)
	}
	if len(raw) != 1 || raw[0].ID != "deep-model" || raw[0].Status != StatusVerified {
		t.Fatalf("persisted = %+v", raw)
	}

	// 重新加载：状态与实测上游名必须存活。
	s2 := New(p)
	got := s2.Annotate("cn", "deep-model", "")
	if got.Status != StatusVerified || got.UpstreamModel != "glm-5.3" || got.Credit != 0.09 {
		t.Errorf("reload lost data: %+v", got)
	}
}

func TestStoreToleratesCorruptFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "model_meta.json")
	if err := os.WriteFile(p, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New(p) // 不得 panic
	if len(s.All()) != 0 {
		t.Error("corrupt file should yield empty store, not garbage")
	}
}

func TestStoreInMemoryMode(t *testing.T) {
	s := New("")
	s.Update("cn", "hy3", Record{Effort: "low"})
	if got := s.EffortFor("cn", "hy3"); got != "low" {
		t.Errorf("effort = %q, want low", got)
	}
	if s.Path() != "" {
		t.Errorf("path = %q, want empty (memory mode)", s.Path())
	}
}

// --- 隐藏与统计 ---

func TestHiddenFlag(t *testing.T) {
	s := New("")
	if s.IsHidden("cn", "default-1.2") {
		t.Error("model should not be hidden by default")
	}
	// Hidden 是 bool，Update 无法区分「未传」与「显式 false」，故走专门入口。
	s.Update("cn", "default-1.2", Record{})
	if s.IsHidden("cn", "default-1.2") {
		t.Error("Update without hidden must not hide the model")
	}
	s.SetHidden("cn", "default-1.2", true)
	if !s.IsHidden("cn", "default-1.2") {
		t.Error("hidden flag not persisted")
	}
	s.SetHidden("cn", "default-1.2", false)
	if s.IsHidden("cn", "default-1.2") {
		t.Error("hidden flag not cleared")
	}
	s.SetHidden("cn", "default-1.2", true)
	if n := s.Count()["hidden"]; n != 1 {
		t.Errorf("hidden count = %d, want 1", n)
	}
}

func TestUpsertKnown(t *testing.T) {
	s := New("")
	s.UpsertKnown("global", []string{"hy3", "gpt-5.5", "hy3"})
	all := s.All()
	if len(all) != 2 {
		t.Fatalf("all = %d, want 2 (dedup)", len(all))
	}
	for _, v := range all {
		if v.Realm != "global" {
			t.Errorf("realm = %q, want global", v.Realm)
		}
		if v.Status != StatusUnverified {
			t.Errorf("%s should be unverified after upsert", v.ID)
		}
	}
}

func TestAnnotateAll(t *testing.T) {
	s := New("")
	views := s.AnnotateAll("cn", []string{"deep-model", "unknown-x"}, map[string]string{"deep-model": "Deep"})
	if len(views) != 2 {
		t.Fatalf("views = %d", len(views))
	}
	if views[0].Display != "Deep" {
		t.Errorf("display = %q, want Deep", views[0].Display)
	}
	if views[1].Display != "unknown-x" {
		t.Errorf("unknown model display = %q, want fallback to id", views[1].Display)
	}
}

func TestRealmKeyNormalization(t *testing.T) {
	cases := map[string]string{"": "cn", "CN": "cn", "cn": "cn", "global": "global", "Global": "global", " GLOBAL ": "global"}
	for in, want := range cases {
		if got := realmKey(in); got != want {
			t.Errorf("realmKey(%q) = %q, want %q", in, got, want)
		}
	}
	if got := Key("CN", "x"); got != "cn:x" {
		t.Errorf("Key = %q, want cn:x", got)
	}
}

func TestSplitKey(t *testing.T) {
	if r, id := splitKey("global:hy3"); r != "global" || id != "hy3" {
		t.Errorf("splitKey = %q,%q", r, id)
	}
	if r, id := splitKey("hy3"); r != "cn" || id != "hy3" {
		t.Errorf("splitKey bare = %q,%q", r, id)
	}
}
