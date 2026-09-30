package modelmeta

import (
	"path/filepath"
	"strings"
	"testing"
)

// 用户核定的剔除名单必须逐条命中（回归保护：名单被误删会在这里炸）。
func TestDefaultRemovedCoversUserList(t *testing.T) {
	want := []string{
		"hunyuan-2.0-instruct", "deepseek-r1-0528-lkeap", "kimi-k2-instruct-taiji",
		"hunyuan-image-alpha-edit", "deepseek-v3-0324", "default-1.2",
		"deepseek-r1-0528", "default-1.1", "hunyuan-chat", "glm-4.6",
	}
	for _, id := range want {
		if why := DefaultRemovedReason(id); why == "" {
			t.Errorf("%s must be in defaultRemoved", id)
		}
	}
}

func TestRegionVariantDetection(t *testing.T) {
	if !IsRegionVariant("deepseek-v4.1-flash-sg") {
		t.Error("deepseek-v4.1-flash-sg should be a region variant")
	}
	if !IsRegionVariant("GLM-5.3-SG") {
		t.Error("region suffix matching must be case-insensitive")
	}
	for _, ok := range []string{"hy3", "deep-model", "glm-5.2", "sglang-model"} {
		if IsRegionVariant(ok) {
			t.Errorf("%s must NOT be a region variant", ok)
		}
	}
}

func TestStoreRemovalReason(t *testing.T) {
	s := New("")
	// 区域变体：规则命中。
	if r := s.RemovalReason("global", "deepseek-v4.1-flash-sg"); !strings.Contains(r, "区域变体") {
		t.Errorf("sg reason = %q", r)
	}
	// 静态遗留名单。
	if r := s.RemovalReason("cn", "default-1.2"); r == "" {
		t.Error("default-1.2 should be removed")
	}
	// 正常模型不剔除。
	if r := s.RemovalReason("cn", "hy3"); r != "" {
		t.Errorf("hy3 should not be removed, got %q", r)
	}
	// 人工删除优先。
	s.SetRemoved("cn", "hy3", true, "业务上不用")
	if r := s.RemovalReason("cn", "hy3"); r != "业务上不用" {
		t.Errorf("manual reason = %q", r)
	}
	if !s.IsRemoved("cn", "hy3") {
		t.Error("IsRemoved must honour manual flag")
	}
	// 恢复。
	s.SetRemoved("cn", "hy3", false, "")
	if s.IsRemoved("cn", "hy3") {
		t.Error("restore failed")
	}
}

func TestRemovalFlowsIntoView(t *testing.T) {
	s := New("")
	v := s.Annotate("cn", "glm-4.6", "GLM-4.6")
	if !v.Removed {
		t.Error("glm-4.6 must be flagged Removed in the view")
	}
	if v.RemovedReason == "" {
		t.Error("RemovedReason must be populated for the panel")
	}
	if v.Status != StatusUnverified {
		t.Errorf("status = %q, removal must not fake a verification status", v.Status)
	}
}

func TestRemovalPersists(t *testing.T) {
	p := filepath.Join(t.TempDir(), "model_meta.json")
	s := New(p)
	s.SetRemoved("global", "hy3", true, "测试")
	s2 := New(p)
	if !s2.IsRemoved("global", "hy3") {
		t.Error("removal flag must survive reload")
	}
}

// 区域变体规则必须只在 -sg 后缀上命中，不能误伤含 sg 的正常名字。
func TestRegionSuffixesExported(t *testing.T) {
	got := RegionSuffixes()
	if len(got) == 0 || got[0] != "-sg" {
		t.Fatalf("RegionSuffixes = %v, want [-sg]", got)
	}
}

// --- 上下文窗口档位 ---

func TestContextTierFormatting(t *testing.T) {
	cases := map[int64]string{0: "跟随上游", 8_000: "8K", 128_000: "128K", 1_000_000: "1M"}
	for in, want := range cases {
		if got := FormatContextTier(in); got != want {
			t.Errorf("FormatContextTier(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestContextTierOptions(t *testing.T) {
	opts := ContextTierOptions()
	if len(opts) != len(ContextTiers) {
		t.Fatalf("options = %d, want %d", len(opts), len(ContextTiers))
	}
	if opts[0]["value"].(int64) != 0 || opts[0]["label"] != "跟随上游" {
		t.Errorf("first option = %v, want 跟随上游", opts[0])
	}
}

func TestSetContextWindow(t *testing.T) {
	s := New("")
	if got := s.ContextWindowFor("cn", "hy3"); got != 0 {
		t.Errorf("default = %d, want 0 (跟随上游)", got)
	}
	s.SetContextWindow("cn", "hy3", 128_000)
	if got := s.ContextWindowFor("cn", "hy3"); got != 128_000 {
		t.Errorf("got %d, want 128000", got)
	}
	s.SetContextWindow("cn", "hy3", 0)
	if got := s.ContextWindowFor("cn", "hy3"); got != 0 {
		t.Errorf("clear failed: %d", got)
	}
}
