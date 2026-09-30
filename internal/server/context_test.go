package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEstimateTokens(t *testing.T) {
	if got := estimateTokens(""); got != 0 {
		t.Errorf("empty = %d, want 0", got)
	}
	ascii := estimateTokens(strings.Repeat("a", 400))
	cjk := estimateTokens(strings.Repeat("中", 200))
	if ascii < 90 || ascii > 130 {
		t.Errorf("400 ascii chars ≈ %d tokens, want ~100", ascii)
	}
	// 中文按 ~1.5 字/token，200 字应在 300 上下（保守偏高）。
	if cjk < 280 || cjk > 340 {
		t.Errorf("200 CJK chars ≈ %d tokens, want ~300", cjk)
	}
	if cjk <= ascii {
		t.Error("CJK must cost more tokens than the same char count of ASCII")
	}
}

func TestTrimToContextBudgetNoopWhenUnderBudget(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	out, dropped := trimToContextBudget(body, 1_000_000)
	if dropped != 0 {
		t.Errorf("dropped = %d, want 0", dropped)
	}
	if string(out) != string(body) {
		t.Error("under-budget body must be returned byte-identical")
	}
}

func TestTrimToContextBudgetDropsOldest(t *testing.T) {
	big := strings.Repeat("x", 4000) // ~1000 tokens each
	var msgs []map[string]string
	msgs = append(msgs, map[string]string{"role": "system", "content": "you are helpful"})
	for i := 0; i < 10; i++ {
		msgs = append(msgs, map[string]string{"role": "user", "content": big})
	}
	msgs = append(msgs, map[string]string{"role": "user", "content": "LAST"})
	body, _ := json.Marshal(map[string]any{"model": "m", "messages": msgs})

	out, dropped := trimToContextBudget(body, 2500)
	if dropped == 0 {
		t.Fatal("expected trimming")
	}
	var got struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("out not json: %v", err)
	}
	// 首条 system 与末条 LAST 必须保留。
	if got.Messages[0]["role"] != "system" {
		t.Errorf("system head lost: %v", got.Messages[0])
	}
	last := got.Messages[len(got.Messages)-1]
	if last["content"] != "LAST" {
		t.Errorf("last message lost: %v", last)
	}
	// 裁剪提示必须注入（否则模型不知道上文被截断）。
	found := false
	for _, m := range got.Messages {
		if s, _ := m["content"].(string); strings.Contains(s, "上下文窗口提示") {
			found = true
		}
	}
	if !found {
		t.Error("trim notice must be injected")
	}
	// 剩余总量应落在预算附近（允许提示词本身的开销）。
	if n := totalMessagesTokens(anySlice(got.Messages)); n > 3500 {
		t.Errorf("still over budget: %d tokens", n)
	}
}

func TestTrimKeepsSystemAndLastWhenSystemAloneOverBudget(t *testing.T) {
	huge := strings.Repeat("y", 40_000) // ~10k tokens
	body, _ := json.Marshal(map[string]any{"model": "m", "messages": []map[string]string{
		{"role": "system", "content": huge},
		{"role": "user", "content": "old"},
		{"role": "user", "content": "NEW"},
	}})
	out, dropped := trimToContextBudget(body, 100)
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1 (only the middle one)", dropped)
	}
	var got struct {
		Messages []map[string]any `json:"messages"`
	}
	_ = json.Unmarshal(out, &got)
	if got.Messages[0]["role"] != "system" {
		t.Error("system must always survive")
	}
	if got.Messages[len(got.Messages)-1]["content"] != "NEW" {
		t.Error("last message must always survive")
	}
}

func TestTrimNoMessagesOrBadJSON(t *testing.T) {
	if _, d := trimToContextBudget([]byte(`not json`), 100); d != 0 {
		t.Error("bad json must be a no-op")
	}
	if _, d := trimToContextBudget([]byte(`{"model":"m"}`), 100); d != 0 {
		t.Error("missing messages must be a no-op")
	}
	if _, d := trimToContextBudget([]byte(`{"messages":[]}`), 100); d != 0 {
		t.Error("empty messages must be a no-op")
	}
	if _, d := trimToContextBudget([]byte(`{"messages":[{"role":"user","content":"x"}]}`), 0); d != 0 {
		t.Error("zero budget means 跟随上游 → no-op")
	}
}

func TestTrimInjectsSystemWhenNoSystemPresent(t *testing.T) {
	big := strings.Repeat("z", 8000)
	body, _ := json.Marshal(map[string]any{"model": "m", "messages": []map[string]string{
		{"role": "user", "content": big},
		{"role": "user", "content": big},
		{"role": "user", "content": "NEW"},
	}})
	out, dropped := trimToContextBudget(body, 1000)
	if dropped == 0 {
		t.Fatal("expected trimming")
	}
	var got struct {
		Messages []map[string]any `json:"messages"`
	}
	_ = json.Unmarshal(out, &got)
	if got.Messages[0]["role"] != "system" {
		t.Errorf("notice should be prepended when no system exists: %v", got.Messages[0])
	}
}

// anySlice 把 []map[string]any 转 []any（测试辅助）。
func anySlice(in []map[string]any) []any {
	out := make([]any, len(in))
	for i := range in {
		out[i] = in[i]
	}
	return out
}

func TestFormatContextTierAndOptions(t *testing.T) {
	if formatContextTier(0) != "跟随上游" || formatContextTier(128_000) != "128K" {
		t.Error("tier formatting wrong")
	}
	if len(contextTierOptions) < 8 {
		t.Errorf("tier options = %d, want >= 8", len(contextTierOptions))
	}
	if contextTierOptions[0] != 0 {
		t.Error("first tier must be 0 (跟随上游)")
	}
}
