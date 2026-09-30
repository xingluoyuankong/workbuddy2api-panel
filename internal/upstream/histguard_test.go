package upstream

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestEnsureDeepSeekEffortSkipsOnInconsistentHistory 历史里 assistant 的
// reasoning_content 有带/有不带时，不补 reasoning_effort（规避上游 11155）。
// 依据：2026-09-19 真实抓包 body 逐字段二分——带档位必 11155，不带档位 200。
func TestEnsureDeepSeekEffortSkipsOnInconsistentHistory(t *testing.T) {
	build := func(rcs ...string) map[string]any {
		msgs := []any{map[string]any{"role": "user", "content": "hi"}}
		for i, rc := range rcs {
			m := map[string]any{"role": "assistant", "content": nil,
				"tool_calls": []any{map[string]any{"id": "c" + string(rune('0'+i))}}}
			if rc != "-" { // "-" 表示不带该字段
				m["reasoning_content"] = rc
			}
			msgs = append(msgs, m)
		}
		return map[string]any{"model": "deepseek-v4.1-flash", "messages": msgs}
	}

	cases := []struct {
		name      string
		rcs       []string
		wantEffrt bool
	}{
		{"全非空 → 保留档位", []string{"a", "b"}, true},
		{"全空字段 → 保留档位（一致地没有）", []string{"", ""}, true},
		{"全不带字段 → 保留档位", []string{"-", "-"}, true},
		{"有非空有缺字段 → 不补档位", []string{"a", "-"}, false},
		{"有非空有空串 → 不补档位", []string{"a", ""}, false},
		{"无 assistant → 保留档位", []string{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := build(c.rcs...)
			injectThinking(obj, "")
			_, has := obj["reasoning_effort"]
			if has != c.wantEffrt {
				t.Fatalf("reasoning_effort present=%v want %v (obj=%v)", has, c.wantEffrt, obj)
			}
			// 无论补不补档位，thinking 都必须开（这是本修复的核心取舍：只开思考、不补档位）
			th, _ := obj["thinking"].(map[string]any)
			if typ, _ := th["type"].(string); typ != "enabled" {
				t.Fatalf("thinking.type=%v want enabled", obj["thinking"])
			}
		})
	}
}

// TestEnsureDeepSeekEffortRespectsExplicit 显式档位一律不动（即使用户自己给的也不改）。
func TestEnsureDeepSeekEffortRespectsExplicit(t *testing.T) {
	obj := map[string]any{
		"model": "deepseek-v4.1-flash",
		"messages": []any{
			map[string]any{"role": "assistant", "reasoning_content": "a"},
			map[string]any{"role": "assistant"},
		},
		"reasoning_effort": "max",
	}
	injectThinking(obj, "")
	if obj["reasoning_effort"] != "max" {
		t.Fatalf("显式档位被改写: %v", obj["reasoning_effort"])
	}
}

// TestReasoningHistoryInconsistent 直接覆盖判定函数。
func TestReasoningHistoryInconsistent(t *testing.T) {
	mk := func(j string) map[string]any {
		var o map[string]any
		if err := json.Unmarshal([]byte(j), &o); err != nil {
			t.Fatal(err)
		}
		return o
	}
	yes := []string{
		`{"messages":[{"role":"assistant","reasoning_content":"a"},{"role":"assistant"}]}`,
		`{"messages":[{"role":"assistant","reasoning_content":" "},{"role":"assistant","reasoning_content":"a"}]}`,
	}
	no := []string{
		`{"messages":[{"role":"user","content":"x"}]}`,
		`{"messages":[{"role":"assistant","reasoning_content":"a"}]}`,
		`{"messages":[{"role":"assistant"},{"role":"assistant"}]}`,
		`{}`,
	}
	for _, j := range yes {
		if !reasoningHistoryInconsistent(mk(j)) {
			t.Errorf("want inconsistent: %s", j)
		}
	}
	for _, j := range no {
		if reasoningHistoryInconsistent(mk(j)) {
			t.Errorf("want consistent: %s", j)
		}
	}
	_ = strings.TrimSpace
}
