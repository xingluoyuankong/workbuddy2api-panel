package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// contentFilterSSE 复刻上游「内容审核空流」实测形态（HTTP 200，2 帧全空 delta，
// 末帧 finish_reason=content_filter，completion_tokens=0）。来源见
// internal/upstream/contentfilter.go 的现场记录。
const contentFilterSSE = `data: {"id":"c1","model":"deep-model","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[]},"finish_reason":""}],"usage":null}` + "\n\n" +
	`data: {"id":"c1","model":"deep-model","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[]},"finish_reason":"content_filter"}],"usage":{"completion_tokens":0,"credit":1.02}}` + "\n\n" +
	"data: [DONE]\n\n"

// TestContentFilterCustomModeNoDegrade custom 模式下上游返回审核空流（HTTP 200 +
// finish_reason=content_filter + 空正文）：
//   - 必须识别为内容拦截，不再把「空回答」当成功透传给客户端（识别见
//     upstream/contentfilter.go）；
//   - **绝不做提示词降级/回退**：只打一次上游，直接回 400 content_filtered，
//     由用户决定换模型还是改提示词。降级会让「这篇回答按谁的提示词生成」悄悄
//     改变，属于行为篡改（本项目明确约束）。
func TestContentFilterCustomModeNoDegrade(t *testing.T) {
	calls := 0
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		calls++
		return 200, contentFilterSSE, true
	})
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: up, PromptMode: "custom", PromptText: "SRC-HUNTER-SYS"})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[{"role":"system","content":"old"},{"role":"user","content":"hi"}]}`)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d want 400 (body=%s)", rec.Code, rec.Body.String())
	}
	if calls != 1 {
		t.Fatalf("calls=%d want 1（禁止任何降级重试）", calls)
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v (body=%s)", err, rec.Body.String())
	}
	if env.Error.Code != "content_filtered" {
		t.Errorf("error.code=%q want content_filtered", env.Error.Code)
	}
	// 不触发粘性降级（否则后续请求的 persona 会被悄悄改掉）。
	if h.degrade.Active() {
		t.Error("content_filter 不应触发粘性降级")
	}
	// 内容问题非账号问题：不冷却、不熔断、不计错。
	st, _ := p.Status("u1")
	if st.Cooling || st.Disabled || st.ErrTotal != 0 {
		t.Fatalf("content_filter should not penalize account: %+v", st)
	}
}

// TestContentFilterPassthroughKeepsOriginalDegradePath passthrough 模式下 HTTP 400
// 内容策略拦截的**既有**降级路径（system 指纹误报）不受影响，仍是零回归。
func TestContentFilterPassthroughKeepsOriginalDegradePath(t *testing.T) {
	calls := 0
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		calls++
		if calls == 1 {
			return 400, `{"code":11128,"msg":"blocked by security policy"}`, false
		}
		return 200, sseOK, true
	})
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: up, PromptMode: "passthrough"})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s (want 200 after legacy degraded retry)", rec.Code, rec.Body)
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2（passthrough 既有降级重试保留）", calls)
	}
}
