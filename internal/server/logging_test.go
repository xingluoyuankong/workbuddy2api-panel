package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// captureStdout 重定向 os.Stdout（连同 chatLogOut，见 SetChatLogOutput 的注入点）
// 并捕获 fn 期间的全部输出。
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	oldOut := chatLogOut
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	chatLogOut = w
	fn()
	os.Stdout = old
	chatLogOut = oldOut
	_ = w.Close()
	raw, _ := io.ReadAll(r)
	return string(raw)
}

// withChatLog 临时开启聊天表格日志（TestMain 默认关闭），测试结束后恢复。
// 仅供断言表格行输出的用例使用。
func withChatLog(t *testing.T) {
	t.Helper()
	old := chatLogEnabled
	chatLogEnabled = true
	t.Cleanup(func() { chatLogEnabled = old })
}

func TestChatStatsReaderTokensFromUsage(t *testing.T) {
	r := newChatStatsReaderSince(strings.NewReader(sseOK), time.Now())
	// 保证 TTFB 跨过时钟粒度：Windows 上纯内存读取不足 1ms，time.Since 可能取 0。
	time.Sleep(3 * time.Millisecond)
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("copy: %v", err)
	}
	toks, ok := r.Tokens()
	if !ok || toks != 1 {
		t.Fatalf("tokens=%d ok=%v, want 1/true (from usage, not rune count)", toks, ok)
	}
	usage := r.Usage()
	if !usage.HasPromptTokens || usage.PromptTokens != 1 || !usage.HasCompletionTokens || usage.CompletionTokens != 1 || !usage.HasTotalTokens || usage.TotalTokens != 2 {
		t.Fatalf("usage=%+v, want prompt=1 completion=1 total=2", usage)
	}
	if r.TTFB() <= 0 {
		t.Errorf("ttfb=%v want >0", r.TTFB())
	}
}

func TestChatStatsReaderNoUsage(t *testing.T) {
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	r := newChatStatsReaderSince(strings.NewReader(sse), time.Now())
	_, _ = io.Copy(io.Discard, r)
	if toks, ok := r.Tokens(); ok || toks != 0 {
		t.Errorf("tokens=%d ok=%v, want 0/false for missing usage", toks, ok)
	}
}

func TestChatStatsReaderLastFrameUsageWins(t *testing.T) {
	sse := "data: {\"usage\":{\"completion_tokens\":5}}\n\n" +
		"data: {\"usage\":{\"completion_tokens\":12}}\n\n" +
		"data: [DONE]\n\n"
	r := newChatStatsReaderSince(strings.NewReader(sse), time.Now())
	_, _ = io.Copy(io.Discard, r)
	toks, ok := r.Tokens()
	if !ok || toks != 12 {
		t.Fatalf("tokens=%d ok=%v, want 12 (last frame wins)", toks, ok)
	}
}

func TestChatStatsReaderTTFBOnlyOnDataFrame(t *testing.T) {
	start := time.Now().Add(-2 * time.Second)
	var s chatStatsReader
	s.start = start
	s.parseSSELine("event: ping")
	if s.TTFB() != 0 {
		t.Errorf("non-data line must not set TTFB: %v", s.TTFB())
	}
	s.parseSSELine("data: {\"choices\":[]}")
	first := s.TTFB()
	if first < time.Second {
		t.Errorf("first data frame TTFB=%v want >=2s", first)
	}
	s.parseSSELine("data: {\"choices\":[]}")
	if s.TTFB() != first {
		t.Errorf("second frame changed TTFB: %v -> %v", first, s.TTFB())
	}
}

func TestChatStatsReaderBytesPassthrough(t *testing.T) {
	sse := "data: {\"content\":\"你好\"}\n\ndata: [DONE]\n\n"
	r := newChatStatsReaderSince(strings.NewReader(sse), time.Now())
	out, _ := io.ReadAll(r)
	if string(out) != sse {
		t.Errorf("passthrough mismatch:\n got %q\nwant %q", out, sse)
	}
}

func TestParseModelFromBody(t *testing.T) {
	if got := parseModelFromBody([]byte(`{"model":"deepseek-v4-flash","stream":true}`)); got != "deepseek-v4-flash" {
		t.Errorf("got %q", got)
	}
	if got := parseModelFromBody([]byte(`{}`)); got != "-" {
		t.Errorf("got %q want -", got)
	}
	if got := parseModelFromBody([]byte(`not json`)); got != "-" {
		t.Errorf("got %q want -", got)
	}
}

func TestCompletionTokensExtraction(t *testing.T) {
	got := completionTokens(map[string]any{
		"usage": map[string]any{"prompt_tokens": 10.0, "completion_tokens": 234.0, "total_tokens": 244.0},
	})
	if got != 234 {
		t.Errorf("got %d want 234", got)
	}
	if got := completionTokens(map[string]any{}); got != -1 {
		t.Errorf("missing usage: got %d want -1", got)
	}
	if got := completionTokens(map[string]any{"usage": map[string]any{}}); got != -1 {
		t.Errorf("missing completion_tokens: got %d want -1", got)
	}
}

func TestUIDPrefix(t *testing.T) {
	if got := uidPrefix("00e26541abcdef012345"); got != "00e26541" {
		t.Errorf("long uid -> %q", got)
	}
	if got := uidPrefix("abc"); got != "abc" {
		t.Errorf("short uid -> %q", got)
	}
	if got := uidPrefix(""); got != "-" {
		t.Errorf("empty uid -> %q", got)
	}
}

func TestLogChatRowFormat(t *testing.T) {
	withChatLog(t)
	out := captureStdout(t, func() {
		logChatRow(412*time.Millisecond, 27100*time.Millisecond, "deepseek-v4-flash", "stream", "00e26541abcdef", http.StatusOK, 1234, 0.08, true, 1, 0, false)
	})
	for _, want := range []string{
		"| #", "deepseek-v4", "| stream |", "| 200 |", "uid=00e26541", "TTFB=412ms", "tok=1234", "tok/s |", "total=",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("row missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "00e26541abcdef") {
		t.Errorf("full uid leaked: %s", out)
	}
}

func TestLogChatRowNoUsageShowsDash(t *testing.T) {
	withChatLog(t)
	out := captureStdout(t, func() {
		logChatRow(0, time.Second, "glm-5.2", "sync", "s1", http.StatusServiceUnavailable, -1, 0, false, 1, 0, false)
	})
	for _, want := range []string{"TTFB=-", "tok=-", "-tok/s", "| 503 |"} {
		if !strings.Contains(out, want) {
			t.Errorf("row missing %q:\n%s", want, out)
		}
	}
}

func TestLogChatRowSeqIncrements(t *testing.T) {
	withChatLog(t)
	out := captureStdout(t, func() {
		logChatRow(0, time.Second, "m", "sync", "u", 200, 1, 0, true, 1, 0, false)
		logChatRow(0, time.Second, "m", "sync", "u", 200, 1, 0, true, 1, 0, false)
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), out)
	}
	first := strings.Fields(lines[0])[1]
	second := strings.Fields(lines[1])[1]
	if !strings.HasPrefix(first, "#") || !strings.HasPrefix(second, "#") {
		t.Fatalf("seq columns missing: %q %q", first, second)
	}
	if first == second {
		t.Errorf("seq not incremented: %q == %q", first, second)
	}
}

func TestChatLogsStreamRow(t *testing.T) {
	withChatLog(t)
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, sseOK, true
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	out := captureStdout(t, func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[]}`))
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	for _, want := range []string{"| stream |", "| 200 |", "uid=u1", "TTFB=", "tok=1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stream row missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "tok=1") {
		t.Errorf("tok: want precise usage completion_tokens: %s", out)
	}
}

func TestChatLogsSyncRowTTFBDash(t *testing.T) {
	withChatLog(t)
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, sseOK, true
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	out := captureStdout(t, func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	for _, want := range []string{"| sync |", "| 200 |", "TTFB=-", "tok=1"} {
		if !strings.Contains(out, want) {
			t.Errorf("sync row missing %q:\n%s", want, out)
		}
	}
}

func TestChatLogsErrorRow(t *testing.T) {
	withChatLog(t)
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 402, `{"code":1,"msg":"余额不足"}`, false
	})
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: up})
	out := captureStdout(t, func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
		h.ServeHTTP(rec, req)
		if rec.Code != 503 {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
		}
	})
	for _, want := range []string{"uid=u1", "| 503 |", "tok=-"} {
		if !strings.Contains(out, want) {
			t.Errorf("error row missing %q:\n%s", want, out)
		}
	}
}

func TestHealthzDoesNotLogTableRow(t *testing.T) {
	withChatLog(t) // 日志开启也应无表格行：非 chat 路由根本不走 logChatRow
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: newFakeUpstream(t, func(string) (int, string, bool) {
		return 200, sseOK, true
	})})
	out := captureStdout(t, func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
		if rec.Code != 200 {
			t.Fatalf("code=%d", rec.Code)
		}
		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, httptest.NewRequest("GET", "/v1/models", nil))
		rec3 := httptest.NewRecorder()
		h.ServeHTTP(rec3, httptest.NewRequest("GET", "/status", nil))
	})
	if strings.Contains(out, "| #") {
		t.Errorf("healthz/models/status must not emit table rows:\n%s", out)
	}
}

// TestLogChatRowCreditColumn 日志行的积分列（cr=）语义：
//   - 有 credit 且 >0 → 显示数值（实际扣费）
//   - 有 credit 且 ==0 → 显示 "0"（实测免费，不是"没数据"）
//   - 无 credit → 显示 "-"（上游没下发）
// 三者必须可区分：把"没数据"渲染成 0 会让人以为这些请求都免费。
func TestLogChatRowCreditColumn(t *testing.T) {
	cases := []struct {
		name      string
		credit    float64
		hasCredit bool
		want      string
	}{
		{"paid", 0.08, true, "cr=0.08"},
		{"free", 0, true, "cr=0"},
		{"missing", 0, false, "cr=-"},
		{"tiny", 0.00115, true, "cr=0.0011"}, // %.4f 截断，够定位量级
		{"large", 1234.5, true, "cr=1234"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			old := chatLogOut
			chatLogOut = &buf
			oldEnabled := chatLogEnabled
			chatLogEnabled = true
			defer func() { chatLogOut = old; chatLogEnabled = oldEnabled }()

			logChatRow(0, time.Second, "m", "sync", "u", 200, 10, c.credit, c.hasCredit, 1, 0, false)
			line := buf.String()
			if !strings.Contains(line, c.want) {
				t.Errorf("line = %q, want %q", line, c.want)
			}
		})
	}
}

// TestHeadModelOfTruncatedBody 413 超限路径的 body 是被 LimitReader 截断的
// （只读了 limit+1 字节），JSON 结构不完整 → json.Unmarshal 必失败、
// parseModelFromBody 只能给 "-"，日志行的模型列因此是空白。
// headModelOf 必须能从头部 4KB 拿到模型名——这是排查"哪个模型超了"的唯一线索
// （2026-09-20 线上实测该列为空，故补此回归）。
func TestHeadModelOfTruncatedBody(t *testing.T) {
	// 截断形态：model 在前，messages 被切断（模拟 32MB 请求只读到 32MB+1）。
	truncated := `{"model":"global:deepseek-v4.1-flash","stream":true,"messages":[{"role":"user","content":"` +
		strings.Repeat("A", 8192)
	if got := parseModelFromBody([]byte(truncated)); got != "-" {
		t.Fatalf("前置断言：截断 body 不该被解析出模型（got %q）——headModelOf 的存在意义即为此", got)
	}
	if got := headModelOf([]byte(truncated)); got != "global:deepseek-v4.1-flash" {
		t.Errorf("headModelOf=%q want global:deepseek-v4.1-flash", got)
	}

	// 键排在 4KB 之外 → 给 "-"（不编造，不越界扫描）。
	late := `{"stream":true,"pad":"` + strings.Repeat("B", 8192) + `","model":"x"}`
	if got := headModelOf([]byte(late)); got != "-" {
		t.Errorf("model 键在 4KB 之外时应给 \"-\"，got %q", got)
	}

	// 完全取不到 → "-"。
	if got := headModelOf([]byte("not json at all")); got != "-" {
		t.Errorf("非 JSON 应给 \"-\"，got %q", got)
	}

	// 空 body → "-"（不 panic）。
	if got := headModelOf(nil); got != "-" {
		t.Errorf("空 body 应给 \"-\"，got %q", got)
	}
}

// 解码期速度口径：hasTokps 时 tok/s 显示解码期速度（分母剔除 TTFB），
// tries>1 时行尾标注 try=N（解释 TTFB 为什么大）。
func TestLogChatRowDecodeTokps(t *testing.T) {
	withChatLog(t)
	out := captureStdout(t, func() {
		// 283 tok，请求级 TTFB 24s，解码期 94.3 tok/s，重试 3 次
		logChatRow(24*time.Second, 27*time.Second, "hy4-preview-f", "stream",
			"00e26541abcdef", http.StatusOK, 283, 0, true, 3, 94.3, true)
	})
	for _, want := range []string{"94.3tok/s", "try=3 |", "TTFB=24000ms", "tok=283"} {
		if !strings.Contains(out, want) {
			t.Errorf("row missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "10.5tok/s") {
		t.Errorf("旧口径速度不应再出现:\n%s", out)
	}
}

// 无解码期数据时回落旧口径（tok/total），try=1 不标注。
func TestLogChatRowFallbackTokps(t *testing.T) {
	withChatLog(t)
	out := captureStdout(t, func() {
		logChatRow(0, 2*time.Second, "m", "sync", "u", 200, 10, 0, true, 1, 0, false)
	})
	if !strings.Contains(out, "5.0tok/s") {
		t.Errorf("回落口径缺失:\n%s", out)
	}
	if strings.Contains(out, "try=") {
		t.Errorf("try=1 不应标注:\n%s", out)
	}
}
