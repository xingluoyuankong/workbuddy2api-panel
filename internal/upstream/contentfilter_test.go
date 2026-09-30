package upstream

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// contentFilterStream 复刻上游「内容审核空流」实测形态（2 帧，全空 delta + content_filter）。
const contentFilterStream = `data: {"id":"cmb-1","model":"deep-model","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[]},"finish_reason":""}],"usage":null}` + "\n\n" +
	`data: {"id":"cmb-1","model":"deep-model","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[]},"finish_reason":"content_filter"}],"usage":{"completion_tokens":0,"credit":1.02}}` + "\n\n" +
	"data: [DONE]\n\n"

type fakeRC struct{ r io.Reader }

func (f *fakeRC) Read(p []byte) (int, error) { return f.r.Read(p) }
func (f *fakeRC) Close() error               { return nil }

// TestSniffContentFilterDetectsEmptyFilteredStream 审核空流必须被识别。
func TestSniffContentFilterDetectsEmptyFilteredStream(t *testing.T) {
	body, hit := sniffContentFilter(&fakeRC{r: strings.NewReader(contentFilterStream)})
	if !hit {
		t.Fatal("content_filter 空流应命中")
	}
	if body != nil {
		t.Fatal("命中时不应返回 body")
	}
}

// TestSniffContentFilterPassesNormalStream 正常流必须原样放行且字节无损。
func TestSniffContentFilterPassesNormalStream(t *testing.T) {
	normal := `data: {"id":"a","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":""}]}` + "\n\n" +
		`data: {"id":"a","choices":[{"index":0,"delta":{"content":"你"},"finish_reason":""}]}` + "\n\n" +
		`data: {"id":"a","choices":[{"index":0,"delta":{"content":"好"},"finish_reason":"stop"}],"usage":{"completion_tokens":2}}` + "\n\n" +
		"data: [DONE]\n\n"
	body, hit := sniffContentFilter(&fakeRC{r: strings.NewReader(normal)})
	if hit {
		t.Fatal("正常流不应命中")
	}
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != normal {
		t.Fatalf("回放字节不一致:\n got=%q\nwant=%q", got, normal)
	}
}

// TestSniffContentFilterPassesEmptyStopStream finish_reason=stop 的空回答不升级为错误。
func TestSniffContentFilterPassesEmptyStopStream(t *testing.T) {
	s := `data: {"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
	_, hit := sniffContentFilter(&fakeRC{r: strings.NewReader(s)})
	if hit {
		t.Fatal("finish_reason=stop 不应判为内容审核")
	}
}

// TestSniffContentFilterKeepsPartialAnswer 已吐正文后再出现 content_filter 不算空流
// （保护残答：用户已经看到内容，不应把整条回答作废）。
func TestSniffContentFilterKeepsPartialAnswer(t *testing.T) {
	s := `data: {"choices":[{"index":0,"delta":{"content":"部分"},"finish_reason":""}]}` + "\n\n" +
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}` + "\n\ndata: [DONE]\n\n"
	_, hit := sniffContentFilter(&fakeRC{r: strings.NewReader(s)})
	if hit {
		t.Fatal("已产出正文的流不应判为审核空流")
	}
}

// TestSniffContentFilterPassesTruncatedStream 读取中途 EOF（无任何终止帧）放行。
func TestSniffContentFilterPassesTruncatedStream(t *testing.T) {
	_, hit := sniffContentFilter(&fakeRC{r: strings.NewReader(`data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}` + "\n\n")})
	if hit {
		t.Fatal("截断流不应判为内容审核")
	}
}

// TestChatStreamContextContentFilterBecomesContentBlocked 端到端（client 层）：
// 上游 200 + 审核空流 → 返回 400 + ErrContentBlocked{ContentFilter:true}，
// 且不再把空流交给调用方当成功。
func TestChatStreamContextContentFilterBecomesContentBlocked(t *testing.T) {
	c := New()
	c.HTTP = &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(contentFilterStream)),
		}, nil
	})}
	c.ChatHTTP = c.HTTP
	c.ChatBaseGlobal = "https://global.example"
	a := &auth.Auth{AccessToken: "at", UID: "u1"}

	rc, status, body, err := c.ChatStreamContext(context.Background(), a, []byte(`{"model":"deep-model"}`), "", ChatMeta{})
	if rc != nil {
		rc.Close()
		t.Fatal("rc 应为 nil")
	}
	if status != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", status)
	}
	ue, ok := err.(*Error)
	if !ok || ue.Kind != ErrContentBlocked || !ue.ContentFilter {
		t.Fatalf("err=%#v want ErrContentBlocked{ContentFilter:true}", err)
	}
	if !strings.Contains(string(body), "content_filtered") {
		t.Fatalf("body=%s", body)
	}
}

// TestContentFilteredBodyIsJSON 透出 body 必须是可被客户端解析的 JSON。
func TestContentFilteredBodyIsJSON(t *testing.T) {
	if !strings.HasPrefix(strings.TrimSpace(ContentFilteredBody), "{") {
		t.Fatalf("ContentFilteredBody 非 JSON: %s", ContentFilteredBody)
	}
}
