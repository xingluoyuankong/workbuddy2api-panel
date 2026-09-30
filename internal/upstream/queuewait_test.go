package upstream

import (
	"strings"
	"testing"
	"time"
)

// 上游排队响应的真实样本（2026-09-22 线上抓取，来自 hy4-preview-f）。
const realQueueBody = `{"code":6020,"msg":"queue.waiting.title","requestId":"ab0b9c9e3fc8638820ec66bcddaaa726",` +
	`"data":{"queue_position":2159,"queue_size":"999+","retry_after":40,"estimated_wait":10795,"client_user_type":"free"}}`

// TestIsQueueWait 排队判定：6020 / queue.waiting 双通道，且不得误伤普通限流。
func TestIsQueueWait(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"真实 6020 排队样本", realQueueBody, true},
		{"code 带空格", `{"code": 6020,"msg":"x"}`, true},
		{"code 字符串形态", `{"code":"6020","msg":"x"}`, true},
		{"只有 queue.waiting 文案（code 缺失）", `{"msg":"queue.waiting.title"}`, true},
		{"6004 模型限额不是排队", `{"code":6004,"msg":"您的使用量已超出频率限制，将在 2026-09-22 00:17:14 UTC+8 重置"}`, false},
		{"11140 限流不是排队", `{"code":11140,"msg":"The model provider is rate-limiting requests."}`, false},
		{"402 余额不足不是排队", `{"code":1,"msg":"余额不足"}`, false},
		{"恰好含 602（前缀不匹配）", `{"code":602,"msg":"x"}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsQueueWait(c.body); got != c.want {
				t.Errorf("IsQueueWait=%v want %v (body=%s)", got, c.want, c.body)
			}
		})
	}
}

// TestParseQueueWait 解析 body 里的建议等待秒数；缺失/非法/非正 → 0（调用方夹取）。
func TestParseQueueWait(t *testing.T) {
	cases := []struct {
		name string
		body string
		want time.Duration
	}{
		{"真实样本 retry_after=40", realQueueBody, 40 * time.Second},
		{"带小数", `{"data":{"retry_after":12.5}}`, 12500 * time.Millisecond},
		{"缺失", `{"code":6020,"msg":"queue.waiting.title"}`, 0},
		{"为 0", `{"data":{"retry_after":0}}`, 0},
		{"负数", `{"data":{"retry_after":-5}}`, 0},
		{"非数字", `{"data":{"retry_after":"soon"}}`, 0},
		{"非 JSON", `queue.waiting.title`, 0},
		{"空 body", ``, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ParseQueueWait(c.body); got != c.want {
				t.Errorf("ParseQueueWait=%v want %v", got, c.want)
			}
		})
	}
}

// TestQueueWaitHint 排队的 gateway_hint 必须指向「换模型」——
// 它与普通限流同 Kind（429），但处置完全不同（账号没坏、额度没光）。
func TestQueueWaitHint(t *testing.T) {
	hint := GatewayHint(ErrSoftRate, realQueueBody, HintContext{Model: "hy4-preview-f"})
	if hint == "" {
		t.Fatal("排队应给出 hint")
	}
	if want := "switch to another model"; !strings.Contains(hint, want) {
		t.Errorf("hint=%q 应含 %q", hint, want)
	}
	// 普通限流的 hint 不应被排队文案污染（回归保护）。
	plain := GatewayHint(ErrSoftRate, `{"code":6004,"msg":"rate limit"}`, HintContext{})
	if strings.Contains(plain, "queue.waiting") {
		t.Errorf("普通限流 hint 不该提排队: %q", plain)
	}
}
