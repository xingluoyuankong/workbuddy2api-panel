package upstream

import (
	"encoding/json"
	"strings"
	"testing"
)

// normImages 跑一遍归一化并把 messages 解回 []any 供断言。
func normImages(t *testing.T, body string) []any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	normalizeImageParts(obj)
	msgs, _ := obj["messages"].([]any)
	return msgs
}

// partAt 取第 msg 条消息 content 数组的第 part 个元素。
func partAt(t *testing.T, msgs []any, msg, part int) map[string]any {
	t.Helper()
	m, ok := msgs[msg].(map[string]any)
	if !ok {
		t.Fatalf("msg %d 非对象", msg)
	}
	c, ok := m["content"].([]any)
	if !ok {
		t.Fatalf("msg %d content 非数组: %#v", msg, m["content"])
	}
	p, ok := c[part].(map[string]any)
	if !ok {
		t.Fatalf("msg %d part %d 非对象", msg, part)
	}
	return p
}

// urlOf 断言 part 是合法的 image_url 形态并返回 url。
func urlOf(t *testing.T, p map[string]any) string {
	t.Helper()
	if got, _ := p["type"].(string); got != "image_url" {
		t.Fatalf("type=%q want image_url (part=%#v)", got, p)
	}
	iu, ok := p["image_url"].(map[string]any)
	if !ok {
		t.Fatalf("image_url 不是对象: %#v", p["image_url"])
	}
	url, _ := iu["url"].(string)
	return url
}

// Anthropic 方言（Claude Code）：{"type":"image","source":{base64}} →
// 上游唯一接受的 image_url 对象形态。改前上游返回
// 11101 Parse message failed: unsupported content type at index N: image。
func TestImageAnthropicBase64Translated(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"system","content":"s"},{"role":"user","content":[
		{"type":"text","text":"看图"},
		{"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"AAAA"}}]}]}`)
	p := partAt(t, msgs, 1, 1)
	if got := urlOf(t, p); got != "data:image/jpeg;base64,AAAA" {
		t.Errorf("url=%q want data:image/jpeg;base64,AAAA", got)
	}
	if _, leftover := p["source"]; leftover {
		t.Error("source 字段未删除（上游会按未知字段处理）")
	}
}

// Anthropic url 形态：source.type=url 直接用 source.url。
func TestImageAnthropicURLTranslated(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"type":"image","source":{"type":"url","url":"https://x/a.png"}}]}]}`)
	if got := urlOf(t, partAt(t, msgs, 0, 0)); got != "https://x/a.png" {
		t.Errorf("url=%q", got)
	}
}

// 裸字符串 image_url → 对象形态。改前上游返回
// 11101 Parse message failed: invalid image_url content（期望对象）。
func TestImageURLStringWrapped(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"type":"image_url","image_url":"data:image/png;base64,BBBB"}]}]}`)
	if got := urlOf(t, partAt(t, msgs, 0, 0)); got != "data:image/png;base64,BBBB" {
		t.Errorf("url=%q", got)
	}
}

// Responses API（Codex）方言：input_image → image_url。
func TestInputImageTranslated(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"type":"input_image","image_url":"data:image/png;base64,CCCC"}]}]}`)
	if got := urlOf(t, partAt(t, msgs, 0, 0)); got != "data:image/png;base64,CCCC" {
		t.Errorf("url=%q", got)
	}
}

// base64 payload 里的换行/空格必须清掉：上游按原样解码，空白即 11135
// "replace the image"（文案与真实原因无关，用户会白折腾）。
func TestBase64WhitespaceStripped(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"type":"image_url","image_url":{"url":"data:image/png;base64,AA\nBB\r\n CC\tDD"}}]}]}`)
	if got := urlOf(t, partAt(t, msgs, 0, 0)); got != "data:image/png;base64,AABBCCDD" {
		t.Errorf("url=%q want 无空白", got)
	}
}

// part 缺 type 字段（上游报 missing type field 整单 400）→ 按携带字段推断补全。
func TestMissingTypeInferred(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"text":"纯文本"},
		{"image_url":{"url":"https://x/b.png"}},
		{"source":{"type":"base64","media_type":"image/png","data":"EEEE"}}]}]}`)
	if got, _ := partAt(t, msgs, 0, 0)["type"].(string); got != "text" {
		t.Errorf("part0 type=%q want text", got)
	}
	if got := urlOf(t, partAt(t, msgs, 0, 1)); got != "https://x/b.png" {
		t.Errorf("part1 url=%q", got)
	}
	if got := urlOf(t, partAt(t, msgs, 0, 2)); got != "data:image/png;base64,EEEE" {
		t.Errorf("part2 url=%q", got)
	}
}

// 已经是上游标准形态的 part 必须**逐字不动**（不做无谓改写，
// 避免给上游制造 body 差异、也保护既有正常流量）。
func TestStandardPartUntouched(t *testing.T) {
	const body = `{"messages":[{"role":"user","content":[
		{"type":"text","text":"hi"},
		{"type":"image_url","image_url":{"url":"https://x/c.png","detail":"high"}}]}]}`
	var obj map[string]any
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		t.Fatal(err)
	}
	if normalizeImageParts(obj) {
		t.Error("标准形态被判定为有改动（应零改动）")
	}
}

// 纯文本请求（content 是字符串）零改动、零开销。
func TestPlainTextUntouched(t *testing.T) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(`{"messages":[{"role":"user","content":"你好"}]}`), &obj); err != nil {
		t.Fatal(err)
	}
	if normalizeImageParts(obj) {
		t.Error("纯文本请求不应有改动")
	}
}

// 无法安全翻译的形态原样保留（不编造 URL 猜测上游语义）：
// Responses 的 file_id 形态需要另调上传接口才能拿到字节。
func TestUntranslatablePartLeftAlone(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"type":"input_image","file_id":"file-abc"}]}]}`)
	p := partAt(t, msgs, 0, 0)
	if got, _ := p["type"].(string); got != "input_image" {
		t.Errorf("type=%q want 原样保留 input_image", got)
	}
	if _, has := p["image_url"]; has {
		t.Error("不应编造 image_url")
	}
}

// Anthropic source 结构残缺（缺 data / 未知 type）时不做猜测性翻译。
func TestBrokenAnthropicSourceLeftAlone(t *testing.T) {
	msgs := normImages(t, `{"messages":[{"role":"user","content":[
		{"type":"image","source":{"type":"base64"}},
		{"type":"image","source":{"type":"weird","url":"https://x/d.png"}}]}]}`)
	for i := 0; i < 2; i++ {
		if got, _ := partAt(t, msgs, 0, i)["type"].(string); got != "image" {
			t.Errorf("part%d type=%q want 原样保留 image", i, got)
		}
	}
}

// 集成：方言图片 part 经完整出站管线（PrepareBodyOpt）后已是上游白名单形态，
// 且不再出现任何 raw 方言 type。
func TestPrepareBodyNormalizesImageDialects(t *testing.T) {
	in := `{"model":"deepseek-v4.1-flash","stream":true,"messages":[
		{"role":"system","content":"s"},
		{"role":"user","content":[
			{"type":"text","text":"看图"},
			{"type":"image","source":{"type":"base64","media_type":"image/png","data":"FF\nFF"}},
			{"type":"input_image","image_url":"data:image/png;base64,GGGG"}]}]}`
	out := string(PrepareBodyOpt([]byte(in), false))
	for _, bad := range []string{`"type":"image"`, `"type":"input_image"`, `"source"`} {
		if strings.Contains(out, bad) {
			t.Errorf("出站 body 仍含方言 %s: %s", bad, out)
		}
	}
	if !strings.Contains(out, "data:image/png;base64,FFFF") {
		t.Errorf("base64 空白未清理: %s", out)
	}
	if !strings.Contains(out, "data:image/png;base64,GGGG") {
		t.Errorf("input_image 未翻译: %s", out)
	}
}

// cleanDataURL 边界：非 data URL / 无 base64 标记 / 无空白 均原样返回。
func TestCleanDataURLEdges(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://x/a.png", "https://x/a.png"},
		{"data:image/png,plainnotbase64", "data:image/png,plainnotbase64"},
		{"data:image/png;base64,AAAA", "data:image/png;base64,AAAA"},
		{"data:image/png;base64,A A", "data:image/png;base64,AA"},
	}
	for _, c := range cases {
		if got := cleanDataURL(c.in); got != c.want {
			t.Errorf("cleanDataURL(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

// ensureLeadingSystem：首条非 system 时前置兜底 system——上游对首条非 system
// 一律 400 code=11128 first message is not system prompt（实测，与内容无关）。
func TestEnsureLeadingSystemPrepends(t *testing.T) {
	out := string(ensureLeadingSystem([]byte(`{"messages":[{"role":"user","content":"你好"}]}`)))
	if !strings.Contains(out, `"role":"system"`) {
		t.Fatalf("未注入 system: %s", out)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(out), &obj); err != nil {
		t.Fatal(err)
	}
	msgs, _ := obj["messages"].([]any)
	first, _ := msgs[0].(map[string]any)
	if role, _ := first["role"].(string); role != "system" {
		t.Errorf("首条 role=%q want system", role)
	}
	if len(msgs) != 2 {
		t.Errorf("消息数=%d want 2（原消息必须保留）", len(msgs))
	}
}

// 首条已是 system：不注入、不重复、内容逐字不动。
func TestEnsureLeadingSystemIdempotent(t *testing.T) {
	const body = `{"messages":[{"role":"system","content":"原样"},{"role":"user","content":"hi"}]}`
	if out := string(ensureLeadingSystem([]byte(body))); out != body {
		t.Errorf("首条已是 system 时不应有任何改动:\n got=%s\nwant=%s", out, body)
	}
}
