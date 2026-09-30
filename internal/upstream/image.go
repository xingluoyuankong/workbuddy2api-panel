// image.go 出站请求体的多模态 content part 归一化。
//
// 背景（2026-09-19 直连上游逐形态实测，网关强制 stream:true 的真实链路）：
// 上游对 content 数组的 part 白名单**只有两种**形态：
//
//	{"type":"text","text":"..."}
//	{"type":"image_url","image_url":{"url":"..."}}
//
// 其余形态全部在上游解析层整单拒绝（HTTP 400），实测原文：
//
//	Anthropic {"type":"image","source":{...}}      → 11101 Parse message failed: unsupported content type at index N: image
//	{"type":"image_url","image_url":"data:..."}    → 11101 Parse message failed: invalid image_url content at index N（期望对象）
//	Responses {"type":"input_image",...}           → 11101 Parse message failed: unsupported content type at index N: input_image
//	part 缺 type 字段                               → 11101 Parse message failed: missing type field in content item at index N
//	base64 payload 里含换行/空格                    → 11135 Please start a new conversation, replace the image, and try again.
//
// 而真实客户端发的恰好都是白名单外的方言（Claude Code 走 Anthropic 方言、
// Codex 走 Responses 方言、部分 SDK 把 image_url 写成裸字符串、有的为了可读性
// 把 base64 折行）——「图片输入整单失败」的真实原因不在图片数据，而在**结构**。
//
// 本模块把各方言翻译成上游唯一接受的形态：同一张图、同一个 media type，只改
// 结构不改语义。无法安全翻译的 part（如 Responses 的 file_id 需要另调上传接口）
// 原样保留，让上游按原语义报错——不编造、不猜测。
//
// 纪律：归一化是**协议兼容**（补上游解析器白名单），与内容脱敏解耦——
// 即使 sanitize=false 也照常执行（同 normalizeRoles）。
package upstream

import (
	"log"
	"strings"
)

// normalizeImageParts 归一化 messages[].content 里的多模态 part。
// 返回是否有任何改动（无改动时调用方零操作，普通纯文本请求零开销）。
func normalizeImageParts(obj map[string]any) bool {
	msgs, ok := obj["messages"].([]any)
	if !ok {
		return false
	}
	changed := false
	for i, m := range msgs {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		switch c := msg["content"].(type) {
		case []any:
			for j, p := range c {
				part, ok := p.(map[string]any)
				if !ok {
					continue
				}
				if np, ch := normalizeImagePart(part); ch {
					c[j] = np
					changed = true
					log.Printf("image part normalized msg=%d part=%d", i, j)
				}
			}
		case map[string]any:
			// 单 part 形态（content 直接是对象而非数组）：上游接受该容器，
			// 但容器内的 part 结构同样受白名单约束，一并翻译。
			if np, ch := normalizeImagePart(c); ch {
				msg["content"] = np
				changed = true
				log.Printf("image part normalized msg=%d part=single", i)
			}
		}
	}
	return changed
}

// normalizeImagePart 归一化单个 content part。返回 (part, 是否有改动)。
// 保持「无改动即原样返回」语义，避免给上游制造无谓的 body 差异。
func normalizeImagePart(part map[string]any) (map[string]any, bool) {
	typ, _ := part["type"].(string)
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "text":
		return part, false
	case "image_url":
		return normalizeImageURL(part)
	case "image":
		// Anthropic 方言。
		return anthropicImageToURL(part)
	case "input_image":
		// Responses API / Codex 方言。
		return inputImageToURL(part)
	case "":
		// 缺 type：上游要求必填（missing type field 整单 400）。按携带的字段推断，
		// 只在无歧义时补——推断不出就原样保留，交给上游报错。
		// 注意：补 type 本身就是改动，即使 part 内部结构无需再翻译（如已是
		// {"image_url":{"url":...}} 的标准对象形态）也必须返回 true。
		switch {
		case part["source"] != nil:
			return anthropicImageToURL(part)
		case part["image_url"] != nil:
			part["type"] = "image_url"
			normalizeImageURL(part)
			return part, true
		case part["text"] != nil:
			part["type"] = "text"
			return part, true
		}
	}
	return part, false
}

// normalizeImageURL 修正 image_url part：
//   - {"image_url":"<字符串>"} → {"image_url":{"url":"<字符串>"}}
//     （上游要求 image_url 是对象，字符串形态报 invalid image_url content）
//   - data URL 内 base64 payload 的空白清理（11135 成因，见 cleanDataURL）
func normalizeImageURL(part map[string]any) (map[string]any, bool) {
	switch u := part["image_url"].(type) {
	case string:
		if strings.TrimSpace(u) == "" {
			return part, false
		}
		part["image_url"] = map[string]any{"url": cleanDataURL(u)}
		return part, true
	case map[string]any:
		url, _ := u["url"].(string)
		if url == "" {
			return part, false
		}
		if cleaned := cleanDataURL(url); cleaned != url {
			u["url"] = cleaned
			return part, true
		}
	}
	return part, false
}

// anthropicImageToURL 把 Anthropic 的 {"type":"image","source":{...}} 翻译成
// 上游认的 image_url part：
//   - source.type=base64 → data:{media_type};base64,{data}
//   - source.type=url    → source.url 直接使用
//
// source 结构无法识别（非对象/未知 type/缺 data）时原样返回——不做猜测性翻译。
func anthropicImageToURL(part map[string]any) (map[string]any, bool) {
	src, ok := part["source"].(map[string]any)
	if !ok {
		return part, false
	}
	styp, _ := src["type"].(string)
	var url string
	switch strings.ToLower(strings.TrimSpace(styp)) {
	case "base64":
		data, _ := src["data"].(string)
		if strings.TrimSpace(data) == "" {
			return part, false
		}
		mt, _ := src["media_type"].(string)
		if strings.TrimSpace(mt) == "" {
			// Anthropic 协议里 media_type 必填；真缺失说明客户端畸形，按最常见
			// 图片类型兜底（上游只看 data URL 的声明，仍是同一份字节）。
			mt = "image/png"
		}
		url = "data:" + mt + ";base64," + cleanBase64(data)
	case "url":
		u, _ := src["url"].(string)
		if strings.TrimSpace(u) == "" {
			return part, false
		}
		url = cleanDataURL(u)
	default:
		return part, false
	}
	part["type"] = "image_url"
	delete(part, "source")
	part["image_url"] = map[string]any{"url": url}
	return part, true
}

// inputImageToURL 把 Responses API（Codex）的 input_image part 翻译成 image_url：
//   - {"type":"input_image","image_url":"<字符串>"}
//   - {"type":"input_image","image_url":{"url":...}}
//
// file_id 形态（需另调文件上传接口取字节）无法在此翻译 → 原样返回（不编造 URL）。
func inputImageToURL(part map[string]any) (map[string]any, bool) {
	switch u := part["image_url"].(type) {
	case string:
		if strings.TrimSpace(u) == "" {
			return part, false
		}
		part["type"] = "image_url"
		part["image_url"] = map[string]any{"url": cleanDataURL(u)}
		return part, true
	case map[string]any:
		url, _ := u["url"].(string)
		if url == "" {
			return part, false
		}
		u["url"] = cleanDataURL(url)
		part["type"] = "image_url"
		return part, true
	}
	return part, false
}

// cleanDataURL 清理 data URL 里 base64 payload 的空白字符。
//
// 成因（实测 2026-09-19）：上游把 ";base64," 之后的内容原样送解码器，payload 里
// 哪怕只有一个换行（部分客户端为可读性折行输出 base64）都会解码失败，返回
// HTTP 400 code=11135 "Please start a new conversation, replace the image, and try again."
// —— 文案指向「换张图/换会话」，与真实原因（一个换行）完全无关，用户会白折腾。
// 去掉空白后同一张图立刻正常。非 data URL 原样返回（不动普通 http(s) 图片链接）。
func cleanDataURL(raw string) string {
	const marker = ";base64,"
	i := strings.Index(raw, marker)
	if i < 0 {
		return raw
	}
	head, payload := raw[:i+len(marker)], raw[i+len(marker):]
	cleaned := cleanBase64(payload)
	if cleaned == payload {
		return raw
	}
	return head + cleaned
}

// cleanBase64 去掉 base64 串里的空白（空格/换行/回车/制表）。
// 无空白时原样返回（热路径零分配）。
func cleanBase64(s string) string {
	if !strings.ContainsAny(s, " \n\r\t") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case ' ', '\n', '\r', '\t':
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
