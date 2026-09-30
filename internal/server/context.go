package server

import (
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/linguo2625469/workbuddy2api-panel/internal/modelmeta"
)

// context.go 上下文窗口档位的手动覆盖与出站回调。
//
// 背景：上游目录下发的 maxInputTokens 常常与模型实际能吃的上下文不一致
// （目录口径 vs 实际口径），而客户端（Codex / ZCode / Cherry Studio）严格按
// /v1/models 的 context_length 决定塞多少历史——目录值偏大会让请求在上游
// 被静默截断或直接 400；偏小又白白丢上下文。
//
// 因此允许在面板按模型手动指定上下文窗口档位，并双重生效：
//  1. /v1/models 的 context_length 用人工值覆盖（客户端据此自行裁剪）；
//  2. 出站前按同一档位裁剪历史消息（兜底：客户端不裁剪时网关也不会超发），
//     并在发生裁剪时注入一行提示词，让模型知道上文被截断（避免凭空续写）。

// estimateTokens 粗估文本 token 数（不引第三方分词器，够裁剪用即可）：
// ASCII 按 4 字节/token，CJK 等非 ASCII 按 1.5 字符/token（中文实际约 1 字 1~2 token，
// 取偏保守值，宁可少塞也别超发）。
func estimateTokens(s string) int {
	if s == "" {
		return 0
	}
	ascii, other := 0, 0
	for _, r := range s {
		if r < unicode.MaxASCII {
			ascii++
		} else {
			other++
		}
	}
	// 未解码成功的字节按非 ASCII 计入（避免畸形输入被估成 0）。
	if extra := utf8.RuneCountInString(s); extra == 0 && len(s) > 0 {
		other += len(s)
	}
	return ascii/4 + other*3/2 + len(s)/64 // 末项：JSON 结构与字段名开销的粗补偿
}

// messageTokens 估一条消息（含内容、role、tool_calls 的 JSON 展开）的 token 数。
func messageTokens(m any) int {
	b, err := json.Marshal(m)
	if err != nil {
		return 0
	}
	return estimateTokens(string(b))
}

// contextTrimNote 历史被裁剪时注入的提示词（system 角色，追加到最后一条 system 之后）。
const contextTrimNote = "[上下文窗口提示] 由于上下文窗口档位限制，本轮请求已省略较早的对话历史。" +
	"请基于当前可见内容作答；若关键信息缺失，直接向用户确认，不要臆测被省略部分的内容。"

// trimToContextBudget 按 token 预算裁剪出站消息数组。
//
// 规则（保持 OpenAI 语义不破）：
//   - 头部连续若干条 system 消息恒定保留（角色定义与工具说明不能丢）；
//   - 最后一条消息恒定保留（当前轮的用户输入，丢了等于丢请求）；
//   - 其余消息从新到旧回填，直到预算耗尽；
//   - tool_calls / tool 消息成对性由 payload.go 的 repack/cleanup 二次兜底，
//     这里裁掉的孤儿会由那一步清掉，不会打出 400 的不成对请求。
//
// 返回裁剪后的 body（未发生裁剪时原样返回）与被丢弃的消息条数。
func trimToContextBudget(body []byte, budget int64) ([]byte, int) {
	if len(body) == 0 || budget <= 0 {
		return body, 0
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body, 0
	}
	msgs, ok := obj["messages"].([]any)
	if !ok || len(msgs) == 0 {
		return body, 0
	}
	// 已在上限内 → 零操作（不重排、不改写，避免无谓的 body 变动）。
	if totalMessagesTokens(msgs) <= budget {
		return body, 0
	}

	// 1) 头部 system 段（可能多条）保留。
	head := 0
	for head < len(msgs) && msgRole(msgs[head]) == "system" {
		head++
	}
	// system 段本身就超预算 → 只能保留 system + 最后一条，其他全丢。
	used := int64(0)
	keep := make([]bool, len(msgs))
	for i := 0; i < head; i++ {
		keep[i] = true
		used += int64(messageTokens(msgs[i]))
	}
	// 2) 最后一条恒定保留。
	last := len(msgs) - 1
	if last >= head {
		keep[last] = true
		used += int64(messageTokens(msgs[last]))
	}
	// 3) 其余从新到旧回填。
	for i := last - 1; i >= head; i-- {
		if keep[i] {
			continue
		}
		t := int64(messageTokens(msgs[i]))
		if used+t > budget {
			break
		}
		keep[i] = true
		used += t
	}
	dropped := 0
	out := make([]any, 0, len(msgs))
	for i := range msgs {
		if keep[i] {
			out = append(out, msgs[i])
		} else {
			dropped++
		}
	}
	if dropped == 0 {
		return body, 0
	}
	// 4) 注入裁剪提示（放在 system 段末尾；无 system 时新建一条置顶）。
	note := map[string]any{"role": "system", "content": contextTrimNote}
	if head > 0 {
		merged := make([]any, 0, len(out)+1)
		merged = append(merged, out[:head]...)
		merged = append(merged, note)
		merged = append(merged, out[head:]...)
		out = merged
	} else {
		out = append([]any{note}, out...)
	}
	obj["messages"] = out
	b, err := json.Marshal(obj)
	if err != nil {
		return body, 0
	}
	return b, dropped
}

func totalMessagesTokens(msgs []any) int64 {
	var t int64
	for _, m := range msgs {
		t += int64(messageTokens(m))
	}
	return t
}

// msgRole 取消息的 role 字段（缺失/非字符串 → 空串）。
func msgRole(m any) string {
	mm, ok := m.(map[string]any)
	if !ok {
		return ""
	}
	r, _ := mm["role"].(string)
	return strings.ToLower(strings.TrimSpace(r))
}

// contextTierOptions 面板可选的上下文窗口档位（定义在 modelmeta，与面板共用同一份）。
var contextTierOptions = modelmeta.ContextTiers

// formatContextTier 档位可读文本（0 → "跟随上游"）。
func formatContextTier(n int64) string { return modelmeta.FormatContextTier(n) }
