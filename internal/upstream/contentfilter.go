// contentfilter.go 识别上游「内容审核空流」（HTTP 200 + finish_reason=content_filter + 空正文）。
//
// 背景（2026-09 实测，global 域）：
//
//	带输入审核的模型（如 global 的 deep-model）会把整段 system 提示词判为违规，
//	但**不报 HTTP 错误**——它返回一个只有 2 帧的 SSE：
//
//	  data: {... "delta":{"role":"assistant","content":"","reasoning_content":"",...},
//	         "finish_reason":""}                      ← 全空占位帧
//	  data: {... 同上全空 delta, "finish_reason":"content_filter",
//	         "usage":{"completion_tokens":0,"credit":1.02,...}}   ← 审核终止帧
//	  data: [DONE]
//
//	网关此前把它当「成功 200」原样透传 → 客户端拿到的是空回复（tok=0），
//	既没有错误也没有任何线索，表现为「global 的模型调不通/没反应」。
//
// 本文件只负责**识别**：把该形态归入内容拦截家族（ErrContentBlocked），
// 交由 handler 决定「换中性提示词在本请求内重试一次」还是「回明确错误」。
//
// 纪律：
//   - 只读不写：嗅探期间读到的字节全部回放（未命中时对外逐字节等价）；
//   - 有界：超过帧数/字节预算即放弃判定并放行（绝不把嗅探变成阻塞点）；
//   - 命中条件很窄：必须出现 finish_reason == "content_filter" 且此前没有任何
//     正文/推理/工具调用片段。已吐出部分内容的流**永不**判命中（保护残答）。
package upstream

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

const (
	// sniffMaxFrames / sniffMaxBytes 嗅探预算：任一超限即放弃判定、原样放行。
	// 审核空流在第 2 帧就带 finish_reason=content_filter，预算绰绰有余；
	// 正常流首帧带 role、次帧即带 reasoning/content（一般第 1 帧就放行），
	// 因此对健康请求几乎不增加任何缓冲延迟。
	sniffMaxFrames = 8
	sniffMaxBytes  = 16 << 10
)

// contentFilteredFinish 上游用于表达「内容审核终止」的 finish_reason 取值。
const contentFilteredFinish = "content_filter"

// ContentFilteredBody 内容审核空流对客户端透出的 body（error-passthrough 口径：
// handler 原样作为 error.message，客户端据此可判断「是模型审核拦了，不是网关挂了」）。
const ContentFilteredBody = `{"code":"content_filtered","message":"content moderation terminated the completion (finish_reason=content_filter) with no output; the model rejected the request content — typically the system prompt. Switch model or use a neutral system prompt."}`

// 嗅探判定结果。
type chunkVerdict int

const (
	chunkUnknown  chunkVerdict = iota // 无信息（占位帧/解析失败）→ 继续嗅探
	chunkHasOutput                    // 已出现正文/推理/工具调用 → 正常流，放行
	chunkFiltered                     // 出现 finish_reason=content_filter → 内容审核空流
)

// sniffContentFilter 嗅探上游 200 SSE 流开头，判定是否为「内容审核空流」。
//
// 返回 (可继续读取的 body, 是否命中)：
//   - 命中：第二个返回值为 true，第一个返回值为 nil（调用方应关闭原 body 并按内容拦截处理）；
//   - 未命中：第一个返回值是**回放式** body（嗅探期读到的字节原样先出，之后接底层流），
//     与直接使用原 body 逐字节等价。
func sniffContentFilter(rc io.ReadCloser) (io.ReadCloser, bool) {
	br := bufio.NewReaderSize(rc, 64*1024)
	var head bytes.Buffer
	frames := 0
	for {
		if head.Len() > sniffMaxBytes || frames >= sniffMaxFrames {
			return replayBody(head.Bytes(), br, rc), false
		}
		line, err := br.ReadString('\n')
		if line != "" {
			head.WriteString(line)
			if payload, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "data: "); ok {
				payload = strings.TrimSpace(payload)
				if payload != "" && payload != "[DONE]" {
					frames++
					switch classifyStreamChunk(payload) {
					case chunkHasOutput:
						return replayBody(head.Bytes(), br, rc), false
					case chunkFiltered:
						return nil, true
					}
				}
			}
		}
		if err != nil {
			// EOF / 读错误：无 finish_reason=content_filter 证据 → 放行，
			// 由下游（Stream / Aggregate）按其既有语义处理。
			return replayBody(head.Bytes(), br, rc), false
		}
	}
}

// classifyStreamChunk 判定单个 SSE data 帧的性质。
//
// 命中 chunkFiltered 的门槛刻意窄：只有 finish_reason 恰好是 content_filter
// 才判命中——上游其他终止原因（stop/length/tool_calls/"") 一律不判，
// 避免把「合法的空回答」误升级成错误。
func classifyStreamChunk(payload string) chunkVerdict {
	var obj struct {
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Refusal          string `json:"refusal"`
				ToolCalls        []any  `json:"tool_calls"`
				FunctionCall     *struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function_call"`
			} `json:"delta"`
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		return chunkUnknown
	}
	for _, ch := range obj.Choices {
		if strings.TrimSpace(ch.Delta.Content) != "" ||
			strings.TrimSpace(ch.Delta.ReasoningContent) != "" ||
			strings.TrimSpace(ch.Delta.Refusal) != "" ||
			strings.TrimSpace(ch.Message.Content) != "" ||
			len(ch.Delta.ToolCalls) > 0 ||
			(ch.Delta.FunctionCall != nil &&
				(ch.Delta.FunctionCall.Name != "" || ch.Delta.FunctionCall.Arguments != "")) {
			return chunkHasOutput
		}
		if ch.FinishReason == contentFilteredFinish {
			return chunkFiltered
		}
	}
	return chunkUnknown
}

// replayReadCloser 把「嗅探期已读字节」拼回底层流前，实现 io.ReadCloser。
type replayReadCloser struct {
	r  io.Reader
	rc io.ReadCloser
}

func replayBody(head []byte, br *bufio.Reader, rc io.ReadCloser) io.ReadCloser {
	return &replayReadCloser{r: io.MultiReader(bytes.NewReader(head), br), rc: rc}
}

func (x *replayReadCloser) Read(p []byte) (int, error) { return x.r.Read(p) }

func (x *replayReadCloser) Close() error { return x.rc.Close() }
