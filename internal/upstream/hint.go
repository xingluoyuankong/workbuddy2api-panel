// hint.go 网关错误附加说明字段（error.gateway_hint）的单一事实来源。
//
// 纪律（任务书 gateway-hint）：
//   - error.message 永远是上游 body 原文透传（5755fe3 透传原则不动）；
//     gateway_hint 只做与 message **并列**的网关视角补充说明，绝不替换/包装 message。
//   - 文案集中在本文件（一张 Kind 表 + 11133/11135 形态判定），按 ErrKind + 上下文
//     （请求带图/模型目录能力）映射，不散落 handler 的 if-else。
//   - 未覆盖形态返回空串 → 响应不带该字段（不编造）。
//   - hint 措辞是英文（错误响应面向客户端工具链，英文是通用口径）。
package upstream

import (
	"encoding/json"
	"net/http"
	"strings"
)

// GatewayHint 按错误形态返回网关视角的补充说明（error.gateway_hint 字段值）。
// msg 是上游错误 body 原文（或 SSE error 帧 payload）；kind 是权威分类
// （Classify / *Error 信封）。返回空串 = 未覆盖形态，调用方不带字段。
//
// ctx 携带判定 hint 所需的请求侧上下文（零值合法，信息缺失时相关形态退为中性
// hint 或无 hint）：
//   - HasImage：请求体是否携带 image_url part（11133 的「模型不支持图片」指向前提）；
//   - Model / ModelInCatalog / ModelSupportsImages：模型目录对该模型的
//     supports_images 声明（目录未收录 → 不做「不支持」判定，防查不到误判成不支持）。
//
// 判定次序：11133/11135 上游业务码**先于** Kind 表——实测这两族归 ErrClient/
// ErrBadParams 皆有可能（Classify 词表不含 11133），hint 层自带判定（hint 是补充
// 说明非权威分类，误判代价只是多一条中性补充说明）；其余走 Kind 一对一映射。
func GatewayHint(kind ErrKind, msg string, ctx HintContext) string {
	// 11133 model_param_invalid 家族（图片回归实测：不支持图片的模型传图，或任意
	// 参数被模型供应商拒绝）。只有请求确实带图、且目录能对该模型做出「不支持图片」
	// 的判定时才给「换模型」指向，否则退中性参数形态（可能是任意参数问题，不点名图片）。
	// 上游排队（6020 queue.waiting）判在 Kind 表之前：它与普通限流同 Kind
	// （HTTP 429），但处置完全不同——不是账号/额度问题，是该模型在排大队，
	// 换模型立刻可用（上游原文也写着"您也可以切换其他模型继续使用"）。
	// 若不单独给 hint，客户端只会看到笼统的「rate limited」而盲目重试。
	if IsQueueWait(msg) {
		return "this model is queued at the upstream right now (queue.waiting, queue_size 999+); switch to another model — retrying the same model will not help"
	}
	if isModelParamInvalid(msg) {
		if ctx.HasImage && ctx.ModelInCatalog && !ctx.ModelSupportsImages {
			return "model " + ctx.Model + " does not support images; pick one with supports_images=true from /v1/models"
		}
		return "request parameters were rejected by the model provider; check message format and model capabilities"
	}
	// 11135 invalid_image_data 家族（图片数据无效，Discussion #77 实测形态）。
	if isInvalidImageData(msg) {
		return "image data rejected by upstream; use a real/valid image, may need a new conversation"
	}
	switch kind {
	case ErrPromptTooLong:
		return "request context exceeds the model's limit; reduce history/message size"
	case ErrWafBlock:
		if IsWafBlockPage(msg) {
			// 内容级拦截（WAF 拦截页形态）：与账号/出口 IP 无关，「等窗口过去再试」
			// 是错误建议——同一个 body 再打一百次还是被拦。给可操作指引。
			return "content-level WAF block: the request body matches attack-signature rules (SQL injection / path traversal / XSS / command execution); the gateway already retried this model on the cn realm — if you still see this, switch to cn:<model> explicitly or remove raw payloads from the conversation"
		}
		// 账号级 WAF 403 与 IP 级 fail-fast 同 hint：两者对客户端的动作一致
		// （等待窗口过去再试，换号/立刻重试无意义）。
		return "upstream WAF blocked the gateway; retry after the block window"
	case ErrSoftRate:
		return "rate limited by upstream; retry after reset"
	case ErrAccountFault:
		return "account-level fault at upstream (auth/quota state); the gateway will rotate or disable this account"
	case ErrSessionDead:
		return "account session expired at upstream; the account is disabled until re-login"
	case ErrHardCredit:
		return "account credits exhausted at upstream; waiting for daily check-in to restore"
	case ErrModelBlocked:
		return "upstream has no such model on this backend; switch model or retry on another account"
	case ErrBadParams:
		// 请求体被上游解析层拒绝（11101 Unmarshal/Parse message failed、
		// 11128 first message is not system prompt 家族）。网关侧已做多模态
		// 方言归一化（image.go）/ 首条 system 兜底（ensureLeadingSystem）/
		// role 归一化（developer→system），仍到这里说明是覆盖不到的形态。
		// 原文已在 message 里透传，hint 只补一条网关视角的结论：这类拒绝是
		// body 层面的，与账号健康无关（对应 applyErrorPolicy 的不罚号口径）。
		return "upstream rejected this request body; check the message/content format (body-level rejection is independent of account health)"
	case ErrContentBlocked:
		// 措辞不含 "upstream"：content_blocked 响应有不含上游字样的既有口径
		// （handler_test 的泄漏守卫），hint 遵守同一口径。
		return "request content was rejected by content policy; adjust the prompt and retry"
	default:
		// ErrNone/ErrNotFound/ErrServer/ErrBadParams/ErrClient 等未覆盖形态：无 hint。
		return ""
	}
}

// HintContext gateway_hint 判定所需的请求侧上下文（handler 侧组装，见
// Handler.hintContext）。零值合法。
type HintContext struct {
	Model               string // 请求裸模型名（可空）
	HasImage            bool   // 请求体是否携带 image_url part
	ModelSupportsImages bool   // 模型目录 supports_images 声明（仅 ModelInCatalog 时有意义）
	ModelInCatalog      bool   // 模型目录是否收录该模型（「不支持」判定的前提）
}

// noHealthyHint 本地调度类错误（池中无健康号可用/传输层抖动，无上游原文可透传）
// 的固定 hint。不进 GatewayHint：它没有 ErrKind，是网关自己的调度事实。
const noHealthyHint = "no healthy account available in pool; check /status or retry later"

// IsWafBlockPage 报告 403 body 是否为「WAF 拦截页」（HTML 形态）。
//
// 与 IsWafBlocked 的分工：后者判「403 且无业务信封」（含空体/纯文本，用于分类）；
// 本函数专判可识别的 HTML 拦截页，用于**错误文案**——几 KB 的 HTML 透传给客户端
// 毫无信息量（用户只看到一坨标签，甚至以为网关挂了），要换成可操作摘要。
//
// 判定依据（2026-09 实测，www.workbuddy.ai 前置腾讯云 WAF）：
// 标题 "WAF Block Page" + "Tencent Cloud WAF" 字样 + HTML 结构。
func IsWafBlockPage(body string) bool {
	if !strings.Contains(body, "<html") {
		return false
	}
	return strings.Contains(body, "WAF Block Page") || strings.Contains(body, "Tencent Cloud WAF")
}

// WafRequestUUID 从 WAF 拦截页里抽出 Request UUID（用户拿它去 WAF 控制台提工单）。
// 无该字段 / 非该形态 → 空串（不编造）。
func WafRequestUUID(body string) string {
	const marker = `<span id="uuid">`
	i := strings.Index(body, marker)
	if i < 0 {
		return ""
	}
	rest := body[i+len(marker):]
	j := strings.IndexByte(rest, '<')
	if j < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:j])
}

// NoHealthyAccountHint 本地调度错误的 gateway_hint（与 no_healthy_account code 配套）。
func NoHealthyAccountHint() string { return noHealthyHint }

// FrameHintFunc 返回 SSE error 帧的 gateway_hint 判定函数（Stream 的可选参数）。
// ctxFn 惰性求值：仅在实际撞到 error 帧才调用（正常流零开销，模型目录查询
// 不会为每个成功请求触发）。
func FrameHintFunc(ctxFn func() HintContext) func(string) string {
	return func(payload string) string {
		if payload == "" || payload == "[DONE]" {
			return ""
		}
		return GatewayHint(FrameKind(payload), payload, ctxFn())
	}
}

// FrameKind 从 SSE error 帧 payload 判定 ErrKind：6004 模型级限流的流式形态
// （IsModelRateLimit 对帧 JSON 直接命中）优先；其余取帧内 error.message 走
// Classify（请求级 400 口径）。判不出 → ErrNone（无 hint）。
func FrameKind(payload string) ErrKind {
	if IsModelRateLimit(payload) {
		return ErrSoftRate
	}
	var f struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(payload), &f) != nil || f.Error.Message == "" {
		return ErrNone
	}
	return Classify(http.StatusBadRequest, f.Error.Message)
}

// isModelParamInvalid 上游 11133 body 判定（code 11133 / extError.code=
// model_param_invalid / msg 文案家族）。子串口径：hint 是补充说明非权威分类，
// 宁宽勿漏。
func isModelParamInvalid(body string) bool {
	lower := strings.ToLower(body)
	return codeMarker(lower, "11133") ||
		strings.Contains(lower, "model_param_invalid") ||
		strings.Contains(lower, "invalid request parameters") ||
		strings.Contains(lower, "request parameters do not meet the current model requirements")
}

// isInvalidImageData 上游 11135 body 判定（code 11135 / invalid_image_data /
// "replace the image" msg 家族）。
func isInvalidImageData(body string) bool {
	lower := strings.ToLower(body)
	return codeMarker(lower, "11135") ||
		strings.Contains(lower, "invalid_image_data") ||
		strings.Contains(lower, "replace the image")
}

// codeMarker JSON code 字段命中（`"code":N` / `"code": N` / `"code":"N"` 形态，
// 与 IsModelBlocked 的 code 判定同容差口径）。lower 须为小写 body。
func codeMarker(lower, code string) bool {
	for _, v := range []string{`"code":` + code, `"code": ` + code, `"code":"` + code + `"`, `"code": "` + code + `"`, `"code":" ` + code + `"`, `"code": '` + code + `'`} {
		if strings.Contains(lower, v) {
			return true
		}
	}
	return false
}
