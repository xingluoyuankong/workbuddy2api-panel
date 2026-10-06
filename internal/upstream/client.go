// Package upstream 封装对 CodeBuddy 上游（chat / billing / auth）的全部 HTTP 调用，
// 以及错误分类（驱动 pool 冷却状态机）。
package upstream

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/logfmt"
)

// ErrKind 错误分类，pool 据此决定冷却时长。
type ErrKind int

const (
	ErrNone             ErrKind = iota // 成功
	ErrHardCredit                      // 余额不足（402 或 body 关键词）→ 长冷却
	ErrSoftRate                        // 429 软限流 → 短冷却
	ErrSessionDead                     // 401 + 12153 offline session 失效 → 禁用
	ErrNotFound                        // 404 上游偶发 → 短冷却，不累计错误计数（防雪崩）
	ErrServer                          // 5xx 上游故障
	ErrContentBlocked                  // 内容策略拦截（400 + 审核文案）→ 不罚账号，走降级重试
	ErrBadParams                       // 请求体解析失败（400 + Unmarshal chat params failed / 11101）→ 不罚账号，仍轮转
	ErrAccountFault                    // 账号级授权/配额故障（11140 request illegal / 14017 trial not activated）→ 冷却轮换，不无限重试
	ErrModelBlocked                    // 11102「该后端无此模型」→ (账号,模型) 负缓存避让，切模型/切账号
	ErrWafBlock                        // 403 + 非业务信封体（APISIX WAF 拦截页/空体）→ 账号软冷却 + 抖动退避
	ErrPromptTooLong                   // 11115「prompt is too long」→ 请求级错误（上下文超限是请求的问题非账号的问题）：不罚号、不轮转，末端透传原文
	ErrClient                          // 其他 4xx / 业务错误
	ErrKind520                         // 520 错误（Cloudflare 解析失败）→ 立即重试，0ms 退避
	ErrTransport                       // 通用传输错误 → 正常退避（500ms·2^i 封顶 8s）
	ErrTransportTimeout                // 传输层超时 → 重退避（1s·2^i 封顶 16s）
	ErrTransportEOF                    // 传输层 EOF → 重退避（1s·2^i 封顶 16s）
)

func (k ErrKind) String() string {
	switch k {
	case ErrHardCredit:
		return "hard_credit"
	case ErrSoftRate:
		return "soft_rate"
	case ErrSessionDead:
		return "session_dead"
	case ErrNotFound:
		return "not_found"
	case ErrServer:
		return "server"
	case ErrContentBlocked:
		return "content_blocked"
	case ErrBadParams:
		return "bad_params"
	case ErrModelBlocked:
		return "model_blocked"
	case ErrWafBlock:
		return "waf_block"
	case ErrPromptTooLong:
		return "prompt_too_long"
	case ErrAccountFault:
		return "account_fault"
	case ErrClient:
		return "client"
	case ErrKind520:
		return "520"
	case ErrTransport:
		return "transport"
	case ErrTransportTimeout:
		return "transport_timeout"
	case ErrTransportEOF:
		return "transport_eof"
	default:
		return "none"
	}
}

// Error 带分类的上游错误。
type Error struct {
	Kind   ErrKind
	Status int
	Msg    string
	// RetryAfter 上游明示的等待时长（Retry-After 秒 / retry-after-ms /
	// x-ratelimit-reset 头解析，见 ParseRetryAfter）。零值 = 上游未明示，
	// 冷却时长回落调用方计算值。挂载点选在 Error 信封：Kind 决定「罚不罚」，
	// RetryAfter 决定「罚多久」，同为上游响应的一等公民。
	RetryAfter time.Duration

	// ContentFilter 标记本次 ErrContentBlocked 来自上游「内容审核空流」
	// （HTTP 200 + finish_reason=content_filter + 空正文，见 contentfilter.go），
	// 而不是 HTTP 400 的内容策略拦截。handler 据此区分处置：
	//   - 400 形态：既有的 system 指纹误报路径（仅 passthrough 粘性降级）；
	//   - content_filter 形态：模型级输入审核（custom 模式也会撞），
	//     允许在**本请求内**用中性提示词重试一次（不触发粘性降级）。
	ContentFilter bool
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream %s (http %d): %s", e.Kind, e.Status, e.Msg)
}

// hardMarkers 余额不足关键词（小写比较 + 中文原文比较双通道）。
var hardMarkers = []string{
	"insufficient credit", "no credit", "credit exhausted", "credits exhausted", "out of credit",
	"quota exceeded", "quota exhaust", "payment required", "credit not enough",
	"not enough credit",
	"积分不足", "额度不足", "余额不足", "积分用完", "额度用尽", "没有积分",
}

// softRateMarkers 限流/节流关键词（小写比较 + 中文原文比较双通道）。
// 上游在状态码非 429 时也会返回限流语义（如 200 + code 11140
// "The model provider is rate-limiting requests."、400 + "rate limit"），
// 此类响应若不识别，账号既不被冷却也不喂熔断，下次请求仍会被选中（issue #28）。
//
// 词表按子串匹配，宁缺毋滥：只收录明确指向「请求速率/模型用量被节流」的措辞。
// 连字符形式（rate-limiting / rate-limited）需单列——Contains 不跨 '-'。
// "too many" 会命中 "too many tokens" 这类客户端参数错误，代价是该号被软冷却
// 一个 SoftCooldown（默认 60s）后自愈，远小于漏判限流导致反复选中同一号的代价。
var softRateMarkers = []string{
	"rate limit", // rate limit / rate limits / rate limiting
	"rate-limiting",
	"rate-limited",
	"too many requests",
	"too many",
	"usage limit", // usage limit reached / model usage limit exceeded（用量节流，非计费余额）
	"请求过于频繁", "限流",
}

var sessionDeadMarkers = []string{"Offline user session not found", "12153"}

// accountFaultMarkers 账号级授权/配额故障关键词（大小写不敏感子串匹配）。
//
// 定位：这类错误是**账号本身状态**决定的本机故障，不是请求格式、不是临时限流、
// 也不是内容误报——继续重试只会反复刷上游风控/配额检查，必须把该账号冷却轮换。
//   - "request illegal"（code 11140）→ 上游 auth/auth_forbidden，账号级授权风控，
//     需重新 OAuth 登录才能恢复，短冷却只能阻止继续送死。
//   - code 14017（"trial not activated" / "The trial version is not yet activated"）→
//     上游 quota/quota_not_activated，register 未完成的试用未激活账号，同样账号级。
//
// 注意 11140 **不能**按 code 判定：该 code 也承载模型级限流文案（"The model provider
// is rate-limiting requests."），那种场景必须保持 ErrSoftRate（下方 softRateMarkers
// 后判定）。故此处只收 msg 关键词 "request illegal"（auth_forbidden 的真实文案）。
// 14017 文案唯一（无软限流歧义），可安全收录。
var accountFaultMarkers = []string{
	"request illegal",
	"trial not activated",
	"trial version is not yet activated",
}

// contentBlockedMarkers 内容策略拦截关键词（大小写不敏感子串匹配）。
//
// 定位：上游按逐字精确指纹审核，system 来源的模板句（如 Claude Code/Codex
// 注入指令）触发 HTTP 400 + 以下文案。这是「误报」（合法流量被审核误杀），
// 非账号问题——该账号余额健康、未限流、session 未死，故 ErrContentBlocked
// 在 applyErrorPolicy 中不罚账号（无冷却/熔断/NoteError），改由网关降级重试。
var contentBlockedMarkers = []string{
	"blocked by security policy",
	"unapproved channel",
	"illegal api invocation",
}

// badParamsMarkers 请求体被上游**解析层**拒绝的关键词（HTTP 400 家族）。
//
// 实测三类（2026-09-19 直连上游，stream:true）：
//   - "Unmarshal chat params failed"（11101）：body JSON 解析失败（issue #41）；
//   - "Parse message failed"（11101）：content/part 结构不在白名单（未知 content
//     type / image_url 不是对象 / part 缺 type）；网关侧已做方言归一化
//     （见 image.go），此处是归一化覆盖不到的残余形态兜底；
//   - "first message is not system prompt"（11128）：messages[0] 不是 system，
//     网关侧已由 ensureLeadingSystem 兜底，此 marker 防漏网。
//
// 三者的共同点：**与账号健康完全无关** —— 同一份 body 换任何账号都被同样拒绝。
// 因此归 ErrBadParams（不冷却/不熔断/不喂连败），handler 侧直接 400 透传上游原文
// 并**终止轮转**（换号只会白烧健康号配额）。
var badParamsMarkers = []string{
	"Unmarshal chat params failed",
	"Parse message failed",
	"first message is not system prompt",
}

// badParamsCodes 与上同源的 code 字段形态（codeMarker 判定，容差口径统一）。
var badParamsCodes = []string{"11101", "11128"}

// badParamsBody 判定上游 400 body 是否属于「请求体本身被拒」（与账号无关）。
// body 为上游原始响应体（原文，非小写）。
func badParamsBody(body string) bool {
	for _, m := range badParamsMarkers {
		if strings.Contains(body, m) {
			return true
		}
	}
	lower := strings.ToLower(body)
	for _, c := range badParamsCodes {
		if codeMarker(lower, c) {
			return true
		}
	}
	return false
}

// promptTooLongMarkers 11115「prompt is too long」判定。
// 定位：上下文超限是**请求的问题不是账号的问题**——同一个 body 换任何账号发都会
// 超限，与 WAF fail-fast 同哲学（确定与账号无关的错误不罚号不轮转，白白浪费健康号
// 的请求配额）。marker 双通道：
//   - `"code":11115`：业务信封 code 字段（JSON 空格容差；`"code":"11115"` 字符串
//     形态也命中）；
//   - "prompt is too long"：msg 文案（大小写不敏感）。
//
// 只在 400/404/413 请求级状态码上判（429+11115 概率极低且属限流语义优先，
// 5xx 属服务端故障优先）。误判代价（好 body 被归 prompt_too_long）：不罚号 +
// 不轮转 + 透传原文，客户端看到上游原文可自行排查，代价可控。
var promptTooLongMarkers = []string{
	`"code":11115`,
	`"code": 11115`,
	`"code":"11115"`,
	"prompt is too long",
}

// isPromptTooLongStatus 11115 只在请求级 4xx 上判（见 promptTooLongMarkers 注释）。
func isPromptTooLongStatus(status int) bool {
	return status == http.StatusBadRequest || status == http.StatusNotFound ||
		status == http.StatusRequestEntityTooLarge
}

// alreadyCheckinMarkers "今天已签到"关键词（上游对重复签到返回 code!=0，
// 实测 code=10001/14001 "今天已签到"/"今日已签到"）。只对 *Error.Msg 做包含匹配，
// 网络层/解析层错误不在此识别（见 IsAlreadyCheckin）。
var alreadyCheckinMarkers = []string{"已签到", "already"}

// softRateResetLoc 上游 429 6004 文案中的重置时间固定按 UTC+8 解释（上游文案如此，
// 与容器时区无关）。
var softRateResetLoc = time.FixedZone("UTC+8", 8*60*60)

// SoftRateResetLoc 暴露重置时间的固定时区（供测试构造/断言同一时区口径）。
func SoftRateResetLoc() *time.Location { return softRateResetLoc }

// modelRateLimitCode 明确指向「模型级 429 限流」的业务 code。
// 上游用它表达"该模型的使用量超限"（code 6004，msg 带「将在 … 重置」），
// 而不是账号整体被限流——账号健康，只是这个模型此刻被限（issue #31）。
const modelRateLimitCode = "6004"

// softRateResetPattern 匹配「将在 … 重置」，捕获中间的时间串。
const softRateResetPattern = `将在 (.+?) 重置`

// 限流判定正则预编译为包级 var：IsModelRateLimit / ParseRateReset 在每次错误
// 分类、每个限流 body 上调用，函数体内 MustCompile 是纯浪费；错误风暴（429
// 轰炸）时尤甚。模式串均为纯常量。regexp 并发安全（匹配只读），无需额外锁。
var (
	reModelRateLimit = regexp.MustCompile(`"code"\s*:\s*"?` + modelRateLimitCode + `"?`)
	reSoftRateReset  = regexp.MustCompile(softRateResetPattern)
)

// queueWaitCode 上游「排队等待」业务 code（实测 2026-09-22）。
//
//	{"code":6020,"msg":"queue.waiting.title","data":{"queue_position":2159,
//	 "queue_size":"999+","retry_after":40,"estimated_wait":10795,...}}
//
// 语义：**不是账号被限流**，是该模型此刻在排大队（位置 2000+），账号本身健康、
// 换模型立刻可用。必须按「模型级」而不是「账号级」处置——按账号级会让一个排队
// 模型把整个账号池依次打成 600s 冷却，池空之后连别的模型一起 503
// （2026-09-22 线上事故：483 次 429 + 146 次 uid=- 的 503，用户误以为"账号全废"）。
const queueWaitCode = "6020"

// queueWaitMarker 6020 的固定 msg（上游未本地化时原样透出的文案 key）。
const queueWaitMarker = "queue.waiting"

// reQueueWait code 字段形态判定（空格/字符串容差，口径同 reModelRateLimit）。
var reQueueWait = regexp.MustCompile(`"code"\s*:\s*"?` + queueWaitCode + `"?`)

// IsQueueWait 报告响应体是否为上游「排队等待」（6020 queue.waiting）。
//
// 双通道（code 6020 或 msg 含 queue.waiting）任一命中即算：宁宽勿漏——
// 漏判的代价是整个账号池被误按账号级冷却（见 queueWaitCode 注释的事故）。
func IsQueueWait(body string) bool {
	return reQueueWait.MatchString(body) || strings.Contains(body, queueWaitMarker)
}

// ParseQueueWait 解析 6020 body 的建议等待时长（data.retry_after，单位秒）。
// 缺失/非法/非正 → 0（冷却时长的夹取区间由策略层 handler 决定）。
func ParseQueueWait(body string) time.Duration {
	var v struct {
		Data struct {
			RetryAfter float64 `json:"retry_after"`
		} `json:"data"`
	}
	if json.Unmarshal([]byte(body), &v) != nil || v.Data.RetryAfter <= 0 {
		return 0
	}
	return time.Duration(v.Data.RetryAfter * float64(time.Second))
}

// softRateTimeLayout 上游重置时间的格式（无时区后缀；时区固定 UTC+8）。
const softRateTimeLayout = "2006-01-02 15:04:05"

// IsModelRateLimit 报告 429 body 是否明确指向模型级限流（业务 code 6004）。
// 用于区分"账号级软限流"（按账号冷却）与"模型级用量限流"（切模型即可用）。
func IsModelRateLimit(body string) bool {
	// `"code":6004` / `"code": 6004` / `"code":"6004"` 均可命中（JSON 空格容差）。
	return reModelRateLimit.MatchString(body)
}

// modelBlockCode 明确指向「该后端无此模型」的业务 code。
const modelBlockCode = "11102"

// modelBlockMsgMarker 11102 答复的确定性文案（官方 error message 固定短语）。
// 只收这个窄短语，不收 "model ... not found" 宽正则——后者会误伤其他业务的
// not found 措辞。
const modelBlockMsgMarker = "service info not found"

// ModelBlockReason 11102 负缓存条目在 pool.modelCooldowns 里的 reason 前缀。
// handler 写 BlockModelBackoff；pool.BlockModelClear 按 "11102" 前缀识别条目
// （与 6004 条目的 "6004 model rate limit" reason 互不干扰）。
const ModelBlockReason = "11102 model not available"

// IsModelBlocked 报告 body 是否是「该后端无此模型」(11102) 的确定性答复。
//
// 只比对 code/msg 等独立字段，绝不做整段文本子串匹配：错误体还带 requestId 等字段，
// 拿整段文本匹配会把 "11102" 撞在 ID 上、误避让一个本来能用的模型。判定 =
// code 字段精确等于 "11102"，或 msg/message 字段命中窄短语 "service info not
// found"（两者任一命中即真）。只看 400/404：429 带 11102 属限流语义。
// 字段遍历覆盖顶层与 error 子对象两层。
func IsModelBlocked(status int, body string) bool {
	if (status != http.StatusBadRequest && status != http.StatusNotFound) || body == "" {
		return false
	}
	// 轻量预检：body 既无 "11102" 又无 marker 时直接短路（大多数 4xx 零分配返回）。
	if !strings.Contains(body, modelBlockCode) && !strings.Contains(strings.ToLower(body), modelBlockMsgMarker) {
		return false
	}
	var root map[string]any
	if err := json.Unmarshal([]byte(body), &root); err != nil {
		return false
	}
	nodes := []map[string]any{root}
	if inner, ok := root["error"].(map[string]any); ok {
		nodes = append(nodes, inner)
	}
	code, msg := "", ""
	for _, node := range nodes {
		for _, key := range []string{"code", "errCode", "error_code"} {
			if v, ok := node[key]; ok && v != nil && code == "" {
				code = strings.TrimSpace(fmt.Sprint(v))
			}
		}
		for _, key := range []string{"msg", "message"} {
			if v, ok := node[key].(string); ok && v != "" && msg == "" {
				msg = strings.TrimSpace(v)
			}
		}
	}
	if code == modelBlockCode {
		return true
	}
	return strings.Contains(strings.ToLower(msg), modelBlockMsgMarker)
}

// hasBusinessEnvelope 报告错误 body 是否携带上游业务信封形态（JSON 且含
// `"code":` 或 `"msg":` 字段）。WAF 403 判定（IsWafBlocked）用「无业务信封」
// 区分 APISIX WAF 拦截页（HTML/空体/纯文本）与上游业务层 403（带 code/msg
// 信封，正常走既有分类）。JSON 解析不做：信封存在性只需字段名命中——
// 畸形 JSON 但含 `"msg":` 字样仍按业务响应保守处理（宁漏判 WAF 也不误罚
// 业务 403，后者有各自的权威分类）。
func hasBusinessEnvelope(body string) bool {
	return strings.Contains(body, `"code":`) || strings.Contains(body, `"msg":`)
}

// IsWafBlocked 报告 403 响应是否为 WAF 拦截形态：HTTP 403 且 body 无业务信封
// （无 `"code":`/`"msg":` JSON 字段——HTML 拦截页、空体、纯文本均命中）。
// 带业务信封的 403（11140 request illegal / 11128 等）仍走既有分类链。
// 403 含 accountFault 文案的维持现状（ErrAccountFault），由 Classify 的规则序保证。
func IsWafBlocked(status int, body string) bool {
	return status == http.StatusForbidden && !hasBusinessEnvelope(body)
}

// retryAfterHeaderCandidates 冷却时长优先解析的响应头候选序列：
// retry-after（秒，RFC 7231）/ retry-after-ms（毫秒）/ x-ratelimit-reset
// （epoch 秒或毫秒，取 now+ 剩余量）。大小写不敏感（http.Header.Get 已归一）。
var retryAfterHeaderCandidates = []string{"Retry-After", "Retry-After-Ms", "X-Ratelimit-Reset"}

// retryAfterSanity 解析结果的上限（超过视为上游异常值丢弃，回落本地计算），
// 与 pool 的 softRateMax 默认 2h 同量级。
const retryAfterSanity = 2 * time.Hour

// ParseRetryAfter 从限流/拦截响应头解析上游明示的等待时长：
// 依次尝试 Retry-After（整数秒）→ retry-after-ms（整数毫秒）→
// x-ratelimit-reset（纯数字按 epoch 秒/毫秒推断；HTTP-Date 形态不支持——
// 上游族实践发的是数字）。任一头缺失/非法/非正/超上限则尝试下一头；
// 全部不可用返回 false（调用方回落既有计算值，绝不臆造等待时长）。
func ParseRetryAfter(h http.Header) (time.Duration, bool) {
	for _, name := range retryAfterHeaderCandidates {
		v := strings.TrimSpace(h.Get(name))
		if v == "" {
			continue
		}
		if !isAllDigits(v) {
			continue // 非纯数字（如 HTTP-Date）不解析，宁缺毋滥
		}
		n, ok := parseRetryNumber(v, name)
		if !ok {
			continue
		}
		if n <= 0 || n > retryAfterSanity {
			continue // 非正/异常大：丢弃（回落本地计算）
		}
		return n, true
	}
	return 0, false
}

// isAllDigits 报告 s 是否为纯数字（前置快筛，免 strconv 之后再判语义）。
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// parseRetryNumber 按头名口径把纯数字串折算成时长。x-ratelimit-reset 是
// epoch 时刻而非时长：秒口径（10 位）与毫秒口径（13 位）都按「now+ 该时刻
// 的剩余量」折算，已在过去则不可用。位数不足（8 位以下）无法判定 epoch
// 语义的丢弃（宁缺毋滥：短串多半是序号之类的误用头）。
func parseRetryNumber(v, headerName string) (time.Duration, bool) {
	// 上限 16 位防 int64 溢出（超过 epoch 毫秒的现实量级必非法）。
	if len(v) > 16 {
		return 0, false
	}
	var n int64
	for _, r := range v {
		n = n*10 + int64(r-'0')
	}
	switch headerName {
	case "Retry-After":
		return time.Duration(n) * time.Second, true
	case "Retry-After-Ms":
		return time.Duration(n) * time.Millisecond, true
	default: // X-Ratelimit-Reset：epoch → 剩余量
		sec := n
		if len(v) >= 12 { // 毫秒口径（13 位）；11 位边界按秒（误判代价是多算 1000 倍）
			sec = n / 1000
		}
		remain := time.Until(time.Unix(sec, 0))
		return remain, true
	}
}

// ParseRateReset 从任何限流响应 body 里统一解析「将在 … 重置」时间（上游 UTC+8 文案）。
// 成功返回解析出的**墙钟时刻**（按 UTC+8 解释），失败返回零值 + false。
//
// 是否走模型级豁免、时日对齐到 until 还是 modelCooldowns，由冷却决策侧（pool）按
// IsModelRateLimit 判定，本函数只负责「把上游明说的恢复时刻抽出来」。没有时间文案
// 的限流也照常由调用方退回有界退避（绝不臆造时间）。
func ParseRateReset(body string) (time.Time, bool) {
	m := reSoftRateReset.FindStringSubmatch(body)
	if len(m) < 2 {
		return time.Time{}, false
	}
	ts := strings.TrimSpace(m[1])
	ts = strings.TrimSuffix(ts, " UTC+8") // 去掉后缀，固定按 softRateResetLoc 解释
	t, err := time.ParseInLocation(softRateTimeLayout, ts, softRateResetLoc)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// Classify 按 HTTP 状态码 + body 判定错误类别。
//
// 判定顺序自「严」到「宽」，每层的先后都有语义依据：
//  0. 11102（IsModelBlocked）——「该后端无此模型」确定性答复，语义最具体，最先判
//     （只认 400/404，429+11102 属限流语义走第 4 层）。
//  1. 402 —— 真正的计费余额耗尽状态码，最严、最不可自愈，最先判。
//  2. sessionDeadMarkers —— 需要人工重登的终态。若 401 body 同时含 "12153" 与
//     "rate limit"（如网关错误页混排），归 session_dead：短冷却救不活失效 session，
//     误判为限流会让该死号留在池中反复被选中；且此层 marker 是精确词（12153 等），
//     比限流层的大范围子串更具体，具体优先于宽泛。
//  3. accountFaultMarkers —— 账号级授权/配额故障（11140 request illegal auth 风控、
//     14017 trial not activated register 未完成）。必须先于 status==429 判定：
//     14017 常带 429 状态码，若落到 status==429 会误归 soft_rate（"限流"语义不符：
//     限流可指数退避等自愈，账号级故障等不来）。11140 的 model 级限流变体
//     （rate-limiting 文案）因 marker 不含该文案而天然落到 softRateMarkers 层，
//     不受影响。
//  4. status==429 —— 限流状态码兜底（先于 hardMarkers）：429 body 高频携带
//     "quota exceeded"/"额度不足" 等跨计费/限流两界的措辞，hardMarkers 先判会把
//     限流误归 ErrHardCredit 硬冷却到次日 04:00，白扔号约 12h。状态码是比关键词
//     更权威的信号；真正的余额耗尽由 402（第 1 层）捕获，非 429 状态码的 quota
//     措辞仍走下方 hardMarkers（第 5 层）。
//  5. hardMarkers —— 非 429 响应携带计费关键词（200 业务信封 / 403 信封等）。
//  6. softRateMarkers —— 非 429 状态码携带限流文案（issue #28 修复点）。
//     位于此处可覆盖 200/400/403/5xx 各状态码。
//  7. 11115 —— 「prompt is too long」请求级语义：判在 404/5xx 与通用 4xx 兜底
//     之前（404 上打 11115 若落 ErrNotFound 会误冷却账号——上下文超限与账号无关）。
//  8. 404 / 5xx —— 与限流无关的常规分类。
//  9. IsWafBlocked —— 403 且无业务信封（HTML 拦截页/空体/纯文本）：APISIX WAF
//     拦截形态。判在通用 4xx 兜底**之前**：此前该形态落 ErrClient → 只换号不罚 →
//     连环 403。带业务信封的 403 已被上方各层捕获，走不到本层。
//  10. 内容策略/参数错误/其他 4xx —— 通用兜底。
func Classify(status int, body string) ErrKind {
	// 11102「该后端无此模型」须最先判：它是「模型在后端不存在」的确定性答复，语义比
	// 计费/限流都更具体——若不先判，msg 里的 "service info not found" 会被更宽的
	// 4xx 兜底归为 ErrClient（只换号不避让），该坏号会留在池内反复被选中。
	// 只认 400/404（见 IsModelBlocked），429+11102 落下方 status==429 层走限流语义。
	if IsModelBlocked(status, body) {
		return ErrModelBlocked
	}
	// 402：真正的计费余额耗尽状态码，最严、最不可自愈，最先判。
	if status == http.StatusPaymentRequired {
		return ErrHardCredit
	}
	lower := strings.ToLower(body)
	// sessionDead / accountFault 先于 status==429：账号级终态等不来自愈，限流状态码
	// 不得掩盖它们（429+14017 必须 accountFault，401+12153 混排 "rate limit" 必须
	// sessionDead——此层 marker 是精确词，比限流层的大范围子串更具体，具体优先于宽泛）。
	for _, m := range sessionDeadMarkers {
		if strings.Contains(body, m) {
			return ErrSessionDead
		}
	}
	for _, m := range accountFaultMarkers {
		if strings.Contains(lower, strings.ToLower(m)) || strings.Contains(body, m) {
			return ErrAccountFault
		}
	}
	// status==429 先于 hardMarkers：限流响应 body 高频携带 "quota exceeded"/
	// "额度不足" 等跨计费/限流两界的措辞，hardMarkers 先判会把限流误归
	// ErrHardCredit 硬冷却到次日 04:00，白扔号约 12h。状态码是比关键词更权威的
	// 信号：上游既然给了 429，就按限流语义处理（宁可短冷却自愈，不可长冷却弃号）；
	// 真正的余额耗尽由 402（上层）捕获，非 429 状态码的 quota 措辞仍走下方
	// hardMarkers（历史语义不变）。
	if status == http.StatusTooManyRequests {
		return ErrSoftRate
	}
	for _, m := range hardMarkers {
		if strings.Contains(lower, strings.ToLower(m)) || strings.Contains(body, m) {
			return ErrHardCredit
		}
	}
	for _, m := range softRateMarkers {
		if strings.Contains(lower, strings.ToLower(m)) || strings.Contains(body, m) {
			return ErrSoftRate
		}
	}
	// 11115「prompt is too long」：判在 404/5xx/WAF/内容策略/参数错误/通用 4xx
	// 之前——请求级语义最具体（上下文超限），须先于宽泛的状态码兜底（404 兜底会
	// 误归 ErrNotFound 只冷却不透传；ErrClient 只换号，浪费健康号配额）。
	if isPromptTooLongStatus(status) {
		for _, m := range promptTooLongMarkers {
			if strings.Contains(body, m) || (m != strings.ToLower(m) && strings.Contains(lower, strings.ToLower(m))) {
				return ErrPromptTooLong
			}
		}
	}
	if status == http.StatusNotFound {
		return ErrNotFound
	}
	if status >= 500 {
		return ErrServer
	}
	// WAF 403（无业务信封的拦截形态）：判在内容策略/参数错误/通用 4xx 之前——
	// 这些层只认带文案的 body，WAF 空体/HTML 永远不会命中它们的 marker，
	// 但落 ErrClient 兜底的代价是「只换号不罚」（连环 403 根因），必须在兜底前分流。
	// 带信封的 403 在上方各层已有权威分类，不受影响。
	if IsWafBlocked(status, body) {
		return ErrWafBlock
	}
	// 内容策略拦截（HTTP 400 + 审核文案）：判在通用 ErrClient 之前。
	// 这是误报信号，不罚账号，由网关降级重试处理（见 handler.applyErrorPolicy）。
	if status >= 400 {
		for _, m := range contentBlockedMarkers {
			if strings.Contains(lower, m) {
				return ErrContentBlocked
			}
		}
		// 请求体被上游解析层拒绝（11101 / 11128 家族，见 badParamsMarkers）：
		// 「发给上游的 body 有问题」。网关侧截断已由 413 消灭（issue #41 commit A）；
		// 多模态方言已由 image.go 归一化；首条非 system 已由 ensureLeadingSystem 兜底。
		// 剩下的残余形态仍归 ErrBadParams：不冷却/不熔断/不计错，且 handler 侧
		// 直接 400 透传上游原文、**不轮转**（换号对这个 body 永远无效）。
		if badParamsBody(body) {
			return ErrBadParams
		}
		return ErrClient
	}
	// HTTP 200 但业务 code 非 0 且含余额关键词的情况已被上面 hardMarkers 捕获。
	return ErrNone
}

// apiEnvelope 上游统一信封。
type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// Client 上游 HTTP 客户端。Base 字段可覆盖便于测试。
type Client struct {
	HTTP *http.Client

	// ChatHTTP 聊天 SSE 专用 client：无总时长上限（Timeout=0），首字节由
	// Transport.ResponseHeaderTimeout 约束，流中空闲由 IdleTimeout 约束。
	// 与 HTTP 共享同一个 *http.Transport 实例，连接池不重复。
	ChatHTTP *http.Client

	// HTTPGlobal / ChatHTTPGlobal 单代理模式下的 global 域专用 client。
	// 仅在只配了一个代理时使用（保持向后兼容）；多代理时走 globalProxies。
	//
	// 背景：国际版上游对网关**出口 IP** 做 WAF 风控——10 秒内两个不同账号都撞
	// 403 即判 IP 级封锁，整片请求 503，换账号完全无效（同 IP）。让 global 走
	// 代理出口是最直接的解法；出口 IP 被盯上时换一个出口即可立刻恢复。
	//
	// 只作用于 global 域：CN 域保持直连——国内 IP 访问国内上游更稳、延迟更低，
	// 而且 CN 域没有这个 WAF 问题。nil 时全局回退 HTTP/ChatHTTP（零行为变化）。
	HTTPGlobal     *http.Client
	ChatHTTPGlobal *http.Client

	// realmProxies 按 realm 隔离的多代理出口池。
	//   global：config upstream.proxy_global（静态）+ 订阅池刷新（动态，见 SubPool）
	//   cn    ：仅订阅池刷新；无订阅时为空 → cn 恒直连（现状不变）。
	// 两池互不互通：global 账号只从 global 池选路，cn 账号只从 cn 池选路。
	// 每个请求轮询选一个出口；连续失败的出口临时降权跳过，其余继续可用——
	// 单一出口被上游 WAF 盯上时不会整片不可用。
	realmProxies map[string][]*proxyEntry
	realmMu      sync.Mutex
	// realmEgressCache 池出口采样缓存（SampleRealmEgress 写，RealmEgressView 读）。
	realmEgressCache sync.Map
	// realmAuthFail realm 级凭据失效截止（407 触发；订阅刷新重建池后清除）。
	// 整池 token 同批次失效，熔断整个 realm 比逐条试快 50 倍。
	realmAuthFail sync.Map
	proxyCursor   atomic.Uint32

	// HeaderTimeout 聊天 SSE 首字节前（响应头）超时；<=0 表示未设置（回落 HTTP.Timeout）。
	HeaderTimeout time.Duration
	// IdleTimeout 聊天 SSE 流中空闲超时；<=0 表示禁用空闲监控。
	IdleTimeout time.Duration

	// effortsMu/efforts 缓存各模型 supportedEfforts（FetchModels 刷新），供请求体 effort 降级。
	// 按 realm 分层桶（cn/global）：同模型名跨域探测的 effort 集合可能不同，
	// 混桶会互相污染（C-2）。
	effortsMu sync.RWMutex
	efforts   map[string]map[string][]string
	// defaultEfforts 缓存各模型 reasoning.defaultEffort（FetchModels 刷新），供
	// thinking.go 补档：缺显式 effort 时优先用模型声明默认档，空串回退硬编码 high。
	// 与 efforts 同 realm 分层桶（同 C-2 隔离原则），共用 effortsMu。
	defaultEfforts map[string]map[string]string

	// globalModels 缓存 global 模型名目录探测结果（成功 ∩ 静态 overlay；
	// 1h TTL + 5min 负缓存），见 global_models.go。按实例持有，测试新建 Client 即隔离。
	globalModels fetchGlobalModelsCache

	// SanitizeFingerprints 出站请求体黑名单指纹脱敏开关（默认 true；false 完全还原）。
	SanitizeFingerprints bool

	// UserAgent 出站 User-Agent 显式覆盖（非空时全路径生效，优先于默认三段式）。
	// 空 = 默认官方形态：chat/refresh/FetchModels 走
	// `WorkBuddy/<ver> WorkBuddy/<ver> CLI/<cliVer>`；billing 走 `WorkBuddy/<ver>`
	// （仅当 client_name 非空）。
	UserAgent string

	// ClientVersion WorkBuddy 客户端版本段（出站 UA 的 `WorkBuddy/<ver>` + X-IDE-Version）。
	// 空 = 内置默认（对齐官方 5.5.4 分发包）。
	ClientVersion string

	// CliVersion 出站 UA 中 `CLI/<ver>` 段版本。空 = 内置默认（官方内置 CLI 2.137.1）。
	CliVersion string

	// ClientName 用量归属头取值（X-Product / X-IDE-Name / X-IDE-Type / X-IDE-Version）。
	// 空 = 旧行为：X-Product="SaaS"，不设 X-IDE-*（向后兼容，不突变归因）。
	ClientName string

	// PassthroughIP 是否透传客户端 IP 给上游（X-Forwarded-For/X-Real-IP 首段）。
	// 缺省 false（反代安全边界）；handler 在 chat 路径按请求把 clientIP 传入 ChatStream。
	PassthroughIP bool

	// DeviceToken 设备风控 Token（X-Device-Token 头）全局兜底来源：config upstream.device_token。
	// 解析优先级：auth.Auth.DeviceToken > DeviceToken（config）> DeviceTokenFile（文件）。
	DeviceToken string

	// DeviceTokenFile 设备 token 文件路径兜底（宿主落盘的桌面端 token，5 分钟读取缓存）。
	DeviceTokenFile string

	ChatBaseCN    string
	BillingBaseCN string
	// WebBaseCN 官网（workbuddy.cn）域：部分「任务领奖」类接口只在此域提供
	// （Web 成长中心用；CLI 域 copilot.tencent.com 的同名路径返回 400）。
	WebBaseCN string

	// ChatBaseGlobal / BillingBaseGlobal 国际版（global realm）上游 base。
	// 空 = 缺省默认 https://www.workbuddy.ai（D5）。
	ChatBaseGlobal    string
	BillingBaseGlobal string

	// AccountProxy 账号级出站代理守卫（per-account 代理绑定 + 出口一致性校验）。
	// nil = 未启用，所有出站行为与既有逻辑逐字一致。
	AccountProxy *AccountProxy
	// GlobalEnabled 是否启用 global realm 路由（config global.enabled，缺省 true）。
	// false 时即便用户 auth 写了 realm=global 也**不**路由到 global base——
	// chatBase/billingBase 返回 CN base，路径也走 CN（双保险，与 auth.Realm() 的开关闸呼应）。
	GlobalEnabled bool
}

// New 生产默认值。Transport 由 newTransport() 集中构造（连接层加固：真正禁 h2 /
// TLS 握手超时 / 短 keepalive 探测 / 失败清池，参数见 transport.go——吸收上游
// kongjianguan 4 连击实测经验）。
func New() *Client {
	tr := newTransport()
	return &Client{
		HTTP:                 &http.Client{Timeout: 120 * time.Second, Transport: tr},
		ChatHTTP:             &http.Client{Timeout: 0, Transport: tr}, // 无总时长；首字节由 ResponseHeaderTimeout 管
		SanitizeFingerprints: true,
		ChatBaseCN:           "https://copilot.tencent.com",
		BillingBaseCN:        "https://www.codebuddy.cn",
		WebBaseCN:            "https://www.workbuddy.cn",
		// GlobalEnabled 缺省 true（与 config global.enabled 缺省 true 一致；纯 CN 部署行为不变：
		// CN 账号恒判 cn，global base 只在 realm=global 的账号上被使用）。
		GlobalEnabled: true,
	}
}

// chatHTTP 返回聊天专用 client；未设置（如测试只注入 HTTP）时回落 HTTP。
func (c *Client) chatHTTP() *http.Client {
	if c.ChatHTTP != nil {
		return c.ChatHTTP
	}
	return c.HTTP
}

// httpFor 按 realm 返回普通出站 client（global 走代理，其余直连）。
func (c *Client) httpFor(realm string) *http.Client {
	if e := c.pickProxy(realm); e != nil {
		return e.http
	}
	return c.HTTP
}

// chatHTTPFor 按 realm 返回聊天出站 client（global 走代理，其余直连）。
func (c *Client) chatHTTPFor(realm string) *http.Client {
	if e := c.pickProxy(realm); e != nil {
		return e.chat
	}
	return c.chatHTTP()
}

// proxyEntry 单个 global 域代理出口。
type proxyEntry struct {
	raw  string
	http *http.Client
	chat *http.Client
	// fails 连续失败次数；deadUntil 在此之前跳过该出口（unix nano）。
	// 用「临时降权」而不是永久剔除：免费代理池抖动很常见，几分钟后可能又好了。
	fails     atomic.Int32
	deadUntil atomic.Int64

	// untrustedUntil TLS 劫持（MITM）嫌疑熔断截止。
	//
	// 与普通失败分开计：连不上/超时只是不可用，**证书校验失败 = 出口在劫持
	// HTTPS**（实测 resin 免费池 2026-09-27 出现自签证书节点）。经过这种出口
	// 的请求会把 Authorization Bearer token 明文交给劫持者，且响应可被篡改。
	// 所以 MITM 嫌疑的出口必须长熔断（30min 起步、翻倍、封顶 6h），而不是
	// 普通失败的 10s~2min——一次成功响应不能洗白它（聚合池背后是随机节点，
	// 这次干净不代表下次干净），到期后重试再犯则继续翻倍。
	untrustedUntil atomic.Int64
	mitmHits       atomic.Int32

	// realm 该出口所属 realm（SetRealmProxy 注入）。
	realm string
	// onAuthFail 407 回调：通知 Client 整个 realm 池凭据失效（同一批次 Link
	// token 一起失效，逐条试没意义）。SetRealmProxy 注入。
	onAuthFail func()

	// latencyEWMA 成功请求「响应头耗时」的指数移动平均（毫秒；0 = 无样本）。
	// 选路依据：resin 池节点延迟差 5 倍（实测 TTFB 5s~22s+），只看「是否失败」
	// 会让慢节点（没失败但 TTFB 20s+）被反复选中——用户感知就是「卡死」。
	// EWMA 平滑单次抖动；pickProxy 优先 EWMA 低的健康出口。
	latencyEWMA atomic.Int64
}

// proxySkipFor 连续失败后跳过该出口的时长（按失败次数递增，封顶 2 分钟）。
func proxySkipFor(fails int32) time.Duration {
	d := 10 * time.Second
	for i := int32(1); i < fails && d < 2*time.Minute; i++ {
		d *= 2
	}
	if d > 2*time.Minute {
		d = 2 * time.Minute
	}
	return d
}

func (e *proxyEntry) noteFail() {
	n := e.fails.Add(1)
	e.deadUntil.Store(time.Now().Add(proxySkipFor(n)).UnixNano())
}

// noteLatency 记录一次响应头耗时，更新 EWMA（α=0.3，兼顾响应速度与抗抖动）。
// 只记样本不做惩罚——慢可能是模型长思考而非链路问题（误降权会饿死合法请求），
// 惩罚交给选路：EWMA 高的健康出口在第一轮被自然跳过，流量流向快的出口。
func (e *proxyEntry) noteLatency(d time.Duration) {
	ms := d.Milliseconds()
	if ms <= 0 {
		return
	}
	old := e.latencyEWMA.Load()
	var v int64
	if old <= 0 {
		v = ms
	} else {
		v = old*7/10 + ms*3/10
	}
	e.latencyEWMA.Store(v)
}

// slowLatencyMS 延迟可接受阈值（EWMA 超过它 = 出口慢，第一轮选路跳过）。
// 实测 resin 池快节点 TTFB~5s、慢节点 20s+，20s 是「明显卡」的分界。
const slowLatencyMS = 20000

// noteUntrusted 记一次 TLS 劫持嫌疑：长熔断 + SECURITY 告警。
// 日志只打 host（raw 里有凭据，不能进日志）。
func (e *proxyEntry) noteUntrusted() {
	n := e.mitmHits.Add(1)
	backoff := 30 * time.Minute
	for i := int32(1); i < n && backoff < 6*time.Hour; i++ {
		backoff *= 2
	}
	// 乘后再 clamp：原条件在乘法前检查，4h<6h 会再乘成 8h，封顶失效。
	if backoff > 6*time.Hour {
		backoff = 6 * time.Hour
	}
	e.untrustedUntil.Store(time.Now().Add(backoff).UnixNano())
	e.deadUntil.Store(e.untrustedUntil.Load())
	e.fails.Store(1 << 20) // 让「健康优先」第一轮必然跳过
	host := e.raw
	if u, err := url.Parse(e.raw); err == nil && u.Host != "" {
		host = u.Host
	}
	log.Printf("SECURITY: global 代理出口 %s 疑似 TLS 劫持（证书校验失败），熔断 %s（第 %d 次）。"+
		"经过该出口的流量（含账号 token）可能已被截获——建议检查该出口来源，"+
		"必要时重新登录经它用过的账号", host, backoff, n)
}

// untrusted 报告出口当前是否处于 MITM 嫌疑熔断期。
func (e *proxyEntry) untrusted(now int64) bool {
	return e.untrustedUntil.Load() > now
}

func (e *proxyEntry) noteSuccess() {
	e.fails.Store(0)
	e.deadUntil.Store(0)
}

// pickProxy 选择本次请求（指定 realm）的出口：健康优先，坏出口退为后备。
// 三轮策略：健康（无失败+不在冷却+无 MITM 嫌疑）→ 冷却期外轮询 → 非 MITM 兜底。
// 全部出口都在 MITM 熔断期 → 返回 nil 回落直连（把 token 交给劫持者比 403 更糟）。
// realm 隔离：global 与 cn 各自独立成池，互不取用。
// markRealmAuthFail 标记整个 realm 池凭据失效（407）：熔断 1h，期间 pickProxy
// 直接回落直连。订阅刷新（SetRealmProxy）会清除标记。
func (c *Client) markRealmAuthFail(realm string) {
	key := realmKey(realm)
	v := time.Now().Add(time.Hour).UnixMilli()
	if old, ok := c.realmAuthFail.Load(key); ok {
		if o, _ := old.(int64); o == v {
			return // 已标记，不重复打日志
		}
	}
	c.realmAuthFail.Store(key, v)
	log.Printf("[upstream] %s 池凭据失效（407），整池熔断 1h 回落直连（订阅刷新后自动恢复）", key)
}

func (c *Client) pickProxy(realm string) *proxyEntry {
	key := realmKey(realm)
	// realm 级凭据失效熔断：整池跳过，直接回落直连（比逐条试 407 快得多）。
	if v, ok := c.realmAuthFail.Load(key); ok {
		if until, _ := v.(int64); time.Now().UnixMilli() < until {
			return nil
		}
	}
	c.realmMu.Lock()
	if c.realmProxies == nil {
		c.realmProxies = map[string][]*proxyEntry{}
	}
	entries := c.realmProxies[key]
	c.realmMu.Unlock()
	n := len(entries)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return entries[0]
	}
	start := int(c.proxyCursor.Add(1)-1) % n
	now := time.Now().UnixNano()
	// mitmBan 永久剔除标记：劫持累计 >=3 次 = 该出口稳定在劫持（不是偶发），
	// 6h 熔断到期后回来只会再劫持——任何轮次都不再选它。
	// 剔除只影响选路，条目保留（订阅刷新时整体重建，人工可换链接救回）。
	const mitmBan = 3
	good := func(e *proxyEntry, now int64) bool { return e.mitmHits.Load() < mitmBan }
	healthy := func(e *proxyEntry, now int64) bool {
		return good(e, now) && e.fails.Load() == 0 && e.deadUntil.Load() <= now && !e.untrusted(now)
	}

	// 第一轮：健康 **且延迟可接受**（EWMA 未超阈值或无样本）→ 轮询分散。
	// 这是「自动用稳定出口」的核心：慢节点（EWMA 20s+）不再被选中，流量自然
	// 集中到快节点；全慢时落到第二轮，仍可用只是慢。
	for i := 0; i < n; i++ {
		if e := entries[(start+i)%n]; healthy(e, now) {
			if l := e.latencyEWMA.Load(); l > 0 && l > slowLatencyMS {
				continue
			}
			return e
		}
	}
	// 第一轮落空（全慢/全冷却）→ 原健康轮询（延迟不设限，可用性优先）。
	for i := 0; i < n; i++ {
		if e := entries[(start+i)%n]; healthy(e, now) {
			return e
		}
	}
	// 第二轮：没有健康出口 → 跳过期外轮询（避开 MITM 嫌疑/永久剔除出口）。
	for i := 0; i < n; i++ {
		if e := entries[(start+i)%n]; good(e, now) && e.deadUntil.Load() <= now && !e.untrusted(now) {
			return e
		}
	}
	// 兜底：存在非 MITM 嫌疑的出口时，宁可试一个可能坏的（它只是普通失败在
	// 退避期，几分钟后可能就好了），也不放弃该 realm。
	// 注意：这里**不检查 deadUntil**——全列表轮询语义要求全在退避期时仍给机会。
	// 凭据失效（407）场景由 realm 级熔断在函数开头拦截（markRealmAuthFail），
	// 整池立即回落直连，不走这条兜底。
	for i := 0; i < n; i++ {
		if e := entries[(start+i)%n]; good(e, now) && !e.untrusted(now) {
			return e
		}
	}
	return nil
}

// proxyRT 包装出站 Transport，统计每个出口的成败（供 pickGlobalProxy 降权）。
type proxyRT struct {
	base  http.RoundTripper
	entry *proxyEntry
}

func (t *proxyRT) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	// 响应头耗时（不含 body）记入 EWMA——不管成败都记：失败请求的耗时同样是
	// 该出口链路质量的信号。noteSuccess/noteFail 已处理成败语义，这里只补延迟。
	lat := time.Since(start)
	switch {
	case err != nil && isTLSInterception(err):
		// 证书校验失败 = 出口在劫持 HTTPS（MITM）：长熔断 + 告警，
		// 绝不能当普通失败 10s 后就放它回来。
		t.entry.noteUntrusted()
	case err != nil:
		t.entry.noteFail() // 连不上代理 / 代理连不上目标
	case resp.StatusCode == http.StatusForbidden:
		// 国际版 WAF 以 403 HTML 拦截页返回。把当前出口临时降权，
		// 下一次 global 请求换另一个 resin/warp 出口；不要把 403 当成功，
		// 否则代理池永远不会避开已被 WAF 盯上的出口。
		t.entry.noteFail()
	case resp.StatusCode == http.StatusProxyAuthRequired:
		// 407 凭据失效：同一批次 Link token 一起失效 → 通知 Client 熔断**整个
		// realm 池**（不是只冷却这一条），否则要逐条试 50 次才轮到直连。
		if t.entry.onAuthFail != nil {
			t.entry.onAuthFail()
		}
		t.entry.deadUntil.Store(time.Now().Add(time.Hour).UnixNano())
		t.entry.noteFail()
	case resp.StatusCode == http.StatusBadGateway:
		// 代理网关自己的 502 通常表示该池节点失效；503 可能是上游业务
		// 失败（如 6004/服务暂时不可用），不能误杀当前出口。
		t.entry.noteFail()
	default:
		// 429/503 等上游业务响应不等于代理坏：让 handler 按账号/模型语义处理。
		t.entry.noteSuccess()
	}
	t.entry.noteLatency(lat)
	return resp, err
}

// isTLSInterception 判定出站错误是否为 TLS 证书校验失败（劫持/MITM 特征）。
// Go 会把 x509 错误包在 *url.Error 里，errors.As 能穿透包装逐层找。
// 同时兜底匹配错误文本——不同 TLS 库版本的包装不完全一致，宁可多报不可漏报。
func isTLSInterception(err error) bool {
	if err == nil {
		return false
	}
	var unknown x509.UnknownAuthorityError
	if errors.As(err, &unknown) {
		return true
	}
	var hostname x509.HostnameError
	if errors.As(err, &hostname) {
		return true
	}
	var invalid x509.CertificateInvalidError
	if errors.As(err, &invalid) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "certificate is not valid for any names") ||
		strings.Contains(msg, "self-signed certificate") ||
		strings.Contains(msg, "certificate signed by unknown authority") ||
		strings.Contains(msg, "certificate has expired") && strings.Contains(msg, "workbuddy")
}

func (t *proxyRT) CloseIdleConnections() {
	if ci, ok := t.base.(closeIdler); ok {
		ci.CloseIdleConnections()
	}
}

// SetRealmProxy 设置指定 realm 的出站代理池（逗号分隔多条），返回错误与否。
// 会**替换**该 realm 现有池（订阅刷新即以此重建）。空串 = 清除该 realm 池。
// 代理不可用不会让启动失败——首次请求时才暴露（连接错误会被现有错误分类
// 归为传输层失败并正常轮转），避免"代理挂掉导致网关起不来"。
func (c *Client) SetRealmProxy(realm, rawURL string) error {
	raw := strings.TrimSpace(rawURL)
	key := realmKey(realm)
	c.realmMu.Lock()
	if c.realmProxies == nil {
		c.realmProxies = map[string][]*proxyEntry{}
	}
	set := func(entries []*proxyEntry) {
		c.realmProxies[key] = entries
	}
	c.realmMu.Unlock()

	if raw == "" {
		c.realmMu.Lock()
		delete(c.realmProxies, key)
		c.realmMu.Unlock()
		return nil
	}
	parts := strings.Split(raw, ",")
	entries := make([]*proxyEntry, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		e, err := newProxyEntry(p)
		if err != nil {
			return err
		}
		e.realm = key
		e.onAuthFail = func() { c.markRealmAuthFail(key) }
		entries = append(entries, e)
	}
	// 新池建立 ≠ 新 token 可用：resin 强刷轮换后，订阅里的链接仍是旧 token。
	// 用数据对比判断：新池与旧池链接集合相同 = token 没变 = 保留 407 熔断
	// （否则「刷新→清熔断→407→熔断」循环）；不同 = 用户重新导入了新链接，清熔断。
	// 用数据对比而非网络探活：探活失败会把正常池误熔断（测试与生产都踩过）。
	if len(entries) > 0 {
		c.realmMu.Lock()
		oldSet := map[string]bool{}
		for _, oe := range c.realmProxies[key] {
			oldSet[oe.raw] = true
		}
		c.realmMu.Unlock()
		same := len(oldSet) == len(entries)
		if same {
			for _, ne := range entries {
				if !oldSet[ne.raw] {
					same = false
					break
				}
			}
		}
		if same {
			if _, failing := c.realmAuthFail.Load(key); failing {
				log.Printf("[upstream] %s 订阅刷新后链接集合未变（token 未更新），保留 407 熔断", key)
			}
		} else {
			c.realmAuthFail.Delete(key)
		}
	}
	c.realmMu.Lock()
	set(entries)
	c.realmMu.Unlock()
	// 旧字段兼容：global 单代理时填充 HTTPGlobal/ChatHTTPGlobal（既有调用方
	// 与测试语义）；多代理时清空走池。cn 域无此遗留字段。
	if key == "global" {
		if len(entries) == 1 {
			c.HTTPGlobal, c.ChatHTTPGlobal = entries[0].http, entries[0].chat
		} else {
			c.HTTPGlobal, c.ChatHTTPGlobal = nil, nil
		}
	}
	return nil
}

// SetGlobalProxy 兼容包装：等价 SetRealmProxy("global", raw)。
func (c *Client) SetGlobalProxy(rawURL string) error {
	return c.SetRealmProxy("global", rawURL)
}

// newProxyEntry 解析一个代理 URL 并构造带成败统计的 client 对。
func newProxyEntry(raw string) (*proxyEntry, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("proxy url %q: %w", raw, err)
	}
	host := u.Host
	if host == "" {
		return nil, fmt.Errorf("proxy url %q: missing host", raw)
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		return nil, fmt.Errorf("proxy url %q: host must be host:port", raw)
	}

	e := &proxyEntry{raw: raw}
	var tr *http.Transport
	switch strings.ToLower(u.Scheme) {
	case "socks5", "socks5h":
		d := &socks5Dialer{addr: host, forward: newCachedDialer(newDialer())}
		if u.User != nil {
			d.user = u.User.Username()
			d.pass, _ = u.User.Password()
		}
		tr = newTransport()
		tr.DialContext = d.DialContext
	case "http", "https":
		tr = newTransport()
		tr.Proxy = http.ProxyURL(u)
	default:
		return nil, fmt.Errorf("proxy url %q: unsupported scheme %q (want socks5/http/https)", raw, u.Scheme)
	}
	rt := &proxyRT{base: tr, entry: e}
	e.http = &http.Client{Timeout: 120 * time.Second, Transport: rt}
	e.chat = &http.Client{Timeout: 0, Transport: rt}
	return e, nil
}

// GlobalProxyActive 报告 global 域是否已启用代理出口（供 /status 与面板透出）。
// GlobalProxyActive 报告 global 域是否已启用代理出口（供 /status 与面板透出）。
func (c *Client) GlobalProxyActive() bool { return c.ProxyCount("global") > 0 }

// GlobalProxyCount 返回当前 global 出口池数量。
// handler 用它决定是否跳过单出口假设的 IP 级 WAF 门控：多出口时应换出口，
// 而不是把整站 global 请求一起限住。
// ProxyCount 指定 realm 的出口池条数（0 = 无池，该 realm 直连）。
func (c *Client) ProxyCount(realm string) int {
	c.realmMu.Lock()
	defer c.realmMu.Unlock()
	return len(c.realmProxies[realmKey(realm)])
}

// GlobalProxyCount 兼容包装（global 池条数）。
func (c *Client) GlobalProxyCount() int { return c.ProxyCount("global") }

// BestProxyFor 返回 realm 池里最稳定的出口链接（排除 excludeRaw），空串=没有
// 可用条目（保持当前绑定不动，不要清掉）。
// 稳定度排序：无劫持（mitmHits<3）→ 连续失败少 → 延迟 EWMA 低。
func (c *Client) BestProxyFor(realm, excludeRaw string) string {
	c.realmMu.Lock()
	entries := append([]*proxyEntry(nil), c.realmProxies[realmKey(realm)]...)
	c.realmMu.Unlock()
	var best *proxyEntry
	for _, e := range entries {
		if e.raw == excludeRaw || e.mitmHits.Load() >= 3 {
			continue
		}
		if best == nil {
			best = e
			continue
		}
		bf, ef := best.fails.Load(), e.fails.Load()
		bl, el := best.latencyEWMA.Load(), e.latencyEWMA.Load()
		if ef < bf || (ef == bf && el > 0 && (bl == 0 || el < bl)) {
			best = e
		}
	}
	if best == nil {
		return ""
	}
	return best.raw
}

// CandidateProxies 返回 realm 池的候选链接，按稳定度排序（最优在前），
// 排除 excludeRaw 与已永久剔除（劫持>=3）的条目。
// 稳定度：无劫持 → 连续失败少 → 延迟 EWMA 低。
func (c *Client) CandidateProxies(realm, excludeRaw string) []string {
	c.realmMu.Lock()
	entries := append([]*proxyEntry(nil), c.realmProxies[realmKey(realm)]...)
	c.realmMu.Unlock()
	// 过滤
	keep := entries[:0:0]
	for _, e := range entries {
		if e.raw == excludeRaw || e.mitmHits.Load() >= 3 {
			continue
		}
		keep = append(keep, e)
	}
	// 稳定度排序（简单插入排序：池也就几十条，够用且无额外依赖）
	for i := 1; i < len(keep); i++ {
		for j := i; j > 0 && candidateBetter(keep[j], keep[j-1]); j-- {
			keep[j], keep[j-1] = keep[j-1], keep[j]
		}
	}
	out := make([]string, 0, len(keep))
	for _, e := range keep {
		out = append(out, e.raw)
	}
	return out
}

// candidateBetter a 是否比 b 更稳定（用于排序）。
func candidateBetter(a, b *proxyEntry) bool {
	af, bf := a.fails.Load(), b.fails.Load()
	if af != bf {
		return af < bf
	}
	al, bl := a.latencyEWMA.Load(), b.latencyEWMA.Load()
	switch {
	case al == 0 && bl == 0:
		return false
	case al == 0:
		return false // 无样本视为未知，排在有样本之后
	case bl == 0:
		return true
	default:
		return al < bl
	}
}

// realmEgressView 池级出口采样缓存（无绑定账号的面板出口视图数据源）。
type realmEgressView struct {
	ip, country, cc, asn string
	at                   int64
}

// SampleRealmEgress 对 realm 池**当前选中**的出口做一次实测采样。
//
// 背景：无绑定账号的出站走 realm 订阅池（chatHTTPFor→pickProxy），但
// EgressView 对无绑定账号返回的是「本机直连出口」（旧假设）——面板显示的
// 服务器 IP 与实际出站不符。此采样为面板提供池出口的真实 IP（5 分钟一轮）。
func (c *Client) SampleRealmEgress(realm string) {
	e := c.pickProxy(realm)
	if e == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p := probeEgress(ctx, e.http, 1) // 单源即可（面板展示，不做 quorum 仲裁）
	if p.IP == "" {
		return
	}
	c.realmEgressCache.Store(realmKey(realm), realmEgressView{
		ip: p.IP, country: p.Country, cc: p.CountryCode, asn: p.ASN,
		at: time.Now().UnixMilli(),
	})
}

// RealmEgressView 无绑定账号（出站走 realm 池）的面板出口视图。
// 池为空（该 realm 直连）或尚无样本返回 nil——调用方回落直连视图。
func (c *Client) RealmEgressView(realm string) *EgressViewInfo {
	if c.ProxyCount(realm) == 0 {
		return nil
	}
	v, ok := c.realmEgressCache.Load(realmKey(realm))
	if !ok {
		return nil
	}
	ev := v.(realmEgressView)
	return &EgressViewInfo{
		Source:      EgressSourceProxy,
		IP:          ev.ip,
		Country:     ev.country,
		CountryCode: ev.cc,
		ASN:         ev.asn,
		CheckedAt:   ev.at,
		State:       "ok",
		Label:       fmt.Sprintf("订阅池出口（%d 条轮换）", c.ProxyCount(realm)),
	}
}

// ProxySummary 返回指定 realm 出口池的健康摘要（面板/日志展示用，不含凭据）。
// 返回形如 ["warp:1080 ok", "172.17.0.1:2269 fails=3 skip 40s"] 的列表。
func (c *Client) ProxySummary(realm string) []string {
	c.realmMu.Lock()
	entries := c.realmProxies[realmKey(realm)]
	c.realmMu.Unlock()
	out := make([]string, 0, len(entries))
	now := time.Now().UnixNano()
	for _, e := range entries {
		label := e.raw
		if u, err := url.Parse(e.raw); err == nil && u.Host != "" {
			label = u.Host // 去掉凭据
		}
		f := e.fails.Load()
		switch {
		case f == 0:
			out = append(out, label+" ok")
		case e.deadUntil.Load() > now:
			left := time.Duration(e.deadUntil.Load()-now) / time.Second
			out = append(out, fmt.Sprintf("%s fails=%d skip %s", label, f, left*time.Second))
		default:
			out = append(out, fmt.Sprintf("%s fails=%d (retrying)", label, f))
		}
	}
	return out
}

// ProxyPoolStat 出口池健康分布（面板展示用；只回计数，不含任何凭据）。
//
// 为什么需要它：面板此前只透出「池内 N 条」，运维看不出池子到底还剩几条能用——
// 15 条里 12 条在冷却和 15 条全健康，是两种完全不同的处境，但显示都是「15 条」。
//
// 分层口径（互斥，Total = Healthy + Cooling + Retrying）：
//   - Total    池内总条数
//   - Healthy  无连续失败（fails==0）——正常参与选路
//   - Cooling  有失败且仍在降权窗口内（deadUntil 未到）——本轮不会被选中
//   - Retrying 有失败但降权窗口已过——下次选路会重试
//   - Hijacked TLS 劫持命中过（mitmHits>0，含已被永久剔除的）
//   - Removed  永久剔除（mitmHits>=3）——不再进入候选
//   - Available 可进入候选的条数（Total - Removed）
type ProxyPoolStat struct {
	Total     int `json:"total"`
	Healthy   int `json:"healthy"`
	Cooling   int `json:"cooling"`
	Retrying  int `json:"retrying"`
	Hijacked  int `json:"hijacked"`
	Removed   int `json:"removed"`
	Available int `json:"available"`
}

// ProxyPoolStats 统计指定 realm 出口池的健康分布（无池时全 0）。
func (c *Client) ProxyPoolStats(realm string) ProxyPoolStat {
	c.realmMu.Lock()
	entries := append([]*proxyEntry(nil), c.realmProxies[realmKey(realm)]...)
	c.realmMu.Unlock()
	st := ProxyPoolStat{Total: len(entries)}
	now := time.Now().UnixNano()
	for _, e := range entries {
		if h := e.mitmHits.Load(); h > 0 {
			st.Hijacked++
			if h >= 3 {
				st.Removed++
			}
		}
		f := e.fails.Load()
		switch {
		case f == 0:
			st.Healthy++
		case e.deadUntil.Load() > now:
			st.Cooling++
		default:
			st.Retrying++
		}
	}
	st.Available = st.Total - st.Removed
	return st
}

// defaultGlobalBase 缺省 global base（D5：config 未覆盖时默认 workbuddy.ai）。
const defaultGlobalBase = "https://www.workbuddy.ai"

// globalChatBase 生效的 global chat base：Client.ChatBaseGlobal 非空取之，否则默认。
func (c *Client) globalChatBase() string {
	if c.ChatBaseGlobal != "" {
		return c.ChatBaseGlobal
	}
	return defaultGlobalBase
}

// globalBillingBase 生效的 global billing base：Client.BillingBaseGlobal 非空取之，否则默认。
func (c *Client) globalBillingBase() string {
	if c.BillingBaseGlobal != "" {
		return c.BillingBaseGlobal
	}
	return defaultGlobalBase
}

// globalOn 报告账号是否路由到 global 上游：GlobalEnabled 开且账号 Realm()==global。
// 双保险：config 开关是第一道闸（上游侧），auth.Realm() 的开关闸是第二道（账号侧）。
func (c *Client) globalOn(a *auth.Auth) bool {
	return c.GlobalEnabled && a != nil && a.Realm() == "global"
}

// 路径常量：CN 现状路径（chatCompletionsPath）与 global 双候选路径。
const (
	chatCompletionsPath   = "/v2/chat/completions"
	globalChatConsolePath = "/console/chat/completions"
)

// chatPaths 按 realm 返回 chat 端点路径候选序列：
//
//	global → [/v2/chat/completions, /console/chat/completions]
//	cn     → [/v2/chat/completions]（逐字不变，零回归）
//
// 【为什么 global 首选 /v2 而不是 /console】2026-09-19 实测：www.workbuddy.ai
// 前置的腾讯云 WAF，其内容规则**只覆盖 /console 路径**。global 首选 /console
// 的代价是——只要 body 带攻击特征串（安全/SRC 类 Agent 的工具输出天然带），
// 每个请求都要先撞一次 403 拦截页、白跑一个往返，再回退到 /v2 才成功
// （实测 global 请求 TTFB 因此被拖到 68s）。
//
// 首选 /v2 后完全不碰 WAF，少一次注定失败的出站。
//
// 【等价性】切换前逐项实测 /console 与 /v2 形态完全一致：
//   - 全部 23 个 global 模型：SSE + content + reasoning + tool 字段全齐，两路一致；
//   - tools 真实调用：两路都正常返回 tool_calls；
//   - 图片输入：两路报同一个上游错误（11128 first message is not system prompt），
//     行为一致；
//   - 多轮回传 reasoning_content、40 轮长上下文：两路均正常。
//
// /console 保留为 fallback（404/405 或 WAF 拦截页 403 时启用，见 chatPathFallback）。
func (c *Client) chatPaths(a *auth.Auth) []string {
	if c.globalOn(a) {
		return []string{chatCompletionsPath, globalChatConsolePath}
	}
	return []string{chatCompletionsPath}
}

func chatFallbackHTTPStatus(status int) bool { return status == 404 || status == 405 }

// chatPathFallback 判定该响应是否应改走下一个候选路径。
//
// 除既有的 404/405（路径不存在）外，追加 **WAF 拦截页 403**：
//
// 实测（2026-09-19，global 域 www.workbuddy.ai，用用户真实抓包 body 验证）：
// 国际版前置的腾讯云 WAF 规则**只覆盖 /console 路径**——同一份触发内容
// （含 SQL 注入 / 路径穿越 / 命令执行等特征串的 Agent 工具输出）走
// /console/chat/completions 被 403 整包拦下，改走同主机的 /v2/chat/completions
// 则**正常进入业务层**（不再返回 WAF 页）。两条路径同一套后端，能力等价。
//
// 因此把「WAF 拦截页」也纳入路径回退条件，而不是把 403 当终局错误去冷却账号、
// 轮转、最终回 503。global 已改为**首选 /v2**（见 chatPaths，正常流量不再碰 WAF），
// 本回退作为保险保留：万一 WAF 规则将来扩到 /v2，仍会自动试 /console。
//
// 判定口径复用 IsWafBlocked（403 且无业务信封）：带业务信封的 403（11140 等）
// 仍走既有分类链，不会被误当路径问题。
func chatPathFallback(status int, body []byte) bool {
	if chatFallbackHTTPStatus(status) {
		return true
	}
	return IsWafBlocked(status, string(body))
}

// billing 域端点路径（billingBase + path）。balance/checkin 与 report（report.go）同域，
// 统一走 billingJSON 发请求。
const (
	billingMeterPath   = "/billing/meter/get-user-resource"    // global 首选（国际版无 /v2 前缀）
	dailyCheckinPath   = "/billing/meter/daily-checkin"        // global 首选
	billingMeterPathV2 = "/v2/billing/meter/get-user-resource" // CN 现状 / global fallback
	dailyCheckinPathV2 = "/v2/billing/meter/daily-checkin"
)

// billingMeterPaths 按 realm 返回 billing/meter 域路径候选序列：
// global → [无 /v2, 有 /v2]（404 时 fallback）；cn → [有 /v2]（现状逐字，零回归）。
// 仅作用于 get-user-resource / daily-checkin（/billing/meter/* 族）；report /v2/report 不参与，
// 其他 billing 端点（growth 等）路径不含 /billing/meter 前缀，走原常量不受影响。
func (c *Client) billingMeterPaths(a *auth.Auth) []string {
	if c.globalOn(a) {
		return []string{billingMeterPath, billingMeterPathV2}
	}
	return []string{billingMeterPathV2}
}

// checkinMeterPaths 同上，针对 daily-checkin。
func (c *Client) checkinMeterPaths(a *auth.Auth) []string {
	if c.globalOn(a) {
		return []string{dailyCheckinPath, dailyCheckinPathV2}
	}
	return []string{dailyCheckinPathV2}
}

func (c *Client) chatBase(a *auth.Auth) string {
	if c.globalOn(a) {
		return c.globalChatBase()
	}
	return c.ChatBaseCN
}

// prepareBody 组装出站请求体（脱敏开关由 Client.SanitizeFingerprints 控制）。
// prepareBody 组装出站请求体（脱敏开关由 Client.SanitizeFingerprints 控制）。
// realm 为账号 Realm()（cn/global），供 efforts 缓存分桶（跨域 effort 集合不互相污染）。
func (c *Client) prepareBody(body []byte, realm, uid, conversationID string) []byte {
	efforts, defs := c.effortsSnapshot(realm), c.defaultEffortsSnapshot(realm)
	if realmKey(realm) == "global" {
		// global 域降级源 = 远端探测桶（权威）∪ 产品静态兜底表（全局 21 名内档位如
		// deepseek-v4.1-flash ['high']）。当前探测桶为空时也按静态表降级，不全程透传
		//（issue #84：往 WorkBuddy 上游发 low/max 非法，须降级到 high）。
		efforts, defs = globalEffortMap(efforts, defs)
	}
	body = PrepareBodyOptWithEffortsAndDefault(body, c.SanitizeFingerprints, efforts, defs)
	// prompt_cache_key 注入（P0 费用优化，费用降 ~17×）：按账号隔离的稳定缓存键，
	// 让同一客户端对同一账号的连续请求命中上游前缀缓存。
	body = InjectPromptCacheKey(body, uid, conversationID)
	return body
}

// effortsSnapshot 返回 effort 能力缓存副本；nil 表示未知（透传不降级）。
func (c *Client) effortsSnapshot(realm string) map[string][]string {
	c.effortsMu.RLock()
	defer c.effortsMu.RUnlock()
	bucket, ok := c.efforts[realmKey(realm)]
	if !ok || len(bucket) == 0 {
		return nil
	}
	cp := make(map[string][]string, len(bucket))
	for k, v := range bucket {
		cp[k] = v
	}
	return cp
}

// defaultEffortsSnapshot 返回指定 realm 的模型 defaultEffort 缓存副本；
// 该域无探测或无声明默认档 → nil（thinking.go 回退硬编码 high）。
func (c *Client) defaultEffortsSnapshot(realm string) map[string]string {
	c.effortsMu.RLock()
	defer c.effortsMu.RUnlock()
	bucket, ok := c.defaultEfforts[realmKey(realm)]
	if !ok || len(bucket) == 0 {
		return nil
	}
	cp := make(map[string]string, len(bucket))
	for k, v := range bucket {
		cp[k] = v
	}
	return cp
}

// realmKey 归一化 efforts 缓存键：cn/global。空 realm 视为 cn（老调用/无前缀模型名）。
func realmKey(realm string) string {
	if realm == "" {
		return "cn"
	}
	return realm
}

func (c *Client) billingBase(a *auth.Auth) string {
	if c.globalOn(a) {
		return c.globalBillingBase()
	}
	return c.BillingBaseCN
}

// webBase 返回官网域（任务领奖类接口；未注入时回落默认）。
// realm 感知：global 账号切国际站 workbuddy.ai，CN 用 workbuddy.cn。
func (c *Client) webBase(a *auth.Auth) string {
	if c.globalOn(a) {
		return defaultGlobalBase
	}
	if c.WebBaseCN != "" {
		return c.WebBaseCN
	}
	return "https://www.workbuddy.cn"
}

// doJSON 发请求并解信封；HTTP 非 2xx 或业务 code != 0 时返回带 body 片段的 *Error。
// body 读失败（连接中断/空闲掐流/截断）返回普通错误（非 *Error）——半截 body 不进
// Classify，不参与账号惩罚（传输层故障不该喂熔断误罚号）。
func (c *Client) doJSON(req *http.Request) (json.RawMessage, error) {
	// 账号感知出站：请求上下文里带账号时走该号绑定的代理，否则回落 c.HTTP（零变化）。
	resp, err := c.httpForReq(req).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		kind := Classify(resp.StatusCode, string(raw))
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: truncate(string(raw), 200)}
	}
	var env apiEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("parse failed: %w (body: %s)", err, truncate(string(raw), 120))
	}
	if env.Code != 0 {
		kind := Classify(resp.StatusCode, env.Msg)
		if kind == ErrNone {
			kind = ErrClient
		}
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: fmt.Sprintf("code=%d msg=%s", env.Code, truncate(env.Msg, 160))}
	}
	return env.Data, nil
}

// RefreshToken 刷新 access token；成功时更新 a 的字段（缺省值保留旧值），
// 调用方负责 SaveAtomic。全程持 a 锁，防止并发 SaveAtomic 读半更新 token。
// refreshIOTimeout 刷新端点网络 I/O 上限（两段式锁外执行，防上游 hang 长占锁）。
const refreshIOTimeout = 30 * time.Second

// RefreshToken 刷新 access token；成功时更新 a 的字段（缺省值保留旧值），
// 调用方负责 SaveAtomic。
//
// 并发安全模型（两段式，缩小持锁窗口）：
//   - 锁内仅做「读 refreshToken 快照」与「校验未变后写回新 token」两小段内存操作；
//   - 网络 I/O（doJSON）在**锁外**执行，带 30s ctx 超时——避免上游 hang 时长时间
//     独占 a.mu，阻塞同账号的 SaveAtomic / 其他刷新（issue:持锁 120s I/O）。
//   - 写回前重新校验快照一致性：若锁外期间另一 goroutine 已完成刷新（refreshToken
//     已变），本次结果直接采用（新 token 已生效），不再重复写回。
func (c *Client) RefreshToken(a *auth.Auth) error {
	// 第 1 段（锁内）：读快照。
	a.Lock()
	rtSnapshot := a.RefreshToken
	atBefore := a.AccessToken
	a.Unlock()
	if strings.TrimSpace(rtSnapshot) == "" {
		return fmt.Errorf("no refreshToken")
	}

	endpoint := c.chatBase(a) + "/v2/plugin/auth/token/refresh"
	ctx, cancel := context.WithTimeout(context.Background(), refreshIOTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	// RefreshHeaders 读取 a 的字段（domain/uid 等）注入请求头——需在锁内取快照值，
	// 用一个显式逐字段拷贝的临时 auth 构造头（不拷贝 sync.Mutex，避免 vet copies-lock）。
	a.Lock()
	hdrSnapshot := auth.Auth{
		AccessToken:  a.AccessToken,
		RefreshToken: rtSnapshot,
		ExpiresAt:    a.ExpiresAt,
		Domain:       a.Domain,
		UID:          a.UID,
		EnterpriseID: a.EnterpriseID,
		Nickname:     a.Nickname,
		DeviceToken:  a.DeviceToken,
	}
	a.Unlock()
	c.RefreshHeaders(req, &hdrSnapshot)

	// 网络 I/O（锁外，30s 上限）。
	reqWithAccount(req, a) // per-account 出站：把账号绑定到请求上下文
	data, err := c.doJSON(req)
	if err != nil {
		return err
	}
	var tok struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int64  `json:"expiresIn"`
		Domain       string `json:"domain"`
	}
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return fmt.Errorf("refresh_failed: no accessToken in response — re-login required")
	}

	// 第 2 段（锁内）：校验快照一致后写回。
	a.Lock()
	defer a.Unlock()
	if a.AccessToken != atBefore && a.RefreshToken != rtSnapshot {
		// 锁外期间另一 goroutine 已完成刷新：新 token 已生效，本次结果不必再写
		// （两个并发刷新拿到的新 token 都有效，后写会覆盖先写，但二者等价可用；
		// 提前返回避免无意义覆盖与 ExpiresAt 抖动）。
		return nil
	}
	a.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		a.RefreshToken = tok.RefreshToken
	}
	if tok.Domain != "" {
		a.Domain = tok.Domain
	}
	// preserveExpiry：响应缺 expiresIn 时保留旧过期时间，避免刷新风暴。
	if tok.ExpiresIn > 0 {
		a.ExpiresAt = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	}
	return nil
}

// ChatStream 发 chat 请求并返回原始 SSE body 流（调用方负责 Close）。
// 等价于 ChatStreamContext(context.Background(), ...)：不带调用方取消语义。
// 需要客户端断开联动的调用方用 ChatStreamContext 传入请求 ctx。
//
// global realm：先打 /console/chat/completions，404/405 时同一 base 二次换 /v2/chat/completions
// （上游新旧路径分叉，PLAN R9 fallback 顺序）。cn：/v2/chat/completions 现状不变。
func (c *Client) ChatStream(a *auth.Auth, body []byte, clientIP string, meta ChatMeta) (rc io.ReadCloser, status int, respBody []byte, err error) {
	return c.ChatStreamContext(context.Background(), a, body, clientIP, meta)
}

// ChatStreamContext 同 ChatStream，但从 ctx 派生请求 context：调用方（handler）传入
// r.Context() 后，客户端断连/请求取消会立即中断在途上游调用、释放连接与账号在途名额，
// 不再空转到 IdleTimeout。ctx 为 nil 时回落 Background。成功流的 cancel 仍由
// monitorBody 的 Close 接管（reqCtx 取消与显式 Close 任一触发即断）。
//
// 错误路径（≥400 且非 fallback 状态码）除 (status, respBody) 外还返回**已分类的**
// *Error（Kind 信封 + Retry-After 头解析）：客户端错误分类在此一次完成，handler
// 不再对 body 二次 Classify（消除「上游分类一次、网关再分类一次」的双路径漂移面），
// Retry-After 也随信封流动。respBody 仍原样返回（错误透传语义：message 透传上游
// 原文）。判定为 ErrNone 的响应（理论上不存在，防御）err 为 nil，handler 按
// respBody 自行兜底。
//
// ensureLeadingSystem 在 prepareBody 后统一套用：首条消息非 system 时前置兜底
// system（防上游 code 11-128 "first message is not system prompt"，所有 realm）。
func (c *Client) ChatStreamContext(ctx context.Context, a *auth.Auth, body []byte, clientIP string, meta ChatMeta) (rc io.ReadCloser, status int, respBody []byte, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// 首条消息 system 兜底对**所有 realm** 套用（不再只管 global）：
	// 上游要求 messages[0] 必须是 system，否则 400 code=11128
	// "first message is not system prompt"（实测见 ensureLeadingSystem）。
	prepared := c.prepareBody(body, a.Realm(), a.UID, meta.ConversationID)
	prepared = ensureLeadingSystem(prepared)
	// codex 系模型参数兼容（2026-09-27 实测）：global:gpt-5.4 / gpt-5.3-codex 对
	// max_tokens / max_completion_tokens 一律 400 11133 model_param_invalid，
	// 不带则 200。剥参代价 = 输出上限走模型默认，远好于整请求失败。
	prepared = stripUnsupportedParamsForModel(prepared, a.Realm())
	// reqCtx 的 cancel 在每个出口显式调用（Do 失败 / ≥400 / 成功分支移交 monitorBody），
	// 循环本身各分支必 return——无循环尾兜底代码（此前外层 var cancel 从未赋值 +
	// 尾部不可达 cancel() 是潜伏 nil-panic，已删；chatPaths 恒非空由构造保证）。
	for attempt, path := range c.chatPaths(a) {
		endpoint := c.chatBase(a) + path
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(prepared))
		if err != nil {
			return nil, 0, nil, err
		}
		c.ChatHeaders(req, a, clientIP, meta)
		// 从调用方 ctx 派生：保留取消传播（父 ctx 取消 → 本 ctx 取消），
		// 同时 monitorBody.Close 仍能独立 cancel 本分支（空闲掐流）。
		reqCtx, cancel := context.WithCancel(ctx)
		req = req.WithContext(reqCtx)
		resp, err := c.chatHTTPForA(a).Do(req)
		if err != nil {
			cancel()
			log.Printf("ERR: [upstream] chat_stream uid=%s realm=%s model=%s: transport error: %v",
				logfmt.UID8(a.UID), realmKey(a.Realm()), modelOf(prepared), err)
			// 传输层失败 → 清空共享连接池的空闲连接（连接层加固）：失败连接可能仍
			// 留在空闲池里，下一个请求会继续捡到它——仅靠 IdleConnTimeout 等过期
			// 不够，主动清池才断根。
			roundTripCloseIdle(c.chatHTTPForA(a).Transport)
			return nil, 0, nil, err
		}
		if resp.StatusCode >= 400 {
			raw, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			cancel()
			// body 读失败（掐流/截断）→ 传输层错误：半截 raw 不交回调用方进 Classify，
			// 否则 handler 侧 applyErrorPolicy 会按误判分类罚号。
			if rerr != nil {
				log.Printf("ERR: [upstream] chat_stream uid=%s realm=%s model=%s: read body: %v",
					logfmt.UID8(a.UID), realmKey(a.Realm()), modelOf(prepared), rerr)
				return nil, 0, nil, fmt.Errorf("read body: %w", rerr)
			}
			kind := Classify(resp.StatusCode, string(raw))
			log.Printf("WARN: [upstream] chat_stream uid=%s realm=%s model=%s: upstream %d %s body=%s",
				logfmt.UID8(a.UID), realmKey(a.Realm()), modelOf(prepared), resp.StatusCode, kind, truncate(string(raw), 200))
			// global 首次路径 404/405 或 **WAF 拦截页 403** → 换 fallback 路径重试
			// （见 chatPathFallback：WAF 规则只覆盖 /console，/v2 可直通）；其余状态码直接返回。
			if attempt < len(c.chatPaths(a))-1 && chatPathFallback(resp.StatusCode, raw) {
				log.Printf("WARN: [upstream] chat_stream uid=%s realm=%s model=%s: %s returned %d, retrying on %s",
					logfmt.UID8(a.UID), realmKey(a.Realm()), modelOf(prepared), path, resp.StatusCode,
					c.chatPaths(a)[attempt+1])
				continue
			}
			// 分类一次、随 Kind 信封返回（含 Retry-After 头解析）：
			// ErrNone 是防御分支（≥400 不应产生 None），返回原文让 handler 兜底。
			if kind == ErrNone {
				return nil, resp.StatusCode, raw, nil
			}
			ue := &Error{Kind: kind, Status: resp.StatusCode, Msg: truncate(string(raw), 200)}
			if d, ok := ParseRetryAfter(resp.Header); ok {
				ue.RetryAfter = d
			}
			return nil, resp.StatusCode, raw, ue
		}
		// 内容审核空流嗅探（contentfilter.go）：上游 200 但 finish_reason=content_filter
		// 且全程无正文时，原样透传会让客户端收到「空回复」（tok=0）且网关完全无感——
		// 这正是 global 域「模型调不通但没有任何报错」的现场。命中即按内容拦截返回
		// （Status 归 400，与 400 内容策略拦截同口径），账号不罚（内容问题非账号问题），
		// 由 handler 决定「换中性提示词在本请求内重试」还是「回明确错误」。
		// 未命中时 body 是回放式包装，对下游逐字节等价（见 sniffContentFilter）。
		body, filtered := sniffContentFilter(resp.Body)
		if filtered {
			resp.Body.Close()
			cancel()
			log.Printf("WARN: [upstream] chat_stream uid=%s: content_filter (finish_reason=%s, empty completion) -> treated as content blocked",
				logfmt.UID8(a.UID), contentFilteredFinish)
			return nil, http.StatusBadRequest, []byte(ContentFilteredBody), &Error{
				Kind:          ErrContentBlocked,
				Status:        http.StatusBadRequest,
				Msg:           ContentFilteredBody,
				ContentFilter: true,
			}
		}
		// 成功分支：cancel 所有权交给 monitorBody（其 Close 会 cancel）；
		// IdleTimeout<=0 时 monitorBody 原样返回底流、无人调 cancel——可接受：
		// 取消传播由 http.Transport 在 body Close / 父 ctx 取消时处理，连接正常清理。
		return monitorBody(body, c.IdleTimeout, cancel), resp.StatusCode, nil, nil
	}
	panic("unreachable: chatPaths is never empty") // for range 空集时编译器仍要求兜底 return；chatPaths 恒非空（构造保证），永不触达
}

// stripUnsupportedParamsForModel 剥掉特定模型不接受、会直接 400 的请求参数。
//
// 现状（2026-09-27 上游实测）：global 域 codex 系（gpt-5.3-codex / gpt-5.4）对
// max_tokens / max_completion_tokens 一律返回 400 code=11133 model_param_invalid，
// 两个参数去掉任何一个都仍然 11133，只有完全不带才 200。CN 域模型与 global 的
// glm/hy4/kimi 系实测无此限制。
// 2026-10-06 追加：global:deepseek-v4.1-flash 也开始拒收 max_tokens
// （48h 内 9 次 400 code=11133，17:49 一分钟内两号连撞；9-27 实测时还不拒，
// 上游行为变了——这份名单会随上游行为漂移，见此注释即加）。
//
// 规则：模型名含 codex / deepseek 或以 gpt- 开头 → 剥 max_tokens /
// max_completion_tokens。误剥的代价是「输出上限走模型默认上限」，远好于整个
// 请求被 400 打回；因此按名字宽匹配，而不是维护一张精确名单。
func stripUnsupportedParamsForModel(body []byte, realm string) []byte {
	var m map[string]any
	if json.Unmarshal(body, &m) != nil {
		return body
	}
	model, _ := m["model"].(string)
	if model == "" {
		return body
	}
	ml := strings.ToLower(model)
	if !strings.Contains(ml, "codex") && !strings.HasPrefix(ml, "gpt-") &&
		!strings.Contains(ml, "deepseek") {
		return body
	}
	changed := false
	stripKeys := []string{"max_tokens", "max_completion_tokens"}
	if strings.Contains(ml, "deepseek") {
		// 研究结论（simplast/workbuddy-api）：reasoning_effort 是正确参数，
		// thinking.type 可能不是正确参数名。只剥 thinking，保留 reasoning_effort。
		// 不分 realm：cn/global 都这么处理。
		stripKeys = append(stripKeys, "thinking")
	}
	for _, k := range stripKeys {
		if _, ok := m[k]; ok {
			delete(m, k)
			changed = true
		}
	}
	if !changed {
		return body
	}
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// ModelInfo 动态模型信息（含 maxInputTokens/maxOutputTokens + 上游模型对象全字段）。
// CN /console 与 global /v2 的模型对象同构，故共用此结构；上游省略的字段保持零值，
// /v1/models 侧按「空值省略」透出（不编造）。
type ModelInfo struct {
	ID            string
	Name          string
	ContextWindow int64    // = maxInputTokens
	MaxTokens     int64    // = maxOutputTokens（思考与最终回答共享此预算，上游无独立思考上限字段）
	Efforts       []string // reasoning.supportedEfforts（空=未知/固定档）
	DefaultEffort string   // reasoning.defaultEffort（新模型键）或 reasoning.effort（老模型键）；空=未返回

	// 模型目录全字段（models-full-fields）：
	Description        string   // descriptionZh 中文描述
	Credits            string   // credits 积分倍率原文（如 "x0.05"），仅展示不参与选号
	Tags               []string // tags 模型标签（含 badge:限时免费 等）
	Vendor             string   // vendor 厂商标识
	IsDefault          bool     // isDefault 是否默认模型
	SupportsReasoning  bool     // supportsReasoning 是否支持推理
	SupportsToolCall   bool     // supportsToolCall 是否支持工具调用
	OnlyReasoning      bool     // onlyReasoning 是否纯推理模型
	SupportsImages     bool     // 顶层 supportsImages（多模态能力，透出到 /v1/models）
	MaxAllowedSize     int64    // maxAllowedSize 最大允许上下文（与 maxInputTokens 口径并列，上游各自下发）
	CanDisableThinking bool     // reasoning.canDisableThinking：思考可关（off 档可用）
	ReasoningEffort    string   // reasoning.effort 推理模式（与 supportedEfforts 数组不同源）
	ReasoningSummary   string   // reasoning.summary 推理摘要模式（如 "auto"）
}

// dynModelEntry 上游模型目录（CN /console 与 global /v2 同构）的单条模型解析形态，
// FetchModels 与 global_models.go 的探测共用。iconUrl/descriptionEn/生成参数等
// 按「不透出」原则不解析。modelInfo() 是 dynEntry→ModelInfo 映射的单一事实来源，
// 杜绝两域映射漂移。
type dynModelEntry struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Description     string   `json:"descriptionZh"`
	Credits         string   `json:"credits"`
	Tags            []string `json:"tags"`
	Vendor          string   `json:"vendor"`
	IsDefault       bool     `json:"isDefault"`
	MaxInputTokens  int64    `json:"maxInputTokens"`
	MaxOutputTokens int64    `json:"maxOutputTokens"`
	MaxAllowedSize  int64    `json:"maxAllowedSize"`
	Disabled        bool     `json:"disabled"`
	SupportsImages  bool     `json:"supportsImages"`
	SupportsReason  bool     `json:"supportsReasoning"`
	SupportsTool    bool     `json:"supportsToolCall"`
	OnlyReasoning   bool     `json:"onlyReasoning"`
	Reasoning       struct {
		Effort             string   `json:"effort"`
		Summary            string   `json:"summary"`
		DefaultEffort      string   `json:"defaultEffort"`
		CanDisableThinking bool     `json:"canDisableThinking"`
		SupportedEfforts   []string `json:"supportedEfforts"`
	} `json:"reasoning"`
}

// modelInfo 按解析条目构造 ModelInfo（dynEntry→ModelInfo 映射的单一事实来源）。
// defaultEffort 新老双键兼容：defaultEffort 优先，缺省回落 effort。
func (m dynModelEntry) modelInfo() ModelInfo {
	def := m.Reasoning.DefaultEffort
	if def == "" {
		def = m.Reasoning.Effort
	}
	return ModelInfo{
		ID:                 m.ID,
		Name:               m.Name,
		ContextWindow:      m.MaxInputTokens,
		MaxTokens:          m.MaxOutputTokens,
		Efforts:            m.Reasoning.SupportedEfforts,
		DefaultEffort:      def,
		SupportsImages:     m.SupportsImages,
		Description:        m.Description,
		Credits:            normalizeCredits(m.Credits),
		Tags:               m.Tags,
		Vendor:             m.Vendor,
		IsDefault:          m.IsDefault,
		SupportsReasoning:  m.SupportsReason,
		SupportsToolCall:   m.SupportsTool,
		OnlyReasoning:      m.OnlyReasoning,
		MaxAllowedSize:     m.MaxAllowedSize,
		CanDisableThinking: m.Reasoning.CanDisableThinking,
		ReasoningEffort:    m.Reasoning.Effort,
		ReasoningSummary:   m.Reasoning.Summary,
	}
}

// normalizeCredits 归一化上游 credits 倍率原文。
//
// 上游同一份目录里格式都不统一（实测 2026-09）：/v3/config 用 IDE UA 时返回纯
// "x0.79"，用 WorkBuddy UA 时部分条目返回 "x0.34 credits"；企业端点
// （/v2/enterprises/personal/models）整表带 " credits" 后缀。不归一化会让
// /v1/models 透出 "x0.77 credits" 这类脏值（面板与客户端解析倍率时踩坑）。
// 只裁后缀、不改数值，空值保持空（不编造）。
func normalizeCredits(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if v, ok := strings.CutSuffix(s, "credits"); ok {
		s = v
	} else if v, ok := strings.CutSuffix(s, "credit"); ok {
		s = v
	}
	return strings.TrimSpace(s)
}

// nonChatModel 判定是否非对话模型（应从模型列表过滤掉）。
// 来源：harness buddy.ts:547-555。三类规则：
//   - id 前缀 nes-/completion-/codewise-：嵌入/补全/代码专用模型，选了报 code=11102。
//   - maxOutputTokens ≤ 256：tiny 输出非对话模型。
//   - tags 含 text-to-image：图片生成模型，非本网关用途。
func nonChatModel(id string, maxOutputTokens int64, tags []string) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	for _, p := range [...]string{"nes-", "completion-", "codewise-"} {
		if strings.HasPrefix(id, p) {
			return true
		}
	}
	if maxOutputTokens > 0 && maxOutputTokens <= 256 {
		return true
	}
	for _, t := range tags {
		if t == "text-to-image" {
			return true
		}
	}
	return false
}

// codeBuddyIDEUA /v3/config 要求能解析出 CodeBuddy 版本号的 UA。
// CLI 三段式 WorkBuddy UA 会拿到精简目录（flash 输出 128K、无 supportedEfforts）；
// 官方 IDE 头 `CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0` 才返回完整能力
// （flash：393216 + low/high/max）。
// 版本号需随上游 IDE 发版跟进：UAn 版本过旧时该端点可能同样返回精简目录。
const codeBuddyIDEUA = "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0"

// FetchModels 调上游动态模型接口（CN 侧；global 账号见 global_models.go 家族）。
//
// v3-config-merge：动态目录 = /v3/config（主，IDE UA 完整能力版）+ 企业端点
// （/console，cli 面过滤，补缺）的并集，两路**并发**探测。合并去重 key = 模型 id，
// v3 条目优先（credits 等字段以 v3 为准），企业端点只补 v3 缺失的模型。
// /v3 失败（400/网络错/解析失败）不拖累企业端点结果——降级为仅企业端点，warn 日志；
// 反之亦然（两路独立容错）。
func (c *Client) FetchModels(a *auth.Auth) ([]ModelInfo, error) {
	type probeResult struct {
		infos []ModelInfo
		err   error
	}
	enterpriseCh := make(chan probeResult, 1)
	v3Ch := make(chan probeResult, 1)
	go func() {
		infos, err := c.fetchEnterpriseModels(a)
		enterpriseCh <- probeResult{infos, err}
	}()
	go func() {
		infos, err := c.fetchV3Models(a)
		v3Ch <- probeResult{infos, err}
	}()
	enterprise := <-enterpriseCh
	v3 := <-v3Ch
	if enterprise.err != nil && v3.err != nil {
		return nil, enterprise.err // 两路全失败：返回企业端点错误（既有调用方语义零漂移）
	}
	if v3.err != nil {
		// /v3 失败降级：不拖累企业端点结果（降级仅企业端点 + warn）。
		log.Printf("WARN: [upstream] fetch models: v3/config probe failed (degraded to enterprise endpoint): %v", v3.err)
	}
	if enterprise.err != nil {
		log.Printf("WARN: [upstream] fetch models: enterprise endpoint failed (v3/config only): %v", enterprise.err)
	}
	out := mergeModelInfos(v3.infos, enterprise.infos)
	if len(out) == 0 {
		return nil, fmt.Errorf("models api returned empty list")
	}
	// 刷新 effort 能力缓存（供请求体降级；无 supportedEfforts 的模型不入桶）。
	// 空桶时跳过写：避免「某探测无档位数据」清掉既有桶。
	cache := make(map[string][]string, len(out))
	defCache := make(map[string]string, len(out))
	for _, mi := range out {
		if len(mi.Efforts) > 0 {
			cache[mi.ID] = mi.Efforts
		}
		if mi.DefaultEffort != "" {
			defCache[mi.ID] = mi.DefaultEffort
		}
	}
	if len(cache) == 0 && len(defCache) == 0 {
		return out, nil
	}
	// 按探测账号的 realm 写入对应桶：CN 探测只进 cn 桶，global 同模型名不被污染（C-2）。
	c.storeEfforts(a.Realm(), cache, defCache)
	return out, nil
}

// mergeModelInfos 合并两路模型目录：primary 为主（同 id 以 primary 条目为权威），
// secondary 既补 primary 缺失的 id，也补 primary 条目里的**空字段**
// （fillModelInfo：权威值不被动，只填空）。
//
// 为什么不再"同 id 直接丢弃 secondary"：/v3/config 与企业端点的字段完整度互有胜负——
// 例：global 的 default-model 在 v3 里 credits 为空、在企业端点里是 x0.79；旧口径
// 会让倍率列凭空少值（面板/客户端显示空白）。
// 去重 key = 模型 id；输出顺序 = primary 原序在前、secondary 补充项（secondary 原序）
// 在后——稳定输出，不依赖 map 迭代序。
func mergeModelInfos(primary, secondary []ModelInfo) []ModelInfo {
	if len(secondary) == 0 {
		return primary
	}
	idx := make(map[string]int, len(primary)+len(secondary))
	out := make([]ModelInfo, 0, len(primary)+len(secondary))
	for _, mi := range primary {
		if mi.ID == "" {
			continue
		}
		if i, ok := idx[mi.ID]; ok {
			out[i] = fillModelInfo(out[i], mi)
			continue
		}
		idx[mi.ID] = len(out)
		out = append(out, mi)
	}
	for _, mi := range secondary {
		if mi.ID == "" {
			continue
		}
		if i, ok := idx[mi.ID]; ok {
			out[i] = fillModelInfo(out[i], mi)
			continue
		}
		idx[mi.ID] = len(out)
		out = append(out, mi)
	}
	return out
}

// fetchEnterpriseModels 单路探测企业模型端点（/console/enterprises/personal/models）。
// 解析口径：agents[cli].models 过滤 + nonChatModel 剔除 + disabled 剔除。
func (c *Client) fetchEnterpriseModels(a *auth.Auth) ([]ModelInfo, error) {
	// 局部变量名避开 url（本包已 import net/url，同名会造成阅读混淆）。
	endpoint := c.chatBase(a) + "/console/enterprises/personal/models"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.CommonHeaders(req, a) // 复用共享请求头（Origin/Referer/UA/Accept/Content-Type）
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	// 出站走 realm 感知 client：global 账号必须经 global 出口池（国际版上游对网关
	// 本机出口 IP 做 WAF 风控，直连会拿到 403 拦截页 / 500，模型目录随之整体拉不出）。
	// 与 globalModelsOnce 同口径；cn 账号 httpFor 返回 c.HTTP，行为零变化。
	resp, err := c.httpForA(a).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		// 读失败 → 传输层错误（handler 侧该路径不 NoteError）。
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("models api status %d: %s", resp.StatusCode, truncate(string(raw), 120))
	}
	var env struct {
		Code int `json:"code"`
		Data struct {
			Models []dynModelEntry `json:"models"`
			Agents []struct {
				Name   string   `json:"name"`
				Models []string `json:"models"`
			} `json:"agents"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("models parse: %w", err)
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("models api code=%d", env.Code)
	}
	var cliIDs []string
	for _, ag := range env.Data.Agents {
		if ag.Name == "cli" {
			cliIDs = ag.Models
			break
		}
	}
	if len(cliIDs) == 0 {
		return nil, fmt.Errorf("no cli agent models found")
	}
	// dynMap 收集模型字段；nonChatModel 过滤在写入 dynMap 前执行，
	// 确保非对话条目（nes-/completion-/codewise- 前缀、maxOutputTokens≤256、
	// tags 含 text-to-image）根本不进返回列表（来源：harness buddy.ts:547-555）。
	dynMap := make(map[string]dynModelEntry, len(env.Data.Models))
	for _, m := range env.Data.Models {
		if nonChatModel(m.ID, m.MaxOutputTokens, m.Tags) {
			continue
		}
		dynMap[m.ID] = m
	}
	out := make([]ModelInfo, 0, len(cliIDs))
	for _, id := range cliIDs {
		if m, ok := dynMap[id]; ok && !m.Disabled {
			out = append(out, m.modelInfo())
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("models api returned empty list")
	}
	return out, nil
}

// fetchV3Models 单路探测 /v3/config（IDE UA 完整能力版，见 codeBuddyIDEUA）。
// v3 面取全量 models（不按 agents[cli] 过滤，与 global 探测口径一致），按同一
// nonChatModel 规则剔除非对话条目（selected 会选模型报 code=11102）。
// 失败返回错误（调用方降级为仅企业端点）。
func (c *Client) fetchV3Models(a *auth.Auth) ([]ModelInfo, error) {
	byID, err := c.fetchV3ConfigModelMap(a)
	if err != nil {
		return nil, err
	}
	out := make([]ModelInfo, 0, len(byID))
	for _, mi := range byID {
		if nonChatModel(mi.ID, mi.MaxTokens, mi.Tags) {
			continue
		}
		out = append(out, mi)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("v3/config returned empty models")
	}
	return out, nil
}

// storeEfforts 按 realm 写入 effort 能力缓存桶（efforts + defaultEfforts），并发安全。
// 供 CN FetchModels 与 global 探测共用：拉取到的模型档位落桶后，出站请求体
// normalizeReasoningEffort 才能按域降级。efforts 与 defs 均空时删除该 realm 桶
// （等价「该域无可降级档位」）。调用方负责在「无新数据」时跳过写。
func (c *Client) storeEfforts(realm string, efforts map[string][]string, defs map[string]string) {
	c.effortsMu.Lock()
	defer c.effortsMu.Unlock()
	if c.efforts == nil {
		c.efforts = make(map[string]map[string][]string)
	}
	if c.defaultEfforts == nil {
		c.defaultEfforts = make(map[string]map[string]string)
	}
	k := realmKey(realm)
	if len(efforts) == 0 && len(defs) == 0 {
		delete(c.efforts, k)
		delete(c.defaultEfforts, k)
		return
	}
	c.efforts[k] = efforts
	c.defaultEfforts[k] = defs
}

// GlobalEffortSnapshot 导出 global 域 effort 能力缓存（探测下发 ∪ 静态兜底合并后的桶），
// 供 /v1/models 输出 reasoning_supported_efforts / reasoning_default_effort。
// 返回副本；桶未填充（无 global 账号或从未探测）→ nil（调用方回落静态兜底表）。
func (c *Client) GlobalEffortSnapshot() (efforts map[string][]string, defaults map[string]string) {
	return c.effortsSnapshot("global"), c.defaultEffortsSnapshot("global")
}

// v3ConfigDomain /v3/config 的 X-Domain：优先账号落盘 domain，否则 chatBase host。
func v3ConfigDomain(a *auth.Auth, chatBase string) string {
	if a != nil {
		if d := strings.TrimSpace(a.Domain); d != "" {
			d = strings.TrimPrefix(d, "https://")
			d = strings.TrimPrefix(d, "http://")
			return strings.TrimSuffix(d, "/")
		}
	}
	if u, err := url.Parse(chatBase); err == nil && u.Host != "" {
		return u.Host
	}
	return "copilot.tencent.com"
}

// fetchV3ConfigModelMap 拉官方配置目录，按模型 id 建能力表（双 UA 并发并集）。
//
// UA 敏感（实测 2026-09）：/v3/config 按 UA 分档下发目录，且当前方向与早先
// 注释里的假设**相反**——
//   - `CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0`：精简目录（global 13 条 / CN 22 条）
//   - `WorkBuddy/<ver> <platform>/<ver> CLI/<ver>`：完整目录（global 22 条 / CN 51 条）
//
// 官方桌面客户端实际用的是后者。此前写死 IDE UA，导致 deepseek-v4.1-flash、
// deepseek-v4.1-flash-sg、gpt-6-astra、hy4-preview、kimi-k2.8-preview、
// minimax-m2.5 等模型在 /v1/models 里整体缺失（客户端能看到、网关看不到）。
// 两路并发取并集：任一 UA 因上游再次改版退化时，另一路仍能兜住完整目录。
// 无版本号的 UA（curl/Mozilla）→ 400 code=12403，故不做无 UA 尝试。
func (c *Client) fetchV3ConfigModelMap(a *auth.Auth) (map[string]ModelInfo, error) {
	// 顺序即优先级：WorkBuddy 三段式（官方桌面端真实形态）作基底，IDE UA 只补前者缺失。
	uas := []string{c.userAgent(a), codeBuddyIDEUA}
	type probeRes struct {
		m   map[string]ModelInfo
		err error
	}
	ch := make(chan probeRes, len(uas))
	for _, ua := range uas {
		go func(ua string) {
			m, err := c.fetchV3ConfigOnce(a, ua)
			ch <- probeRes{m: m, err: err}
		}(ua)
	}
	merged := make(map[string]ModelInfo)
	var lastErr error
	okCount := 0
	for range uas {
		r := <-ch
		if r.err != nil {
			lastErr = r.err
			continue
		}
		okCount++
		for id, mi := range r.m {
			if prev, seen := merged[id]; seen {
				merged[id] = fillModelInfo(prev, mi)
			} else {
				merged[id] = mi
			}
		}
	}
	if okCount == 0 {
		return nil, lastErr
	}
	if len(merged) == 0 {
		return nil, fmt.Errorf("v3/config returned empty models")
	}
	return merged, nil
}

// fillModelInfo 用 src 的非空字段填补 dst 的空字段（dst 已有值不动），布尔能力位取或。
//
// 用于多源目录合并（双 UA / v3 × 企业端点）：dst 是权威源，src 只做补缺。
// 关键是不能让「精简源的零值」抹掉「完整源的有效值」——例：global 的
// default-model 在 /v3/config 里 credits 为空、在企业端点里是 x0.79，旧口径
// （同 id 只认主源）会把 x0.79 丢掉，倍率列显示为空。
// 布尔位取或是刻意宽松：任一路声明支持即视为支持，不因精简目录丢能力位。
func fillModelInfo(dst, src ModelInfo) ModelInfo {
	if dst.Name == "" {
		dst.Name = src.Name
	}
	if dst.ContextWindow == 0 {
		dst.ContextWindow = src.ContextWindow
	}
	if dst.MaxTokens == 0 {
		dst.MaxTokens = src.MaxTokens
	}
	if dst.MaxAllowedSize == 0 {
		dst.MaxAllowedSize = src.MaxAllowedSize
	}
	if len(dst.Efforts) == 0 {
		dst.Efforts = src.Efforts
	}
	if dst.DefaultEffort == "" {
		dst.DefaultEffort = src.DefaultEffort
	}
	if dst.Description == "" {
		dst.Description = src.Description
	}
	if dst.Credits == "" {
		dst.Credits = src.Credits
	}
	if len(dst.Tags) == 0 {
		dst.Tags = src.Tags
	}
	if dst.Vendor == "" {
		dst.Vendor = src.Vendor
	}
	if dst.ReasoningEffort == "" {
		dst.ReasoningEffort = src.ReasoningEffort
	}
	if dst.ReasoningSummary == "" {
		dst.ReasoningSummary = src.ReasoningSummary
	}
	dst.IsDefault = dst.IsDefault || src.IsDefault
	dst.SupportsReasoning = dst.SupportsReasoning || src.SupportsReasoning
	dst.SupportsToolCall = dst.SupportsToolCall || src.SupportsToolCall
	dst.OnlyReasoning = dst.OnlyReasoning || src.OnlyReasoning
	dst.SupportsImages = dst.SupportsImages || src.SupportsImages
	dst.CanDisableThinking = dst.CanDisableThinking || src.CanDisableThinking
	return dst
}

// fetchV3ConfigOnce 用指定 UA 拉一次 /v3/config，按模型 id 建能力表。
// 该端点对 UA 敏感：必须带 CodeBuddy/CodeBuddyIDE 版本，否则 400 code=12403。
func (c *Client) fetchV3ConfigOnce(a *auth.Auth, ua string) (map[string]ModelInfo, error) {
	req, err := http.NewRequest(http.MethodGet, c.chatBase(a)+"/v3/config", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	if a != nil && a.UID != "" {
		req.Header.Set("X-User-Id", a.UID)
	}
	req.Header.Set("X-Domain", v3ConfigDomain(a, c.chatBase(a)))
	req.Header.Set("X-Product", "SaaS")
	req.Header.Set("User-Agent", ua)
	c.injectCodeBuddyRequest(req)
	// realm 感知出站：global 走 global 出口池（见 fetchEnterpriseModels 注释）。
	resp, err := c.httpForA(a).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		// 读失败 → 传输层错误：半截 body 不进解析（不罚号）。
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("v3/config status %d: %s", resp.StatusCode, truncate(string(raw), 120))
	}
	var env struct {
		Code int `json:"code"`
		Data struct {
			Models []dynModelEntry `json:"models"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("v3/config parse: %w", err)
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("v3/config code=%d", env.Code)
	}
	out := make(map[string]ModelInfo, len(env.Data.Models))
	for _, m := range env.Data.Models {
		if strings.TrimSpace(m.ID) == "" {
			continue
		}
		out[m.ID] = m.modelInfo()
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("v3/config returned empty models")
	}
	return out, nil
}

// UserResource 查询账号积分余额与总额度（所有套餐聚合）。remain 负值钳 0；
// total 取与 remain 同源的额度字段（CycleCapacitySize 优先，无周期额度退
// CapacitySize），上游缺 size 的套餐按 remain 兜底，保证百分比不超 100%。
// CreditPackage 单个积分包的构成明细（面板「积分构成」用）。
//
// 两个账号即使任务完成度完全一致，余额也可能相差上千——差别藏在包的**面额与
// 来源**里（「国内运营裂变包」「拉新权益包」按次发放，面额 6~1500 不等）。
// 只看聚合值看不出这件事，所以把逐包明细暴露出来。
type CreditPackage struct {
	Name   string `json:"name"`
	Remain int64  `json:"remain"`
	Used   int64  `json:"used"`
	Size   int64  `json:"size"`
	// EndTime 该包的周期结束时间（上游 ExpiredTime / PackageEndTime 二者取有值者）。
	EndTime string `json:"end_time,omitempty"`
	// CreatedAt 发放时刻，RFC3339。**这是区分「首登赠送」与「活动奖励」的唯一依据**：
	// 两类包的 PackageName 与 PackageCode 完全相同（例如都是「国内运营裂变包」+
	// TCACA_code_007_*），只看名字无法区分，只有时间能说明它是不是账号首次授权那刻发的。
	CreatedAt string `json:"created_at,omitempty"`
	// PackageCode / SubProductCode 上游的包类型标识。同 Name 不同 Code 的包可能
	// 是不同来源；同 Code 不同面额则是同来源分批发放（首登 1500 与活动 300 即如此）。
	PackageCode    string `json:"package_code,omitempty"`
	SubProductCode string `json:"sub_product_code,omitempty"`
	SubProductName string `json:"sub_product_name,omitempty"`
	// Cycle 为 true 表示按周期发放的包（读 Cycle* 字段），否则读 Capacity*。
	Cycle bool `json:"cycle,omitempty"`
}

// CreditPackages 返回账号当前的逐包构成。remain/size 为各包求和。
//
// 字段选择与 UserResourceDetailed 的聚合口径一致：CycleCapacitySize > 0 时按
// 周期字段算，否则按 Capacity 字段算——两条路径不能混，否则同一个包会被算两次。
func (c *Client) CreditPackages(a *auth.Auth) ([]CreditPackage, int64, int64, error) {
	now := time.Now()
	body := map[string]any{
		"PageNumber":               1,
		"PageSize":                 100,
		"ProductCode":              "p_tcaca",
		"Status":                   []int{0, 3},
		"PackageEndTimeRangeBegin": now.Format(packageEndLayout),
		"PackageEndTimeRangeEnd":   now.Add(365 * 101 * 24 * time.Hour).Format(packageEndLayout),
	}
	data, err := c.billingMeterJSON(a, c.billingMeterPaths(a), http.MethodPost, body)
	if err != nil {
		return nil, 0, 0, err
	}
	// 注意层级：doJSON 已经解过 apiEnvelope 并返回 env.Data，所以这里从
	// Response 开始解析——**不能**再套一层 Code/Data，否则 Accounts 恒为空，
	// 表现为「每个号都 0 个包」（实测踩过）。
	var resp struct {
		Response struct {
			Data struct {
				Accounts []struct {
					PackageName         string `json:"PackageName"`
					CapacityRemain      int64  `json:"CapacityRemain"`
					CapacityUsed        int64  `json:"CapacityUsed"`
					CapacitySize        int64  `json:"CapacitySize"`
					CycleCapacityRemain int64  `json:"CycleCapacityRemain"`
					CycleCapacityUsed   int64  `json:"CycleCapacityUsed"`
					CycleCapacitySize   int64  `json:"CycleCapacitySize"`
					// 到期时间字段名在上游同时存在两种口径，都读，谁有值用谁。
					ExpiredTime    string `json:"ExpiredTime"`
					PackageEndTime string `json:"PackageEndTime"`
					// 发放时刻（epoch 毫秒）。
					CreateTime     int64  `json:"CreateTime"`
					PackageCode    string `json:"PackageCode"`
					SubProductCode string `json:"SubProductCode"`
					SubProductName string `json:"SubProductName"`
				} `json:"Accounts"`
			} `json:"Data"`
		} `json:"Response"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, 0, 0, fmt.Errorf("packages parse: %w", err)
	}
	packs := resp.Response.Data.Accounts
	out := make([]CreditPackage, 0, len(packs))
	var sumRemain, sumSize int64
	for _, p := range packs {
		cp := CreditPackage{
			Name:           p.PackageName,
			PackageCode:    p.PackageCode,
			SubProductCode: p.SubProductCode,
			SubProductName: p.SubProductName,
		}
		if p.ExpiredTime != "" {
			cp.EndTime = p.ExpiredTime
		} else {
			cp.EndTime = p.PackageEndTime
		}
		// CreateTime 是 epoch 毫秒；0 表示上游没给，留空而不是伪造 1970。
		if p.CreateTime > 0 {
			cp.CreatedAt = time.UnixMilli(p.CreateTime).Format(time.RFC3339)
		}
		if p.CycleCapacitySize > 0 {
			cp.Cycle = true
			cp.Remain, cp.Size = p.CycleCapacityRemain, p.CycleCapacitySize
			cp.Used = cp.Size - cp.Remain
			if p.CycleCapacityUsed > cp.Used {
				cp.Used = p.CycleCapacityUsed
				cp.Remain = cp.Size - cp.Used
			}
			if cp.Remain < 0 {
				cp.Remain = 0
			}
		} else {
			cp.Remain, cp.Used, cp.Size = p.CapacityRemain, p.CapacityUsed, p.CapacitySize
			if cp.Used == 0 && cp.Size > cp.Remain {
				cp.Used = cp.Size - cp.Remain
			}
		}
		sumRemain += cp.Remain
		sumSize += cp.Size
		out = append(out, cp)
	}
	// 面额降序：大包一眼可见，正是差异最可能出现的地方。
	sort.SliceStable(out, func(i, j int) bool { return out[i].Size > out[j].Size })
	return out, sumRemain, sumSize, nil
}

func (c *Client) UserResource(a *auth.Auth) (remain, total int64, err error) {
	remain, total, _, err = c.UserResourceDetailed(a, 0)
	return remain, total, err
}

// packageEndLayout 上游套餐到期时间的墙钟格式（UTC+8，与 softRateResetLoc 同口径）。
const packageEndLayout = "2006-01-02 15:04:05"

// UserResourceDetailed 在 UserResource 基础上额外返回「快过期」积分子集：
// soon > 0 且套餐 PackageEndTime 解析成功且到期时刻 ≤ now+soon 的余额计入 expiring
// （pool 据此优先消耗，避免官方活动赠送的奖励积分到期作废）；soon ≤ 0 时 expiring
// 恒 0（禁用分桶，行为与引入前一致）。expiring 是 remain 的一部分。
func (c *Client) UserResourceDetailed(a *auth.Auth, soon time.Duration) (remain, total, expiring int64, err error) {
	now := time.Now()
	body := map[string]any{
		"PageNumber":               1,
		"PageSize":                 100,
		"ProductCode":              "p_tcaca",
		"Status":                   []int{0, 3},
		"PackageEndTimeRangeBegin": now.Format(packageEndLayout),
		"PackageEndTimeRangeEnd":   now.Add(365 * 101 * 24 * time.Hour).Format(packageEndLayout),
	}
	data, err := c.billingMeterJSON(a, c.billingMeterPaths(a), http.MethodPost, body)
	if err != nil {
		return 0, 0, 0, err
	}
	var resp struct {
		Response struct {
			Data struct {
				Accounts []struct {
					PackageName         string `json:"PackageName"`
					PackageEndTime      string `json:"PackageEndTime"` // "2006-01-02 15:04:05"，缺省/空 = 无到期
					CapacitySize        int64  `json:"CapacitySize"`
					CapacityRemain      int64  `json:"CapacityRemain"`
					CapacityUsed        int64  `json:"CapacityUsed"`
					CycleCapacitySize   int64  `json:"CycleCapacitySize"`
					CycleCapacityRemain int64  `json:"CycleCapacityRemain"`
					CycleCapacityUsed   int64  `json:"CycleCapacityUsed"`
				} `json:"Accounts"`
			} `json:"Data"`
		} `json:"Response"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, 0, 0, fmt.Errorf("resource parse: %w", err)
	}
	for _, acct := range resp.Response.Data.Accounts {
		var r, size int64
		switch {
		case acct.CycleCapacitySize > 0:
			r, size = acct.CycleCapacityRemain, acct.CycleCapacitySize
		case acct.CycleCapacityRemain > 0 || acct.CycleCapacityUsed > 0:
			r, size = acct.CycleCapacityRemain, acct.CycleCapacitySize
		default:
			r, size = acct.CapacityRemain, acct.CapacitySize
		}
		if r < 0 {
			r = 0
		}
		if size < r {
			size = r
		}
		remain += r
		total += size
		// 分桶：仅 soon>0 且能解析出有效到期时间、且确实在窗口内 → expiring。
		if soon > 0 && r > 0 && acct.PackageEndTime != "" {
			if end, perr := time.ParseInLocation(packageEndLayout, acct.PackageEndTime, softRateResetLoc); perr == nil {
				if !end.After(now.Add(soon)) {
					expiring += r
				}
			}
		}
	}
	return remain, total, expiring, nil
}

// DailyCheckin 执行每日签到。已签到（业务 code 非 0）也返回错误，调用方按 msg 区分。
func (c *Client) DailyCheckin(a *auth.Auth) error {
	_, err := c.billingMeterJSON(a, c.checkinMeterPaths(a), http.MethodPost, map[string]any{})
	return err
}

// IsAlreadyCheckin 报告 err 是否表示"今天已签到"（上游幂等拒绝重复签到）。
// 只认带分类的 *Error（业务 code 或 HTTP 错误）：网络层/解析层错误不得当作幂等成功，
// 否则停机补签遇到抖动会误记为 already，账号当天实际未签到却被判定正常。
func IsAlreadyCheckin(err error) bool {
	var ue *Error
	if !errors.As(err, &ue) {
		return false
	}
	for _, m := range alreadyCheckinMarkers {
		if strings.Contains(ue.Msg, m) || strings.Contains(strings.ToLower(ue.Msg), strings.ToLower(m)) {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n]
	}
	return s
}
