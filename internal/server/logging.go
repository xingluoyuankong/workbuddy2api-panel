// logging.go 请求级表格日志：每个 /v1/chat/completions 请求结束后打印一行到 stdout。
package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
)

// chatSeq 进程级请求序号。
var chatSeq atomic.Int64

// chatLogEnabled 聊天表格日志总开关。生产恒 true；
// 测试包经 TestMain 置 false 关闭 stdout 噪音，需要断言行输出的测试用 withChatLog 临时开启（R5）。
var chatLogEnabled = true

// chatLogOut 聊天表格日志的输出目标。生产默认 os.Stdout；main 在启用管理面板时
// 经 SetChatLogOutput 注入 MultiWriter，把每行镜像进 /panel/api/logs 的环形缓冲，
// stdout 行为不变。需在开始服务前调用一次（无并发竞争窗口）。
var chatLogOut io.Writer = os.Stdout

// SetChatLogOutput 替换聊天表格日志输出目标（仅 main 启动期调用一次）。
func SetChatLogOutput(w io.Writer) { chatLogOut = w }

// chatStat 单个 chat 请求的日志统计；handler 挂 defer，请求出口后落一行。
type chatStat struct {
	start  time.Time
	model  string
	mode   string // "stream" | "sync"
	uid    string // 完整 uid，展示时只取前 8 位
	ttfb   time.Duration
	toks   int // <0 表示 usage 缺失 → 显示 "-"
	status int
	// credit 本次请求实际消耗的工作积分（上游 usage.credit，权威口径）。
	// hasCredit=false 表示上游没下发（失败请求/旧上游），日志显示 "-"，
	// 与"实测为 0（免费模型）"区分开。
	credit    float64
	hasCredit bool
	tries     int     // 本请求实际尝试的账号数（含换号重试）
	tokps     float64 // 解码期吐字速度（分母剔除首包等待），hasTokps=false 时回落旧口径
	hasTokps  bool

	logged bool
}

// newChatStat 以请求进入 handler 的时刻为起点构造统计对象；toks 默认 -1（usage 缺失）。
func newChatStat(now time.Time, body []byte, stream bool) *chatStat {
	return newChatStatWithModel(now, parseModelFromBody(body), stream)
}

// newChatStatWithModel 用**已知的** model/stream 构造统计对象。
//
// 给已经 peek 过 model 的路径用（413 超限分支）：那里 body 可能几十 MB，
// 再调一次 parseModelFromBody 等于为一个日志字段白解析整份 JSON。
func newChatStatWithModel(now time.Time, model string, stream bool) *chatStat {
	mode := "sync"
	if stream {
		mode = "stream"
	}
	return &chatStat{start: now, model: model, mode: mode, toks: -1}
}

// done 幂等落一行表格日志。
func (s *chatStat) done() {
	if s.logged {
		return
	}
	s.logged = true
	logChatRow(s.ttfb, time.Since(s.start), s.model, s.mode, s.uid, s.status, s.toks, s.credit, s.hasCredit,
		s.tries, s.tokps, s.hasTokps)
}

// chatStatsReader 在流式透传时抓取 SSE 末帧的 usage.completion_tokens 精确值，
// 并记录首个 data 帧的 TTFB；原始字节原样返回给下游透传。
// 注意：不做 rune 估算，token 数一律采信上游 usage。
type chatStatsReader struct {
	br                  *bufio.Reader
	start               time.Time
	ttfb                time.Duration
	seen                bool // 已见过首个 data 帧（TTFB 只记一次）
	promptTokens        int
	completionTokens    int
	totalTokens         int
	hasPromptTokens     bool
	hasCompletionTokens bool
	hasTotalTokens      bool
	// credit 上游末帧 usage.credit（本次真实扣费积分），供成本台账（NoteModelCost）。
	hasCredit bool
	credit    float64
	// cacheHitTokens 上游末帧 usage.prompt_cache_hit_tokens（前缀缓存命中 tokens），
	// 供用量统计的账号缓存率。hasCacheHitTokens=false 表示上游没下发该字段。
	hasCacheHitTokens bool
	cacheHitTokens    int
	pend              []byte // 已读未返回的行缓存
}

// newChatStatsReaderSince 以 since 为 TTFB 计时起点（通常是请求进入 handler 的时刻）。
func newChatStatsReaderSince(r io.Reader, since time.Time) *chatStatsReader {
	return &chatStatsReader{br: bufio.NewReaderSize(r, 64*1024), start: since}
}

// TTFB 返回首个 data 帧到达耗时；无帧时为 0。
func (s *chatStatsReader) TTFB() time.Duration { return s.ttfb }

// Tokens 返回末帧 usage.completion_tokens 与是否缺失；无 usage 时 ok=false。
func (s *chatStatsReader) Tokens() (int, bool) { return s.completionTokens, s.hasCompletionTokens }

// Credit 返回末帧 usage.credit（本次真实扣费积分）与是否缺失。
func (s *chatStatsReader) Credit() (float64, bool) { return s.credit, s.hasCredit }

// CacheHitTokens 返回末帧 usage.prompt_cache_hit_tokens（前缀缓存命中）与是否缺失。
func (s *chatStatsReader) CacheHitTokens() (int, bool) {
	return s.cacheHitTokens, s.hasCacheHitTokens
}

// TotalTokens 返回末帧 usage.total_tokens 与是否缺失。
func (s *chatStatsReader) TotalTokens() (int, bool) { return s.totalTokens, s.hasTotalTokens }

// Usage 返回流式响应中已收到的 token usage 字段。
func (s *chatStatsReader) Usage() pool.TokenUsageDelta {
	return pool.TokenUsageDelta{
		HasPromptTokens:     s.hasPromptTokens,
		PromptTokens:        int64(s.promptTokens),
		HasCompletionTokens: s.hasCompletionTokens,
		CompletionTokens:    int64(s.completionTokens),
		HasTotalTokens:      s.hasTotalTokens,
		TotalTokens:         int64(s.totalTokens),
		HasCacheHitTokens:   s.hasCacheHitTokens,
		CacheHitTokens:      int64(s.cacheHitTokens),
	}
}

// parseSSELine 解析一行 "data: {...}"：首帧记 TTFB，含 usage 时采信精确 completion_tokens。
func (s *chatStatsReader) parseSSELine(line string) {
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "data: ") {
		return
	}
	payload := strings.TrimPrefix(line, "data: ")
	if payload == "[DONE]" {
		return
	}
	if !s.seen {
		s.seen = true
		s.ttfb = time.Since(s.start)
	}
	var chunk struct {
		Usage *struct {
			PromptTokens     *int     `json:"prompt_tokens"`
			CompletionTokens *int     `json:"completion_tokens"`
			TotalTokens      *int     `json:"total_tokens"`
			Credit           *float64 `json:"credit"`
			CacheHitTokens   *int     `json:"prompt_cache_hit_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal([]byte(payload), &chunk) != nil || chunk.Usage == nil {
		return
	}
	if chunk.Usage.PromptTokens != nil {
		s.hasPromptTokens = true
		s.promptTokens = *chunk.Usage.PromptTokens
	}
	if chunk.Usage.CompletionTokens != nil {
		s.hasCompletionTokens = true
		s.completionTokens = *chunk.Usage.CompletionTokens
	}
	if chunk.Usage.TotalTokens != nil {
		s.hasTotalTokens = true
		s.totalTokens = *chunk.Usage.TotalTokens
	}
	if chunk.Usage.Credit != nil {
		s.hasCredit = true
		s.credit = *chunk.Usage.Credit
	}
	if chunk.Usage.CacheHitTokens != nil {
		s.hasCacheHitTokens = true
		s.cacheHitTokens = *chunk.Usage.CacheHitTokens
	}
}

// Read 返回原始数据，同时解析统计 TTFB/token。
func (s *chatStatsReader) Read(p []byte) (int, error) {
	if len(s.pend) > 0 {
		n := copy(p, s.pend)
		s.pend = s.pend[n:]
		return n, nil
	}
	line, err := s.br.ReadString('\n')
	if line != "" {
		s.parseSSELine(line)
		s.pend = []byte(line)
		n := copy(p, s.pend)
		s.pend = s.pend[n:]
		return n, nil
	}
	return 0, err
}

// rewriteModel 把 outbound chat body 的 model 字段替换为 bare（保留其余字段原样）。
// 仅当 bare != 原 model 时由 chatCompletions 调用；body 不可解析时原样返回（不二次错误化）。
func rewriteModel(body []byte, bare string) []byte {
	if len(body) == 0 || bare == "" {
		return body
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	if cur, ok := obj["model"].(string); !ok || cur == bare {
		return body
	}
	obj["model"] = bare
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

// injectEffortIfAbsent 在 outbound body 未显式携带思考档位时补入档位（面板手动指定值）。
//
// 语义：客户端显式档位 > 面板手动档位 > 上游/静态默认档。
// 只写 snake_case 的 reasoning_effort（与 payload.go normalizeReasoningEffort 的
// 首选键一致）；camelCase 的 reasoningEffort 若已存在同样视为「显式」而不动，
// 避免同一请求里两个键打架。
// body 不可解析 / 已带档位 / 档位为空 → 原样返回（不二次错误化）。
func injectEffortIfAbsent(body []byte, effort string) []byte {
	if len(body) == 0 || effort == "" {
		return body
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	for _, k := range []string{"reasoning_effort", "reasoningEffort"} {
		if v, ok := obj[k]; ok {
			if s, isStr := v.(string); isStr && strings.TrimSpace(s) != "" {
				return body // 客户端显式指定，不覆盖
			}
		}
	}
	obj["reasoning_effort"] = effort
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

// parseModelFromBody 从请求 JSON 取 model 字段，缺省标 "-"。
func parseModelFromBody(body []byte) string {
	var obj struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &obj); err != nil || obj.Model == "" {
		return "-"
	}
	return obj.Model
}

// headModelRe 头部扫描用的 model 键值正则（截断容错，见 headModelOf）。
var headModelRe = regexp.MustCompile(`"model"\s*:\s*"([^"]{1,200})"`)

// headModelOf 从**可能被截断**的请求体头部提取 model，仅用于日志显示。
//
// 为什么单独一个函数：413 超限路径的 body 只读了 limit+1 字节（LimitReader 的
// 探测手法），JSON 结构必然不完整 → json.Unmarshal 一定失败、parseModelFromBody
// 只能返回 "-"，于是日志行里"哪个模型超了"变成空白——而这恰恰是排查时最需要的
// 字段（2026-09-20 实测：413 行 model 列为空）。
//
// 客户端几乎都把 "model" 放在 JSON 最外层靠前位置，扫头部 4KB 即可拿到。
// 只用于显示、不参与路由（选号仍用 json.Unmarshal 的 peek.Model）。
// 取不到返回 "-"（不编造）。
func headModelOf(body []byte) string {
	head := body
	if len(head) > 4096 {
		head = head[:4096]
	}
	if m := headModelRe.FindSubmatch(head); m != nil {
		return string(m[1])
	}
	return "-"
}

// usageDeltaFromResponse 从非流式聚合响应中提取明确存在的 token 字段。
func usageDeltaFromResponse(resp map[string]any) pool.TokenUsageDelta {
	delta := pool.TokenUsageDelta{}
	u, ok := resp["usage"].(map[string]any)
	if !ok {
		return delta
	}
	read := func(key string) (int64, bool) {
		v, ok := u[key]
		if !ok {
			return 0, false
		}
		switch n := v.(type) {
		case float64:
			return int64(n), true
		case float32:
			return int64(n), true
		case int:
			return int64(n), true
		case int64:
			return n, true
		case json.Number:
			i, err := n.Int64()
			return i, err == nil
		default:
			return 0, false
		}
	}
	if n, ok := read("prompt_tokens"); ok {
		delta.HasPromptTokens, delta.PromptTokens = true, n
	}
	if n, ok := read("completion_tokens"); ok {
		delta.HasCompletionTokens, delta.CompletionTokens = true, n
	}
	if n, ok := read("total_tokens"); ok {
		delta.HasTotalTokens, delta.TotalTokens = true, n
	}
	if n, ok := read("prompt_cache_hit_tokens"); ok {
		delta.HasCacheHitTokens, delta.CacheHitTokens = true, n
	}
	return delta
}

// completionTokens 从 Aggregate 返回的响应中提取 usage.completion_tokens；缺失返回 -1。
func completionTokens(resp map[string]any) int {
	u, ok := resp["usage"].(map[string]any)
	if !ok {
		return -1
	}
	v, ok := u["completion_tokens"].(float64)
	if !ok {
		return -1
	}
	return int(v)
}

// trimCredit 积分紧凑格式化：去掉无意义的尾随零，保持日志列宽稳定。
// 0 → "0"；0.0800 → "0.08"；0.00115 → "0.0012"；12.30 → "12.3"。
func trimCredit(v float64) string {
	if v == 0 {
		return "0"
	}
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs < 0.01:
		return strconv.FormatFloat(v, 'f', 4, 64)
	case abs < 1:
		return strconv.FormatFloat(v, 'f', 3, 64)
	case abs < 1000:
		return strconv.FormatFloat(v, 'f', 2, 64)
	default:
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
}

// uidPrefix 只显示 uid 前 8 位；空 uid 显示 "-"。
func uidPrefix(uid string) string {
	if uid == "" {
		return "-"
	}
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// chatLogModelWidth 表格日志里模型列的显示宽度（超出截断）。
// 取值见 logChatRow 内注释：必须能完整放下 "global:deepseek-v4.1-flash"（26 字符），
// 否则跨域/同前缀的模型名会被截成同一个字符串，日志失去排障价值。
const chatLogModelWidth = 26

// logChatRow 打印一行请求级表格日志（直接输出 stdout，无 log 时间戳前缀）。
// toks<0 表示 usage 缺失，显示 "-"。
// credit/hasCredit 为本次请求真实消耗的工作积分（上游 usage.credit）：
// hasCredit=false 显示 "-"（上游没下发），credit==0 显示 "0"（实测免费）。
// 两者必须分开——把"没数据"显示成 0 会让人误以为这些请求都免费。
func logChatRow(ttfb, total time.Duration, model, mode, uid string, status int, toks int, credit float64, hasCredit bool,
	tries int, tokps float64, hasTokps bool) {
	if !chatLogEnabled {
		return
	}
	seq := chatSeq.Add(1)
	// 模型列宽 26（历史值 11）。
	//
	// 为什么改：11 会把 "global:deep-model" 与 "global:deepseek-v4.1-flash" 一并截成
	// "global:deep" —— 两者在日志里完全无法分辨，排障时据此定位模型必然张冠李戴
	// （2026-09-19 实际踩过：按截断名认定是 deep-model，真实失败模型是
	// deepseek-v4.1-flash）。26 足以放下现役最长模型名 global:deepseek-v4.1-flash。
	if len(model) > chatLogModelWidth {
		model = model[:chatLogModelWidth]
	}
	tokField := "-"
	tokpsField := "-"
	if toks >= 0 {
		tokField = fmt.Sprintf("%d", toks)
		if hasTokps {
			// 解码期速度（分母已剔除首包等待）：这才是「字节输出速度」。
			tokpsField = fmt.Sprintf("%.1f", tokps)
		} else if total > 0 {
			tokpsField = fmt.Sprintf("%.1f", float64(toks)/total.Seconds())
		} else {
			tokpsField = "0.0"
		}
	}
	tryField := ""
	if tries > 1 {
		tryField = fmt.Sprintf(" try=%d |", tries)
	}
	ttfbMS := "-"
	if ttfb > 0 {
		ttfbMS = fmt.Sprintf("%dms", ttfb.Milliseconds())
	}
	// 积分列：3 种语义必须可区分
	//   "-"  → 上游没下发 credit（失败请求 / 旧上游）
	//   "0"  → 实测免费
	//   "0.08" → 实际扣费
	creditField := "-"
	if hasCredit {
		creditField = trimCredit(credit)
	}
	fmt.Fprintf(chatLogOut, "| #%03d | %s | %s | %s | %d | uid=%s | TTFB=%s | tok=%s | %stok/s | cr=%s | total=%.1fs |%s\n",
		seq,
		time.Now().Format("15:04:05"),
		model,
		mode,
		status,
		uidPrefix(uid),
		ttfbMS,
		tokField,
		tokpsField,
		creditField,
		total.Seconds(),
		tryField,
	)
}
