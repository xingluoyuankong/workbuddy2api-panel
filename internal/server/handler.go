// Package server 暴露 OpenAI 兼容 HTTP 接口，内部驱动 pool 挑号 + upstream 转发。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"sync"
	"sync/atomic"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/httpauth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/livecfg"
	"github.com/linguo2625469/workbuddy2api-panel/internal/modelmeta"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
	"github.com/linguo2625469/workbuddy2api-panel/internal/prompt"
	"github.com/linguo2625469/workbuddy2api-panel/internal/session"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
	"github.com/linguo2625469/workbuddy2api-panel/internal/usage"
)

// Config handler 依赖。
type Config struct {
	Pool      *pool.Pool
	Upstream  *upstream.Client
	APIKey    string // 空 = 不鉴权（静态值；与 Live 同时给出时 Live 优先）
	MaxRotate int    // 单请求最多换号次数，默认 3
	// MaxBodyBytes 聊天请求体大小上限；<=0 兜底 8<<20（8MB）。
	// 超限直接 413 request_body_too_large（不再静默截断喂给上游，issue #41）。
	MaxBodyBytes int64
	// Session 会话粘性路由器（可选；nil = 关闭粘性，纯 Pick 轮换）。
	Session *session.Router
	// StickyCount 返回当前粘性会话绑定数（供 /status）；nil 时报告 0。
	StickyCount func() int
	// RedisMode 观测字段（"upstash" / "noop"），供 /status 透出。
	RedisMode    string
	SoftCooldown time.Duration // 429/限流文案软冷却基数，默认 600s（连续触发指数退避，封顶 soft_rate_max）
	RefreshSkew  time.Duration // token 提前刷新窗口，默认 10m

	// Panel 管理面板 handler（可选；nil = 不挂载）。挂载在 /panel/ 前缀下，
	// 面板自带 Bearer 鉴权（同一 api_key）与内嵌静态资源，主路由只做转发。
	Panel http.Handler

	// Live 运行期可变配置（面板在线改 api_key / soft_rate / 脱敏开关时立即生效）。
	// nil 时回退静态字段（测试与裸用场景）。
	Live *livecfg.Holder

	// PromptMode "custom"（网关用自有提示词替换 system）/ "passthrough"（透传）。
	// 静态初值来自 config.Prompt.Mode；面板在线改提示词后由 cfg.Live 覆盖
	// （见 promptMode/promptText：Live 非空优先），无需重启即可生效。
	PromptMode string
	// PromptText custom 模式下注入的系统提示词文本（来自 config.PromptText）。
	PromptText string

	// GlobalEnabled global realm 路由开关（config global.enabled，缺省 true）。
	// handler 侧第三道闸（与 main 注入 auth 开关、upstream.GlobalEnabled 呼应）：
	// false（显式逃生门）时即便 auth realm=global 也不提供 global: 模型名
	// （modelList 不列 global 名单）。
	GlobalEnabled bool

	// Usage 逐请求用量记录器（可选；nil = 不记录）。
	// 在 recordAttempt 这一唯一汇聚点调用，因此流式/非流式、成功/失败都会计入，
	// 且与 pool 的每账号累计器同源，两条口径不会漂移。
	Usage *usage.Recorder

	// ModelMeta 模型元数据仓库（可选；nil = 不启用元数据透出与过滤）。
	// 提供上游真实名（别名映射）、可调用性验证状态、备注/分类/手动思考档。
	ModelMeta *modelmeta.Store

	// OnlyVerified 默认只向 /v1/models 暴露「实测可调用」的模型（config 项）。
	// 默认 false（兼容性优先）：未跑过验证时若默认开启会返回空列表，
	// 直接打挂所有下游客户端。查询参数 only_verified=1|0 可单次覆盖。
	OnlyVerified bool

	// ModelProbe 验证用真实调用入口（可选；nil = 验证接口返回 501）。
	// 由 main 注入经网关回路的调用，验证的是完整下游链路。
	ModelProbe modelmeta.ProbeFunc
}

// loadLive 返回当前运行期快照；Live 为 nil 时用静态字段合成。
func (h *Handler) loadLive() livecfg.Snapshot {
	if h.cfg.Live != nil {
		return h.cfg.Live.Load()
	}
	return livecfg.Snapshot{
		APIKey:       h.cfg.APIKey,
		SoftCooldown: h.cfg.SoftCooldown,
	}
}

// softCooldown 返回当前生效的软冷却基数（热改优先，<=0 回退默认）。
func (h *Handler) softCooldown() time.Duration {
	if d := h.loadLive().SoftCooldown; d > 0 {
		return d
	}
	if h.cfg.SoftCooldown > 0 {
		return h.cfg.SoftCooldown
	}
	return 600 * time.Second
}

// notFoundCooldown 上游 404 的固定短冷却时长。
// 与 SoftCooldown 分流的原因：404 是上游**偶发**路径缺失，不是"本账号在限流"，
// 若共用 soft_rate（600s 起 + 指数升级），一次偶发 404 会把好账号罚 10 分钟并逐次加倍。
// 故固定 60s 防雪崩即可，不随 soft_rate 配置、也不参与软退避指数。
const notFoundCooldown = 60 * time.Second

// ServiceName 网关身份标识。经 /healthz 响应体 service 字段与 X-Service 头同时透出：
// 宿主（如 workbuddy-switch 托管网关子进程）探测同端口的旧服务/其他服务时，对方即使
// 返回 2xx 也不带本标识，宿主据此可识别"假成功"。
const ServiceName = "workbuddy2api"

// Handler 主路由。
type Handler struct {
	cfg     Config
	mux     *http.ServeMux
	degrade degradeGate
	// wafIP WAF IP 级拦截状态机（fail-fast，wafip.go）：短窗多号 WAF 403 →
	// 激活期轮转遇 WAF 403 直接终止（不放大请求量）。进程内状态、重启清零。
	wafIP wafIPGate
	// maxBodyBytes 请求体上限的运行期值（cfg.MaxBodyBytes 的原子镜像）。
	// 面板在线改 server.max_body_mb 时经 SetMaxBodyBytes 热生效，无需重启
	// （issue #17：改了配置却静默不生效，用户仍被 8MB 413 拦截）。
	maxBodyBytes atomic.Int64
}

// SetMaxBodyBytes 热更新请求体上限（面板保存配置路径调用）。
// n<=0 与 NewHandler 兜底口径一致：回落 8MB。
func (h *Handler) SetMaxBodyBytes(n int64) {
	if n <= 0 {
		n = 8 << 20
	}
	h.maxBodyBytes.Store(n)
}

// NewHandler 构建 handler。
func NewHandler(cfg Config) *Handler {
	if cfg.MaxRotate <= 0 {
		cfg.MaxRotate = 3
	}
	if cfg.SoftCooldown <= 0 {
		cfg.SoftCooldown = 600 * time.Second // 软限流基数（连续触发按指数退避放大）
	}
	if cfg.RefreshSkew <= 0 {
		cfg.RefreshSkew = 10 * time.Minute
	}
	if cfg.PromptMode == "" {
		cfg.PromptMode = "custom" // 缺省 custom：网关自有提示词
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 8 << 20 // 请求体上限兜底 8MB
	}
	h := &Handler{cfg: cfg, mux: http.NewServeMux()}
	h.maxBodyBytes.Store(cfg.MaxBodyBytes)
	h.mux.HandleFunc("POST /v1/chat/completions", h.withAuth(h.chatCompletions))
	h.mux.HandleFunc("GET /v1/models", h.withAuth(h.models))
	h.mux.HandleFunc("GET /status", h.withAuth(h.status))
	h.mux.HandleFunc("GET /healthz", h.healthz)
	if cfg.Panel != nil {
		h.mux.Handle("/panel/", cfg.Panel) // /panel → /panel/ 由 ServeMux 自动重定向
	}
	return h
}

// promptMode 生效的提示词模式：Live（面板在线改过）非空优先，否则用静态配置。
func (h *Handler) promptMode() string {
	if h.cfg.Live != nil {
		if s := h.cfg.Live.Load(); s.PromptMode != "" {
			return s.PromptMode
		}
	}
	return h.cfg.PromptMode
}

// promptText 生效的提示词全文：Live 非空优先，否则用静态配置。
// 与 promptMode 成对读取，避免"模式改了、文本还是旧的"这种半热状态。
func (h *Handler) promptText() string {
	if h.cfg.Live != nil {
		if s := h.cfg.Live.Load(); s.PromptText != "" {
			return s.PromptText
		}
	}
	return h.cfg.PromptText
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !httpauth.VerifyBearer(r, h.loadLive().APIKey) {
			writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
			return
		}
		next(w, r)
	}
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	total, healthy, _, _, _ := h.cfg.Pool.CountsDetailed()
	// 用 ServableNow 判定：healthy>0 但全占满在途时 chat 会 503，探活必须同口径，
	// 否则负载均衡器会把流量持续打进无法受理的实例。
	status := http.StatusOK
	if !h.cfg.Pool.ServableNow() {
		status = http.StatusServiceUnavailable
	}
	// realm_servable 域可服务维度：不改判活语义（存在性探活保持不变），
	// 只新增 CN/global 各自可达性供双域部署运维观察（任一域不可用单独告警）。
	realmServable := map[string]bool{
		"cn":     h.cfg.Pool.ServableForRealm("cn"),
		"global": h.cfg.Pool.ServableForRealm("global"),
	}
	// 恒无鉴权（负载均衡/编排探活只需 2xx/503 语义），身份靠 service 字段 + X-Service 头双保险。
	w.Header().Set("X-Service", ServiceName)
	writeJSON(w, status, map[string]any{
		"healthy":        healthy,
		"total":          total,
		"service":        ServiceName,
		"realm_servable": realmServable,
	})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	total, healthy, cooling, disabled, inFlightFull := h.cfg.Pool.CountsDetailed()
	sticky := 0
	if h.cfg.StickyCount != nil {
		sticky = h.cfg.StickyCount()
	}
	redisMode := h.cfg.RedisMode
	if redisMode == "" {
		redisMode = "noop"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts":       h.cfg.Pool.List(),
		"total":          total,
		"healthy":        healthy,
		"cooling":        cooling,
		"disabled":       disabled,
		"in_flight_full": inFlightFull,
		// realm_totals 按域分组的计数汇总（双 realm 并存时运维一眼看到各域可用性）：
		// 只新增字段，既有 total/healthy/cooling/disabled/in_flight_full 汇总键不变（零回归）。
		"realm_totals": map[string]map[string]int{
			"cn":     countsMapFrom(h.cfg.Pool.CountsDetailedForRealm("cn")),
			"global": countsMapFrom(h.cfg.Pool.CountsDetailedForRealm("global")),
		},
		"sticky_sessions": sticky,
		"redis_mode":      redisMode,
	})
}

// countsMapFrom 把 CountsDetailed 五元组打包成 /status 的域分组建模。
func countsMapFrom(total, healthy, cooling, disabled, inFlightFull int) map[string]int {
	return map[string]int{
		"total":          total,
		"healthy":        healthy,
		"cooling":        cooling,
		"disabled":       disabled,
		"in_flight_full": inFlightFull,
	}
}

// dynamicModelsCache 动态模型缓存。
var dynamicModelsCache struct {
	sync.RWMutex
	ids      []upstream.ModelInfo
	fetched  time.Time // 最近一次成功拉取时间
	lastFail time.Time // 最近一次拉取失败时间（负缓存）
}

const (
	dynamicModelsTTL        = time.Hour
	modelsFetchFailCooldown = 5 * time.Minute
)

// modelFilter /v1/models 的过滤条件（查询参数解析结果）。
type modelFilter struct {
	OnlyVerified bool   // 仅返回实测可调用的模型
	Realm        string // "cn" / "global"；空 = 全部
	Keyword      string // id/展示名子串匹配（大小写不敏感）
}

// models 返回模型列表：纯动态（缓存 1h），失败/无号返回空列表（无静态兜底——
// 拉不出目录即意味着上游不可用，假名单只会让客户端选到 11102 的模型）。
//
// 支持的查询参数（全部可选，缺省 = 兼容旧行为全量返回）：
//   - only_verified=1|0：只返回实测调用成功的模型（默认取 config.models.only_verified，缺省 false）。
//   - realm=cn|global：只返回指定域。
//   - q=关键字：按 id / 展示名 / 上游真实名子串过滤。
//
// 过滤只在「验证数据存在」时才可能剔除模型：从未验证过（unverified）的模型在
// only_verified=1 下会被剔除（保守语义：只留确认能用的）。
func (h *Handler) models(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// only_verified：静态配置（config.json）为基，运行期面板热改（Live）覆盖。
	f := modelFilter{OnlyVerified: h.cfg.OnlyVerified || h.loadLive().OnlyVerified}
	if v := q.Get("only_verified"); v != "" {
		f.OnlyVerified = v == "1" || v == "true" || v == "yes"
	}
	f.Realm = strings.ToLower(strings.TrimSpace(q.Get("realm")))
	f.Keyword = strings.ToLower(strings.TrimSpace(q.Get("q")))
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   h.modelListWith(f),
	})
}

// fmtCreditsPrefix 从上游 credits 原文提取倍率并格式化为 "[x0.05 credit]"。
// 上游格式不统一："x0.05 credits" / "x0.29" / "x0.00 credits" 等，
// 统一提取 x数字 部分，去 "credits" 后缀。
func fmtCreditsPrefix(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimSuffix(s, "credits")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return "[" + s + " credit]"
}

// applyModelInfoFields 把上游模型对象全字段（ModelInfo）按「空值省略」写出规则
// 合入 /v1/models 条目：name/description/credits/tags/vendor/能力旗标/
// max_allowed_size/reasoning_effort/reasoning_summary。CN 动态分支与 global
// 探测命中分支共用（两域模型对象同构），保证输出字段集一致。
// 不覆盖 id/object/created/owned_by 及调用方先前写好的基础字段；上游未下发的
// 字段（零值）整体省略——不编造。
func applyModelInfoFields(entry map[string]any, mi upstream.ModelInfo) map[string]any {
	if mi.Name != "" {
		entry["name"] = mi.Name
	}
	if mi.Description != "" {
		// 积分倍率前缀：从 "x0.05 credits" / "x0.29" 等格式提取纯数字，
		// 统一为 "[x0.05 credit]" 前缀拼入 description，方便下游面板直接展示。
		if mi.Credits != "" {
			entry["description"] = fmtCreditsPrefix(mi.Credits) + " " + mi.Description
		} else {
			entry["description"] = mi.Description // descriptionZh 中文描述
		}
	}
	if mi.Credits != "" {
		entry["credits"] = mi.Credits // 积分倍率原文（如 "x0.05"），仅展示
	}
	if len(mi.Tags) > 0 {
		entry["tags"] = mi.Tags
	}
	if mi.Vendor != "" {
		entry["vendor"] = mi.Vendor
	}
	if mi.IsDefault {
		entry["is_default"] = true
	}
	if mi.SupportsImages {
		entry["supports_images"] = true // 多模态能力透出
	}
	if mi.SupportsReasoning {
		entry["supports_reasoning"] = true
		if mi.CanDisableThinking {
			entry["can_disable_thinking"] = true
		}
	}
	if mi.SupportsToolCall {
		entry["supports_tool_call"] = true
	}
	if mi.OnlyReasoning {
		entry["only_reasoning"] = true
	}
	if mi.MaxAllowedSize > 0 {
		entry["max_allowed_size"] = mi.MaxAllowedSize
	}
	if mi.ReasoningEffort != "" {
		entry["reasoning_effort"] = mi.ReasoningEffort
	}
	if mi.ReasoningSummary != "" {
		entry["reasoning_summary"] = mi.ReasoningSummary
	}
	return entry
}

// modelList 模型列表：CN 模型输出统一加 "cn:" 前缀（gateway 路由协议，与 resolveModel
// 对称）；global.enabled=true 时追加 global: 前缀的国际版名单。
// 纯动态：动态拉取失败/无号 → 该域空列表，无静态兜底。
func (h *Handler) modelList() []map[string]any { return h.modelListWith(modelFilter{}) }

// InvalidateModelCache 清空模型目录缓存，强制下次 /v1/models 重新拉上游。
//
// 供每日定时刷新使用：上游的 credits 倍率与模型上下架每天都在变，缓存 1h 会
// 让面板显示过期的倍率。清空后由下一次请求触发重新拉取（不在本方法内同步拉，
// 避免定时任务里阻塞）。
func (h *Handler) InvalidateModelCache() {
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = nil
	dynamicModelsCache.fetched = time.Time{}
	dynamicModelsCache.lastFail = time.Time{}
	dynamicModelsCache.Unlock()
}

// ModelList 导出 /v1/models 同口径的完整条目（不含任何过滤），供管理面板复用：
// 面板「模型与映射」页展示的就是客户端实际能拿到的模型，两侧永不漂移。
func (h *Handler) ModelList() []map[string]any { return h.modelList() }

// applyModelMeta 把模型元数据（上游真实名/验证状态/备注/分类/档位）写入 /v1/models 条目，
// 并返回 false 表示该模型应被过滤掉（人工隐藏 或 only_verified 下未通过验证）。
//
// 字段一律「空值省略」：无元数据时输出与历史完全一致（下游客户端零感知）。
func (h *Handler) applyModelMeta(entry map[string]any, realm, id string, f modelFilter) bool {
	if h.cfg.ModelMeta == nil {
		return true // 未启用元数据 → 不过滤、不增字段（完全兼容旧行为）
	}
	v := h.cfg.ModelMeta.Annotate(realm, id, stringField(entry, "name"))
	// 删除优先于隐藏：Removed 的模型不该出现在任何下游视图（含面板主表）。
	if v.Removed {
		return false
	}
	if v.Hidden {
		return false
	}
	if f.OnlyVerified && !v.Verified {
		return false
	}
	// 名称对齐上游：出站 ID 恒为 realm + 上游原生 ID，网关不改写一个字符
	// （不做任何改名/别名/后缀处理）——下游看到的后半段就是上游目录里的原 ID。
	// 手动上下文档位只覆盖对外声称的 context_length，不动 id / name。
	if v.ContextWindow > 0 {
		entry["context_length"] = v.ContextWindow
		entry["context_length_manual"] = true
	}
	if v.UpstreamModel != "" {
		entry["upstream_model"] = v.UpstreamModel // 上游真实映射名（实测回显）
		entry["is_alias"] = v.IsAlias
	}
	if v.Status != "" {
		entry["status"] = string(v.Status)
		entry["verified"] = v.Verified
	}
	if v.Category != "" {
		entry["category"] = string(v.Category)
	}
	if v.Note != "" {
		entry["note"] = v.Note
	}
	if v.Effort != "" {
		entry["reasoning_effort_override"] = v.Effort
	}
	if v.Credit > 0 || v.Samples > 0 {
		entry["measured"] = map[string]any{
			"credit":     v.Credit,
			"samples":    v.Samples,
			"latency_ms": v.LatencyMS,
			"updated_at": v.UpdatedAt,
		}
	}
	if v.ErrCode != "" {
		entry["last_error"] = map[string]any{"code": v.ErrCode, "message": v.ErrMsg}
	}
	// 展示名：人工覆盖优先于上游 name（面板/客户端可直接用 display_name 排序展示）。
	if v.Display != "" && v.Display != id {
		entry["display_name"] = v.Display
	}
	return true
}

// stringField 从 map 取字符串字段（缺失/非字符串 → 空串）。
func stringField(m map[string]any, k string) string {
	if s, ok := m[k].(string); ok {
		return s
	}
	return ""
}

// matchKeyword 关键字匹配：id / 展示名 / 上游真实名 三者任一命中。
func matchKeyword(v modelmeta.View, kw string) bool {
	if kw == "" {
		return true
	}
	return strings.Contains(strings.ToLower(v.ID), kw) ||
		strings.Contains(strings.ToLower(v.Display), kw) ||
		strings.Contains(strings.ToLower(v.UpstreamModel), kw)
}

func (h *Handler) modelListWith(f modelFilter) []map[string]any {
	out := make([]map[string]any, 0)
	cnInfos := h.fetchDynamicModels()
	// 目录登记：把上游目录里的模型写进元数据仓库（只建记录不改状态），
	// 让面板能列出「目录里有、但还没实测过」的模型并逐个验证。
	h.registerModels("cn", modelIDs(cnInfos))
	for _, mi := range cnInfos {
		if f.Realm != "" && f.Realm != "cn" {
			continue
		}
		entry := map[string]any{
			"id":       "cn:" + mi.ID,
			"object":   "model",
			"created":  1753600000,
			"owned_by": "workbuddy",
		}
		// context_length / max_output_tokens 四级查找（upstream.model_catalog）：
		// 上游动态值（maxInputTokens/maxOutputTokens）权威 → 静态种子表 →
		// model.json 本地缓存 → models.dev 按需拉取（异步不阻塞本次响应，拉到后
		// 写 model.json 供下次命中）→ 1M 兜底 / max_output_tokens 省略。
		// 上游零值不再透出假 131072（误导 Codex/ZCode 等按 context_length 提前
		// 截断、白白丢上下文）。
		entry["context_length"] = upstream.ContextWindowListingV4(mi.ID, mi.ContextWindow, h.cfg.Upstream.HTTP)
		if mo, ok := upstream.MaxOutputTokensListingV4(mi.ID, mi.MaxTokens, h.cfg.Upstream.HTTP); ok {
			entry["max_output_tokens"] = mo
		}
		// 上游模型对象全字段透出（name/描述/标签/倍率/能力旗标等，空值省略）。
		entry = applyModelInfoFields(entry, mi)
		// effort 能力透出——远端 supportedEfforts 权威，缺失落到 CN 静态兜底表
		// （客户端可发现档位，不再盲传）。无档位 → 省略字段。
		if efforts, def := upstream.EffortListing("cn", mi.ID, mi.Efforts, mi.DefaultEffort); efforts != nil {
			entry["reasoning_supported_efforts"] = efforts
			if def != "" {
				entry["reasoning_default_effort"] = def
			}
		}
		// 元数据透出 + 过滤（人工隐藏 / only_verified）。必须在全字段写完后调用：
		// 展示名兜底依赖 entry["name"]。
		if !h.applyModelMeta(entry, "cn", mi.ID, f) {
			continue
		}
		if !matchKeyword(h.metaView("cn", mi.ID, entry), f.Keyword) {
			continue
		}
		out = append(out, entry)
	}
	// global 模型名单：仅 GlobalEnabled=true 时列出（逃生门）。
	// 名单 = 纯动态探测结果（fetchGlobalModels，失败/无号 → 空）。
	if h.cfg.GlobalEnabled {
		// global 域 effort 能力三级查找：探测下发桶（权威）→ 静态兜底表 → 省略。
		// 先 fetchGlobalModels（内部探测并落 effort 桶），再按 id 取快照。
		globalIDs, globalAccount := h.fetchGlobalModels()
		h.registerModels("global", globalIDs)
		// 探测对象形态的全字段条目（与 fetchGlobalModels 共享同一次探测缓存）：
		// 命中 id 才透出富字段；窄表/失败 → nil，按裸 ID 条目输出（不编造字段）。
		// globalAccount 为 nil（无 global 号）时返回 nil，跳过富字段映射。
		globalInfos := map[string]upstream.ModelInfo{}
		for _, mi := range h.cfg.Upstream.FetchGlobalModelInfos(globalAccount) {
			globalInfos[mi.ID] = mi
		}
		globalEfforts, globalDefaults := h.cfg.Upstream.GlobalEffortSnapshot()
		for _, id := range globalIDs {
			entry := map[string]any{
				"id":       "global:" + id,
				"object":   "model",
				"created":  1753600000,
				"owned_by": "workbuddy",
			}
			// context_length / max_output_tokens 四级查找（与 CN 动态分支同口径）。
			var remoteCtx, remoteOut int64
			if mi, ok := globalInfos[id]; ok {
				entry = applyModelInfoFields(entry, mi)
				remoteCtx, remoteOut = mi.ContextWindow, mi.MaxTokens
			}
			entry["context_length"] = upstream.ContextWindowListingV4(id, remoteCtx, h.cfg.Upstream.HTTP)
			if mo, ok := upstream.MaxOutputTokensListingV4(id, remoteOut, h.cfg.Upstream.HTTP); ok {
				entry["max_output_tokens"] = mo
			}
			if efforts, def := upstream.EffortListing("global", id, globalEfforts[id], globalDefaults[id]); efforts != nil {
				entry["reasoning_supported_efforts"] = efforts
				if def != "" {
					entry["reasoning_default_effort"] = def
				}
			}
			if !h.applyModelMeta(entry, "global", id, f) {
				continue
			}
			if !matchKeyword(h.metaView("global", id, entry), f.Keyword) {
				continue
			}
			out = append(out, entry)
		}
	}
	return out
}

// registerModels 把目录里的模型登记进元数据仓库（无仓库 / 空名单 → 直接返回）。
// 只 Upsert 不落盘：登记是幂等的目录同步，不值得每次 /v1/models 都写文件；
// 真正的落盘只在人工编辑与验证结果写入时发生。
func (h *Handler) registerModels(realm string, ids []string) {
	if h.cfg.ModelMeta == nil || len(ids) == 0 {
		return
	}
	h.cfg.ModelMeta.UpsertKnown(realm, ids)
}

// modelIDs 从目录条目提取 ID 列表（登记用）。
func modelIDs(infos []upstream.ModelInfo) []string {
	out := make([]string, 0, len(infos))
	for _, mi := range infos {
		out = append(out, mi.ID)
	}
	return out
}

// metaView 取模型的元数据视图（未启用元数据时返回零值视图，关键字匹配退化为仅匹配 ID）。
func (h *Handler) metaView(realm, id string, entry map[string]any) modelmeta.View {
	if h.cfg.ModelMeta == nil {
		return modelmeta.View{ID: id, Display: id, FullID: realm + ":" + id}
	}
	return h.cfg.ModelMeta.Annotate(realm, id, stringField(entry, "name"))
}

// fetchGlobalModels 返回 global 模型名单（纯动态探测结果）及被探测账号。
// 缓存/失败回落封在 upstream.FetchGlobalModels（内部 1h + 5min 负缓存）。
// 本方法只负责"何时探测"：池中无 global 账号 → 空名单 + nil 账号（零上游调用）。
// 返回的 acct 供调用方在同一账号上取富 ModelInfo（FetchGlobalModelInfos 与
// FetchGlobalModels 共享缓存，不会触发第二次上游探测）。
// GlobalEnabled=false 时 modelList 已不进入本分支（逃生门在调用方 gate）。
func (h *Handler) fetchGlobalModels() ([]string, *auth.Auth) {
	acct := h.cfg.Pool.PickExcludingForRealm(nil, "", "global")
	if acct == nil {
		return nil, nil
	}
	return h.cfg.Upstream.FetchGlobalModels(acct), acct
}

// fetchDynamicModels 从池中任一健康 CN 账号拉模型列表（含 contextWindow/maxTokens），缓存 1h。
// 拉取失败记录时间戳进入 5min 负缓存，冷却期内直接返回 nil（纯动态，无静态表兜底），
// 避免反复打上游。只从 CN realm 账号拉取（global 走独立探测）。
func (h *Handler) fetchDynamicModels() []upstream.ModelInfo {
	dynamicModelsCache.RLock()
	if len(dynamicModelsCache.ids) > 0 && time.Since(dynamicModelsCache.fetched) < dynamicModelsTTL {
		out := dynamicModelsCache.ids
		dynamicModelsCache.RUnlock()
		return out
	}
	// 失败负缓存：冷却期内不再请求上游。
	if !dynamicModelsCache.lastFail.IsZero() && time.Since(dynamicModelsCache.lastFail) < modelsFetchFailCooldown {
		dynamicModelsCache.RUnlock()
		return nil
	}
	dynamicModelsCache.RUnlock()

	acct := h.cfg.Pool.Pick()
	if acct == nil {
		return nil
	}
	infos, err := h.cfg.Upstream.FetchModels(acct)
	if err != nil || len(infos) == 0 {
		// 拉取失败只进负缓存（5min lastFail），不 NoteError：NoteError 喂的是 chat
		// 熔断器，models 端点偶发 5xx 跨界惩罚 chat 通道健康的账号；
		// models 拉取失败 ≠ 账号 chat 不可用。
		dynamicModelsCache.Lock()
		dynamicModelsCache.lastFail = time.Now()
		dynamicModelsCache.Unlock()
		return nil
	}
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = infos
	dynamicModelsCache.fetched = time.Now()
	dynamicModelsCache.lastFail = time.Time{} // 成功则清空负缓存
	dynamicModelsCache.Unlock()
	return infos
}

// cachedModelsSnapshot 只读模型目录缓存（TTL 内快照）；缓存冷/空 → nil。
// 不发起任何上游调用（hint 判定用：错误路径加一次 FetchModels 网络调用既拖慢
// 错误响应、又污染上游调用语义）。
func cachedModelsSnapshot() []upstream.ModelInfo {
	dynamicModelsCache.RLock()
	defer dynamicModelsCache.RUnlock()
	if len(dynamicModelsCache.ids) == 0 || time.Since(dynamicModelsCache.fetched) >= dynamicModelsTTL {
		return nil
	}
	return dynamicModelsCache.ids
}

func (h *Handler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	// 客户端 IP 提取（按请求传递到 ChatStream，不透传时 upstream 侧忽略）；
	// 消除早年共享字段方案的并发交叉污染（issue：ClientIP 竞态）。
	clientIP := upstream.ExtractClientIP(r)
	// 请求体上限：LimitReader 读 limit+1 以探测"超限"（读到 limit+1 字节即已超），
	// 超限直接 413，不把截断的半截 JSON 喂给上游（issue #41：截断 body 让上游
	// unmarshal 报 unexpected EOF，网关却罚号轮空）。
	// 413 是网关侧的客户端问题，不打上游、不罚账号、不轮转。
	limit := h.maxBodyBytes.Load()
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}
	// peek 提前到上限判定之前：413 分支也要落日志（见下），需要 model/stream。
	var peek struct {
		Stream bool   `json:"stream"`
		Model  string `json:"model"`
	}
	_ = json.Unmarshal(body, &peek)
	if int64(len(body)) > limit {
		// 413 必须留痕。此前这条路径直接 return——**既不落表格行也不落系统日志**，
		// 服务端完全看不到：客户端报 MODEL_EXECUTION_ERROR「请求体超过 8 MB 上限」
		// 时我们查无此事、只能靠猜（2026-09-20 用户实际反馈"任务老是断"）。
		// 表格行让它在面板「对话」频道与正常请求并列可见，系统行带字节数/上限/模型。
		// 超限 body 被截断 → json 解析必失败，peek.Model 恒空；用头部扫描拿模型名
		// （见 headModelOf），否则日志行里"哪个模型超了"是空白。
		modelName := headModelOf(body)
		// 真实大小：超限时 len(body) 恒为 limit+1（LimitReader 读到上限即停），
		// 直接报会得出"32.0 MB 超过 32 MB"这种自相矛盾的说法（2026-09-20 实测）。
		// 优先采信客户端声明的 Content-Length；拿不到（chunked/无头）才退回
		// ">已读上限"，语义上也不撒谎。
		sizeText := fmt.Sprintf("≥%.1f MB", float64(len(body))/(1<<20))
		if r.ContentLength > int64(len(body)) {
			sizeText = fmt.Sprintf("%.1f MB", float64(r.ContentLength)/(1<<20))
		}
		st := newChatStatWithModel(time.Now(), modelName, peek.Stream)
		st.status = http.StatusRequestEntityTooLarge
		defer st.done()
		log.Printf("WARN: [server] 413 request_body_too_large: size=%s (len=%d bytes, content_length=%d, limit=%d MB, model=%s) — 多图/长上下文会话易触发（历史图片每轮以 base64 重发）；压缩图片或调大 server.max_body_mb",
			sizeText, len(body), r.ContentLength, limit>>20, modelName)
		// 文案带上**实际大小**：客户端只会原样转述这句话，给出"超了多少"用户才知道
		// 该把上限调到多少、或者该删几张图（原来只说"超过 8 MB"，无从判断）。
		writeOpenAIError(w, http.StatusRequestEntityTooLarge, "request_body_too_large",
			fmt.Sprintf("请求体 %s 超过 %d MB 上限：多图/长上下文会话易触发（历史图片每轮以 base64 重发）；请压缩图片或调大 server.max_body_mb（面板修改即时生效）后重试",
				sizeText, limit>>20))
		return
	}

	// realm 前缀解析（D6）：model 名可能带 "[realm:]" 前缀。剥出 realm + bareModel，
	// bareModel 用于选号/粘性/出站 body 重写（前缀是网关侧路由协议，上游只认裸名）。
	// 裸名 → ("cn", 原串)，CN 现状零回归。
	realm, bareModel := resolveModel(peek.Model)

	// 请求级统计：出口即打一行表格日志（任何路径都会走到）。
	st := newChatStatWithModel(time.Now(), peek.Model, peek.Stream)
	defer st.done()

	tried := map[string]bool{}
	var lastErr error

	// 会话粘性：从请求体提取会话键并解析绑定号（找不到/无效则 stickyUID 为空，走普通轮换）。
	// ExtractKey 与粘性开关解耦（issue #35 侧）：关闭粘性时会话头族的聚合主键仍按
	// 会话级（RequestIDForKey(sessKey)），不悄悄退化成轮级——提取本身与粘性无关。
	sessKey := session.ExtractKey(body)
	stickyUID := ""
	if h.cfg.Session != nil && sessKey != "" {
		// 按模型解析：绑定号在**当前模型**被 6004 限额时视为不可用 → 重新分配，
		// 而不是钉在限额号上反复失败（"限额后换不动号"的正解）。
		if uid, ok := h.cfg.Session.ResolveForModel(sessKey, peek.Model); ok {
			stickyUID = uid
		}
	}

	// 轮级兜底聚合键：无会话键的客户端（OpenAI 兼容协议——dsh / Codex / Cherry
	// Studio 等请求体里既无 conversationId 也无 metadata）sessKey 恒空，会话头族的
	// 聚合主键只能逐请求新生成，agent 多轮在上游用量明细里仍是一条请求一条记录。
	// 这里按 body 里最后一条 user 消息派生轮级键（同轮内所有上游调用同键）。
	// 必须在下方 prompt.Rewrite 之前取——改写会动 messages 内容，之后取会让键漂移。
	turnKey := ""
	if sessKey == "" {
		turnKey = session.TurnKey(body)
	}

	// gateway_hint 判定所需的请求形态（image_url part）：在改写前取（与 turnKey
	// 同理）。11133「模型不支持图片」指向的前提。
	reqHasImage := hasImagePart(body)

	// 在途租约：成功选中即占名额；函数出口（含成功 return 与 panic）统一释放。
	var heldUID string
	defer func() {
		if heldUID != "" {
			h.cfg.Pool.Release(heldUID)
		}
	}()
	releaseHeld := func() {
		if heldUID != "" {
			h.cfg.Pool.Release(heldUID)
			heldUID = ""
		}
	}
	// unbindSticky 解绑当前会话粘性号（stickyUID 非空时）。供「粘性号不可用/被抢」与 fail 共用。
	// 幂等：stickyUID 已空则空操作；不会误解绑其他轮的绑定。仅当 Session != nil 时 stickyUID 才会非空。
	unbindSticky := func() {
		if stickyUID != "" {
			h.cfg.Session.Unbind(sessKey)
			stickyUID = ""
		}
	}
	// fail 在轮转失败分支统一：释放租约 + 若失败号正是粘性号则解绑（下次请求重新分配）。
	fail := func(uid string) {
		releaseHeld()
		if stickyUID != "" && uid == stickyUID {
			unbindSticky()
		}
	}
	recordAttempt := func(uid string, delta pool.TokenUsageDelta, started time.Time) {
		delta.Model = peek.Model
		latency := time.Since(started)
		latencyMs := latency.Milliseconds()
		if latencyMs < 1 {
			latencyMs = 1
		}
		delta.HasLatencyMs = true
		delta.LatencyMs = latencyMs
		if delta.HasCompletionTokens && delta.CompletionTokens >= 0 && latencyMs > 0 {
			delta.HasTokensPerSecond = true
			delta.TokensPerSecond = float64(delta.CompletionTokens) * 1000 / float64(latencyMs)
		}
		h.cfg.Pool.RecordTokenUsage(uid, delta)

		// 用量时序记录。ok 以「上游是否给了 usage」判定：空 delta 意味着这次尝试
		// 没拿到任何 token 统计（传输错误 / >=400 / 解析失败），计为失败尝试。
		// 失败也计入请求数——否则重试放大在「用量」视图里看不见。
		// 上游真的回了 usage → 出口链路已恢复（WAF 若在封禁，这里不可能成功）。
		// 清零 IP 级拦截的连击计数，让下次偶发触发的冷却回到基础值而不是继续翻倍。
		if delta.HasTotalTokens || delta.HasCompletionTokens || delta.HasPromptTokens {
			h.wafIP.noteSuccess()
		}

		if h.cfg.Usage != nil {
			realm := "cn"
			if a, ok := h.cfg.Pool.Status(uid); ok && a.Realm != "" {
				realm = a.Realm
			}
			h.cfg.Usage.Add(time.Now(), realm, uid, delta.Model, usage.Delta{
				PromptTokens:     delta.PromptTokens,
				HasPromptTokens:  delta.HasPromptTokens,
				CompletionTokens: delta.CompletionTokens,
				HasCompletion:    delta.HasCompletionTokens,
				TotalTokens:      delta.TotalTokens,
				HasTotal:         delta.HasTotalTokens,
				LatencyMs:        delta.LatencyMs,
				HasLatency:       delta.HasLatencyMs,
				TokensPerSecond:  delta.TokensPerSecond,
				HasTPS:           delta.HasTokensPerSecond,
				// 积分：上游 usage.credit（真实扣费，与 token 数不同源）。
				Credit:    delta.Credit,
				HasCredit: delta.HasCredit,
			}, delta.HasTotalTokens || delta.HasCompletionTokens || delta.HasPromptTokens)
		}
	}

	// 系统提示词改写（出站前、轮转前；每个请求一次）。
	//   - custom：用自有提示词替换客户端 system/developer（从源头消灭 system 指纹误报）。
	//   - passthrough + 降级期：换 Degraded 中性提示词直达，不再先撞 400。
	//   - passthrough 非降级期：透传客户端原始 system（不改写）。
	degradedApplied := false
	if h.promptMode() == "custom" && h.promptText() != "" {
		body = prompt.Rewrite(body, h.promptText())
	} else if h.promptMode() == "passthrough" && h.degrade.Active() {
		body = prompt.Rewrite(body, prompt.Degraded)
		degradedApplied = true
	}

	// outbound model 名重写为 bareModel（D6）：realm 前缀是网关侧路由协议，
	// 上游不认前缀（global 账号也请求裸模型名）。裸名时 bareModel==peek.Model 恒等。
	if bareModel != peek.Model {
		body = rewriteModel(body, bareModel)
	}

	// 手动思考档位回调（面板按模型指定）：请求未显式带 reasoning_effort /
	// reasoningEffort 时，用人工指定的档位补上并回传上游。
	// 显式档位一律优先（不覆盖客户端选择）；未指定档位的模型零改动。
	// 补入的档位随后仍会走 payload.go 的 supportedEfforts 降级管线，
	// 不会把上游不支持的档位打出去。
	if h.cfg.ModelMeta != nil {
		if effort := h.cfg.ModelMeta.EffortFor(realm, bareModel); effort != "" {
			body = injectEffortIfAbsent(body, effort)
		}
		// 手动上下文窗口档位回调：超出档位时裁剪历史 + 注入截断提示词。
		// 先于 payload.go 的 tool 配对清理执行——裁剪产生的孤儿 tool 消息
		// 由那一步兜底剔除，不会把不成对的请求打向上游。
		if limit := h.cfg.ModelMeta.ContextWindowFor(realm, bareModel); limit > 0 {
			if trimmed, dropped := trimToContextBudget(body, limit); dropped > 0 {
				log.Printf("context_trim model=%s realm=%s budget=%d dropped=%d", bareModel, realm, limit, dropped)
				body = trimmed
			}
		}
	}

	// 会话头族（issue #35）：后台按 X-Conversation-Request-ID（对话轮级）聚合请求，
	// 官方客户端一次 user send 内所有 tool call/重试/换号复用同一个 ID。此处**轮转
	// 循环外**生成一次，循环内每次出站原样复用 → 换号/重试/降级全部同 ID，后台不再
	// 碎片化（此前网关一个都不发，上游按 HTTP 请求逐条记账，同一对话几十上百个
	// RequestID）。
	//   - conversationID：body 提取（透传客户端原值，缺省空串——不伪造）；
	//   - conversationRequestID：入站 X-Conversation-Request-ID 透传优先，否则按
	//     粘性 key 进程内稳定生成；粘性 key 也空时走轮级兜底（TurnKey/TurnRequestID），
	//     无 user 消息时退化成本请求级随机——轮转内捕获一次即共享；
	//   - messageID 在 ChatHeaders 内每条消息生成（消息级独立，无需外部可见）。
	chatMeta := upstream.ChatMeta{ConversationID: session.ResolveConversationID(body)}
	if v := r.Header.Get("X-Conversation-Request-ID"); v != "" {
		chatMeta.ConversationRequestID = v
	} else if sessKey != "" {
		chatMeta.ConversationRequestID = session.RequestIDForKey(sessKey)
	} else {
		// 无会话键客户端：轮级兜底——同轮内 tool call 多轮 / 换号重试 / 降级重发
		// 共享同键，用户发下一条消息自动换键。
		chatMeta.ConversationRequestID = session.TurnRequestID(turnKey)
	}
	chatMeta.TraceID = r.Header.Get("X-Trace-ID")

	for i := 0; i < h.cfg.MaxRotate; i++ {
		// 选号：粘性号优先（PickByUIDForModel 已校验该模型可用性 + 在途未满），否则普通轮换。
		var acct *auth.Auth
		if stickyUID != "" {
			acct = h.cfg.Pool.PickByUIDForModel(stickyUID, bareModel)
			if acct == nil || (realm != "" && acct.Realm() != realm) {
				// 粘性号在当前模型不可用（冷却/占满/该模型被 6004 限额）或 realm 不符 → 解绑，
				// 本次回落普通轮换。
				unbindSticky()
				acct = nil
			}
		}
		if acct == nil {
			// 模型感知 + realm 感知选号：模型非空时启用 6004 模型级冷却豁免
			// （healthyForModel），realm 谓词过滤跨域账号。
			acct = h.cfg.Pool.PickExcludingForRealm(tried, bareModel, realm)
		}
		if acct == nil {
			st.status = http.StatusServiceUnavailable
			break
		}
		st.uid = acct.UID
		tried[acct.UID] = true

		// 占用在途名额：Pick 已跳过满额账号，此处 CAS 兜底并发抢名额的竞态。
		if !h.cfg.Pool.Acquire(acct.UID) {
			// 若被抢的正是粘性号，立即解绑并回落普通轮换，避免下一轮仍撞同一个
			// 满载粘性号再浪费一次 PickByUID 往返（语义与 fail()/PickByUID-nil 的解绑一致）。
			if stickyUID != "" && acct.UID == stickyUID {
				unbindSticky()
			}
			if !rotateBackoff(i, r.Context(), upstream.ErrTransport) {
				// 客户端已断连：换号重试无意义，终止轮转走末端错误透传。
				break
			}
			continue // 最后一个名额被并发抢走 → 换号
		}
		heldUID = acct.UID

		// token 临近过期 → 先 refresh（失败冷却换号）
		if acct.NeedsRefresh(h.cfg.RefreshSkew) {
			if err := h.cfg.Upstream.RefreshToken(acct); err != nil {
				lastErr = err
				var ue *upstream.Error
				if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
					h.cfg.Pool.Disable(acct.UID, "refresh session dead")
				} else {
					h.cfg.Pool.NoteError(acct.UID)
				}
				fail(acct.UID)
				if !rotateBackoff(i, r.Context(), upstream.ErrTransport) {
					break // ctx 取消：终止轮转（refresh 失败换号退避）
				}
				continue
			}
			if err := acct.SaveAtomic(); err != nil {
				// 刷新成功但落盘失败：下次启动会用旧 token，必须暴露
				log.Printf("chat refresh uid=%s: save auth failed: %v", acct.UID, err)
			}
		}

		// 客户端 IP 按请求传递（PassthroughIP 开启时注入；消除共享字段竞态）。
		attemptStarted := time.Now()
		rc, status, respBody, terr := h.cfg.Upstream.ChatStreamContext(r.Context(), acct, body, clientIP, chatMeta)
		// 分类信封一次成型：upstream 已在错误路径返回 *upstream.Error（Kind +
		// Retry-After 头解析）。传输层错误（非 *Error）走抖动换号分支；防御分支
		// （terr 为 nil 但 status>=400，如 ErrNone 兜底）回落本地 Classify，双保险。
		var uerr *upstream.Error
		if errors.As(terr, &uerr) {
			status = uerr.Status
		}
		if uerr == nil && terr != nil {
			// 网络层抖动：只换号，不喂熔断计数（传输层错误对连续失败连坐熔断过于严苛）。
			// 连败兜底（issue #114）：喂连败计数——连不上上游是「不知道原因的失败」，
			// 连败 N 次临时出池，单次/偶发不罚（NoteFailures 内部达阈才动作）。
			// 上游 client 已打 transport error 日志。
			recordAttempt(acct.UID, pool.TokenUsageDelta{}, attemptStarted)
			st.status = http.StatusServiceUnavailable
			lastErr = terr
			h.cfg.Pool.NoteFailures(acct.UID)
			fail(acct.UID)
			// 传输层错误区分：timeout/EOF 重退避，其他正常退避
			kind := upstream.ErrTransport
			if strings.Contains(terr.Error(), "timeout") {
				kind = upstream.ErrTransportTimeout
			} else if strings.Contains(terr.Error(), "EOF") {
				kind = upstream.ErrTransportEOF
			}
			if !rotateBackoff(i, r.Context(), kind) {
				break // ctx 取消：终止轮转（传输层错误换号退避）
			}
			continue
		}
		if status >= 400 {
			recordAttempt(acct.UID, pool.TokenUsageDelta{}, attemptStarted)
			st.status = status
			var kind upstream.ErrKind
			if uerr != nil {
				kind = uerr.Kind
			} else {
				kind = upstream.Classify(status, string(respBody))
				uerr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody)}
			}
			// 内容拦截误报（passthrough 模式首遇）：判定为 system 指纹误报，
			// 触发降级到次日 00:00 CST，换 Degraded 中性提示词同请求内重试。
			// 第二次仍被拦（用户内容本身触发审核）→ 回内容防火墙错误（见下分支）。
			// 内容问题非账号问题：applyErrorPolicy 不罚账号（见 ErrContentBlocked 分支）。
			//
			// 【本 fork 明确约束】content_filter 形态（上游 200 + finish_reason=
			// content_filter + 空正文，见 upstream/contentfilter.go）**不做**任何
			// 提示词降级/回退——降级会让"按谁的提示词生成"悄悄改变，属于行为篡改。
			// 该形态只做识别与明确报错（见下分支 code=content_filtered），由用户决定
			// 换模型还是改提示词。
			contentFiltered := uerr != nil && uerr.ContentFilter
			if kind == upstream.ErrContentBlocked && h.promptMode() == "passthrough" && !degradedApplied {
				h.degrade.Trigger()
				body = prompt.Rewrite(body, prompt.Degraded)
				degradedApplied = true
				delete(tried, acct.UID) // 单账号池也能拿到重试机会（降级重试占一次名额）
				releaseHeld()
				log.Printf("content-blocked (likely fingerprint false positive) -> degraded prompt retry")
				continue
			}
			if kind == upstream.ErrContentBlocked {
				// 内容命中网关内容防火墙：立即回客户端，**不轮转**——换任何账号都会撞同一
				// 审核，轮转纯属浪费时间。不罚账号（ErrContentBlocked 分支无冷却/熔断/NoteError）。
				// error-passthrough：message 装上游 body 原文（code/msg/requestId 原样），
				// 不再改写成网关固定文案——客户端必须看到真实错误才能排查。
				h.applyErrorPolicy(acct.UID, kind, string(respBody), bareModel, uerr)
				fail(acct.UID)
				// 内容审核空流（见 upstream/contentfilter.go）单独给码：客户端据此能
				// 区分「模型级输入审核拦了这段内容」与「HTTP 400 内容策略误报」，
				// 前者换模型/换提示词才有解，后者重试即可。
				code := "content_blocked"
				if contentFiltered {
					code = "content_filtered"
				}
				msg := string(respBody)
				if strings.TrimSpace(msg) == "" {
					// 空 body 兜底：无上游原文可透传，保留可读分类文案（不编造原文）。
					msg = "content blocked by upstream content firewall"
				}
				writeOpenAIErrorHint(w, http.StatusBadRequest, code, msg,
					h.hintOf(upstream.ErrContentBlocked, string(respBody), bareModel, reqHasImage, uerr))
				st.status = http.StatusBadRequest
				return
			}
			// 11115「prompt is too long」：立即透传上游原文回客户端，**不罚号不轮转**
			// ——上下文超限是请求的问题（同一 body 换任何号都超限，白扔健康号配额；
			// 与 WAF IP fail-fast 同哲学：确定与账号无关的错误直接终止轮转）。
			// applyErrorPolicy ErrPromptTooLong 分支零动作，fail 只释放租约。
			// message 装上游 body 原文（含真实 token 数与上限值——上游原文是最有价值
			// 的错误信息，客户端必须看到，禁止固定词覆盖）。
			if kind == upstream.ErrPromptTooLong {
				h.applyErrorPolicy(acct.UID, kind, string(respBody), bareModel, uerr)
				fail(acct.UID)
				writeOpenAIErrorHint(w, http.StatusBadRequest, "prompt_too_long", promptTooLongMessage(string(respBody)),
					h.hintOf(upstream.ErrPromptTooLong, string(respBody), bareModel, reqHasImage, uerr))
				st.status = http.StatusBadRequest
				return
			}
			// lastErr 携带完整 body（uerr.Msg 在 upstream 侧截断 200 字符，透传语义
			// 要求原文全量）+ Kind/RetryAfter（末端映射与冷却时长共用）。
			lastErr = &upstream.Error{Kind: kind, Status: status, Msg: string(respBody), RetryAfter: uerr.RetryAfter}
			h.applyErrorPolicy(acct.UID, kind, string(respBody), bareModel, uerr)
			fail(acct.UID)
			// WAF IP 级 fail-fast（优先于 rotateBackoff 退避——IP 级拦截时退避无意义）：
			// 该次 WAF 403 喂入 IP 级状态机，若激活（短窗多号命中，IP 被拦而非账号）
			// 则立即终止轮转——继续换号只会把请求放大 MaxRotate 倍打同一出口 IP，
			// 加重风控。账号级软冷却已在上方 applyErrorPolicy 照常记账。
			// 单出口/直连时才启用进程级 IP WAF 门控；global 多出口池不做这个
			// 全局限制：当前出口 403 已由 proxyRT 标记降权，下一次轮转换另一个
			// resin/warp 出口即可。若这里仍 break，会把某一个出口的 WAF 扩散成
			// 整个 global 域 503，正是用户反馈的「不要乱限制」。
			multiProxyGlobal := realm == "global" && h.cfg.Upstream != nil && h.cfg.Upstream.GlobalProxyCount() > 1
			if kind == upstream.ErrWafBlock && !multiProxyGlobal && h.wafIP.noteWaf(acct.UID) {
				break
			}
			if !rotateBackoff(i, r.Context(), kind) {
				break // ctx 取消：终止轮转（分类错误换号退避）
			}
			continue
		}
		h.cfg.Pool.NoteSuccess(acct.UID)
		if degradedApplied {
			// 内容拦截后的中性提示词重试成功：这条日志说明「本次回答不是按用户
			// 配置的提示词生成的」——排查 persona 不符时先看这里。
			log.Printf("content-blocked -> degraded prompt retry succeeded uid=%s model=%s mode=%s",
				acct.UID, bareModel, h.promptMode())
		}
		// 11102 负缓存清命：该账号该模型实测成功，立即解除避让（不必等 TTL 到期）。
		// BlockModelClear 按 "11102" reason 前缀识别，只清 11102 条目、不碰 6004 独立冷却。
		h.cfg.Pool.BlockModelClear(acct.UID, bareModel)
		// 粘性跟随最终成功号：本轮成功的账号成为该会话的粘性绑定（覆盖旧绑定）。
		// 若 sticky 号失败、轮换到别的号成功，这里把会话重绑到新号，多轮对话下一跳不再随机抽。
		if sessKey != "" && h.cfg.Session != nil {
			h.cfg.Session.Bind(sessKey, acct.UID)
		}
		if peek.Stream {
			// 流式：透传结束后立即关闭上游 body，避免 defer 在轮转场景下堆积 fd。
			st.status = http.StatusOK
			stats := newChatStatsReaderSince(rc, st.start)
			// gateway_hint（SSE）：成功状态 200 已开流，中途 error 帧透传时附加
			// hint 字段（hintFn 惰性求值——正常流零开销，只有真撞到 error 帧才
			// 组装请求上下文做判定）。
			_ = upstream.StreamHint(w, stats, upstream.FrameHintFunc(func() upstream.HintContext {
				return h.hintContext(bareModel, reqHasImage)
			}))
			st.ttfb = stats.TTFB()
			st.toks, _ = stats.Tokens()
			// 成本账本 + 用量积分：末帧 usage 带 credit 与 token 总数时，
			// 既记实测单价（供选号排序），也把积分计入用量时序（供用量页统计）。
			du := stats.Usage()
			if credit, ok := stats.Credit(); ok {
				du.HasCredit, du.Credit = true, credit
				// 日志行的积分列（cr=）与用量统计同源：上游末帧 usage.credit。
				st.credit, st.hasCredit = credit, true
				if total, tok := stats.TotalTokens(); tok && total > 0 {
					h.cfg.Pool.NoteModelCost(acct.UID, bareModel, credit, total)
				}
			}
			recordAttempt(acct.UID, du, attemptStarted)
			rc.Close()
			return
		}
		resp, err := upstream.Aggregate(rc)
		rc.Close()
		if err != nil {
			recordAttempt(acct.UID, pool.TokenUsageDelta{}, attemptStarted)
			// 上游流解析失败：客户端还没看到任何输出，回 502 并告知原因。
			writeOpenAIError(w, http.StatusBadGateway, "upstream_parse", err.Error())
			st.status = http.StatusBadGateway
			return
		}
		du := usageDeltaFromResponse(resp)
		// 成本账本 + 用量积分（非流式）：从聚合响应的 usage 取 credit 与 token 总数。
		if credit, total, ok := usageCreditTotal(resp); ok {
			du.HasCredit, du.Credit = true, credit
			st.credit, st.hasCredit = credit, true
			h.cfg.Pool.NoteModelCost(acct.UID, bareModel, credit, total)
		}
		recordAttempt(acct.UID, du, attemptStarted)
		writeJSON(w, http.StatusOK, resp)
		st.status = http.StatusOK
		st.toks = completionTokens(resp)
		return
	}
	// 末端错误透传（error-passthrough）：上游返回的错误原样透传，不再规范化成固定文案。
	// 上游返回（*upstream.Error）→ error.message 装**上游 body 原文**（code/msg/
	// requestId 原样保留）。HTTP 状态码按 OpenAI 兼容口径映射类别：ErrSoftRate → 429
	// （限流语义、客户端应等待重试），其余保持 503。本地调度类错误（无可用账号/
	// 传输层抖动/非上游返回的 lastErr）→ 保留自有文案 no_healthy_account（本地错误
	// 没有上游原文可透传，不编造）。
	status := http.StatusServiceUnavailable
	code := "no_healthy_account"
	msg := "all accounts are temporarily unavailable, please retry later"
	// gateway_hint（末端透传）：上游错误按 Kind + 原文 + 请求形态判定；本地调度类
	// 错误（无上游原文）固定 no_healthy_account hint。
	hint := upstream.NoHealthyAccountHint()
	var ue *upstream.Error
	if !errors.As(lastErr, &ue) && h.cfg.Pool.ModelBlockedEverywhere(bareModel, realm) {
		// 本地选号失败（没有上游原文），但**全部健康账号**都已对该模型做过 11102
		// 负缓存 —— 问题在模型，不在账号。此时报 503 no_healthy_account 会让用户
		// 以为"账号全挂了"，客户端还会不停换号重试（换号对这个模型永远无效）。
		// 与上游直返 11102 的分支保持同一语义：404 + model_not_found。
		writeOpenAIErrorHint(w, http.StatusNotFound, "model_not_found",
			"model "+bareModel+" is listed in the upstream catalog but has no service on this backend",
			h.hintOf(upstream.ErrModelBlocked, "", bareModel, reqHasImage, nil))
		st.status = http.StatusNotFound
		return
	}
	if errors.As(lastErr, &ue) {
		hint = h.hintOf(ue.Kind, ue.Msg, bareModel, reqHasImage, ue)
		switch ue.Kind {
		case upstream.ErrSoftRate:
			status = http.StatusTooManyRequests
			code = "rate_limit_exceeded"
			msg = "rate limited: all accounts are cooling down, please wait a moment and try again"
		case upstream.ErrModelBlocked:
			// 11102「该后端无此模型」：上游**目录里有这个 ID**，但实际调用时该后端
			// 没有它的服务信息（幽灵模型，如 auto-chat / default-model）。
			//
			// 这是**模型级**问题，与账号健康无关。此前掉进默认分支被报成
			// 503 no_healthy_account —— 用户会以为"账号全挂了"，客户端还会不断
			// 换号重试（换号根本没用，任何账号都调不了这个模型）。
			// 按 OpenAI 规范报 404 model_not_found，让客户端立刻停止重试并提示
			// 用户换模型；同时 404 不参与 Retry-After（下方只对 503/429 生效）。
			status = http.StatusNotFound
			code = "model_not_found"
			msg = "model " + bareModel + " is listed in the upstream catalog but has no service on this backend"
		case upstream.ErrWafBlock:
			// 多出口 global 已经由代理池做出口级切换，不把某个出口的 WAF
			// 状态包装成全局 waf_ip_blocked；只有直连/单出口才显示该限制。
			multiProxyGlobal := realm == "global" && h.cfg.Upstream != nil && h.cfg.Upstream.GlobalProxyCount() > 1
			if h.wafIP.active() && !multiProxyGlobal {
				// IP 级拦截措辞（fail-fast 终止路径）：网关出口 IP 被 WAF 拦截、
				// 轮转已止损、窗口过后自动解除。客户端提前重试无意义（换号不换 IP）；
				// 有上游原文时原文优先（下方统一）。
				code = "waf_ip_blocked"
				msg = "waf ip-level block: upstream firewall is blocking the gateway IP, rotation stopped; retry after the block window expires"
			} else {
				code = "upstream_waf_blocked"
				msg = "upstream WAF blocked the request"
			}
		}
		if s := strings.TrimSpace(ue.Msg); s != "" {
			if upstream.IsWafBlockPage(s) {
				// WAF 拦截页（HTML）不做原文透传：几 KB 的标签对客户端零信息量，
				// 用户只会看到一坨 HTML 并误以为网关坏了（2026-09-19 实际反馈）。
				// 换成可操作摘要：拦截类型 + 请求 UUID（提工单用）+ 怎么办。
				// 下方 gateway_hint 已有完整处置建议，与此处并列不重复。
				msg = "upstream content firewall (WAF) rejected this request: the request body matches attack-signature rules " +
					"(SQL injection / path traversal / XSS / command execution). Switch to the same model on the cn realm (cn:<model>) or strip raw payloads from the conversation"
				if u := upstream.WafRequestUUID(s); u != "" {
					msg += " (waf_request_uuid=" + u + ")"
				}
			} else {
				// 上游原文优先：透传 code/msg/requestId，不拼接本地前缀。
				msg = s
			}
		}
	}
	// Retry-After：503（无可用账号）/ 429（限流）此前不带任何退避信号，客户端
	// （含官方 IDE）会以约 3 次/秒 的密度重打——实测一次 503 故障窗口内单客户端
	// 打了 298 次（约 0.3s 一次），纯噪音且放大上游压力。优先用上游明示值
	// （ue.RetryAfter，来自 Retry-After / x-ratelimit-reset 头），无则给保守缺省：
	// 503 用 5s（等冷却/换号窗口），429 用 10s（限流恢复通常更慢）。
	if status == http.StatusServiceUnavailable || status == http.StatusTooManyRequests {
		ra := 5 * time.Second
		if status == http.StatusTooManyRequests {
			ra = 10 * time.Second
		}
		if ue != nil && ue.RetryAfter > 0 {
			ra = ue.RetryAfter
		}
		if secs := int(ra.Seconds()); secs > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", secs))
		}
	}
	writeOpenAIErrorHint(w, status, code, msg, hint)
	st.status = status
}

// promptTooLongMessage 11115 透传 message：上游 body 原文（含真实 token 数/
// 上限值/requestId，客户端自行排查）；空 body 兜底为可读分类短文案（不编造原文）。
func promptTooLongMessage(body string) string {
	if strings.TrimSpace(body) == "" {
		return "prompt is too long"
	}
	return body
}

// usageCreditTotal 从聚合响应取 usage.credit 与 total_tokens（成本台账非流式入口）。
// 任一字段缺失/非法 → ok=false（不记录）。
func usageCreditTotal(resp map[string]any) (credit float64, total int, ok bool) {
	usage, _ := resp["usage"].(map[string]any)
	if usage == nil {
		return 0, 0, false
	}
	c, _ := usage["credit"].(float64)
	t, _ := usage["total_tokens"].(float64)
	if t <= 0 {
		return 0, 0, false
	}
	return c, int(t), true
}

// rotateBackoff 轮转间指数退避 + 抖动（WAF 403 修复 P0-2）：第 i 次轮转失败
// （continue 换号前）等待 backoffAfter(i)（500ms·2^i 封顶 8s，±25% 抖动），
// ctx 取消（客户端断连/优雅停机）返回 false——调用方立即终止轮转（客户端已走，
// 换号重试无意义）。退避是「换号前歇一下」让上游频控窗口滑过；正常单号请求
// （首次成功）不经过本函数，零开销。
func rotateBackoff(i int, ctx context.Context, kind upstream.ErrKind) bool {
	d := backoffAfter(i, kind)
	if d <= 0 {
		return ctx.Err() == nil
	}
	if !sleepCtx(ctx, d) {
		log.Printf("WARN: [server] rotate backoff aborted: ctx cancelled")
		return false
	}
	return true
}

// applyErrorPolicy 按错误分类对账号施加冷却/禁用/熔断策略（最终版状态机）。
// kind 是唯一权威分类（来自 upstream.Classify / ChatStreamContext 的 *Error 信封），
// 此处不再按原始 status 二次判断。仅在 chatCompletions 轮转循环内调用：内容拦截
// 会立即 400 返回，其余种类 continue 换号（continue 前由 rotateBackoff 退避）。
//
// 九条路径，各司其职：
//   - ErrHardCredit → CooldownUntilTomorrow4AM：即时硬冷却到次日 04:00（等签到恢复）。
//   - ErrSoftRate → 优先对齐上游重置墙钟（带「将在 … 重置」时 6004 走模型级豁免、
//     非 6004 走账号级，均不指数堆加）；无重置时间才走有界退避。冷却时长优先采信
//     Retry-After 头（uerr.RetryAfter，body 文案墙钟之外的头形态来源）。
//   - ErrWafBlock → 账号级软冷却：**不 Disable**——WAF 403 是 IP/指纹维频控信号，
//     罚过即走、到期自愈。时长优先 Retry-After 头；缺失按 wafCooldownBase(60s)
//     起 · softStreak 指数、封顶 soft_rate_max 的既有 CooldownSoftRate 有界退避。
//     基数经 jitterDur 抖动（防多账号同相位冷却到期再聚团）。
//   - ErrNotFound → Cooldown(CoolSoft, notFoundCooldown 固定 60s)：短冷却防雪崩。
//   - ErrSessionDead → Disable：session 死亡，永久禁用（需人工重登）。
//   - ErrContentBlocked → 不罚账号；passthrough 首遇触发降级重试，最终仍拦则回 400。
//   - ErrBadParams → 不罚账号（同 ErrContentBlocked 待遇），但仍轮转。
//   - ErrPromptTooLong → 11115：请求的问题不是账号的问题。零动作（不冷却/不熔断/
//     不 NoteError、不喂连败），chatCompletions 已直接透传原文返回不轮转。
//   - ErrModelBlocked → BlockModelBackoff：(账号, 模型) 11102 负缓存避让。
//   - ErrServer → NoteError：喂单一连续失败计数器 fails + 累计错误 errTotal，
//     达到 breakerThreshold 触发熔断（指数退避）。
//   - 其他（default：ErrClient/ErrNone）→ 只换号不罚（防雪崩），不喂熔断；ErrClient
//     额外喂连败计数（NoteFailures，issue #114）：未知 4xx 连败 N 次临时出池。
//
// body 仅在 ErrSoftRate/ErrAccountFault 分支用于解析重置时间/分野；model 为请求
// 携带的模型名。uerr 是 ChatStreamContext 返回的分类信封（可携带 RetryAfter）；
// 零值/防御路径下为 nil，冷却时长回落既有计算。
func (h *Handler) applyErrorPolicy(uid string, kind upstream.ErrKind, body, model string, uerr *upstream.Error) {
	switch kind {
	case upstream.ErrHardCredit:
		// 402 + 余额关键词即积分耗尽：同步冷却到次日 04:00（签到任务 09/21 点恢复），
		// 不需要异步核查（冗余）。立即换号。
		h.cfg.Pool.CooldownUntilTomorrow4AM(uid, "余额不足")
	case upstream.ErrSoftRate:
		// 上游排队（6020 queue.waiting）**优先判**：它不是"账号被限流"，而是该模型
		// 此刻在排大队（实测 queue_position 2000+）。必须走模型级冷却——账号保持
		// 健康、其它模型照常可用。若按账号级处理，一个排队模型会把池里的号依次
		// 打成 600s 冷却，池空之后**连别的模型一起 503**（2026-09-22 线上事故：
		// 483 次 429 + 146 次 uid=- 的 503，用户以为"账号全废了"而删号）。
		// 冷却时长取 body 的 data.retry_after（上游建议值），夹到 [15s, 2min]。
		if upstream.IsQueueWait(body) {
			wait := upstream.ParseQueueWait(body)
			if wait < queueWaitMin {
				wait = queueWaitMin
			}
			if wait > queueWaitMax {
				wait = queueWaitMax
			}
			h.cfg.Pool.CooldownSoftForModel(uid, h.softCooldown(), time.Now().Add(wait), model, "6020 upstream queue")
			return
		}
		// 统一对齐上游重置时间：只要 body 带「将在 … 重置」，无论业务 code 是
		// 6004 还是 11140 rate-limiting 等形态，都精确冷却到该墙钟、绝不指数堆加。
		//   - 模型级（6004）→ CooldownSoftForModel：写 modelCooldowns[model]，切模型豁免。
		//   - 账号级（非 6004）→ CooldownSoftRate：写账号级 until，不产生模型豁免。
		if resetAt, ok := upstream.ParseRateReset(body); ok {
			if upstream.IsModelRateLimit(body) {
				h.cfg.Pool.CooldownSoftForModel(uid, h.softCooldown(), resetAt, model, "6004 model rate limit")
				return
			}
			h.cfg.Pool.CooldownSoftRate(uid, h.softCooldown(), resetAt, "429 rate limit")
			return
		}
		// body 无重置文案但带 Retry-After 头 → 冷却到该时刻（不做指数堆加）。
		// 头优先于「有界退避」，但低于 body 重置文案（文案是上游更权威的口径）。
		if uerr != nil && uerr.RetryAfter > 0 {
			h.cfg.Pool.CooldownSoftRate(uid, h.softCooldown(), time.Now().Add(uerr.RetryAfter), "429 rate limit (retry-after)")
			return
		}
		// 无重置时间 → 账号级有界退避（soft_rate 基数起、softStreak 翻倍、封顶
		// soft_rate_max；已在冷却中的兜底探测不翻倍）。基数取 h.softCooldown()
		// （热改优先），管理面板改 soft_rate 后立即生效。
		h.cfg.Pool.CooldownSoftRate(uid, h.softCooldown(), time.Time{}, "429 rate limit")
	case upstream.ErrWafBlock:
		// WAF 403（无业务信封拦截形态）。软冷却复用 CooldownSoftRate 家族：基数
		// wafCooldownBase（60s，抖动后落 [45s,75s]）、softStreak 指数升级、封顶
		// soft_rate_max、冷却中兜底探测不翻倍——全部继承既有语义。
		// Retry-After 头优先（WAF 拦截页可能带该头）。不 Disable。
		// P0-WAF-IP-SINGLE：走 CooldownWaf（有界、不推进 softStreak）而非
		// CooldownSoftRate（指数堆加到 2h）——WAF 403 与账号无关，堆加只会放大伤害。
		if uerr != nil && uerr.RetryAfter > 0 {
			h.cfg.Pool.CooldownWaf(uid, jitterDur(wafCooldownBase), time.Now().Add(uerr.RetryAfter), "waf 403 block (retry-after)")
			return
		}
		h.cfg.Pool.CooldownWaf(uid, jitterDur(wafCooldownBase), time.Time{}, "waf 403 block")
	case upstream.ErrSessionDead:
		h.cfg.Pool.Disable(uid, "12153 session dead")
	case upstream.ErrNotFound:
		// 404 短冷却（软冷却），防雪崩。固定 notFoundCooldown，不随 soft_rate 退避：
		// 偶发路径缺失不是限流信号，不该按限流惩罚升级。
		h.cfg.Pool.Cooldown(uid, pool.CoolSoft, notFoundCooldown, "upstream 404")
	case upstream.ErrAccountFault:
		// 账号级授权/配额故障按 msg 分野（口径与 Classify 的 accountFaultMarkers 一致）：
		//   - "request illegal"（code 11140）→ 账号级**授权封禁**：硬禁用（Disable）。
		//   - 14017（trial not activated）→ register 未完成，补完 register 后可能自愈，
		//     **保持软冷却**（禁用会让用户补完 register 后仍无法用）。
		// 大小写不敏感（与 Classify 的 marker 匹配同口径）。
		if strings.Contains(strings.ToLower(body), "request illegal") {
			h.cfg.Pool.Disable(uid, "account banned by upstream (11140 request illegal), re-login required")
			return
		}
		h.cfg.Pool.Cooldown(uid, pool.CoolSoft, h.cfg.SoftCooldown, "account fault (14017)")
	case upstream.ErrServer:
		// 5xx 上游故障：Classify 已把 ≥500 判为 ErrServer，在此喂熔断计数（不再手写 status>=500）。
		h.cfg.Pool.NoteError(uid)
	case upstream.ErrContentBlocked:
		// 内容策略拦截（误报）：内容问题非账号问题，不罚账号（无冷却/熔断/NoteError）。
		// passthrough 模式由 chatCompletions 内降级重试处理；custom 模式本不会到此分支。
	case upstream.ErrPromptTooLong:
		// 11115「prompt is too long」：请求的问题不是账号的问题（同一 body 换任何
		// 号都超限）。零动作（不冷却/不熔断/不 NoteError，同 ErrContentBlocked 待遇），
		// chatCompletions 已直接透传原文返回不轮转——该分支只为文档完备。
	case upstream.ErrBadParams:
		// 请求体解析失败（400 + Unmarshal chat params failed / 11101）：发给上游的 body
		// 有问题（网关截断已由 413 消灭，剩余为客户端畸形 JSON）。换了账号照样 400，
		// 不罚账号（无冷却/熔断/NoteError，同 ErrContentBlocked 待遇）；但**仍然轮转**
		// ——不同账号可能有不同的模型权限，值得换号再试一次。
	case upstream.ErrModelBlocked:
		// 11102「该后端无此模型」：(账号, 模型) 负缓存避让。复用 modelCooldowns 机制
		// （与 6004 同域），选号侧 healthyForModel 对该账号自动避开该模型。
		// 立即换号（本轮 continue），该账号该模型冷却，下次选号避开。
		h.cfg.Pool.BlockModelBackoff(uid, model, upstream.ModelBlockReason)
	default:
		// 其余（ErrClient/ErrNone）：只换号不罚（防雪崩），不喂熔断。
		// ErrClient（未知 4xx）喂连败计数（issue #114）：连续 N 次该形态失败 →
		// 账号临时出池（NoteFailures 达阈降权），单次/偶发不罚（不误伤）。ErrNone
		// 到这里属防御路径（status>=400 但分类成功），语义不明不喂。
		if kind == upstream.ErrClient {
			h.cfg.Pool.NoteFailures(uid)
		}
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    "api_error",
			"code":    code,
		},
	})
}

// queueWaitMin / queueWaitMax 上游「排队等待」（6020 queue.waiting）模型级冷却的
// 夹取区间，与 upstream.ParseQueueWait 的建议值配合使用。上限刻意压到 2 分钟：
// retry_after 是"建议重试间隔"而非恢复时刻，队列场景下取太长等于把该模型封死。
const (
	queueWaitMin = 15 * time.Second
	queueWaitMax = 2 * time.Minute
)

// wafCooldownBase WAF 403 软冷却基数（建议 60s 起；抖动 ±25% 后落 [45s,75s]，
// 实际进入 CooldownSoftRate 后再按 softStreak 指数、封顶 soft_rate_max）。
// 与 SoftCooldown 分流的原因：WAF 403 是 IP/指纹维频控，信号比 429「账号级限流」轻
// （账号本身健康），但比 404 重（带粘性会连环）；60s 级的快速避让已足够让频控窗口
// 滑过。抖动复用 backoff.go jitterDur（单一来源）。
const wafCooldownBase = 60 * time.Second

// writeOpenAIErrorHint 同 writeOpenAIError，另在 error 对象上附加
// error.gateway_hint（hint 为空串时不带字段——未覆盖形态不编造）。
// message 仍是上游原文透传（hint 只做并列补充，绝不替换/包装 message）。
func writeOpenAIErrorHint(w http.ResponseWriter, status int, code, msg, hint string) {
	if hint == "" {
		writeOpenAIError(w, status, code, msg)
		return
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message":      msg,
			"type":         "api_error",
			"code":         code,
			"gateway_hint": hint,
		},
	})
}

// hasImagePart 报告聊天请求体是否携带多模态 image_url part（OpenAI 兼容形态
// messages[].content[] {type:"image_url"}）。畸形/其他形态一律 false（hint 侧
// 宁缺勿滥：判不出带图就不给「模型不支持图片」指向）。
func hasImagePart(body []byte) bool {
	var peek struct {
		Messages []struct {
			Content []struct {
				Type string `json:"type"`
			} `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &peek) != nil {
		return false
	}
	for _, m := range peek.Messages {
		for _, p := range m.Content {
			if p.Type == "image_url" {
				return true
			}
		}
	}
	return false
}

// hintContext 组装 chatCompletions 的 gateway_hint 判定上下文：请求裸模型名 +
// 是否带图 + 模型目录 supports_images 声明（目录未收录 → ModelInCatalog=false，
// 不做「不支持」判定，防查不到误判）。仅错误路径调用（成功请求零开销）。
//
// 目录查询只读既有缓存快照（cachedModelsSnapshot），**不触发上游拉取**：错误路径
// 加一次 FetchModels 网络调用既拖慢错误响应、又污染上游调用语义（错误风暴时放大
// 请求量——与 WAF IP fail-fast 的「不放大请求量」哲学相悖）。缓存冷（最近 1h 未
// 拉过）→ ModelInCatalog=false，11133 退中性 hint（宁缺勿滥，不编造能力事实）。
func (h *Handler) hintContext(bareModel string, hasImage bool) upstream.HintContext {
	ctx := upstream.HintContext{Model: bareModel, HasImage: hasImage}
	if bareModel == "" {
		return ctx
	}
	for _, mi := range cachedModelsSnapshot() {
		if mi.ID == bareModel {
			ctx.ModelInCatalog = true
			ctx.ModelSupportsImages = mi.SupportsImages
			return ctx
		}
	}
	return ctx
}

// hintOf 末端错误透传的统一 hint 入口：kind + 上游原文 + 请求上下文 →
// gateway_hint 文案（upstream.GatewayHint 单一事实来源）。uerr 为 nil 时回落
// body 原文判定（防御路径）。transport 层错误（lastErr 非 *upstream.Error 且
// 上游没回 body）→ 无 hint（不编造）。
func (h *Handler) hintOf(kind upstream.ErrKind, body, bareModel string, hasImage bool, uerr *upstream.Error) string {
	msg := body
	if uerr != nil && uerr.Msg != "" {
		msg = uerr.Msg
	}
	return upstream.GatewayHint(kind, msg, h.hintContext(bareModel, hasImage))
}
