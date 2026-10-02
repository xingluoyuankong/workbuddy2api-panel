// Package panel 内嵌式 Web 管理面板：账号池总览、单号运维（解冻/禁用/签到/
// 刷新余额/移除）、浏览器内 OAuth 添加账号（免重启热加载进池）、手动批量
// 签到/保活，以及运行日志环形缓冲（镜像 log 包与 chat 表格日志）。
//
// 设计约束：
//   - 前端 go:embed 单文件（index.html），无任何外部构建依赖，与二进制同体部署；
//   - 鉴权复用网关 api_key（Bearer），与 /v1/* 同一口径；api_key 为空 = 不鉴权
//     （仅本机/私网使用）。面板 HTML 本身无秘密，可匿名加载，密钥只发给 /panel/api/*；
//   - 不改写既有池语义：所有运维操作落到 pool 已有入口（Revive/Disable/Remove...），
//     添加账号走 auth.SaveAtomic + pool.Add，重启后与 auths/ 目录天然对齐。
package panel

import (
	"context"
	"encoding/json"
	"github.com/linguo2625469/workbuddy2api-panel/internal/prompt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/httpauth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/livecfg"
	"github.com/linguo2625469/workbuddy2api-panel/internal/modelmeta"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
	"github.com/linguo2625469/workbuddy2api-panel/internal/scheduler"
	"github.com/linguo2625469/workbuddy2api-panel/internal/session"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
	"github.com/linguo2625469/workbuddy2api-panel/internal/usage"
)

// Config 面板依赖（main 装配注入）。
type Config struct {
	Pool      *pool.Pool
	Upstream  *upstream.Client
	SubPool   *upstream.SubPool
	Scheduler *scheduler.Scheduler // 手动触发签到/保活；nil 时对应接口返回 501
	AuthDir   string               // OAuth 登录完成后凭证落盘目录
	APIKey    string               // 空 = 不鉴权（与主服务同语义）；与 Live 同时给出时 Live 优先
	RedisMode string               // "upstash" / "noop"，仅观测透出
	Version   string               // 面板版本号（展示用）

	// Live 运行期可变配置（在线改配置立即生效）。
	Live *livecfg.Holder

	// ConfigPath config.json 路径与加载器（配置页读写用）。
	// LoadConfig 返回解析后的配置对象（前端展示/校验用，具体类型由 main 注入的闭包决定）；
	// nil 时配置页返回 501。
	ConfigPath string
	LoadConfig func() (any, error)
	// SaveConfig 校验并落盘配置，返回需要重启才能生效的字段列表；随后由 main 注入的
	// ApplyConfig 闭包完成热生效（池参数/排程/密钥/脱敏）。error 时配置不写盘。
	SaveConfig func(raw []byte) (restartRequired []string, err error)

	// StickyCount 返回粘性会话绑定数；nil 时报告 0。
	StickyCount func() int

	// Session 会话粘性路由器（可选；nil = 粘性管理接口返回 501）。
	// 供面板「重置/清理粘性会话」用：List 查看绑定、Clear 全部重置、
	// ClearUID 解开钉在某个账号上的会话、GCNow 立即清过期。
	Session *session.Router

	// Usage 逐请求用量记录器（nil = 用量接口返回 501）。
	Usage *usage.Recorder

	// ProbeFile 模型输出上限探测结果文件（scripts/probe_max_tokens.py --panel-out
	// 写入；空或文件不存在 = model_probes 端点返回空集，面板不显示任何实测标注）。
	// 只读展示：网关不解析、不依赖其内容做任何路由/出站决策。
	ProbeFile string

	// ModelMeta 模型元数据仓库（可选；nil = 元数据相关接口返回 501）。
	ModelMeta *modelmeta.Store

	// ModelVerifier 模型可调用性验证器（可选；nil = 验证接口返回 501）。
	ModelVerifier *modelmeta.Verifier

	// ListModels 返回 /v1/models 同口径的模型条目（可选；nil = 模型管理页退回
	// 上游 CN 直连名单）。由 main 注入 server.Handler 的 modelList，
	// 保证「面板看到的」与「客户端拿到的」永不漂移。
	ListModels func() []map[string]any

	// ModelProbe 验证用的真实调用入口（可选；ModelVerifier 非 nil 时必需）。
	ModelProbe modelmeta.ProbeFunc
}

// Panel 管理面板 handler。挂载方式：外层 mux Handle("/panel/", panel)，
// 本 mux 的 pattern 均带 /panel 前缀（外层不做前缀剥离）。
type Panel struct {
	cfg     Config
	subPool *upstream.SubPool
	mux     *http.ServeMux
	started time.Time
	logs    *Ring

	// logins 进行中的 OAuth 设备授权会话（state → 会话信息）。
	// poll 成功或超时（loginTTL）后剔除；面板常驻进程，容量天然有界。
	loginMu sync.Mutex
	logins  map[string]loginSession

	// taskMu/taskLocks 一键完成任务的 per-account 互斥：同一账号的任务动作
	// （单任务 / 全量）同时只允许一条在跑。重复点击直接返回 409"仍在执行"，
	// 而不是并发跑两遍浪费上游请求（动作虽幂等，expert 系每遍含 8 次真实对话）。
	// 不同账号之间不互斥（并行照旧）。TryLock 语义，锁条目常驻（账号数有界）。
	taskMu    sync.Mutex
	taskLocks map[string]*sync.Mutex

	// 任务中心执行队列（taskcenter.go）。
	queueOnce sync.Once
	q         *queueState
}

// tryLockAccount 尝试锁定账号的任务执行；已在执行返回 false。
func (p *Panel) tryLockAccount(uid string) bool {
	p.taskMu.Lock()
	if p.taskLocks == nil {
		p.taskLocks = make(map[string]*sync.Mutex)
	}
	mu := p.taskLocks[uid]
	if mu == nil {
		mu = &sync.Mutex{}
		p.taskLocks[uid] = mu
	}
	p.taskMu.Unlock()
	return mu.TryLock()
}

// unlockAccount 释放账号任务锁（与 tryLockAccount 配对）。
func (p *Panel) unlockAccount(uid string) {
	p.taskMu.Lock()
	mu := p.taskLocks[uid]
	p.taskMu.Unlock()
	if mu != nil {
		mu.Unlock()
	}
}

// loginTTL 授权 URL 的最长有效期：超时的 state 直接回收，
// 防止"开了添加账号弹窗就走开"的会话永久滞留。
const loginTTL = 15 * time.Minute

// loginSession 进行中的 OAuth 会话：创建时刻 + realm（cn/global，用于落盘与端点切换）。
type loginSession struct {
	created time.Time
	realm   string // "cn" / "global"，缺省 cn
}

// New 构建面板。
func New(cfg Config) *Panel {
	if cfg.RedisMode == "" {
		cfg.RedisMode = "noop"
	}
	p := &Panel{
		cfg:     cfg,
		subPool: cfg.SubPool,
		mux:     http.NewServeMux(),
		started: time.Now(),
		logs:    NewRing(500),
		logins:  map[string]loginSession{},
	}
	p.routes()
	return p
}

// Logs 返回日志环形缓冲（main 经 MultiWriter 镜像 log 与 chat 表格日志进来）。
func (p *Panel) Logs() *Ring { return p.logs }

func (p *Panel) routes() {
	p.mux.HandleFunc("GET /panel/{$}", p.index)
	p.mux.HandleFunc("GET /panel/app.js", p.appScript)
	p.mux.HandleFunc("GET /panel/api/overview", p.withAuth(p.overview))
	p.mux.HandleFunc("GET /panel/api/logs", p.withAuth(p.logsHandler))
	p.mux.HandleFunc("GET /panel/api/models", p.withAuth(p.models))
	p.mux.HandleFunc("GET /panel/api/modelmeta", p.withAuth(p.modelMeta))
	p.mux.HandleFunc("POST /panel/api/models/verify", p.withAuth(p.modelVerify))
	p.mux.HandleFunc("POST /panel/api/models/meta", p.withAuth(p.modelMetaSave))
	p.mux.HandleFunc("POST /panel/api/models/only_verified", p.withAuth(p.modelOnlyVerified))
	p.mux.HandleFunc("POST /panel/api/models/purge_ghosts", p.withAuth(p.modelPurgeGhosts))
	p.mux.HandleFunc("POST /panel/api/login/start", p.withAuth(p.loginStart))
	p.mux.HandleFunc("GET /panel/api/login/poll", p.withAuth(p.loginPoll))
	p.mux.HandleFunc("GET /panel/api/login/regions", p.withAuth(p.loginRegions))
	p.mux.HandleFunc("GET /panel/api/prompt", p.withAuth(p.promptGet))
	p.mux.HandleFunc("POST /panel/api/prompt", p.withAuth(p.promptSave))
	p.mux.HandleFunc("GET /panel/api/sessions", p.withAuth(p.sessionList))
	p.mux.HandleFunc("POST /panel/api/sessions/clear", p.withAuth(p.sessionClear))
	p.mux.HandleFunc("POST /panel/api/sessions/gc", p.withAuth(p.sessionGC))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/revive", p.withAuth(p.accountRevive))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/disable", p.withAuth(p.accountDisable))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/checkin", p.withAuth(p.accountCheckin))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/balance", p.withAuth(p.accountBalance))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/remove", p.withAuth(p.accountRemove))
	p.mux.HandleFunc("GET /panel/api/accounts/{uid}/tasks", p.withAuth(p.accountTasks))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/tasks/accept", p.withAuth(p.accountTaskAccept))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/tasks/accept_all", p.withAuth(p.taskAcceptAll))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/tasks/claim", p.withAuth(p.accountTaskClaim))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/tasks/auto", p.withAuth(p.accountTaskAuto))
	p.mux.HandleFunc("POST /panel/api/accounts/{uid}/tasks/auto_all", p.withAuth(p.accountTaskAutoAll))
	p.mux.HandleFunc("POST /panel/api/tasks/scan_all", p.withAuth(p.tasksScanAll))
	p.mux.HandleFunc("POST /panel/api/tasks/run_queue", p.withAuth(p.tasksRunQueue))
	p.mux.HandleFunc("GET /panel/api/tasks/queue", p.withAuth(p.tasksQueueStatus))
	p.mux.HandleFunc("GET /panel/api/school/status", p.withAuth(p.schoolStatus))
	p.mux.HandleFunc("POST /panel/api/school/run_all", p.withAuth(p.schoolRunAll))
	p.mux.HandleFunc("GET /panel/api/school/vouchers", p.withAuth(p.schoolVouchers))
	p.mux.HandleFunc("POST /panel/api/checkin_all", p.withAuth(p.checkinAll))
	p.mux.HandleFunc("POST /panel/api/travel_all", p.withAuth(p.travelAll))
	p.mux.HandleFunc("POST /panel/api/activity_all", p.withAuth(p.activityAll))
	p.mux.HandleFunc("POST /panel/api/keepalive_all", p.withAuth(p.keepaliveAll))
	p.mux.HandleFunc("POST /panel/api/balance_all", p.withAuth(p.balanceAll))
	p.mux.HandleFunc("GET /panel/api/packages", p.withAuth(p.packages))
	p.mux.HandleFunc("GET /panel/api/usage", p.withAuth(p.usage))
	p.mux.HandleFunc("POST /panel/api/usage/save", p.withAuth(p.usageSave))
	p.mux.HandleFunc("GET /panel/api/model_probes", p.withAuth(p.modelProbes))
	p.mux.HandleFunc("GET /panel/api/config", p.withAuth(p.getConfig))
	// 账号代理（每账号一条固定出口 + 出口一致性校验）
	p.mux.HandleFunc("GET /panel/api/proxies", p.withAuth(p.proxyList))
	p.mux.HandleFunc("POST /panel/api/proxies/save", p.withAuth(p.proxySave))
	p.mux.HandleFunc("POST /panel/api/proxies/delete", p.withAuth(p.proxyDelete))
	p.mux.HandleFunc("POST /panel/api/proxies/check", p.withAuth(p.proxyCheck))
	p.mux.HandleFunc("POST /panel/api/proxies/check_all", p.withAuth(p.proxyCheckAll))
	p.mux.HandleFunc("POST /panel/api/proxies/bulk", p.withAuth(p.proxyBulk))
	// 订阅链接池（realm 隔离：global/cn 各自独立的订阅 URL 列表）
	p.mux.HandleFunc("GET /panel/api/subpool", p.withAuth(p.subPoolList))
	p.mux.HandleFunc("POST /panel/api/subpool/save", p.withAuth(p.subPoolSave))
	p.mux.HandleFunc("POST /panel/api/subpool/clear", p.withAuth(p.subPoolClear))
	p.mux.HandleFunc("POST /panel/api/subpool/refresh", p.withAuth(p.subPoolRefresh))
	p.mux.HandleFunc("POST /panel/api/config", p.withAuth(p.saveConfig))
}

// ServeHTTP 统一入口：先写安全响应头再分发，保证页面、静态资源、API
// 与 401 错误响应全都带上（API 也可能在浏览器里被直接打开）。
func (p *Panel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	p.mux.ServeHTTP(w, r)
}

// withAuth 与 server 包同口径的 Bearer 鉴权（经 httpauth 常量时间比较）；
// api_key 为空时放行。密钥经 livecfg 快照读取：面板里改了 api_key，下一个请求
// 即用新值（无需重启）。
func (p *Panel) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !httpauth.VerifyBearer(r, p.apiKey()) {
			writeErr(w, http.StatusUnauthorized, "invalid_api_key")
			return
		}
		next(w, r)
	}
}

// apiKey 当前生效密钥（Live 优先，回落静态字段）。
func (p *Panel) apiKey() string {
	if p.cfg.Live != nil {
		return p.cfg.Live.Load().APIKey
	}
	return p.cfg.APIKey
}

// ---------------------------------------------------------------------------
// 只读接口
// ---------------------------------------------------------------------------

// overview 总览：池计数 + 每账号状态 + 面板元信息。
func (p *Panel) overview(w http.ResponseWriter, r *http.Request) {
	total, healthy, cooling, disabled, inFlightFull := p.cfg.Pool.CountsDetailed()
	sticky := 0
	if p.cfg.StickyCount != nil {
		sticky = p.cfg.StickyCount()
	}
	// 账号代理健康汇总：让「有账号的出口不对」在总览页就能看见，而不是要切到
	// 代理页才知道。未启用时恒 0（前端据此隐藏该行）。
	pxBound, pxBad := 0, 0
	if m := p.proxyManager(); m != nil && m.Active() {
		for _, s := range m.Statuses() {
			if !s.Enabled {
				continue
			}
			pxBound++
			if s.State != "ok" && s.State != "unchecked" {
				pxBad++
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":         p.cfg.Version,
		"uptime_sec":      int(time.Since(p.started).Seconds()),
		"auth_required":   p.apiKey() != "",
		"redis_mode":      p.cfg.RedisMode,
		"sticky_sessions": sticky,
		"total":           total,
		"healthy":         healthy,
		"cooling":         cooling,
		"disabled":        disabled,
		"in_flight_full":  inFlightFull,
		// 账号代理：bound = 已绑定且启用的账号数，bad = 出口异常的账号数。
		"proxy_bound":     pxBound,
		"proxy_bad":       pxBad,
		"accounts":        p.cfg.Pool.List(),
	})
}

// logsHandler 返回日志环形缓冲快照（时间升序，含频道标记 chat/task/sys）。
func (p *Panel) logsHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"entries": p.logs.Snapshot()})
}

// models 实时查询上游模型列表与 reasoning 实际档位（直连上游，不读路由层 1h 缓存）：
// 回答"该模型到底支持哪几档思考"。顺带刷新 client 的 effort 降级能力缓存。
// 无可用账号 503（先添加账号）；上游失败 502。
func (p *Panel) models(w http.ResponseWriter, r *http.Request) {
	acct := p.cfg.Pool.Pick()
	if acct == nil {
		writeErr(w, http.StatusServiceUnavailable, "没有可用账号：请先在面板添加账号再查询")
		return
	}
	infos, err := p.cfg.Upstream.FetchModels(acct)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "fetch models: "+err.Error())
		return
	}
	out := make([]map[string]any, 0, len(infos))
	for _, mi := range infos {
		entry := map[string]any{
			"id":                   mi.ID,
			"name":                 mi.Name,
			"default_effort":       mi.DefaultEffort,
			"supported_efforts":    mi.Efforts,
			"can_disable_thinking": mi.CanDisableThinking,
			"supports_reasoning":   mi.SupportsReasoning,
			"supports_images":      mi.SupportsImages,
			"credits":              mi.Credits,
			"description":          mi.Description,
			"tags":                 mi.Tags,
			"vendor":               mi.Vendor,
			"is_default":           mi.IsDefault,
			"supports_tool_call":   mi.SupportsToolCall,
			"only_reasoning":       mi.OnlyReasoning,
			"reasoning_effort":     mi.ReasoningEffort,
			"reasoning_summary":    mi.ReasoningSummary,
		}
		if mi.MaxAllowedSize > 0 {
			entry["max_allowed_size"] = mi.MaxAllowedSize
		}
		// 与 /v1/models 同口径：context_length / max_output_tokens 走四级查找链
		// （上游动态值 → 静态知识表 → model.json → models.dev → 1M 兜底），
		// effort 档位走 EffortListing（远端权威 ∪ CN 静态兜底表）——面板展示的
		// 数值即客户端实际拿到的数值，两侧不再漂移。
		entry["context_length"] = upstream.ContextWindowListingV4(mi.ID, mi.ContextWindow, p.cfg.Upstream.HTTP)
		if mo, ok := upstream.MaxOutputTokensListingV4(mi.ID, mi.MaxTokens, p.cfg.Upstream.HTTP); ok {
			entry["max_output_tokens"] = mo
		}
		if efforts, def := upstream.EffortListing("cn", mi.ID, mi.Efforts, mi.DefaultEffort); efforts != nil {
			entry["supported_efforts"] = efforts
			if def != "" {
				entry["default_effort"] = def
			}
		}
		out = append(out, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "models": out})
}

// modelMeta 模型元数据总表（面板「模型与映射」页数据源）。
//
// 条目来源（去重合并，保证「面板看到的」=「客户端从 /v1/models 拿到的」）：
//  1. p.cfg.ListModels —— 网关 /v1/models 同口径（CN + global，含全字段）；
//  2. 仓库中目录外的条目 —— 手工登记 / 实测发现的遗留模型（如 hunyuan-2.0-instruct）。
//
// 每条附带：realm、真实映射名（upstream_model）、验证状态、备注、分类、
// 手动思考档、实测积分/延迟/样本数、最近一次错误。
func (p *Panel) modelMeta(w http.ResponseWriter, r *http.Request) {
	if p.cfg.ModelMeta == nil {
		writeErr(w, http.StatusNotImplemented, "模型元数据未启用（ModelMeta 未注入）")
		return
	}
	seen := map[string]bool{}
	out := make([]map[string]any, 0, 64)

	// 成本账本快照（按裸模型名）：面板要据此标出「免费通道」与实测单价。
	// 只用 samples>=2 的观测——单次小 token 样本会把 per1k 抬高一两个数量级。
	// key 必须带 realm：CN 与 global 是两套定价，同名模型（如 deepseek-v4.1-flash）
	// 在两个域的价格可能完全不同，混在一起聚合会得出两边一样的假数据。
	ledger := map[string]ledgerRow{}
	for _, st := range p.cfg.Pool.List() {
		for _, mc := range st.ModelCosts {
			if mc.Model == "" || mc.Samples < 2 {
				continue
			}
			k := st.Realm + ":" + mc.Model
			r := ledger[k]
			r.Per1k = mc.CostPer1k
			r.Samples += mc.Samples
			r.Accounts++
			ledger[k] = r
		}
	}

	appendEntry := func(m map[string]any, realm, id, name string) {
		k := realm + ":" + id
		if seen[k] {
			return
		}
		seen[k] = true
		v := p.cfg.ModelMeta.Annotate(realm, id, name)
		m["realm"] = v.Realm
		m["bare_id"] = v.ID
		m["full_id"] = v.FullID
		m["display_name"] = v.Display
		m["upstream_model"] = v.UpstreamModel
		m["is_alias"] = v.IsAlias
		m["status"] = string(v.Status)
		m["verified"] = v.Verified
		m["category"] = string(v.Category)
		m["meta_note"] = v.Note
		m["manual_effort"] = v.Effort
		m["context_window"] = v.ContextWindow
		m["hidden"] = v.Hidden
		m["removed"] = v.Removed
		if v.Removed {
			m["removed_reason"] = v.RemovedReason
		}
		if v.Samples > 0 || v.Credit > 0 {
			m["measured"] = map[string]any{
				"credit":     v.Credit,
				"samples":    v.Samples,
				"latency_ms": v.LatencyMS,
				"updated_at": v.UpdatedAt,
			}
		}
		if v.ErrCode != "" {
			m["last_error"] = map[string]any{"code": v.ErrCode, "message": v.ErrMsg}
		}
		// 免费判定双源（任一为真即免费）：
		//   ① 上游目录声明 credits=x0.00 —— 最权威，是上游的官方定价；
		//   ② 成本账本实测 per1k<=0 —— 验证声明（如 hy4-preview-f 396 次全 0）。
		// 只看账本会漏掉「刚限免、还没跑过请求」的模型（如 global:deepseek-v4.1-flash
		// 目录写 x0.00 但账本还留着限免前的旧单价）。
		if raw, _ := m["credits"].(string); creditsIsZero(raw) {
			m["free_declared"] = true
		}
		// 成本账本：实测单价与免费判定。面板据此标「免费」徽标，
		// 让用户一眼看出该用哪个变体（如 hy4-preview-f 免费、hy4-preview 收费）。
		if r, ok := ledger[v.Realm+":"+v.ID]; ok {
			m["ledger"] = map[string]any{
				"per1k":    r.Per1k,
				"free":     r.Per1k <= 0,
				"samples":  r.Samples,
				"accounts": r.Accounts,
			}
			if _, has := m["measured"]; !has {
				m["measured"] = map[string]any{"credit": r.Per1k, "samples": r.Samples, "per1k": true}
			}
		}
		out = append(out, m)
	}

	if p.cfg.ListModels != nil {
		for _, e := range p.cfg.ListModels() {
			full, _ := e["id"].(string)
			realm, id := splitModelID(full)
			name, _ := e["name"].(string)
			appendEntry(e, realm, id, name)
		}
	}
	// 仓库中未进主表的条目：
	//   - Removed（被剔除的遗留模型 / 区域变体别名）：面板「已删除」筛选下可见，可恢复；
	//   - 目录外条目（手工登记 / 实测发现的遗留模型）。
	for _, v := range p.cfg.ModelMeta.All() {
		if seen[v.Key] {
			continue
		}
		appendEntry(map[string]any{
			"id":       v.FullID,
			"object":   "model",
			"owned_by": "workbuddy",
			"orphan":   !v.Removed, // 目录外：上游目录已不再下发（遗留或手工登记）
		}, v.Realm, v.ID, "")
	}
	sort.Slice(out, func(i, j int) bool {
		ri, _ := out[i]["realm"].(string)
		rj, _ := out[j]["realm"].(string)
		if ri != rj {
			return ri < rj
		}
		ii, _ := out[i]["bare_id"].(string)
		jj, _ := out[j]["bare_id"].(string)
		return ii < jj
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"models": out,
		"counts": p.cfg.ModelMeta.Count(),
		"config": map[string]any{"only_verified": p.onlyVerified()},
		// 前端下拉与图例需要的静态规则（与网关判定同源，避免两处漂移）。
		"rules": map[string]any{
			"context_tiers":   modelmeta.ContextTierOptions(),
			"region_suffixes": modelmeta.RegionSuffixes(),
			"default_removed": modelmeta.DefaultRemovedList(),
		},
	})
}

// ledgerRow 成本账本的一个模型聚合行（跨账号）。
type ledgerRow struct {
	Per1k    float64
	Samples  int
	Accounts int
}

// creditsIsZero 判定上游目录的 credits 倍率原文是否为 0（免费）。
// 上游格式不统一："x0.00" / "x0.00 credits" / "0.00" / ""（空=未知，不算免费）。
// 空值/无法解析一律返回 false —— 宁可漏标免费，也不能把收费模型标成免费。
func creditsIsZero(raw string) bool {
	s := strings.TrimSpace(raw)
	if s == "" {
		return false
	}
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(s, "credits"), "credit"))
	s = strings.TrimPrefix(strings.ToLower(s), "x")
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return err == nil && f == 0
}

// splitModelID 拆 "cn:deep-model" → ("cn","deep-model")；无前缀 → ("cn", full)。
func splitModelID(full string) (realm, id string) {
	if i := strings.Index(full, ":"); i > 0 {
		return full[:i], full[i+1:]
	}
	return "cn", full
}

// onlyVerified 当前「只展示已验证模型」开关（Live 优先，回退静态）。
func (p *Panel) onlyVerified() bool { return p.cfg.Live != nil && p.cfg.Live.Load().OnlyVerified }

// modelVerify 触发模型可调用性实测。
//
// 请求体（全部可选）：{"models":["cn:deep-model",...],"concurrency":4}
// 不传 models = 验证仓库中已知的全部模型（含目录内 + 手工登记）。
// 逐个发起真实 chat 请求，记录上游回显名（别名映射）、积分消耗与错误码。
func (p *Panel) modelVerify(w http.ResponseWriter, r *http.Request) {
	if p.cfg.ModelMeta == nil || p.cfg.ModelVerifier == nil {
		writeErr(w, http.StatusNotImplemented, "模型验证未启用（ModelMeta / ModelVerifier 未注入）")
		return
	}
	if p.cfg.ModelProbe == nil {
		writeErr(w, http.StatusNotImplemented, "模型验证未启用（ModelProbe 未注入）")
		return
	}
	var req struct {
		Models      []string `json:"models"`
		Concurrency int      `json:"concurrency"`
	}
	if b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20)); len(b) > 0 {
		_ = json.Unmarshal(b, &req)
	}
	opt := modelmeta.VerifyOption{Concurrency: req.Concurrency}
	for _, full := range req.Models {
		realm, id := splitModelID(strings.TrimSpace(full))
		if id == "" {
			continue
		}
		opt.Only = append(opt.Only, modelmeta.ModelRef{Realm: realm, ID: id, Full: realm + ":" + id})
	}
	conc := req.Concurrency
	if conc <= 0 {
		conc = 4
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(20+len(opt.Only)*6)*time.Second)
	defer cancel()
	views, err := p.cfg.ModelVerifier.Verify(ctx, p.cfg.ModelProbe, opt)
	if err != nil && len(views) == 0 {
		writeErr(w, http.StatusBadGateway, "verify: "+err.Error())
		return
	}
	ok := 0
	for _, v := range views {
		if v.Verified {
			ok++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"total":   len(views),
		"passed":  ok,
		"failed":  len(views) - ok,
		"results": views,
	})
}

// modelMetaSave 人工编辑模型元数据（备注/展示名/分类/手动档位/隐藏/上游名覆盖）。
//
// 请求体：{"realm":"cn","id":"deep-model","note":"...","display":"...",
//
//	"category":"别名","effort":"high","upstream_model":"glm-5.3","hidden":false}
//
// 只覆盖出现的非零字段；status / 实测结果由验证接口写入，人工不直接改
// （避免手改出「假已验证」）。effort 传 "" 表示清空手动档位。
func (p *Panel) modelMetaSave(w http.ResponseWriter, r *http.Request) {
	if p.cfg.ModelMeta == nil {
		writeErr(w, http.StatusNotImplemented, "模型元数据未启用")
		return
	}
	var req struct {
		Realm         string `json:"realm"`
		ID            string `json:"id"`
		Full          string `json:"full_id"`
		Note          string `json:"note"`
		Display       string `json:"display"`
		Category      string `json:"category"`
		Effort        string `json:"effort"`
		UpstreamModel string `json:"upstream_model"`
		Hidden        *bool  `json:"hidden"`
		ClearEffort   bool   `json:"clear_effort"`
		Removed       *bool  `json:"removed"`        // 删除 / 恢复
		ContextWindow *int64 `json:"context_window"` // 手动上下文窗口档位（0 = 跟随上游）
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	realm, id := req.Realm, req.ID
	if id == "" && req.Full != "" {
		realm, id = splitModelID(req.Full)
	}
	if id == "" {
		writeErr(w, http.StatusBadRequest, "缺少模型 id")
		return
	}
	patch := modelmeta.Record{
		Note:          req.Note,
		Display:       req.Display,
		Category:      modelmeta.Category(req.Category),
		Effort:        req.Effort,
		UpstreamModel: req.UpstreamModel,
	}
	// Hidden 是 bool，零值无法区分「未传」与「显式 false」，用 *bool 判定后走专门入口。
	if req.Hidden != nil {
		p.cfg.ModelMeta.SetHidden(realm, id, *req.Hidden)
	}
	if req.ClearEffort {
		p.cfg.ModelMeta.SetEffort(realm, id, "")
	}
	if req.Removed != nil {
		p.cfg.ModelMeta.SetRemoved(realm, id, *req.Removed, req.Note)
	}
	if req.ContextWindow != nil {
		p.cfg.ModelMeta.SetContextWindow(realm, id, *req.ContextWindow)
	}
	v := p.cfg.ModelMeta.Update(realm, id, patch)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "model": v})
}

// promptGet 返回当前生效的系统提示词（供面板展示/编辑）。
func (p *Panel) promptGet(w http.ResponseWriter, r *http.Request) {
	mode, text := p.effectivePrompt()
	src := "config"
	if p.cfg.Live != nil && p.cfg.Live.Load().PromptText != "" {
		src = "live"
	}
	file := p.promptFileFromConfig()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "mode": mode, "text": text, "file": file, "source": src,
	})
}

// promptSave 保存系统提示词：写提示词文件 + 更新 config.json + 热生效。
//
// 请求体：{"mode":"custom"|"passthrough", "file":"<容器内路径>", "text":"<全文>"}
//
// 安全约束（重要）：file 必须落在可写白名单目录（/app/data、/app/auths）内，
// 且 Clean 后不得含 ".."。面板虽带鉴权，但这是写文件入口，必须防路径穿越
// ——否则可覆盖 /app 下任意文件。mode 只接受 custom / passthrough。
func (p *Panel) promptSave(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string `json:"mode"`
		File string `json:"file"`
		Text string `json:"text"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	mode := strings.TrimSpace(req.Mode)
	if mode != "custom" && mode != "passthrough" {
		writeErr(w, http.StatusBadRequest, `mode 必须是 "custom" 或 "passthrough"`)
		return
	}
	file := strings.TrimSpace(req.File)
	if file != "" {
		if !promptPathAllowed(file) {
			writeErr(w, http.StatusBadRequest,
				"file 必须落在 /app/data/ 或 /app/auths/ 内（容器可写目录），且不得包含 ..")
			return
		}
		if err := os.WriteFile(file, []byte(req.Text), 0o644); err != nil {
			writeErr(w, http.StatusInternalServerError, "写入提示词文件失败: "+err.Error())
			return
		}
	}
	p.updatePromptConfig(mode, file)
	// 热生效：写 Live 快照，下一个请求立即用新提示词，无需重启。
	if p.cfg.Live != nil {
		s := p.cfg.Live.Load()
		s.PromptMode = mode
		s.PromptText = req.Text
		p.cfg.Live.Store(s)
	}
	log.Printf("[panel] 系统提示词已更新: mode=%s file=%q bytes=%d", mode, file, len(req.Text))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mode": mode, "file": file, "bytes": len(req.Text)})
}

// effectivePrompt 返回当前生效的 mode 与文本（Live 优先）。
func (p *Panel) effectivePrompt() (string, string) {
	if p.cfg.Live != nil {
		if s := p.cfg.Live.Load(); s.PromptText != "" {
			return orDefault(s.PromptMode, "custom"), s.PromptText
		}
	}
	// 静态：直接读 config.json 的 prompt 段，再读 file 指向的文件。
	//
	// 不能直接断言 LoadConfig() 的返回值为 map——main 注入的是 *config.Config
	// 结构体，断言会失败并静默回退到内置提示词，面板就会显示"和实际生效的
	// 不是同一份"，改了也白改。这里自己解析 config.json，只看 prompt 段。
	mode := "custom"
	text := ""
	if pm := p.readPromptSection(); pm != nil {
		if md, _ := pm["mode"].(string); md != "" {
			mode = md
		}
		if f, _ := pm["file"].(string); f != "" {
			if b, err := os.ReadFile(f); err == nil {
				text = string(b)
			}
		}
	}
	if text == "" {
		text = promptBuiltin()
	}
	return mode, text
}

// readPromptSection 读 config.json 的 prompt 段（读失败返回 nil）。
func (p *Panel) readPromptSection() map[string]any {
	if p.cfg.ConfigPath == "" {
		return nil
	}
	raw, err := os.ReadFile(p.cfg.ConfigPath)
	if err != nil {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	pm, _ := obj["prompt"].(map[string]any)
	return pm
}

// promptFileFromConfig 读取 config.json 里配置的提示词文件路径。
func (p *Panel) promptFileFromConfig() string {
	pm := p.readPromptSection()
	if pm == nil {
		return ""
	}
	f, _ := pm["file"].(string)
	return f
}

// promptBuiltin 内置默认提示词（与 internal/prompt 包内嵌的同一份）。
func promptBuiltin() string {
	t, err := prompt.Load("custom", "")
	if err != nil || t == "" {
		return ""
	}
	return t
}

// promptPathAllowed 提示词文件路径白名单校验。
// 允许 /app/data/* 与 /app/auths/*（compose 已挂载、容器可写）；
// 拒绝相对路径、含 ".." 的穿越，以及任何其它前缀。
func promptPathAllowed(p string) bool {
	if !filepath.IsAbs(p) {
		return false
	}
	clean := filepath.Clean(p)
	if strings.Contains(clean, "..") {
		return false
	}
	for _, prefix := range []string{"/app/data/", "/app/auths/"} {
		if strings.HasPrefix(clean, prefix) {
			return true
		}
	}
	return false
}

// updatePromptConfig 把 mode/file 写回 config.json（失败只记日志，不影响热生效）。
func (p *Panel) updatePromptConfig(mode, file string) {
	if p.cfg.ConfigPath == "" {
		return
	}
	raw, err := os.ReadFile(p.cfg.ConfigPath)
	if err != nil {
		log.Printf("[panel] 提示词写回配置失败（读取）: %v", err)
		return
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		log.Printf("[panel] 提示词写回配置失败（解析）: %v", err)
		return
	}
	pm, _ := obj["prompt"].(map[string]any)
	if pm == nil {
		pm = map[string]any{}
	}
	pm["mode"] = mode
	pm["file"] = file
	obj["prompt"] = pm
	out, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(p.cfg.ConfigPath, out, 0o600); err != nil {
		log.Printf("[panel] 提示词写回配置失败（写入）: %v", err)
	}
}

// orDefault 空串回退到 def。
func orDefault(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}

// sessionList 列出当前粘性会话绑定（供面板查看"哪个对话粘在哪个号上"）。
//
// 会话 key 可能是客户端传入的会话标识（可含用户相关信息），面板展示前统一
// 截断到前 12 位 + 长度提示——够定位，又不把完整 key 暴露在页面上。
func (p *Panel) sessionList(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Session == nil {
		writeErr(w, http.StatusNotImplemented, "会话粘性未启用")
		return
	}
	binds := p.cfg.Session.List()
	rows := make([]map[string]any, 0, len(binds))
	for _, b := range binds {
		rows = append(rows, map[string]any{
			"key":         shortKey(b.Key),
			"uid":         b.UID,
			"uid_short":   uidShort(b.UID),
			"last_active": b.LastActive.Format(time.RFC3339),
			"idle":        b.Idle,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": len(rows), "sessions": rows})
}

// sessionClear 手动重置粘性会话。
//
// body 支持两种形态：
//   - {"all": true}            清空全部绑定
//   - {"uid": "<uid>"}         只解开绑定到该账号的会话（更精准）
//   - {"key": "<会话key>"}      解开单个会话
// 三者都为空时按 all 处理（面板主按钮语义：全部重置）。
//
// 为什么要手动入口：粘性会把对话长期钉在一个账号上。账号进入冷却/被 6004
// 限额后，用户会看到"这个对话一直失败、别的对话正常"，而 TTL 默认 30 分钟
// ——干等太久。手动重置可以立刻让会话重新分配到健康账号。
func (p *Panel) sessionClear(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Session == nil {
		writeErr(w, http.StatusNotImplemented, "会话粘性未启用")
		return
	}
	var req struct {
		All bool   `json:"all"`
		UID string `json:"uid"`
		Key string `json:"key"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req)

	n := 0
	switch {
	case strings.TrimSpace(req.Key) != "":
		if p.cfg.Session.Unbind(strings.TrimSpace(req.Key)) {
			n = 1
		}
	case strings.TrimSpace(req.UID) != "":
		n = p.cfg.Session.ClearUID(strings.TrimSpace(req.UID))
	default:
		n = p.cfg.Session.Clear()
	}
	log.Printf("[panel] 手动重置粘性会话: 清除 %d 条（uid=%q key=%q all=%v）", n, req.UID, req.Key, req.All)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "cleared": n, "remaining": p.cfg.Session.Count()})
}

// sessionGC 立即清理过期粘性绑定（不等下一个 GC 周期）。
func (p *Panel) sessionGC(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Session == nil {
		writeErr(w, http.StatusNotImplemented, "会话粘性未启用")
		return
	}
	n := p.cfg.Session.GCNow()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "expired": n, "remaining": p.cfg.Session.Count()})
}

// shortKey 会话 key 脱敏：短 key 原样显示，长 key 截断。
//
// keep 取 24 而不是 12：早期取 12 时，形如 "sticky-test-A" / "sticky-test-B"
// 的会话 ID 被截成同一个 "sticky-test-…"，面板上根本分不清哪个是哪个——
// 脱敏的前提是还能辨认。24 位已能区分绝大多数会话 ID，超长部分才省略。
func shortKey(k string) string {
	const keep = 24
	r := []rune(k)
	if len(r) <= keep {
		return k
	}
	return string(r[:keep]) + "…"
}

// uidShort uid 前 8 位（与日志口径一致）。
func uidShort(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// modelPurgeGhosts 一键清理幽灵模型。
//
// 判定「幽灵」= 实测失败且错误明确指向「上游没有这个模型」：
//   - err_code == model_not_found（上游 11102 经网关分类后的码）
//   - 或 err_msg 命中 "service info not found" / "11102"（兼容分类修复前的旧记录）
//
// 刻意**不**清理其它失败：限流（rate_limit_exceeded）、无可用账号
// （no_healthy_account）是临时状态，清了会把本来能用的模型误删。
func (p *Panel) modelPurgeGhosts(w http.ResponseWriter, r *http.Request) {
	if p.cfg.ModelMeta == nil {
		writeErr(w, http.StatusNotImplemented, "模型元数据未启用")
		return
	}
	removed := make([]string, 0)
	for _, v := range p.cfg.ModelMeta.All() {
		if v.Removed || v.Status != modelmeta.StatusFailed {
			continue
		}
		if !isGhostModel(v.ErrCode, v.ErrMsg) {
			continue
		}
		p.cfg.ModelMeta.SetRemoved(v.Realm, v.ID, true, "上游无此模型（11102），已清理")
		removed = append(removed, v.FullID)
	}
	sort.Strings(removed)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "removed": removed, "count": len(removed)})
}

// isGhostModel 判定失败原因是否属于「上游没有这个模型」。
func isGhostModel(code, msg string) bool {
	if code == "model_not_found" {
		return true
	}
	m := strings.ToLower(msg)
	return strings.Contains(m, "service info not found") || strings.Contains(m, "11102")
}

// modelOnlyVerified 热切换「/v1/models 只暴露已验证可调用的模型」。
// 请求体：{"only_verified":true}
// 立即生效（写 livecfg），不落 config.json——重启后回到 config.json 的静态值。
func (p *Panel) modelOnlyVerified(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Live == nil {
		writeErr(w, http.StatusNotImplemented, "运行期配置未启用")
		return
	}
	var req struct {
		OnlyVerified *bool `json:"only_verified"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if req.OnlyVerified == nil {
		writeErr(w, http.StatusBadRequest, "缺少 only_verified")
		return
	}
	s := p.cfg.Live.Load()
	s.OnlyVerified = *req.OnlyVerified
	p.cfg.Live.Store(s)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "only_verified": s.OnlyVerified})
}

// modelProbes 返回模型输出上限的探测结果（scripts/probe_max_tokens.py --panel-out
// 写入的契约文件），供前端在「模型与档位」的实测列做风险标注。
//
// 设计边界：纯只读透传——文件缺失/未配置返回空集（面板退化为无标注，与历史行为
// 一致），网关自身不解析字段语义、不据此做任何路由或出站决策；上游改了限制后
// 重跑一次工具、下次查询即刷新，无需重启网关。
func (p *Panel) modelProbes(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"probes": map[string]json.RawMessage{}, "exists": false}
	if p.cfg.ProbeFile == "" {
		writeJSON(w, http.StatusOK, out)
		return
	}
	raw, err := os.ReadFile(p.cfg.ProbeFile)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusOK, out)
			return
		}
		writeErr(w, http.StatusInternalServerError, "read probes: "+err.Error())
		return
	}
	var f struct {
		Version int                        `json:"version"`
		Probes  map[string]json.RawMessage `json:"probes"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		writeErr(w, http.StatusBadGateway, "parse probes: "+err.Error())
		return
	}
	if f.Probes == nil {
		f.Probes = map[string]json.RawMessage{}
	}
	out["probes"] = f.Probes
	out["exists"] = true
	if fi, err := os.Stat(p.cfg.ProbeFile); err == nil {
		out["updated_at"] = fi.ModTime().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// 账号运维
// ---------------------------------------------------------------------------

// accountRevive 手动复活：清禁用 + 冷却 + 熔断（运维口径无条件恢复）。
func (p *Panel) accountRevive(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if _, ok := p.cfg.Pool.Status(uid); !ok {
		writeErr(w, http.StatusNotFound, "account not found")
		return
	}
	p.cfg.Pool.Revive(uid)
	log.Printf("panel: revive uid=%s（人工清除禁用/冷却/熔断）", uid)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// accountDisable 人工禁用（不再参与选号，需面板 revive 或重登恢复）。
func (p *Panel) accountDisable(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if _, ok := p.cfg.Pool.Status(uid); !ok {
		writeErr(w, http.StatusNotFound, "account not found")
		return
	}
	p.cfg.Pool.Disable(uid, "manual disable (panel)")
	log.Printf("panel: disable uid=%s（人工禁用）", uid)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// accountCheckin 单号签到：DailyCheckin + 余额查询解冻（已签到等业务错误不阻塞余额刷新），
// 与 scheduler.RunCheckinNow 的单号语义一致。
func (p *Panel) accountCheckin(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := p.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeErr(w, http.StatusNotFound, "account not found")
		return
	}
	checkinMsg := ""
	if err := p.cfg.Upstream.DailyCheckin(a); err != nil {
		checkinMsg = err.Error() // "今天已签到"等业务错误照常查余额
	}
	resp := map[string]any{"ok": true}
	if checkinMsg != "" {
		resp["checkin_message"] = checkinMsg
	}
	remain, total, err := p.cfg.Upstream.UserResource(a)
	if err != nil {
		resp["balance_error"] = err.Error()
		writeJSON(w, http.StatusOK, resp)
		return
	}
	p.cfg.Pool.ReenableIfCredits(uid, remain, total)
	resp["credits"] = remain
	resp["credits_total"] = total
	log.Printf("panel: checkin uid=%s msg=%q credits=%d/%d", uid, checkinMsg, remain, total)
	writeJSON(w, http.StatusOK, resp)
}

// accountBalance 单号余额刷新：UserResource → SetCredits（不触碰冷却状态）。
func (p *Panel) accountBalance(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := p.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeErr(w, http.StatusNotFound, "account not found")
		return
	}
	remain, total, err := p.cfg.Upstream.UserResource(a)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "user resource: "+err.Error())
		return
	}
	p.cfg.Pool.SetCredits(uid, remain, total)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "credits": remain, "credits_total": total})
}

// accountRemove 移除账号：先出池（立即落盘 state），再删 auth 文件。
func (p *Panel) accountRemove(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := p.cfg.Pool.Remove(uid)
	if a == nil {
		writeErr(w, http.StatusNotFound, "account not found")
		return
	}
	fileMsg := ""
	if a.FilePath != "" {
		if err := os.Remove(a.FilePath); err != nil && !os.IsNotExist(err) {
			fileMsg = err.Error()
		}
	}
	if fileMsg != "" {
		log.Printf("panel: remove uid=%s（auth 文件删除失败: %s）", uid, fileMsg)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "file_error": fileMsg})
		return
	}
	log.Printf("panel: remove uid=%s（已出池并删除凭证文件）", uid)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---------------------------------------------------------------------------
// 批量任务
// ---------------------------------------------------------------------------

// checkinAll 手动触发全量签到（异步执行，进度看日志区/账号状态变化）。
func (p *Panel) checkinAll(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Scheduler == nil {
		writeErr(w, http.StatusNotImplemented, "scheduler not available")
		return
	}
	go p.cfg.Scheduler.RunCheckinNow()
	log.Printf("panel: 手动全量签到已触发（含猫猫旅行）")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "started": true})
}

// travelAll 手动触发全量猫猫旅行巡检（异步执行）。
func (p *Panel) travelAll(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Scheduler == nil {
		writeErr(w, http.StatusNotImplemented, "scheduler not available")
		return
	}
	go p.cfg.Scheduler.RunTravelNow()
	log.Printf("panel: 手动全量旅行巡检已触发")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "started": true})
}

// activityAll 手动触发全量活跃上报（异步执行；点亮连登 + 解锁领养前置）。
func (p *Panel) activityAll(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Scheduler == nil {
		writeErr(w, http.StatusNotImplemented, "scheduler not available")
		return
	}
	go p.cfg.Scheduler.RunActivityNow()
	log.Printf("panel: 手动全量活跃上报已触发")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "started": true})
}

// keepaliveAll 手动触发全量 token 保活（异步执行）。
func (p *Panel) keepaliveAll(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Scheduler == nil {
		writeErr(w, http.StatusNotImplemented, "scheduler not available")
		return
	}
	go p.cfg.Scheduler.RunKeepaliveNow()
	log.Printf("panel: 手动全量保活已触发")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "started": true})
}

// balanceAll 手动全量刷新余额：并发查上游、写回池内 credits（含解冻语义），
// 完成后返回——面板紧接着拉 overview 即是最新值。账号量小（个位数），
// 同步等待（上限受短 RPC 超时约束）比"触发后盲刷"体验更确定。
func (p *Panel) balanceAll(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Scheduler == nil {
		writeErr(w, http.StatusNotImplemented, "scheduler not available")
		return
	}
	p.cfg.Scheduler.RunBalanceRefreshNow()
	log.Printf("panel: 手动全量余额刷新完成")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "accounts": p.cfg.Pool.List()})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// usage 返回逐请求用量聚合。hours 查询参数控制小时粒度时序窗口（默认 72，
// 上限 1440=60 天）；更早的数据自动折叠为日点，因此长期趋势不会丢。
func (p *Panel) usage(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Usage == nil {
		writeErr(w, http.StatusNotImplemented, "usage recorder not available")
		return
	}
	hours := 72
	if v := r.URL.Query().Get("hours"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hours = n
		}
	}
	if hours > 1440 {
		hours = 1440
	}
	// 昵称仅用于展示，取自池快照（不含任何凭证）。顺带收集成本账本，
	// 用于回填「网关开始记录 usage.credit 之前」的历史桶积分。
	nicks := map[string]string{}
	perKey := map[string]float64{}     // "uid|model" → 实测 per1k（样本足够）
	perModel := map[string][]float64{} // model → 各账号 per1k（跨账号兜底）
	// 样本不足的单价不采信：NoteModelCost 的 EMA 由 credit/tokens*1000 得来，
	// 单次小 token 请求（如缓存命中只回 200 token）会把 per1k 抬高一两个数量级，
	// 再乘上百万级 token 就是完全失真的天文数字。宁可显示"—"也不给错数。
	const minSamples = 2
	for _, s := range p.cfg.Pool.List() {
		if s.Nickname != "" {
			nicks[s.UID] = s.Nickname
		}
		for _, mc := range s.ModelCosts {
			if mc.Model == "" || mc.Samples < minSamples {
				continue // 低样本单价不采信（见上）
			}
			perModel[mc.Model] = append(perModel[mc.Model], mc.CostPer1k)
			// per1k<=0 也登记：实测免费（如 hy4-preview-f，396 次样本全 0）
			// 是一个**确定结论**——它明确回答「这段历史就是 0 积分」，
			// 与「无单价、算不出来」是两回事，前者不该显示为 —。
			perKey[s.UID+"|"+mc.Model] = mc.CostPer1k
		}
	}
	// 回填器：优先用同账号同模型的实测单价；该账号没这个模型的记录时，
	// 退回「同一模型在所有账号上的中位数」——模型定价是上游属性，
	// 跨账号可比，比留空更接近事实（且明确标注为估算）。
	//
	// 注意口径差异：用量桶里的 Model 是**下游名**（带 cn:/global: 前缀，来自
	// 客户端请求的 model 字段），而成本账本（NoteModelCost）记的是**裸名**
	// （realm 前缀在路由时已被剥掉）。不剥前缀会永远查不到 → 回填恒为 0。
	est := func(uid, model string) (float64, bool) {
		bare := model
		if i := strings.Index(bare, ":"); i > 0 {
			bare = bare[i+1:]
		}
		// 1) 同账号同模型（最可信）。
		if v, ok := perKey[uid+"|"+bare]; ok {
			return v, true
		}
		// 2) 同模型跨账号中位数（模型定价是上游属性，跨账号可比）。
		if vs := perModel[bare]; len(vs) > 0 {
			return median(vs), true
		}
		// 3) 同族基名：hy4-preview-f → hy4-preview，hy3-x → hy3。
		//    变体后缀（-f / -x / -pro / -turbo / -mini / -flash / -preview）通常
		//    只改路由不改定价量级，用基名单价估比留空更有参考价值。
		for _, base := range familyBases(bare) {
			if vs := perModel[base]; len(vs) > 0 {
				return median(vs) * 0.5, true // 变体折半：宁可低估，不高估
			}
		}
		return 0, false
	}
	writeJSON(w, http.StatusOK, p.cfg.Usage.SnapshotWithCost(hours, nicks, est))
}

// familyBases 由变体 ID 推出同族基名候选（按"最接近原模型"排序）。
// 例：hy4-preview-f → [hy4-preview, hy4]；hy3-x → [hy3]；glm-5.3-flash → [glm-5.3]。
//
// 只做后缀剥离（不做模糊匹配）：模型族的命名规律是「基名 + 变体后缀」，
// 剥到任一层命中账本即停。剥不出来就返回空——不猜。
func familyBases(id string) []string {
	suffixes := []string{"-f", "-x", "-pro", "-turbo", "-mini", "-flash", "-preview", "-instruct", "-chat"}
	var out []string
	cur := id
	for i := 0; i < 2; i++ { // 最多剥两层（hy4-preview-f → hy4-preview → hy4）
		cut := ""
		for _, s := range suffixes {
			if strings.HasSuffix(cur, s) && len(cur) > len(s) {
				cut = strings.TrimSuffix(cur, s)
				break
			}
		}
		if cut == "" {
			break
		}
		out = append(out, cut)
		cur = cut
	}
	return out
}

// median 返回中位数（不改动入参顺序）。空切片返回 0。
// 用中位数而非均值：个别账号的异常观测（首包/缓存命中导致的极低单价）
// 会把均值拖偏，中位数更抗离群点。
func median(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	cp := append([]float64(nil), vs...)
	sort.Float64s(cp)
	n := len(cp)
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

// usageSave 立即把内存中的用量桶落盘（正常由后台 30s 防抖刷新负责）。
func (p *Panel) usageSave(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Usage == nil {
		writeErr(w, http.StatusNotImplemented, "usage recorder not available")
		return
	}
	p.cfg.Usage.Save()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// packages 返回全部账号的积分包构成，供「积分构成」视图对比。
//
// 逐个账号向上游查（并发有上限，避免瞬时打满上游限流），失败只在对应账号上
// 标 error，不影响其它账号——一个号 token 失效不该让整页空白。
func (p *Panel) packages(w http.ResponseWriter, r *http.Request) {
	accts := p.cfg.Pool.List()
	type row struct {
		UID      string                   `json:"uid"`
		Nickname string                   `json:"nickname"`
		Realm    string                   `json:"realm"`
		Remain   int64                    `json:"remain"`
		Size     int64                    `json:"size"`
		Packages []upstream.CreditPackage `json:"packages"`
		Error    string                   `json:"error,omitempty"`
	}
	out := make([]row, len(accts))

	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for i, s := range accts {
		wg.Add(1)
		go func(i int, s pool.Status) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			it := row{UID: s.UID, Nickname: s.Nickname, Realm: s.Realm}
			a := p.cfg.Pool.AuthByUID(s.UID)
			if a == nil {
				it.Error = "account not loaded"
				out[i] = it
				return
			}
			packs, remain, size, err := p.cfg.Upstream.CreditPackages(a)
			if err != nil {
				it.Error = err.Error()
				out[i] = it
				return
			}
			it.Packages = packs
			it.Remain = remain
			it.Size = size
			out[i] = it
		}(i, s)
	}
	wg.Wait()

	// 余额降序：多的在前，便于和少的对比。
	sort.SliceStable(out, func(i, j int) bool { return out[i].Remain > out[j].Remain })
	writeJSON(w, http.StatusOK, map[string]any{"accounts": out})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"ok": false, "error": msg})
}
