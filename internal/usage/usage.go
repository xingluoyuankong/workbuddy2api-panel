// Package usage 记录并聚合逐请求 token 用量，供面板「用量」视图展示。
//
// 与 internal/pool 的 TokenUsage 的区别：
//   - pool 的 TokenUsage 是**每账号一个累计计数器**，只保留总量与「最近一次」，
//     没有时间维度，也无法按模型/时间下钻；
//   - 本包按 (时间片, realm, uid, model) 分桶累计，因此可以出「今天各模型各用了多少」
//     「这一小时 prompt 涨得多快」这类问题，且能长期保留。
//
// 保留策略（分片粒度自动降级，总量因此有界）：
//   - 近 hourlyKeep 小时内：小时桶（细粒度，看尖峰）
//   - 更早：折叠为日桶，**永久保留**（看长期趋势）
//
// 落盘：data/usage.json，原子替换 + 防抖刷新（默认 30s），重启不丢。
// 桶数上界 ≈ 账号数 × 模型数 × (hourlyKeep + 已过天数)，实测单桶约 90 字节。
package usage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// hourlyKeep 小时桶的保留时长；超出后折叠为日桶。
const hourlyKeep = 90 * 24 * time.Hour

// flushInterval 防抖落盘间隔。
const flushInterval = 30 * time.Second

// maxBuckets 桶数硬上限。超过时立即触发一次折叠，避免异常流量把内存/文件撑爆。
const maxBuckets = 400_000

// hourLayout / dayLayout 分片键的时间格式（本地时区，与用户直觉一致）。
const (
	hourLayout = "2006-01-02T15"
	dayLayout  = "2006-01-02"
)

// bucket 一个 (时间片, realm, uid, model) 的累计量。
// JSON 字段名刻意取短，因为桶数量会随时间增长。
type bucket struct {
	Scope string  `json:"s"` // "h:2006-01-02T15" 或 "d:2006-01-02"
	Realm string  `json:"r"`
	UID   string  `json:"u"`
	Model string  `json:"m"`
	Req   int64   `json:"q"`  // 请求数（含失败）
	Err   int64   `json:"e"`  // 失败数
	PT    int64   `json:"p"`  // prompt tokens
	CT    int64   `json:"c"`  // completion tokens
	TT    int64   `json:"t"`  // total tokens（上游给什么用什么的合计）
	LatMs int64   `json:"l"`  // 延迟累计（ms）
	LatN  int64   `json:"ln"` // 延迟样本数
	TPS   float64 `json:"v"`  // 吐字速率累计
	TPSN  int64   `json:"vn"` // 速率样本数
	CR    float64 `json:"cr"` // 积分消耗累计（上游 usage.credit）
	CRN   int64   `json:"cn"` // 积分样本数（拿到 credit 的请求数）
	CHT   int64   `json:"ch"` // 前缀缓存命中 tokens 累计（上游 usage.prompt_cache_hit_tokens）
	CHP   int64   `json:"cp"` // 带缓存信息样本的 prompt tokens 累计（缓存率分母）
}

// file 落盘结构。
type file struct {
	Version int      `json:"version"`
	Saved   string   `json:"saved"`
	Buckets []bucket `json:"buckets"`
}

// Recorder 并发安全的用量记录器。
type Recorder struct {
	mu      sync.Mutex
	path    string
	buckets map[string]*bucket // key: scope|realm|uid|model
	dirty   bool
	started time.Time

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// New 创建记录器。path 为空时禁用落盘（纯内存，测试用）。
func New(path string) *Recorder {
	r := &Recorder{
		path:    path,
		buckets: make(map[string]*bucket),
		started: time.Now(),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if path != "" {
		if err := r.load(); err != nil {
			log.Printf("[usage] 读取 %s 失败（从零开始）: %v", path, err)
		}
	}
	return r
}

// Start 启动后台防抖落盘与折叠。Stop 前一直运行。
func (r *Recorder) Start() {
	go func() {
		defer close(r.done)
		t := time.NewTicker(flushInterval)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				r.flush(true)
				return
			case <-t.C:
				r.mu.Lock()
				n := len(r.buckets)
				r.mu.Unlock()
				if n > maxBuckets {
					r.Rollup(time.Now())
				}
				r.flush(false)
			}
		}
	}()
}

// Stop 停止后台循环并做最后一次落盘。
func (r *Recorder) Stop() {
	r.stopOnce.Do(func() { close(r.stop) })
	<-r.done
}

// Delta 一次请求尝试的用量增量（与 pool.TokenUsageDelta 同形，避免包间依赖）。
type Delta struct {
	PromptTokens     int64
	HasPromptTokens  bool
	CompletionTokens int64
	HasCompletion    bool
	TotalTokens      int64
	HasTotal         bool
	LatencyMs        int64
	HasLatency       bool
	TokensPerSecond  float64
	HasTPS           bool
	// Credit 本次实际消耗的积分（上游 usage.credit）。失败尝试通常没有。
	Credit    float64
	HasCredit bool
	// CacheHitTokens 本次命中的前缀缓存 tokens（上游 usage.prompt_cache_hit_tokens）。
	// 与 PromptTokens 同帧下发；失败尝试通常没有。
	CacheHitTokens    int64
	HasCacheHitTokens bool
}

// Add 记录一次请求尝试。
//
// ok=false 表示该次尝试失败（传输错误 / 上游 >=400 / 解析失败）。失败尝试通常
// 没有 usage，但**仍要计入请求数与失败数**——重试放大正是靠这一列才看得出来。
func (r *Recorder) Add(now time.Time, realm, uid, model string, d Delta, ok bool) {
	if r == nil {
		return
	}
	if realm == "" {
		realm = "cn"
	}
	if model == "" {
		model = "(unknown)"
	}
	scope := "h:" + now.Format(hourLayout)
	key := scope + "|" + realm + "|" + uid + "|" + model

	r.mu.Lock()
	defer r.mu.Unlock()

	b := r.buckets[key]
	if b == nil {
		b = &bucket{Scope: scope, Realm: realm, UID: uid, Model: model}
		r.buckets[key] = b
	}
	b.Req++
	if !ok {
		b.Err++
	}
	if d.HasPromptTokens {
		b.PT += d.PromptTokens
	}
	if d.HasCompletion {
		b.CT += d.CompletionTokens
	}
	if d.HasTotal {
		b.TT += d.TotalTokens
	} else if d.HasPromptTokens || d.HasCompletion {
		// 上游没给 total：用 pt+ct 兜底，保证总量口径连续。
		b.TT += d.PromptTokens + d.CompletionTokens
	}
	if d.HasLatency {
		b.LatMs += d.LatencyMs
		b.LatN++
	}
	if d.HasTPS {
		b.TPS += d.TokensPerSecond
		b.TPSN++
	}
	// 积分：只有上游真的下发了 usage.credit 才计样本（没下发不填 0，
	// 否则「0 积分」与「未知积分」在数据里无法区分）。
	if d.HasCredit {
		b.CR += d.Credit
		b.CRN++
	}
	// 前缀缓存命中：分母只计「带缓存信息样本」的 prompt——网关上线该字段前的
	// 历史桶 CHT/CHP 恒 0，不参与分母，避免历史 prompt 稀释新口径的缓存率。
	if d.HasCacheHitTokens {
		b.CHT += d.CacheHitTokens
		if d.HasPromptTokens {
			b.CHP += d.PromptTokens
		}
	}
	r.dirty = true
}

// Rollup 把超出 hourlyKeep 的小时桶折叠为日桶（按本地日历日）。
// 幂等：同一小时反复折叠不会重复计数（先累加再删源桶）。
func (r *Recorder) Rollup(now time.Time) {
	if r == nil {
		return
	}
	cutoff := now.Add(-hourlyKeep)

	r.mu.Lock()
	defer r.mu.Unlock()

	type move struct{ from, to string }
	var moves []move
	for k, b := range r.buckets {
		if !strings.HasPrefix(b.Scope, "h:") {
			continue
		}
		ts, err := time.ParseInLocation(hourLayout, strings.TrimPrefix(b.Scope, "h:"), time.Local)
		if err != nil || !ts.Before(cutoff) {
			continue
		}
		day := "d:" + ts.Format(dayLayout)
		moves = append(moves, move{from: k, to: day + "|" + b.Realm + "|" + b.UID + "|" + b.Model})
	}
	for _, m := range moves {
		src := r.buckets[m.from]
		if src == nil {
			continue
		}
		dst := r.buckets[m.to]
		if dst == nil {
			cp := *src
			cp.Scope = strings.SplitN(m.to, "|", 2)[0]
			dst = &cp
			r.buckets[m.to] = dst
		} else {
			dst.Req += src.Req
			dst.Err += src.Err
			dst.PT += src.PT
			dst.CT += src.CT
			dst.TT += src.TT
			dst.LatMs += src.LatMs
			dst.LatN += src.LatN
			dst.TPS += src.TPS
			dst.TPSN += src.TPSN
			dst.CR += src.CR
			dst.CRN += src.CRN
			dst.CHT += src.CHT
			dst.CHP += src.CHP
		}
		delete(r.buckets, m.from)
	}
	if len(moves) > 0 {
		r.dirty = true
		log.Printf("[usage] 折叠 %d 个小时桶为日桶（保留 %v 细粒度）", len(moves), hourlyKeep)
	}
}

// ---------------------------------------------------------------- 持久化 ----

func (r *Recorder) load() error {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var f file
	if err := json.Unmarshal(raw, &f); err != nil {
		return err
	}
	for i := range f.Buckets {
		b := f.Buckets[i]
		r.buckets[b.Scope+"|"+b.Realm+"|"+b.UID+"|"+b.Model] = &b
	}
	log.Printf("[usage] 已恢复 %d 个用量桶（%s）", len(r.buckets), r.path)
	return nil
}

func (r *Recorder) flush(force bool) {
	if r == nil || r.path == "" {
		return
	}
	r.mu.Lock()
	if !r.dirty && !force {
		r.mu.Unlock()
		return
	}
	snap := file{Version: 2, Saved: time.Now().Format(time.RFC3339), Buckets: make([]bucket, 0, len(r.buckets))}
	for _, b := range r.buckets {
		snap.Buckets = append(snap.Buckets, *b)
	}
	r.dirty = false
	r.mu.Unlock()

	raw, err := json.Marshal(snap)
	if err != nil {
		log.Printf("[usage] 序列化失败: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		log.Printf("[usage] 建目录失败: %v", err)
		return
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("[usage] 写临时文件失败: %v", err)
		return
	}
	if err := os.Rename(tmp, r.path); err != nil {
		log.Printf("[usage] 原子替换失败: %v", err)
	}
}

// Save 立即落盘（面板「刷新」或关闭前调用）。
func (r *Recorder) Save() { r.flush(true) }

// ---------------------------------------------------------------- 聚合 ----

// Agg 一组累计量。
type Agg struct {
	Requests      int64   `json:"requests"`
	Errors        int64   `json:"errors"`
	PromptTokens  int64   `json:"prompt_tokens"`
	CompletionTok int64   `json:"completion_tokens"`
	TotalTokens   int64   `json:"total_tokens"`
	Credits       float64 `json:"credits"`        // 积分消耗合计（上游 usage.credit 累计）
	CreditSamples int64   `json:"credit_samples"` // 拿到 credit 的请求数（0 → 上游未下发积分）
	AvgLatencyMs  float64 `json:"avg_latency_ms"`
	AvgTPS        float64 `json:"avg_tokens_per_second"`
	// CreditsPer1k 实测千 token 单价（credits/total*1000），0 = 无数据。
	// 与「积分倍率」不同：这是**真实扣费**反推的单价，含模型倍率与实际输出长度。
	CreditsPer1k float64 `json:"credits_per_1k,omitempty"`
	// CreditsEst 回填的估算积分：历史桶产生时网关还没记录 usage.credit，
	// 用成本账本（model_costs 的实测 per1k）× 该桶 token 数反推，让历史数据
	// 也能对上账。与 Credits 分开存放，前端按「估算」标注，绝不冒充实测值。
	CreditsEst     float64 `json:"credits_est"`
	CreditsEstToks int64   `json:"credits_est_tokens"` // 参与估算的 token 数（0 = 无单价可估）
	// 前缀缓存命中（上游 usage.prompt_cache_hit_tokens 聚合）。
	CacheHitTokens int64 `json:"cache_hit_tokens"`
	// CachePromptToks 缓存率分母：带缓存信息样本的 prompt tokens（0 = 无数据）。
	CachePromptToks int64 `json:"cache_prompt_tokens,omitempty"`
	// CacheHitRate 缓存命中率 = CacheHitTokens/CachePromptToks；分母为 0 时为 0
	// （此时按 CachePromptToks==0 判「无数据」，不要把 0 读成「命中率为 0」）。
	CacheHitRate float64 `json:"cache_hit_rate"`
}

// CreditsTotal 对外口径的积分合计：有实测用实测，否则用回填估算。
func (a Agg) CreditsTotal() float64 {
	if a.CreditSamples > 0 {
		return a.Credits
	}
	return a.CreditsEst
}

// CreditsKind 积分来源标注："measured" / "estimated" / "none"。
func (a Agg) CreditsKind() string {
	switch {
	case a.CreditSamples > 0:
		return "measured"
	case a.CreditsEstToks > 0:
		return "estimated"
	default:
		return "none"
	}
}

// aggAcc 是聚合过程中的累加器：Agg 只放已算好的结果，均值需要样本数才能
// 正确加权（不能对每桶的均值再取平均），所以样本数留在这里。
type aggAcc struct {
	Agg
	latSum     int64
	latSamples int64
	tpsSum     float64
	tpsSamples int64
	cacheHit   int64 // prompt_cache_hit_tokens 累计
	cachePrompt int64 // 带缓存信息样本的 prompt 累计（缓存率分母）
	// est 历史积分回填器（nil = 不回填）。放在累加器里，避免给每处 add 调用
	// 都加一个参数、漏传就静默丢估算。
	est CostEstimator
}

func (g *aggAcc) add(b *bucket) {
	g.Requests += b.Req
	g.Errors += b.Err
	g.PromptTokens += b.PT
	g.CompletionTok += b.CT
	g.TotalTokens += b.TT
	g.latSum += b.LatMs
	g.latSamples += b.LatN
	g.tpsSum += b.TPS
	g.tpsSamples += b.TPSN
	g.Credits += b.CR
	g.CreditSamples += b.CRN
	g.cacheHit += b.CHT
	g.cachePrompt += b.CHP
	// 回填：只有「该桶完全没有实测 credit」且「有 token 数」时才估算，
	// 避免与实测值重复计入（重复计会让总量虚高一倍）。
	if b.CRN == 0 && b.TT > 0 && g.est != nil {
		// per1k==0 是有效结论（实测免费），也要计 tokens——否则前端无法区分
		// 「估算为 0（免费）」与「没单价、算不出来」，两者都显示成 —。
		if per1k, ok := g.est(b.UID, b.Model); ok {
			g.CreditsEst += float64(b.TT) / 1000 * per1k
			g.CreditsEstToks += b.TT
		}
	}
}

func (g *aggAcc) finish() Agg {
	a := g.Agg
	if g.latSamples > 0 {
		a.AvgLatencyMs = float64(g.latSum) / float64(g.latSamples)
	}
	if g.tpsSamples > 0 {
		a.AvgTPS = g.tpsSum / float64(g.tpsSamples)
	}
	// 千 token 单价：按真实扣费与真实 token 数反推（不是目录里的倍率）。
	// 分子用「实测 + 回填」的合计——两者覆盖的是互不相交的 token 集合
	// （回填只对无实测 credit 的桶生效），所以合计 / 总 token 才是全局单价。
	// 只取实测会让单价虚高（分母含未被实测覆盖的历史 token）。
	if a.TotalTokens > 0 {
		if total := a.Credits + a.CreditsEst; total > 0 {
			a.CreditsPer1k = total / float64(a.TotalTokens) * 1000
		}
	}
	// 缓存命中率：分母只含带缓存信息样本的 prompt，上线前历史不稀释。
	a.CacheHitTokens = g.cacheHit
	a.CachePromptToks = g.cachePrompt
	if g.cachePrompt > 0 {
		a.CacheHitRate = float64(g.cacheHit) / float64(g.cachePrompt)
	}
	return a
}

// KeyedAgg 按某个维度聚合的一行。
type KeyedAgg struct {
	Key   string `json:"key"`
	Realm string `json:"realm,omitempty"`
	Extra string `json:"extra,omitempty"` // 账号行放昵称
	Agg
}

// Point 时序上的一个点。
type Point struct {
	T     string `json:"t"`
	Scope string `json:"scope"` // "hour" | "day"
	Agg
}

// Snapshot 面板一次拉取的全部用量视图数据。
type Snapshot struct {
	Totals    Agg        `json:"totals"`
	ByRealm   []KeyedAgg `json:"by_realm"`
	ByAccount []KeyedAgg `json:"by_account"`
	ByModel   []KeyedAgg `json:"by_model"`
	// ByDay 窗口内按日聚合（柱状图用）：每天一根柱，升序。
	ByDay []Point `json:"by_day"`
	Series []Point `json:"series"`
	Buckets   int        `json:"buckets"`
	FileBytes int64      `json:"file_bytes"`
	Since     string     `json:"since,omitempty"`
	Generated string     `json:"generated"`
}

// CostEstimator 历史积分回填器：给定 (账号, 模型) 返回实测千 token 单价。
// 由调用方（面板）从 pool 的成本账本（model_costs）构造；ok=false 表示该组合
// 无单价可估（对应桶的积分留空，不编造）。
type CostEstimator func(uid, model string) (per1k float64, ok bool)

// Snapshot 聚合当前全部桶（不回填历史积分）。兼容旧调用方与测试。
func (r *Recorder) Snapshot(hours int, nicks map[string]string) Snapshot {
	return r.SnapshotWithCost(hours, nicks, nil)
}

// SnapshotWithCost 聚合当前全部桶，并用 est 回填历史积分。
//
// 背景：网关从某个版本才开始把上游 usage.credit 记进用量桶，之前的桶只有
// token 数没有积分——直接展示会让「964 次请求只有 0.1 积分」这种明显失真的
// 数字。成本账本（pool.model_costs）一直在按 (账号,模型) 记录实测单价，
// 用它乘以桶里的 token 数即可把历史积分补回来。
//
// hours 控制时序返回多少个小时点（其余按日折叠）；nicks 仅用于展示。
func (r *Recorder) SnapshotWithCost(hours int, nicks map[string]string, est CostEstimator) Snapshot {
	if r == nil {
		return Snapshot{Generated: time.Now().Format(time.RFC3339)}
	}
	// hours<=0 = 全量（不按窗口过滤），供「全部历史」视图；
	// hours>上限 → 夹到上限。默认 72。
	allTime := hours <= 0
	if hours > 24*60 {
		hours = 24 * 60
	}
	if hours == 0 && !allTime {
		hours = 72
	}
	if hours <= 0 {
		hours = 72 // allTime 时仅用于 hourFrom 兜底，实际 inWindow 恒真
	}

	r.mu.Lock()
	bs := make([]bucket, 0, len(r.buckets))
	for _, b := range r.buckets {
		bs = append(bs, *b)
	}
	r.mu.Unlock()

	newAcc := func() *aggAcc { return &aggAcc{est: est} }
	var total = newAcc()
	realmAgg := map[string]*aggAcc{}
	acctAgg := map[string]*aggAcc{}
	acctRealm := map[string]string{}
	modelAgg := map[string]*aggAcc{}
	hourSeries := map[string]*aggAcc{}
	daySeries := map[string]*aggAcc{}

	nowHour := time.Now().Truncate(time.Hour)
	hourFrom := nowHour.Add(-time.Duration(hours-1) * time.Hour)
	// dayFrom 窗口起点（含当日）：24h 窗口 → 昨天；7 天窗口 → 6 天前。
	// 日桶无时分，用日历日边界判定；小时桶用精确时刻判定。
	dayFrom := time.Date(nowHour.Year(), nowHour.Month(), nowHour.Day(),
		0, 0, 0, 0, time.Local).AddDate(0, 0, -(hours/24 - 1))
	if hours < 24 {
		// 不足一天的窗口：日桶也只看当天（小时桶负责精度）
		dayFrom = time.Date(nowHour.Year(), nowHour.Month(), nowHour.Day(),
			0, 0, 0, 0, time.Local)
	}
	if allTime {
		// 全量：窗口起点退到零值，inWindow 恒真（「全部历史」视图）。
		hourFrom = time.Time{}
		dayFrom = time.Time{}
	}

	// inWindow 判定一个桶是否落在本次查询的 hours 窗口内。
	// **这是关键**：原来 total/ByRealm/ByAccount/ByModel 无条件累加全部桶
	//（含 90 天内所有历史），导致 24h / 3 天 / 7 天的总量完全相同。
	inWindow := func(b *bucket) bool {
		if allTime {
			return true
		}
		if strings.HasPrefix(b.Scope, "h:") {
			ts, err := time.ParseInLocation(hourLayout, strings.TrimPrefix(b.Scope, "h:"), time.Local)
			return err == nil && !ts.Before(hourFrom)
		}
		ts, err := time.ParseInLocation(dayLayout, strings.TrimPrefix(b.Scope, "d:"), time.Local)
		return err == nil && !ts.Before(dayFrom)
	}

	for i := range bs {
		b := &bs[i]
		// 窗口过滤唯一入口：inWindow 含 allTime（hours<=0 恒真）语义。
		// 此前有局部 var inWindow bool 遮蔽同名函数，day 桶被 hourLayout
		// 解析失败误过滤（totals 少算 100 天前数据）——已删除。
		if !inWindow(b) {
			continue
		}

		total.add(b)

		if realmAgg[b.Realm] == nil {
			realmAgg[b.Realm] = newAcc()
		}
		realmAgg[b.Realm].add(b)

		if acctAgg[b.UID] == nil {
			acctAgg[b.UID] = newAcc()
		}
		acctAgg[b.UID].add(b)
		// 一个账号只属于一个 realm，这里记下来供前端展示「域」列；
		// keyed() 的 Realm 字段默认是空的（它按 key 分组，不知道 realm）。
		if acctRealm[b.UID] == "" {
			acctRealm[b.UID] = b.Realm
		}

		if modelAgg[b.Model] == nil {
			modelAgg[b.Model] = newAcc()
		}
		modelAgg[b.Model].add(b)

		scope := strings.TrimPrefix(b.Scope, "h:")
		if strings.HasPrefix(b.Scope, "h:") {
			ts, err := time.ParseInLocation(hourLayout, scope, time.Local)
			if err != nil {
				continue
			}
			if !ts.Before(hourFrom) {
				if hourSeries[scope] == nil {
					hourSeries[scope] = newAcc()
				}
				hourSeries[scope].add(b)
			} else {
				// 超出小时窗口的细粒度数据并入其所在日，避免时序出现空洞。
				d := ts.Format(dayLayout)
				if daySeries[d] == nil {
					daySeries[d] = &aggAcc{}
				}
				daySeries[d].add(b)
			}
		} else {
			// 日桶（Rollup 折叠产物，scope 形如 "d:2026-06-23"）：key 统一剥掉
			// "d:" 前缀，与小时分支的裸日期 key 对齐。
			dk := strings.TrimPrefix(scope, "d:")
			if daySeries[dk] == nil {
				daySeries[dk] = newAcc()
			}
			daySeries[dk].add(b)
		}
	}

	// ByDay（柱状图）：hour 桶按日归并 + day 桶，与 Series 解耦——
	// 24h 窗口下 Series 只有小时点，ByDay 也要有当天的柱子。
	byDayAgg := map[string]*aggAcc{}
	for i := range bs {
		b := &bs[i]
		if !inWindow(b) {
			continue
		}
		var dk string
		if strings.HasPrefix(b.Scope, "h:") {
			ts, err := time.ParseInLocation(hourLayout, strings.TrimPrefix(b.Scope, "h:"), time.Local)
			if err != nil {
				continue
			}
			dk = ts.Format(dayLayout)
		} else {
			dk = strings.TrimPrefix(b.Scope, "d:")
		}
		if byDayAgg[dk] == nil {
			byDayAgg[dk] = newAcc()
		}
		byDayAgg[dk].add(b)
	}

	snap := Snapshot{
		Totals:  total.finish(),
		ByRealm: keyed(realmAgg, func(k string) (string, string) { return k, "" }),
		ByAccount: keyed(acctAgg, func(k string) (string, string) {
			return k, nicks[k]
		}),
		ByModel:   keyed(modelAgg, func(k string) (string, string) { return k, "" }),
		Buckets:   len(bs),
		Generated: time.Now().Format(time.RFC3339),
	}
	for i := range snap.ByAccount {
		snap.ByAccount[i].Realm = acctRealm[snap.ByAccount[i].Key]
	}

	// 日点（升序）+ 小时点（升序）拼成一条连续时序。
	dayKeys := make([]string, 0, len(daySeries))
	for k := range daySeries {
		dayKeys = append(dayKeys, k)
	}
	sort.Strings(dayKeys)
	for _, k := range dayKeys {
		snap.Series = append(snap.Series, Point{T: k, Scope: "day", Agg: daySeries[k].finish()})
	}
	hourKeys := make([]string, 0, len(hourSeries))
	for k := range hourSeries {
		hourKeys = append(hourKeys, k)
	}
	sort.Strings(hourKeys)
	for _, k := range hourKeys {
		snap.Series = append(snap.Series, Point{T: k, Scope: "hour", Agg: hourSeries[k].finish()})
	}

	// ByDay 升序组装（柱状图数据）。
	byDayKeys := make([]string, 0, len(byDayAgg))
	for k := range byDayAgg {
		byDayKeys = append(byDayKeys, k)
	}
	sort.Strings(byDayKeys)
	for _, k := range byDayKeys {
		snap.ByDay = append(snap.ByDay, Point{T: k, Scope: "day", Agg: byDayAgg[k].finish()})
	}

	if r.path != "" {
		if fi, err := os.Stat(r.path); err == nil {
			snap.FileBytes = fi.Size()
		}
	}
	// 最早的分片即数据起点。
	if len(snap.Series) > 0 {
		snap.Since = snap.Series[0].T
	}
	return snap
}

// CacheStat 单个账号的前缀缓存命中聚合（账号池「缓存率」列的数据源）。
type CacheStat struct {
	HitTokens    int64 `json:"hit_tokens"`    // prompt_cache_hit_tokens 累计
	PromptTokens int64 `json:"prompt_tokens"` // 带缓存信息样本的 prompt 累计（分母）
	Samples      int64 `json:"samples"`       // 带缓存信息的请求样本数
}

// Rate 缓存命中率 = HitTokens/PromptTokens；无数据时 ok=false（调用方显示"-"）。
func (c CacheStat) Rate() (rate float64, ok bool) {
	if c.PromptTokens <= 0 {
		return 0, false
	}
	return float64(c.HitTokens) / float64(c.PromptTokens), true
}

// CacheByAccount 全量桶按账号聚合缓存命中（轻量：只扫桶做加法，不经过
// Snapshot 的成本回填与窗口过滤；账号池列表每轮刷新调用，开销 O(桶数)）。
func (r *Recorder) CacheByAccount() map[string]CacheStat {
	out := map[string]CacheStat{}
	if r == nil {
		return out
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.buckets {
		if b.CHP == 0 && b.CHT == 0 {
			continue
		}
		c := out[b.UID]
		c.HitTokens += b.CHT
		c.PromptTokens += b.CHP
		if b.CHP > 0 {
			c.Samples++
		}
		out[b.UID] = c
	}
	return out
}

func keyed(m map[string]*aggAcc, label func(string) (string, string)) []KeyedAgg {
	out := make([]KeyedAgg, 0, len(m))
	for k, v := range m {
		key, extra := label(k)
		out = append(out, KeyedAgg{Key: key, Extra: extra, Agg: v.finish()})
	}
	// 按总量降序；同量按 key 升序，保证输出稳定（前端 diff 不抖）。
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalTokens != out[j].TotalTokens {
			return out[i].TotalTokens > out[j].TotalTokens
		}
		if out[i].Requests != out[j].Requests {
			return out[i].Requests > out[j].Requests
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Describe 返回一行人类可读的占用摘要（启动日志用）。
func (r *Recorder) Describe() string {
	if r == nil {
		return "disabled"
	}
	r.mu.Lock()
	n := len(r.buckets)
	r.mu.Unlock()
	var sz int64
	if r.path != "" {
		if fi, err := os.Stat(r.path); err == nil {
			sz = fi.Size()
		}
	}
	return fmt.Sprintf("%d buckets, file %d bytes", n, sz)
}
