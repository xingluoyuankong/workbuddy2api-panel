package modelmeta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Record 单个模型（realm + 下游暴露 ID）的验证结果与人工标注。
// 与静态知识库 Fact 的关系：Record 是「持久化 + 可写」层（实测结果与人工覆盖），
// Fact 是「只读」层（代码内人工整理）。两者在 Annotate 里合成输出。
type Record struct {
	Realm string `json:"realm"`
	ID    string `json:"id"`

	// UpstreamModel 上游真实回显名（实测）。非空且 != ID → 该模型是别名。
	UpstreamModel string `json:"upstream_model,omitempty"`

	Status Status `json:"status"`

	// 人工可写字段（面板编辑，写入后不再被自动验证覆盖）。
	Note     string   `json:"note,omitempty"`     // 备注覆盖（空 = 用 Fact.Note）
	Display  string   `json:"display,omitempty"`  // 展示名覆盖
	Category Category `json:"category,omitempty"` // 分类覆盖
	Effort   string   `json:"effort,omitempty"`   // 手动指定默认思考档（回调上游用）
	Hidden   bool     `json:"hidden,omitempty"`   // 隐藏：不出现在 /v1/models，记录保留
	Removed  bool     `json:"removed,omitempty"`  // 删除：该模型不存在（遗留/区域变体/非对话）

	// ContextWindow 手动指定的上下文窗口档位（0 = 跟随上游）。
	// 同时作用于 /v1/models 的 context_length 与出站请求的上下文裁剪。
	ContextWindow int64 `json:"context_window,omitempty"`

	// 验证结果（自动写入）。
	ErrCode   string  `json:"err_code,omitempty"`
	ErrMsg    string  `json:"err_msg,omitempty"`
	Credit    float64 `json:"credit,omitempty"`     // 实测单次积分消耗
	LatencyMS int64   `json:"latency_ms,omitempty"` // 实测耗时
	Samples   int     `json:"samples,omitempty"`    // 实测成功次数
	UpdatedAt string  `json:"updated_at,omitempty"`

	// CatalogCredits/CatalogDay 当日目录价快照：0 点每日刷新（或当天首次目录
	// 拉取）时从上游目录捕获的 credits 倍率原文。**当天内不变**——上游目录
	// 双源竞速、两次拉取结果可能不一致，面板免费判定必须以当天 0 点冻结价
	// 为准，否则免费清单会在一天内反复翻转。
	CatalogCredits string `json:"catalog_credits,omitempty"`
	CatalogDay     string `json:"catalog_day,omitempty"`
}

// View 合并 Fact 与 Record 后的对外视图（面板与 /v1/models 消费）。
type View struct {
	Realm         string   `json:"realm"`
	ID            string   `json:"id"`
	Key           string   `json:"key"`
	FullID        string   `json:"full_id"` // "cn:deep-model" 形式（下游调用名）
	Display       string   `json:"display"`
	UpstreamModel string   `json:"upstream_model,omitempty"` // 真实映射名
	CallName      string   `json:"call_name"`                // 实际调用名（= FullID）
	IsAlias       bool     `json:"is_alias"`
	Status        Status   `json:"status"`
	Verified      bool     `json:"verified"`
	Category      Category `json:"category,omitempty"`
	Note          string   `json:"note,omitempty"`
	Effort        string   `json:"effort,omitempty"`
	Hidden        bool     `json:"hidden,omitempty"`
	Removed       bool     `json:"removed,omitempty"`
	RemovedReason string   `json:"removed_reason,omitempty"`
	// ContextWindow 手动指定的上下文窗口档位（0 = 跟随上游目录值）。
	ContextWindow  int64   `json:"context_window,omitempty"`
	ErrCode        string  `json:"err_code,omitempty"`
	ErrMsg         string  `json:"err_msg,omitempty"`
	Credit         float64 `json:"credit,omitempty"`
	LatencyMS      int64   `json:"latency_ms,omitempty"`
	Samples        int     `json:"samples,omitempty"`
	UpdatedAt      string  `json:"updated_at,omitempty"`
	CatalogCredits string  `json:"catalog_credits,omitempty"`
	CatalogDay     string  `json:"catalog_day,omitempty"`
}

// CaptureCatalog 把目录价快照（credits 倍率原文）按 (realm, 模型) 批量落库。
// entries 的键为全调用名（"cn:deepseek-v4.1-flash"）。0 点每日刷新与面板
// 当天首次目录拉取都会调用；同键重复捕获直接覆盖（价格以最近一次拉取为准）。
// 一次加锁、至多一次落盘。返回写入条数。
func (s *Store) CaptureCatalog(day string, entries map[string]string) int {
	s.mu.Lock()
	for full, credits := range entries {
		i := strings.Index(full, ":")
		if i <= 0 || i == len(full)-1 || credits == "" {
			continue
		}
		r := s.ensureLocked(full[:i], full[i+1:])
		r.CatalogCredits = credits
		r.CatalogDay = day
	}
	n := len(entries)
	s.mu.Unlock()
	if n > 0 {
		s.save()
	}
	return n
}

// Store 模型元数据仓库（JSON 持久化 + 读写锁）。
type Store struct {
	mu      sync.RWMutex
	path    string
	records map[string]*Record // key = "realm:id"
}

// New 创建仓库。path 为空 → 纯内存（不落盘，测试/无写权限场景仍可用）。
func New(path string) *Store {
	s := &Store{path: path, records: map[string]*Record{}}
	s.load()
	return s
}

// Path 返回持久化文件路径（空 = 内存模式）。
func (s *Store) Path() string { return s.path }

func (s *Store) load() {
	if s.path == "" {
		return
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		return // 首次运行/文件不存在 → 空仓库，不阻塞启动
	}
	var raw []Record
	if err := json.Unmarshal(b, &raw); err != nil {
		return // 坏文件不致命：宁可空仓库也不 panic（与 usage.json 同策略）
	}
	for i := range raw {
		r := raw[i]
		s.records[Key(r.Realm, r.ID)] = &r
	}
}

// save 落盘（调用方持写锁）。坏路径只记一次失败，不影响内存态。
func (s *Store) save() {
	if s.path == "" {
		return
	}
	list := make([]Record, 0, len(s.records))
	for _, r := range s.records {
		list = append(list, *r)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Realm != list[j].Realm {
			return list[i].Realm < list[j].Realm
		}
		return list[i].ID < list[j].ID
	})
	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	_ = os.WriteFile(s.path, b, 0o644)
}

// Get 取记录（不存在返回 nil）。
func (s *Store) Get(realm, id string) *Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.records[Key(realm, id)]
}

// Ensure 取记录；不存在则按静态知识库初始化一条（不落盘，由写操作触发）。
func (s *Store) ensureLocked(realm, id string) *Record {
	k := Key(realm, id)
	if r, ok := s.records[k]; ok {
		return r
	}
	r := &Record{Realm: realmKey(realm), ID: id, Status: StatusUnverified}
	if f, ok := LookupFact(realm, id); ok {
		r.Category = f.Category
		r.UpstreamModel = f.Upstream
	}
	s.records[k] = r
	return r
}

// Annotate 合成视图：静态知识库（只读）+ 持久化记录（可写）+ 上游 name 兜底展示名。
// upstreamName 为上游目录里的 name 字段，用于无人工覆盖时的展示名。
// 人工字段（Note/Display/Category/Effort）优先级高于静态知识库。
func (s *Store) Annotate(realm, id, upstreamName string) View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.annotateLocked(realm, id, upstreamName)
}

func (s *Store) annotateLocked(realm, id, upstreamName string) View {
	rk := realmKey(realm)
	rec := s.records[Key(rk, id)]
	fact, hasFact := LookupFact(rk, id)

	v := View{
		Realm:   rk,
		ID:      id,
		Key:     Key(rk, id),
		FullID:  rk + ":" + id,
		Status:  StatusUnverified,
		Display: upstreamName,
	}
	if hasFact {
		v.Note = fact.Note
		v.Category = fact.Category
		if v.Display == "" {
			v.Display = fact.Display
		}
	}
	if rec != nil {
		if rec.Status != "" {
			v.Status = rec.Status
		}
		if rec.UpstreamModel != "" {
			v.UpstreamModel = rec.UpstreamModel
		}
		if rec.Note != "" {
			v.Note = rec.Note
		}
		if rec.Display != "" {
			v.Display = rec.Display
		}
		if rec.Category != "" {
			v.Category = rec.Category
		}
		v.Effort = rec.Effort
		v.Hidden = rec.Hidden
		v.ContextWindow = rec.ContextWindow
		v.ErrCode = rec.ErrCode
		v.ErrMsg = rec.ErrMsg
		v.Credit = rec.Credit
		v.LatencyMS = rec.LatencyMS
		v.Samples = rec.Samples
		v.UpdatedAt = rec.UpdatedAt
		v.CatalogCredits = rec.CatalogCredits
		v.CatalogDay = rec.CatalogDay
	} else if hasFact && v.UpstreamModel == "" {
		v.UpstreamModel = fact.Upstream
	}
	if v.Display == "" {
		v.Display = id
	}
	// 剔除判定：人工删除 → 静态遗留名单 → 区域变体规则。
	// 语义：Removed 的模型对该网关而言不存在，不进 /v1/models、不进面板主表。
	v.Removed = rec != nil && rec.Removed
	if !v.Removed {
		if why := defaultRemoved[id]; why != "" {
			v.Removed, v.RemovedReason = true, why
		} else if IsRegionVariant(id) {
			v.Removed, v.RemovedReason = true, "区域变体别名，上游归一化为基模型"
		}
	}
	if v.Removed && v.RemovedReason == "" {
		v.RemovedReason = "人工删除"
	}
	v.CallName = v.FullID
	v.Verified = v.Status == StatusVerified
	v.IsAlias = v.UpstreamModel != "" && v.UpstreamModel != id
	return v
}

// AnnotateAll 批量合成（一次加锁）。
func (s *Store) AnnotateAll(realm string, ids []string, names map[string]string) []View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]View, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.annotateLocked(realm, id, names[id]))
	}
	return out
}

// All 返回全部视图（按 realm、id 排序）。
func (s *Store) All() []View {
	s.mu.RLock()
	keys := make([]string, 0, len(s.records))
	recs := make(map[string]struct{}, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
		recs[k] = struct{}{}
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	out := make([]View, 0, len(keys))
	for _, k := range keys {
		realm, id := splitKey(k)
		out = append(out, s.Annotate(realm, id, ""))
	}
	return out
}

func splitKey(k string) (realm, id string) {
	for i := 0; i < len(k); i++ {
		if k[i] == ':' {
			return k[:i], k[i+1:]
		}
	}
	return "cn", k
}

// Update 人工编辑（备注/展示名/分类/档位/隐藏/上游名覆盖）。
// 只覆盖非零值字段；status 与实测结果由 Verify 写入，人工不改。
func (s *Store) Update(realm, id string, patch Record) View {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(realm, id)
	if patch.Note != "" {
		r.Note = patch.Note
	}
	if patch.Display != "" {
		r.Display = patch.Display
	}
	if patch.Category != "" {
		r.Category = patch.Category
	}
	if patch.Effort != "" {
		r.Effort = patch.Effort
	}
	if patch.UpstreamModel != "" {
		r.UpstreamModel = patch.UpstreamModel
	}
	// Hidden 是 bool，零值无法区分「未传」与「显式 false」，故不在 Update 里处理：
	// 由 SetHidden 单独写入（HTTP 层用 *bool 判定是否显式传参）。
	r.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	s.save()
	return s.annotateLocked(realm, id, "")
}

// EffortFor 取模型手动指定的思考档（空 = 未指定，走上游默认）。
func (s *Store) EffortFor(realm, id string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.records[Key(realm, id)]; ok {
		return r.Effort
	}
	return ""
}

// SetHidden 直接设置隐藏标记（bool 无法表达「未传」，故单独入口）。
func (s *Store) SetHidden(realm, id string, hidden bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(realm, id)
	r.Hidden = hidden
	r.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	s.save()
}

// SetEffort 设置/清空（effort=""）手动指定的思考档位。
// 该档位在 chat 出站前注入 body（客户端显式档位优先，不被覆盖），
// 随后仍走 supportedEfforts 降级管线，不会把上游不支持的档位打出去。
func (s *Store) SetEffort(realm, id, effort string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(realm, id)
	r.Effort = effort
	r.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	s.save()
}

// IsHidden 判定模型是否被人工隐藏。
func (s *Store) IsHidden(realm, id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.records[Key(realm, id)]; ok {
		return r.Hidden
	}
	return false
}

// ContextWindowFor 取模型手动指定的上下文窗口档位（0 = 未指定，跟随上游）。
func (s *Store) ContextWindowFor(realm, id string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.records[Key(realm, id)]; ok {
		return r.ContextWindow
	}
	return 0
}

// SetContextWindow 设置/清空（0）手动上下文窗口档位。
func (s *Store) SetContextWindow(realm, id string, tokens int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(realm, id)
	r.ContextWindow = tokens
	r.UpdatedAt = nowRFC3339()
	s.save()
}

// nowRFC3339 UTC 时间戳（统一口径，避免各处 time.Now().Format 漂移）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// Count 各状态计数（面板概览用）。
func (s *Store) Count() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]int{"total": len(s.records)}
	for _, r := range s.records {
		out[string(r.Status)]++
		if r.Hidden {
			out["hidden"]++
		}
	}
	return out
}
