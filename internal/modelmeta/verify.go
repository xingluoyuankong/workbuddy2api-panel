package modelmeta

import (
	"context"
	"sort"
	"sync"
	"time"
)

// ModelRef 待验证模型引用（下游暴露形态）。
type ModelRef struct {
	Realm string `json:"realm"`
	ID    string `json:"id"`
	// Full 下游完整调用名（realm:id），用于实际发起请求。
	Full string `json:"full"`
}

// ProbeResult 单次实测结果。
type ProbeResult struct {
	OK            bool
	UpstreamModel string  // 上游响应里回显的 model（真实映射名）
	Credit        float64 // 本次消耗积分（usage.credit）
	Latency       time.Duration
	ErrCode       string // 网关错误码（如 no_healthy_account / upstream_error）
	ErrMsg       string // 错误原文（截断）
}

// ProbeFunc 发起一次真实调用的函数；由上层注入（server 注入经网关回路的调用，
// 以便验证完整下游链路：鉴权 → 选号 → 上游 → 计费）。
type ProbeFunc func(ctx context.Context, full string) ProbeResult

// Verifier 模型可调用性验证器。
type Verifier struct {
	store *Store
}

// NewVerifier 创建验证器。
func NewVerifier(s *Store) *Verifier { return &Verifier{store: s} }

// VerifyOption 验证参数。
type VerifyOption struct {
	Concurrency int           // 并发度（<=0 → 4）
	Timeout     time.Duration // 单模型超时（<=0 → 90s）
	Only        []ModelRef    // 只验证这些（空 = 全部已知模型）
}

// Verify 并发实测并落盘结果，返回更新后的视图（按 realm/id 排序）。
func (v *Verifier) Verify(ctx context.Context, probe ProbeFunc, opt VerifyOption) ([]View, error) {
	if probe == nil {
		return nil, errNoProbe
	}
	conc := opt.Concurrency
	if conc <= 0 {
		conc = 4
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 90 * time.Second
	}

	targets := opt.Only
	if len(targets) == 0 {
		targets = v.knownRefs()
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := range targets {
		wg.Add(1)
		go func(ref ModelRef) {
			defer wg.Done()
			// 先判 ctx：select 的 case 在「信号量可拿」与「ctx 已取消」同时就绪时
			// 是随机选择，不先判会让已取消的验证照样发请求并写 verified 状态。
			if ctx.Err() != nil {
				return
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			cctx, cancel := context.WithTimeout(ctx, opt.Timeout)
			defer cancel()
			res := probe(cctx, ref.Full)

			mu.Lock()
			v.apply(ref, res)
			mu.Unlock()
		}(targets[i])
	}
	wg.Wait()

	// 落盘一次（apply 内部不落盘，避免并发写文件）。
	v.store.flush()
	return v.viewsFor(targets), ctx.Err()
}

// knownRefs 仓库中已知模型的引用列表。
func (v *Verifier) knownRefs() []ModelRef {
	out := make([]ModelRef, 0)
	for _, view := range v.store.All() {
		out = append(out, ModelRef{Realm: view.Realm, ID: view.ID, Full: view.FullID})
	}
	return out
}

// apply 把实测结果写入内存记录（调用方持 mu）。
func (v *Verifier) apply(ref ModelRef, res ProbeResult) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	r := v.store.ensureLocked(ref.Realm, ref.ID)
	r.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	r.LatencyMS = res.Latency.Milliseconds()
	if res.OK {
		r.Status = StatusVerified
		r.Samples++
		r.Credit = res.Credit
		r.ErrCode = ""
		r.ErrMsg = ""
		if res.UpstreamModel != "" {
			r.UpstreamModel = res.UpstreamModel
		}
		return
	}
	r.Status = StatusFailed
	r.ErrCode = res.ErrCode
	if len(res.ErrMsg) > 300 {
		r.ErrMsg = res.ErrMsg[:300]
	} else {
		r.ErrMsg = res.ErrMsg
	}
}

// viewsFor 取指定引用的视图（排序稳定）。
func (v *Verifier) viewsFor(refs []ModelRef) []View {
	out := make([]View, 0, len(refs))
	for _, ref := range refs {
		out = append(out, v.store.Annotate(ref.Realm, ref.ID, ""))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Realm != out[j].Realm {
			return out[i].Realm < out[j].Realm
		}
		return out[i].ID < out[j].ID
	})
	return out
}

var errNoProbe = errString("modelmeta: probe function is nil")

type errString string

func (e errString) Error() string { return string(e) }

// flush 落盘（包内调用）。
func (s *Store) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.save()
}

// UpsertKnown 把目录中的模型登记进仓库（不写状态，只保证存在记录，
// 让面板能列出「目录里有但还没测过」的模型）。
func (s *Store) UpsertKnown(realm string, ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.ensureLocked(realm, id)
	}
}
