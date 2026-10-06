package panel

import (
	"math"

	"github.com/linguo2625469/workbuddy2api-panel/internal/usage"
)

// cacheSummaryJSON 账号池顶部"上下文缓存率"汇总块的数据源。
// 口径与按账号缓存率列一致：全量历史桶聚合；无数据时 rate 缺席（前端显示"-"）。
func cacheSummaryJSON(cs usage.CacheStat) map[string]any {
	m := map[string]any{
		"hit_tokens":    cs.HitTokens,
		"prompt_tokens": cs.PromptTokens,
		"samples":       cs.Samples,
		"window":        "全量历史（与账号缓存率列同口径）",
	}
	if rate, ok := cs.Rate(); ok {
		m["rate"] = rate
	}
	return m
}

// 免费模型稳定值（0-100）：回答"这个免费模型有多稳"。
//
// 三维度加权（权重在注释与面板 tooltip 中明示，不许黑盒）：
//
//	成功率分 ×50：近 7 天 usage 成功率 (1 - e/q)×100；q<10 判"样本不足"，
//	  整个稳定值置空（分母太小打分是玄学）。
//	额度分 ×30：今日跨账号 6004 耗尽率 (1 - exhausted/accounts)×100。
//	  注：理想口径是"7 天耗尽频率 = 1 - 耗尽天数/观测天数"，但当前无
//	  6004 历史台账（pool 只保留当日 RateLimitedModels，usage 桶不记
//	  6004），暂用今日值代理；口径在 note 中注明。
//	价格分 ×20：
//	  今日目录价 x0.00 → 100（目录仍标免费）；
//	  今日目录涨价/缺席、但近 7 天实扣为 0 → 50（目录与实扣背离，降级风险）；
//	  近 7 天实扣 >0 → 0（已开始收费）。
//	  注：理想口径是"目录价近 7 天保持 x0.00"，但 modelmeta 只存当日快照
//	  （无历史），暂用"今日目录 + 7 天实扣"代理。
//
// 综合分 = 50×成功率 + 30×额度 + 20×价格（四舍五入取整）；
// 额度分缺失（无账号在用）时权重按比例归一到其余两项。
type stability struct {
	Score   *int     `json:"score"`             // 综合分 0-100；样本不足时 nil
	Success *float64 `json:"success,omitempty"` // 成功率分 0-100
	Quota   *float64 `json:"quota,omitempty"`   // 额度分 0-100（今日值，见注）
	Price   float64  `json:"price"`             // 价格分 0-100
	Samples int64    `json:"samples"`           // 近 7 天请求样本数
	Note    string   `json:"note,omitempty"`    // 口径说明 / 降级原因
}

// stabilityInput 稳定值计算的输入（调用方从 usage 快照 / qstat / 目录价组装）。
type stabilityInput struct {
	Requests  int64   // 近 7 天请求数
	Errors    int64   // 近 7 天失败数
	Credits   float64 // 近 7 天实扣积分合计
	Accounts  int     // 今日使用该模型的账号数
	Exhausted int     // 今日已耗尽账号数
	Catalog   string  // 今日目录价（如 "x0.00"；空 = 目录缺席/未下发）
}

func calcStability(in stabilityInput) stability {
	s := stability{Samples: in.Requests}
	// 成功率分：样本 <10 不打分。
	if in.Requests >= 10 {
		v := (1 - float64(in.Errors)/float64(in.Requests)) * 100
		if v < 0 {
			v = 0
		}
		s.Success = &v
	} else {
		s.Note = "样本不足（近 7 天请求 <10 次，成功率无统计意义）"
		return s
	}
	// 额度分：今日跨账号耗尽率（7 天频率无历史台账，见类型注释）。
	if in.Accounts > 0 {
		v := (1 - float64(in.Exhausted)/float64(in.Accounts)) * 100
		if v < 0 {
			v = 0
		}
		s.Quota = &v
	} else {
		s.Note = "今日无账号使用，额度分缺失（权重已归一）"
	}
	// 价格分：今日目录价 × 近 7 天实扣。
	switch {
	case in.Credits > 0:
		s.Price = 0
		s.Note = appendNote(s.Note, "近 7 天已产生实扣，价格分 0")
	case creditsIsZero(in.Catalog):
		s.Price = 100
	default:
		// 目录涨价或缺席、但实扣仍为 0：降级风险，50 分。
		s.Price = 50
		if in.Catalog == "" {
			s.Note = appendNote(s.Note, "今日目录缺席（快照无该模型），靠实扣 0 兜着")
		} else {
			s.Note = appendNote(s.Note, "目录价已变为 "+in.Catalog+" 但实扣仍为 0")
		}
	}
	// 综合分。
	total := 50*(*s.Success) + 20*s.Price
	weight := 70.0
	if s.Quota != nil {
		total += 30 * (*s.Quota)
		weight = 100.0
	}
	score := int(math.Round(total / weight))
	s.Score = &score
	return s
}

func appendNote(note, add string) string {
	if note == "" {
		return add
	}
	return note + "；" + add
}

// （isZeroCredits 已有 creditsIsZero 实现，此处复用，不重复定义）
