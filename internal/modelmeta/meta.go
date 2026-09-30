// Package modelmeta 模型元数据：上游真实名（别名映射）、可调用性验证结果、
// 人工备注/分类/展示名，以及「只展示已验证可调用模型」的过滤策略。
//
// 背景（2026-09 实测）：上游模型目录存在三类坑，纯靠目录无法分辨：
//  1. 别名型：deep-model 在 cn 域实际回显 glm-5.3，在 global 域回显 deep-model 自身；
//     deepseek-v4.1-flash-sg 在 global 域被归一化为 deepseek-v4.1-flash。
//  2. 幽灵型：目录里有、但调用报 11102 service info not found（cn:default-model）。
//  3. 隐藏型：目录里没有、但调用成功（cn:hunyuan-2.0-instruct、cn:deepseek-r1-0528-lkeap）。
//
// 本包把「实测结论」持久化下来（data/model_meta.json），供 /v1/models 与面板消费，
// 让下游客户端不必逐个试错。
package modelmeta

import "strings"

// Status 模型验证状态。
type Status string

const (
	// StatusUnverified 未验证：目录中存在，尚未实测过。
	StatusUnverified Status = "unverified"
	// StatusVerified 已验证可调用：实测返回 200。
	StatusVerified Status = "verified"
	// StatusFailed 已验证不可调用：实测返回错误（如 11102 not found）。
	StatusFailed Status = "failed"
)

// Category 模型分类（面板分组/排序用）。
type Category string

const (
	CatFlagship Category = "旗舰"
	CatReason   Category = "推理"
	CatFast     Category = "轻量"
	CatAlias    Category = "别名"
	CatRegion   Category = "区域变体"
	CatLegacy   Category = "遗留/隐藏"
	CatOther    Category = "其他"
)

// Fact 静态知识库条目：人工整理 + 实测佐证的语义说明。
// 未实测时也能给出可信备注（status 仍为 unverified，绝不谎称已验证）。
type Fact struct {
	Note     string   // 备注（含义、来源、坑）
	Category Category // 分类
	Display  string   // 展示名覆盖（空 = 用上游 name）
	Upstream string   // 已知/推断的上游真实名（实测值优先，见 Record.UpstreamModel）
}

// known 静态知识库：key 优先按 "realm:id" 精确匹配，未命中再按 "id" 匹配。
// 内容来源：2026-09-18 对 https://wb2api.xzxyuan.ccwu.cc 的实测 + 上游目录比对。
var known = map[string]Fact{
	// ---- sg 后缀：新加坡区域变体 ----
	"global:deepseek-v4.1-flash-sg": {
		Note:     "sg = Singapore（新加坡节点）区域变体。实测 global 域可调用，上游回显归一化为 deepseek-v4.1-flash（去掉 -sg）；CN 域返回 11102 service info not found，不可调用。积分与基模型同档。",
		Category: CatRegion,
		Display:  "DeepSeek-V4.1-Flash（新加坡节点）",
		Upstream: "deepseek-v4.1-flash",
	},
	"cn:deepseek-v4.1-flash-sg": {
		Note:     "sg = Singapore 区域变体，仅对国际版（global）账号开放。实测 CN 域调用返回 11102 model service info not found，不可调用（映射目标与 global 同为 deepseek-v4.1-flash，但 CN 侧无法落地）。",
		Category: CatRegion,
		Display:  "DeepSeek-V4.1-Flash（新加坡节点·CN 不可用）",
		Upstream: "deepseek-v4.1-flash",
	},
	"deepseek-v4.1-flash-sg": {
		Note:     "sg = Singapore（新加坡节点）区域变体别名，上游归一化为基础模型 deepseek-v4.1-flash；仅 global 域可用。",
		Category: CatRegion,
		Upstream: "deepseek-v4.1-flash",
	},

	// ---- deep-model 别名 ----
	"cn:deep-model": {
		Note:     "上游别名（非固定）。实测回显 glm-5.3，单次约 0.09 积分。随上游策略变化，勿硬编码。",
		Category: CatAlias,
		Upstream: "glm-5.3",
	},
	"global:deep-model": {
		Note:     "实测回显 deep-model 本身（非别名），深度推理模型，单次约 0.81 积分、延迟显著高于常规模型。",
		Category: CatReason,
		Upstream: "deep-model",
	},
	"deep-model": {
		Note:     "上游「深度推理」档位模型。实测 CN 域回显 glm-5.3（别名），global 域为独立模型。映射随上游策略变化，以实测回显为准。",
		Category: CatReason,
	},

	// ---- default 系列 ----
	"cn:default-model": {
		Note:     "上游占位默认模型。实测 CN 域返回 11102 service info not found——目录里有但不可调用，勿选。",
		Category: CatOther,
	},
	"global:default-model": {
		Note:     "实测 global 域可调用，回显 default-model，单次约 0.04 积分。为上游默认路由模型，实际供应商不固定。",
		Category: CatOther,
	},
	"default-1.2": {
		Note:     "上游已下线/不存在的默认模型版本号。实测返回 11102 service info not found，不可调用（历史遗留 ID，勿使用）。",
		Category: CatLegacy,
	},
	"default-model": {
		Note:     "上游默认路由模型，实际供应商随上游策略变化。CN 域实测不可调用。",
		Category: CatOther,
	},

	// ---- 目录外但实测可调用（遗留/隐藏模型）----
	"hunyuan-2.0-instruct": {
		Note:     "混元 2.0 instruct 版。当前 /v3/config 目录未下发该 ID，但实测 CN 域调用成功（约 0.03 积分）——属遗留/隐藏模型，可用但不在官方目录中，随时可能下线。",
		Category: CatLegacy,
	},
	"deepseek-r1-0528-lkeap": {
		Note:     "LKEAP（腾讯知识引擎增强版）部署的 DeepSeek-R1-0528。当前目录未下发，但实测 CN 域调用成功（约 0.03 积分）——遗留部署，可用但无官方支持保证。",
		Category: CatLegacy,
	},

	// ---- 免费/低倍率模型 ----
	"deepseek-v4.1-flash": {Note: "实测单次积分 0（限时免费档），支持深度思考。", Category: CatFast},
	"hy3":                 {Note: "实测单次积分 0（限时免费档）。", Category: CatFast},
	"hy4-preview": {
		Note:     "收费通道。实测单次约 0.08 积分（约 540 token 的短请求）；长请求按 token 计费会显著更高（账本实测千 token 单价 0.094，长上下文请求单次可达数积分）。能力与 hy4-preview-f 一致（同 token 消耗、同推理量），**仅计费不同**——想省钱请改用 cn:hy4-preview-f。",
		Category: CatReason,
	},
	"hy4-preview-f": {
		Note:     "**免费通道**（-f = free 变体）。实测多次调用 credit 恒为 0（396 次样本全 0），能力与 hy4-preview 完全一致（同 prompt 下 token 消耗 566、推理 token 15~16 均相同）。**优先使用这个**。",
		Category: CatFast,
	},
	"hy4-preview-x": {
		Note:     "hy4 系列的扩展变体。账本实测千 token 单价 0.0187（低于 hy4-preview 的 0.094）。",
		Category: CatReason,
	},
}

// LookupFact 查静态知识库：先 "realm:id"，再 "id"。未命中返回 zero Fact。
func LookupFact(realm, id string) (Fact, bool) {
	if f, ok := known[realmKey(realm)+":"+id]; ok {
		return f, true
	}
	f, ok := known[id]
	return f, ok
}

// realmKey 归一化 realm（空 → cn），与 upstream.realmKey 同口径。
func realmKey(realm string) string {
	switch strings.ToLower(strings.TrimSpace(realm)) {
	case "global":
		return "global"
	default:
		return "cn"
	}
}

// Key 记录主键 "realm:id"。
func Key(realm, id string) string { return realmKey(realm) + ":" + id }

// ContextTiers 可选的上下文窗口档位（token）。
// 0 表示「跟随上游目录值」——不覆盖、不裁剪，网关完全不介入。
// 档位覆盖主流客户端与上游实际口径（8K → 1M）。
var ContextTiers = []int64{
	0, 8_000, 16_000, 32_000, 64_000, 100_000, 128_000, 200_000, 256_000, 400_000, 512_000, 1_000_000,
}

// FormatContextTier 档位可读文本（0 → "跟随上游"）。
func FormatContextTier(n int64) string {
	if n <= 0 {
		return "跟随上游"
	}
	if n >= 1_000_000 {
		return itoa(n/1_000_000) + "M"
	}
	return itoa(n/1000) + "K"
}

// ContextTierOptions 给前端下拉用的档位列表（value/label 对）。
func ContextTierOptions() []map[string]any {
	out := make([]map[string]any, 0, len(ContextTiers))
	for _, v := range ContextTiers {
		out = append(out, map[string]any{"value": v, "label": FormatContextTier(v)})
	}
	return out
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
