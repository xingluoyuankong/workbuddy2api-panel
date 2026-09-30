package modelmeta

import "strings"

// deny.go 模型剔除名单：把「不需要 / 不存在 / 无法调用 / 区域变体别名」的模型
// 从 /v1/models 与面板中彻底移除（区别于 Hidden——Hidden 只是不对外列，
// 记录仍保留；Removed 是删除，语义上等于该模型不存在）。
//
// 三层判定（任一命中即剔除）：
//  1. 静态默认名单 defaultRemoved：已确认下线/重复的遗留模型（用户核定）；
//  2. 区域变体规则 regionSuffixes：形如 -sg 的区域节点别名（上游会归一化成基模型，
//     下游暴露等于同一个模型出现两次，纯噪音）；
//  3. 人工标记 Record.Removed（面板可增删，写 model_meta.json）。

// defaultRemoved 静态默认剔除名单。
// 来源：用户核定（2026-09-18）——这批模型要么已下线、要么是旧版重复条目、
// 要么是非文本模型（生图），不应出现在对话网关的下游映射里。
var defaultRemoved = map[string]string{
	// 遗留/重复的 default 版本号（上游早已切到 default-model，这两个是化石 ID）
	"default-1.1": "上游遗留版本号，已下线",
	"default-1.2": "上游遗留版本号，已下线（实测 11102 not found）",

	// 旧版混元：已被 hunyuan 新系列取代
	"hunyuan-chat":             "旧版混元入口，已被新系列取代",
	"hunyuan-2.0-instruct":     "旧版混元 2.0，目录外遗留模型",
	"hunyuan-image-alpha-edit": "生图/图像编辑模型，非对话模型（text-to-image）",

	// 旧版 DeepSeek：被 v3.1 / v4 系列取代
	"deepseek-v3-0324":        "旧版 DeepSeek-V3-0324，已被新版本取代",
	"deepseek-r1-0528":        "旧版 DeepSeek-R1-0528，已被新版本取代",
	"deepseek-r1-0528-lkeap":  "LKEAP 旧部署，目录外遗留模型",
	"deepseek-r1-0528-taiji":  "旧版 R1 变体，已下线",

	// 旧版 Kimi / GLM
	"kimi-k2-instruct-taiji": "Kimi-K2 旧 instruct 变体，已下线",
	"glm-4.6":                "旧版 GLM-4.6，已被 GLM-5.x 取代",
}

// regionSuffixes 区域变体别名后缀：命中即剔除。
//
// 依据：sg = Singapore（新加坡节点）。实测 global:deepseek-v4.1-flash-sg
// 被上游归一化为 deepseek-v4.1-flash（响应回显无 -sg），即它并不是一个
// 独立模型，只是同一模型的新加坡入口。对外暴露等于同一模型出现两次，
// 且 CN 域调用直接 11102——纯噪音，一律剔除。
var regionSuffixes = []string{"-sg"}

// IsRegionVariant 判定模型 ID 是否为区域变体别名（-sg 等）。
func IsRegionVariant(id string) bool {
	low := strings.ToLower(id)
	for _, s := range regionSuffixes {
		if strings.HasSuffix(low, s) {
			return true
		}
	}
	return false
}

// DefaultRemovedReason 返回静态默认剔除原因（未命中返回空）。
func DefaultRemovedReason(id string) string { return defaultRemoved[id] }

// DefaultRemovedList 返回静态默认剔除名单（面板展示用）。
func DefaultRemovedList() map[string]string {
	out := make(map[string]string, len(defaultRemoved))
	for k, v := range defaultRemoved {
		out[k] = v
	}
	return out
}

// RegionSuffixes 返回当前生效的区域变体后缀规则。
func RegionSuffixes() []string {
	out := make([]string, len(regionSuffixes))
	copy(out, regionSuffixes)
	return out
}

// RemovalReason 返回模型被剔除的原因（未被剔除返回空串）。
// 判定顺序：人工标记 → 静态名单 → 区域变体规则。
func (s *Store) RemovalReason(realm, id string) string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.records[Key(realm, id)]; ok && r.Removed {
		if r.Note != "" {
			return r.Note
		}
		return "人工删除"
	}
	if why := defaultRemoved[id]; why != "" {
		return why
	}
	if IsRegionVariant(id) {
		return "区域变体别名（" + strings.TrimPrefix(id, "") + "），上游归一化为基模型"
	}
	return ""
}

// IsRemoved 判定模型是否应被剔除。
func (s *Store) IsRemoved(realm, id string) bool { return s.RemovalReason(realm, id) != "" }

// SetRemoved 人工删除 / 恢复（面板操作，落盘）。
func (s *Store) SetRemoved(realm, id string, removed bool, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(realm, id)
	r.Removed = removed
	if note != "" {
		r.Note = note
	}
	r.UpdatedAt = nowRFC3339()
	s.save()
}
