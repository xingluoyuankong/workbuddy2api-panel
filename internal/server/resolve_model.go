package server

import "strings"

// resolveModel 解析模型名协议（PLAN D6）：
//
//	分布式前缀： "[realm:]model"
//
// 取第一个 ":"，前段恰为 "cn"/"global" 才剥离；否则视为裸名，realm=cn、bare=原串。
// 大小写敏感（前缀必须是精确的小写枚举）。bare 即出站/选号/账本使用的裸模型名。
//
// 导出为 ResolveModel（cmd/server/main.go 粘性闭包需要），包内简写 resolveModel。
func resolveModel(model string) (realm, bare string) {
	// 客户端自定义前缀（如 custom-local:）是客户端本地网关的标记，
	// 不是 wb2api 的 realm 协议，先剥掉再解析。
	// 否则 custom-local:global:deepseek-x 会被误判为 realm=cn、
	// bare=custom-local:global:deepseek-x，导致路由错误和参数剥除失效。
	if strings.HasPrefix(model, "custom-local:") {
		model = strings.TrimPrefix(model, "custom-local:")
	}
	idx := strings.IndexByte(model, ':')
	if idx < 0 {
		return "cn", model
	}
	prefix := model[:idx]
	if prefix != "cn" && prefix != "global" {
		return "cn", model
	}
	return prefix, model[idx+1:]
}

// ResolveModel 是 resolveModel 的导出面（跨包调用）。
func ResolveModel(model string) (realm, bare string) { return resolveModel(model) }
