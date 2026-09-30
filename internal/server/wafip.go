// wafip.go WAF IP 级拦截 fail-fast 状态机（任务书 waf-ip-failfast）。
//
// 背景（fork-scan-absorb T-1 / BulidH 实测）：WAF 403 拦的是网关出口 IP 而非
// 账号——3 个账号 1 秒内全 403。既有 ErrWafBlock 账号级软冷却（wafCooldownBase
// 起的有界退避）在 IP 级拦截时不够：轮转会把一次客户端请求放大 MaxRotate 倍，
// 同一出口 IP 继续打上游只会加重风控。
//
// 【本 fork 修正 P0-WAF-IP-SINGLE】原判定「窗内 ≥wafIPThreshold 个**不同** UID」
// 在单账号池（如本部署仅 1 个 global 号）下恒不触发——len(hits) 永远 ≤1 < 2，
// IP 级保护完全失效，实测 new-api 渠道测速连打 55 次全部 503 且轮转不止损。
// 修正为双通道判定，任一命中即激活：
//
//	A. 不同 UID 通道（原语义保留）：窗内 ≥wafIPThreshold 个不同 UID 命中；
//	B. 单号连击通道（新增）：同一 UID 在 wafIPRepeatWindow 内命中
//	   ≥wafIPRepeatThreshold 次 —— 单号在数秒内被反复 403 同样是 IP 级证据
//	   （账号级偶发不会在 3 秒内连撞 3 次）。
//
// 两通道都把「命中次数」而非「不同号数」纳入判定，单账号池不再是盲区。
//
// 激活期内新命中不续期（保守：不做主动探测，窗口自然解除）。
//
// 归属层评估：放 server（Handler 局部）而非 pool——IP 级状态唯一消费者是
// chatCompletions 轮转循环（是否继续轮转），pool 是账号级记账层，跨 UID 语义
// 不属于任何账号；server 已有进程级状态先例 degradeGate（mu+until 同风格）。
// 账号级软冷却照常记账（applyErrorPolicy 不变），IP 级状态只改变「是否继续
// 轮转」——协同不叠加。进程内状态、重启清零（窗口 60s，重建成本极低）。
package server

import (
	"log"
	"sync"
	"time"
)

// wafIPWindow IP 级判定滑动窗 + 激活时长：窗内不同账号命中 WAF 403 达阈值即
// 判 IP 级拦截，激活同样长（到期自然解除）。var 仅供测试注入短窗（生产恒 60s，
// 任务书「如 60s 滑动窗」口径）。
var wafIPWindow = 60 * time.Second

// wafIPThreshold 不同 UID 通道阈值：窗内不同 UID 数达到该值激活。取 2——「多号」
// 的最小定义：两个不同号在 60s 内接连被拦（同一出口 IP）已是 IP 级证据
// （BulidH 实测 3 号 1s 全拦，阈值 2 更早止损，少放大一次轮转）。
const wafIPThreshold = 2

// 【P0-WAF-IP-SINGLE 新增】单号连击通道参数。
// wafIPRepeatWindow 同一 UID 的连击判定窗：比 wafIPWindow 短——单号要在「数秒内」
// 被反复拦才算 IP 级；把窗收窄到 10s 可避免把正常使用中的偶发 403（隔几十秒一次）
// 误判成 IP 级拦截。
var wafIPRepeatWindow = 10 * time.Second

// wafIPRepeatThreshold 同一 UID 在 wafIPRepeatWindow 内的命中次数阈值，达即激活。
// 取 3：网关轮转 MaxRotate 默认 3，单次客户端请求最多同号尝试 3 次；单号 10s 内
// 连撞 3 次 403 是「换号换不掉、IP 被拦」的确定性证据（账号级偶发不会这么密）。
const wafIPRepeatThreshold = 3

// wafIPBaseCooldown IP 级拦截的基础冷却时长，与判定窗（wafIPWindow）分离。
//
// 取值逻辑：判定窗 60s 是「多快能识别出这是 IP 级封锁」，冷却时长是「多久之后
// 才值得再试一次」。两者是独立概念，共用一个值会导致「刚解除就再撞」。
//
// 取 60s（与判定窗同量级）而不是更长：global 域已配多出口代理池，出口 IP 每次
// 请求轮询变化，60s 后重试大概率已换到另一个出口，能立刻恢复。冷却太长反而
// 会让用户在最需要的时候干等——「不要因为 WAF 就限制我几分钟不能用」。
// 真正的自愈靠换出口，而不是靠等。
var wafIPBaseCooldown = 60 * time.Second

// wafIPMaxCooldown 连续触发时的冷却上限（指数退避封顶）。
// 连续触发说明多个出口都被盯上，逐步拉长比固定时长更能自愈，但也不宜过久。
const wafIPMaxCooldown = 5 * time.Minute

// wafIPGate WAF IP 级拦截状态机（Handler 内嵌，零值可用）。
type wafIPGate struct {
	mu    sync.Mutex
	hits  map[string]time.Time // uid → 最近一次 WAF 403 时刻（不同 UID 窗内，惰性剪枝）
	rep   map[string]int       // uid → wafIPRepeatWindow 内命中次数（单号连击通道）
	until time.Time            // IP 级拦截激活截止；零值 = 未激活
	// streak 连续触发次数：冷却到期后再次触发即 +1，成功请求后清零。
	// 用于冷却时长指数退避（5min → 10min → 20min → 30min 封顶）。
	streak int
}

// cooldownFor 按连续触发次数计算本次冷却时长（指数退避，封顶 wafIPMaxCooldown）。
func (g *wafIPGate) cooldownFor(streak int) time.Duration {
	d := wafIPBaseCooldown
	for i := 1; i < streak; i++ {
		d *= 2
		if d >= wafIPMaxCooldown {
			return wafIPMaxCooldown
		}
	}
	return d
}

// noteSuccess 记一次成功的上游请求：清零连击计数，让下次触发的冷却回到基础值。
// 不打上游就不可能知道 WAF 是否解除——成功请求是唯一的「已恢复」证据。
func (g *wafIPGate) noteSuccess() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.streak = 0
}

// noteWaf 记一次某账号的 WAF 403，返回记账后 IP 级拦截是否激活（调用方据此
// fail-fast 终止轮转，优先于 rotateBackoff 退避）。
//   - 已激活（now < until）：不续期、不记账（窗口期内不重置——保守自然解除）→ true；
//   - 未激活：同时喂两个通道——
//     A. 不同 UID：记 hits[uid]=now（同号重复覆盖不累计），剪窗外，不同 UID 数
//     达 wafIPThreshold → 激活；
//     B. 单号连击：同号上次命中在 wafIPRepeatWindow 内则 rep[uid]++，否则重置为 1；
//     达 wafIPRepeatThreshold → 激活。
//     任一通道命中即激活到 now+wafIPWindow（打一条 WARN 供观测），两通道计数一并
//     清空（解除后需全新命中重新判定，不叠旧账）。
//
// 通道 B 的窗判断必须在写入 hits[uid] **之前**取旧值（lastSeen），否则读到的
// 永远是刚写入的 now，「连击」恒成立——这是本实现的顺序要点。
func (g *wafIPGate) noteWaf(uid string) bool {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if now.Before(g.until) {
		return true // 激活期内新命中：不续期（自然解除语义，任务书第 3 条）
	}
	if g.hits == nil {
		g.hits = map[string]time.Time{}
	}
	if g.rep == nil {
		g.rep = map[string]int{}
	}

	// —— 通道 B：单号连击。先取上次命中时刻（写入新值前），再判定窗。 ——
	lastSeen, seen := g.hits[uid]
	if seen && now.Sub(lastSeen) <= wafIPRepeatWindow {
		g.rep[uid]++
	} else {
		g.rep[uid] = 1
	}

	// —— 通道 A：不同 UID 窗。写入并惰性剪枝（同号重复命中覆盖，判定口径是「不同号数」）。 ——
	g.hits[uid] = now
	for u, t := range g.hits {
		if now.Sub(t) > wafIPWindow {
			delete(g.hits, u)
		}
	}

	distinct := len(g.hits)
	repeat := g.rep[uid]
	if distinct >= wafIPThreshold || repeat >= wafIPRepeatThreshold {
		g.streak++
		cool := g.cooldownFor(g.streak)
		g.until = now.Add(cool)
		reason := "distinct-uid"
		if distinct < wafIPThreshold {
			reason = "single-uid-repeat"
		}
		log.Printf("WARN: [server] waf ip-level block (%s): distinct_uid=%d repeat_uid=%d within %s, rotate fail-fast for %s until %s (streak=%d)",
			reason, distinct, repeat, wafIPRepeatWindow, cool, g.until.Format(time.RFC3339), g.streak)
		g.hits = map[string]time.Time{}
		g.rep = map[string]int{}
		return true
	}
	return false
}

// active 报告 IP 级拦截是否激活（末端错误文案区分 IP 级/账号级措辞用）。
func (g *wafIPGate) active() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.until)
}
