// proxyapi.go 账号代理管理接口（面板「账号代理」页的数据面）。
//
// 与全局代理（upstream.proxy_global）的区别见 internal/upstream/accountproxy.go
// 头注释：这里是「一账号一条固定出口」。本文件只做面板与守卫之间的搬运：
// 绑定表 CRUD + 手动触发出口校验 + 状态展示，不含任何判定逻辑。
package panel

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

// proxyCheckTimeout 面板触发校验的总上限（N 个账号串行，每个最多 probe_timeout）。
const proxyCheckTimeout = 120 * time.Second

// proxyManager 取账号代理守卫；未启用返回 nil（路由层统一 501）。
func (p *Panel) proxyManager() *upstream.AccountProxy {
	if p.cfg.Upstream == nil {
		return nil
	}
	return p.cfg.Upstream.AccountProxy
}

// proxyRow 面板一行：账号身份 + 代理绑定 + 实测状态。
type proxyRow struct {
	upstream.AccountProxyStatus
	Nickname string `json:"nickname"`
	Realm    string `json:"realm"`
	Bound    bool   `json:"bound"` // 是否已配置代理（false = 走默认出口）
}

// proxyList 全部账号的代理视图（未绑定的账号也要出现，否则没法给它配）。
func (p *Panel) proxyList(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用（config.json: account_proxy.enabled=false）")
		return
	}
	byUID := map[string]upstream.AccountProxyStatus{}
	for _, s := range m.Statuses() {
		byUID[s.UID] = s
	}
	// 绑定表里可能有账号已被删除的残留（SyncAccounts 只在启动时跑一次），
	// 这些也要列出来，否则面板上看不见、也就删不掉。
	for uid := range m.Entries() {
		if _, ok := byUID[uid]; !ok {
			byUID[uid] = upstream.AccountProxyStatus{UID: uid, State: "unbound"}
		}
	}
	rows := make([]proxyRow, 0, len(byUID))
	for _, st := range p.cfg.Pool.List() {
		s, ok := byUID[st.UID]
		if !ok {
			s = upstream.AccountProxyStatus{UID: st.UID, State: "unbound"}
		}
		delete(byUID, st.UID)
		rows = append(rows, proxyRow{AccountProxyStatus: s, Nickname: st.Nickname, Realm: st.Realm, Bound: ok})
	}
	// 残余（账号已删）追加在末尾，标 unbound 便于清理。
	for _, s := range byUID {
		rows = append(rows, proxyRow{AccountProxyStatus: s, Nickname: "(账号已移除)", Realm: "", Bound: true})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "accounts": rows})
}

// proxySave 写入 / 更新一个账号的代理绑定。
func (p *Panel) proxySave(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	var body struct {
		UID        string `json:"uid"`
		Proxy      string `json:"proxy"`
		ExpectedIP string `json:"expected_ip"`
		Enabled    *bool  `json:"enabled"`
		Label      string `json:"label"`
		Note       string `json:"note"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	body.UID = strings.TrimSpace(body.UID)
	if body.UID == "" {
		writeErr(w, http.StatusBadRequest, "uid 不能为空")
		return
	}
	e := upstream.AccountProxyEntry{
		Proxy:      strings.TrimSpace(body.Proxy),
		ExpectedIP: strings.TrimSpace(body.ExpectedIP),
		Enabled:    true, // 新建缺省启用（显式 false 才关）
		Label:      strings.TrimSpace(body.Label),
		Note:       strings.TrimSpace(body.Note),
	}
	if body.Enabled != nil {
		e.Enabled = *body.Enabled
	}
	if err := m.SetEntry(body.UID, e); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	st, _ := m.Status(body.UID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "account": st})
}

// proxyDelete 删除一个账号的代理绑定（账号本身不动）。
func (p *Panel) proxyDelete(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	var body struct {
		UID string `json:"uid"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	body.UID = strings.TrimSpace(body.UID)
	if body.UID == "" {
		writeErr(w, http.StatusBadRequest, "uid 不能为空")
		return
	}
	if err := m.Remove(body.UID); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// proxyDirect 把账号切为「直连」：删除其代理绑定，出站回落本机直连出口。
//
// 与 proxyDelete 等价但语义更明确——面板「直连」按钮表达的是「让这个账号走
// 本机出口、不走任何代理」，而不是「删除一条配置」。返回删除后的账号状态。
func (p *Panel) proxyDirect(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	var body struct {
		UID string `json:"uid"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	body.UID = strings.TrimSpace(body.UID)
	if body.UID == "" {
		writeErr(w, http.StatusBadRequest, "uid 不能为空")
		return
	}
	if err := m.Remove(body.UID); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "uid": body.UID})
}

// proxyAutobind 「代理链接池自动配置」：从该账号所属 realm 的订阅池里挑一条
// 当前最稳定的链接绑定上去（面板「自动池配」按钮）。
//
// realm 解析优先级：请求体显式 realm > 账号池里该 uid 的 realm > global。
func (p *Panel) proxyAutobind(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	var body struct {
		UID   string `json:"uid"`
		Realm string `json:"realm"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	uid := strings.TrimSpace(body.UID)
	if uid == "" {
		writeErr(w, http.StatusBadRequest, "uid 不能为空")
		return
	}
	realm := strings.ToLower(strings.TrimSpace(body.Realm))
	if realm == "" {
		for _, st := range p.cfg.Pool.List() {
			if st.UID == uid {
				realm = strings.ToLower(st.Realm)
				break
			}
		}
	}
	if realm == "" {
		realm = "global"
	}
	if !m.AutoBindForce(uid, realm) {
		writeErr(w, http.StatusBadGateway,
			"池内暂无可用的代理出口（检查 "+realm+" 订阅池是否已配置/刷新）")
		return
	}
	st, _ := m.Status(uid)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "account": st})
}

// proxyCheck 校验单个账号出口（面板「校验」按钮）。
func (p *Panel) proxyCheck(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	var body struct {
		UID string `json:"uid"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	body.UID = strings.TrimSpace(body.UID)
	ctx, cancel := context.WithTimeout(r.Context(), proxyCheckTimeout)
	defer cancel()
	st, err := m.Check(ctx, body.UID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "account": st})
}

// proxyCheckAll 校验全部绑定（串行，N 个账号 × probe_timeout 上限内完成）。
func (p *Panel) proxyCheckAll(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), proxyCheckTimeout)
	defer cancel()
	sts := m.CheckAll(ctx)
	bad := 0
	for _, s := range sts {
		if s.State != "ok" && s.State != "disabled" {
			bad++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "accounts": sts, "checked": len(sts), "bad": bad})
}

// proxyBulk 批量写入绑定。
//
// 两种输入二选一：
//   - items：结构化数组（前端/脚本用）；
//   - text：直接粘贴的文本，每行一条 `uid 代理链接 [声明IP] [备注]`，
//     分隔符支持空格/Tab/逗号，`#` 开头为注释行，空行忽略。
//
// 为什么要有这个接口：账号一多起来，逐个点「配置 → 填链接 → 保存 → 校验」是
// 纯体力活，而且很容易复制错行。批量接口一次贴完，逐条返回结果（成功的写盘，
// 失败的带上原因），不做「一条失败就全回滚」——已验证可用的绑定不该被一条
// 拼错的链接拖回去。
func (p *Panel) proxyBulk(w http.ResponseWriter, r *http.Request) {
	m := p.proxyManager()
	if m == nil || !m.Active() {
		writeErr(w, http.StatusNotImplemented, "账号代理未启用")
		return
	}
	var body struct {
		Items []struct {
			UID        string `json:"uid"`
			Proxy      string `json:"proxy"`
			ExpectedIP string `json:"expected_ip"`
			Label      string `json:"label"`
			Enabled    *bool  `json:"enabled"`
		} `json:"items"`
		Text string `json:"text"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	type row struct {
		UID  string `json:"uid"`
		OK   bool   `json:"ok"`
		Err  string `json:"error,omitempty"`
		Line string `json:"line,omitempty"`
	}
	out := []row{}
	saved := 0
	apply := func(uid, proxy, exp, label string, enabled *bool) {
		uid = strings.TrimSpace(uid)
		proxy = strings.TrimSpace(proxy)
		label = strings.TrimSpace(label)
		exp = strings.TrimSpace(exp)
		ent := row{UID: uid}
		if uid == "" || proxy == "" {
			ent.Err = "uid 与代理链接都不能为空"
			out = append(out, ent)
			return
		}
		e := upstream.AccountProxyEntry{
			Proxy:      proxy,
			ExpectedIP: exp,
			Enabled:    true,
			Label:      label,
		}
		if enabled != nil {
			e.Enabled = *enabled
		}
		if err := m.SetEntry(uid, e); err != nil {
			ent.Err = err.Error()
			out = append(out, ent)
			return
		}
		ent.OK = true
		saved++
		out = append(out, ent)
	}

	for _, it := range body.Items {
		apply(it.UID, it.Proxy, it.ExpectedIP, it.Label, it.Enabled)
	}
	// 文本模式：逐行解析（# 注释 / 空行跳过）
	for _, ln := range strings.Split(body.Text, "\n") {
		ln = strings.TrimSpace(strings.ReplaceAll(ln, "\r", ""))
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		fields := strings.FieldsFunc(ln, func(ch rune) bool {
			return ch == ' ' || ch == '\t' || ch == ',' || ch == '；' || ch == ';'
		})
		if len(fields) < 2 {
			r := row{Err: "至少需要 uid 与代理链接两列", Line: ln}
			out = append(out, r)
			continue
		}
		uid, proxy := fields[0], fields[1]
		exp, label := "", ""
		if len(fields) >= 3 {
			// 第三列若是 IP 则当期望出口 IP，否则当备注（粘贴来源格式不统一）。
			if net.ParseIP(fields[2]) != nil {
				exp = fields[2]
			} else {
				label = fields[2]
			}
		}
		if len(fields) >= 4 {
			label = strings.Join(fields[3:], " ")
		} else if len(fields) == 3 && exp != "" {
			// 只有三列且第三列是 IP 时，备注为空
		}
		apply(uid, proxy, exp, label, nil)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "saved": saved, "total": len(out), "results": out,
	})
}

// subPoolManager 取订阅池；未启用返回 nil（路由层统一 501）。
func (p *Panel) subPoolManager() *upstream.SubPool { return p.subPool }

// subPoolRealmView 订阅池一行（面板视图）：配置 + 解析结果 + 出口池健康分布。
//
// 三个数字刻意分开透出，回答三个不同问题：
//   - SubsCount  配了几条链接（用户维护的输入）
//   - Entries    上次刷新解析出多少条出口（拉取结果）
//   - Pool       这些出口当前几条能打（实时健康，和 entries 会不一致：
//     拉取成功 15 条、其中 12 条正在冷却，是"池子快空了"的强信号）
type subPoolRealmView struct {
	Realm       string                 `json:"realm"`
	Subs        []string               `json:"subs"`
	SubsCount   int                    `json:"subs_count"`
	Entries     int                    `json:"entries"`
	RefreshedAt int64                  `json:"refreshed_at"`
	LastErr     string                 `json:"last_error,omitempty"`
	Pool        upstream.ProxyPoolStat `json:"pool"`
}

// subPoolList 订阅池状态（global/cn 两行）+ 各自出口池健康分布。
func (p *Panel) subPoolList(w http.ResponseWriter, r *http.Request) {
	m := p.subPoolManager()
	if m == nil {
		writeErr(w, http.StatusNotImplemented, "订阅池未启用")
		return
	}
	sts := m.Status()
	views := make([]subPoolRealmView, 0, len(sts))
	for _, s := range sts {
		v := subPoolRealmView{
			Realm:       s.Realm,
			Subs:        s.Subs,
			SubsCount:   len(s.Subs),
			Entries:     s.Entries,
			RefreshedAt: s.RefreshedAt,
			LastErr:     s.LastErr,
		}
		if p.cfg.Upstream != nil {
			v.Pool = p.cfg.Upstream.ProxyPoolStats(s.Realm)
		}
		views = append(views, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"realms": views})
}

// subPoolSave 变更指定 realm 的订阅 URL 列表并立即刷新。
// mode=replace（默认，整体覆盖）/ append（尾部追加，去重不动既有条目）。
func (p *Panel) subPoolSave(w http.ResponseWriter, r *http.Request) {
	m := p.subPoolManager()
	if m == nil {
		writeErr(w, http.StatusNotImplemented, "订阅池未启用")
		return
	}
	var body struct {
		Realm string   `json:"realm"`
		Subs  []string `json:"subs"`
		Mode  string   `json:"mode"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<18)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	realm := strings.ToLower(strings.TrimSpace(body.Realm))
	if realm != "global" && realm != "cn" {
		writeErr(w, http.StatusBadRequest, "realm 只能是 global 或 cn（两池不互通）")
		return
	}
	mode := strings.ToLower(strings.TrimSpace(body.Mode))
	if mode != "" && mode != upstream.SubModeReplace && mode != upstream.SubModeAppend {
		writeErr(w, http.StatusBadRequest, "mode 只能是 replace（覆盖）或 append（追加）")
		return
	}
	if err := m.ApplySubs(realm, body.Subs, mode); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// subPoolClear 清空指定 realm 的订阅列表与出口池（该 realm 回落直连）。
// 与"保存空列表"等价，单独给端点是为了让前端能明确区分"清除"这个破坏性动作
// （前端会二次确认），而不是让用户靠"把文本框删空再点保存"来猜。
func (p *Panel) subPoolClear(w http.ResponseWriter, r *http.Request) {
	m := p.subPoolManager()
	if m == nil {
		writeErr(w, http.StatusNotImplemented, "订阅池未启用")
		return
	}
	var body struct {
		Realm string `json:"realm"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	realm := strings.ToLower(strings.TrimSpace(body.Realm))
	if realm != "global" && realm != "cn" {
		writeErr(w, http.StatusBadRequest, "realm 只能是 global 或 cn")
		return
	}
	if err := m.Clear(realm); err != nil {
		writeErr(w, http.StatusBadGateway, "清除失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// subPoolRefresh 手动刷新指定 realm 的订阅。
func (p *Panel) subPoolRefresh(w http.ResponseWriter, r *http.Request) {
	m := p.subPoolManager()
	if m == nil {
		writeErr(w, http.StatusNotImplemented, "订阅池未启用")
		return
	}
	var body struct {
		Realm string `json:"realm"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	realm := strings.ToLower(strings.TrimSpace(body.Realm))
	if realm != "global" && realm != "cn" {
		writeErr(w, http.StatusBadRequest, "realm 只能是 global 或 cn")
		return
	}
	if err := m.Refresh(realm); err != nil {
		writeErr(w, http.StatusBadGateway, "刷新失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
