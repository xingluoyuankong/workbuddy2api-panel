// accountproxy.go 账号级出站代理绑定 + 出口一致性守卫。
//
// 【要解决的事】
// upstream.proxy_global 是**全局**的：所有 global 域请求共用一个出口池。风控按
// 「同一 IP 短时多账号」聚号时，全局代理只是把风险从网关 IP 平移到代理 IP——
// N 个账号仍然一起被标记。真正的隔离粒度是「一个账号 ↦ 一条固定的住宅代理」：
// 出口 IP 恒定、与其它账号不复用、地理位置自洽。
//
// 【为什么配了还不够，必须校验】
// 配错 / 代理失效时请求照样成功（见 egress.go 头注释的四种情形），唯一症状是
// 出口 IP 没按预期改变，而这在线上几乎不可观测。所以本模块把「这条代理链接
// 现在真的按声明的 IP 出去吗」变成一等状态，并在失配时按策略处置（回落 /
// 隔离），而不是继续假装它在工作。
//
// 【数据流】
//   data/proxy.json       绑定表（uid → 代理链接 / 声明 IP / 开关），面板可写
//   data/proxy_state.json 运行期实测快照（IP / 国家 / 状态 / 时刻），每次校验写回
//   Client.httpForA()     每请求按 uid 取 client（per-account 优先于 realm 默认）
//   pool 代理闸门         为对话隔离的账号不参与选号（quarantine 策略）
package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/logfmt"
)

// proxyState 单个账号代理的处置状态。
type proxyState int

const (
	proxyStateUnchecked proxyState = iota // 还没校验过（启动后首次请求先放行）
	proxyStateOK                          // 出口 == 声明 IP，代理确实生效
	proxyStateUnreachable                 // 代理连不上 / 回显全挂
	proxyStateNotEffective                // 出口 IP == 直连出口 IP：代理没生效
	proxyStateRotating                    // 多源不一致：出口在轮换
	proxyStateMismatch                    // 出口 != 声明 IP
	proxyStateLeak                        // 检测到来源头泄漏（XFF 链）
	proxyStateDisabled                    // 手动关闭 / 未配置
)

// String 状态的机读名（面板与日志消费，勿改字面量）。
func (s proxyState) String() string {
	switch s {
	case proxyStateOK:
		return "ok"
	case proxyStateUnreachable:
		return "unreachable"
	case proxyStateNotEffective:
		return "not_effective"
	case proxyStateRotating:
		return "rotating"
	case proxyStateMismatch:
		return "mismatch"
	case proxyStateLeak:
		return "leak"
	case proxyStateDisabled:
		return "disabled"
	default:
		return "unchecked"
	}
}

// OnMismatch 失配处置策略。
const (
	// MaxPerIPWarn 同出口 IP 承载账号超限时只告警（默认）。
	MaxPerIPWarn = "warn"
	// MaxPerIPQuarantine 超限时把多出来的账号隔离出选号池。
	MaxPerIPQuarantine = "quarantine"
)

// 失配处置策略（出口 IP 与声明不符 / 代理根本没生效时怎么办）。
const (
	// MismatchIgnore 仍走该代理，只记录与展示（排障期用，想看看失配时上游到底认不认）。
	MismatchIgnore = "ignore"
	// MismatchFallback 失配时不用该代理，回落 realm 默认出口（global 代理池 / 直连）。
	// 默认策略：宁可退回旧路径，也不把账号送到一个身份不明的出口。
	MismatchFallback = "fallback"
	// MismatchQuarantine 失配时连账号一起隔离：该号不参与选号（代理是它的身份，
	// 身份不可信时不该用它）。适用于「每号必须固定住宅 IP」的强约束部署。
	MismatchQuarantine = "quarantine"
)

// AccountProxyOptions 守卫运行参数（由 cmd/server/config.go 注入）。
type AccountProxyOptions struct {
	// File 绑定表路径（通常 data/proxy.json）。
	File string
	// StateFile 运行期实测快照路径（通常 data/proxy_state.json）。
	StateFile string
	// CheckInterval 后台周期性校验间隔；0 = 只在启动与手动时校验。
	CheckInterval time.Duration
	// ProbeTimeout 单个账号单次完整校验的上限。
	ProbeTimeout time.Duration
	// Quorum 出口 IP 交叉验证一致源数门槛，默认 2。
	Quorum int
	// OnMismatch 失配处置：ignore / fallback / quarantine，默认 fallback。
	OnMismatch string
	// LockFirstIP 【已废弃，保留字段兼容旧 config】。
	// 原语义「首次校验锁定出口 IP，之后漂移即失配」与聚合代理的出口轮换天然
	// 冲突（轮换池必被判 mismatch）。出口一致性现在定义为「调用确实走这条
	// 代理链路」：未声明时不比对；强校验请显式填 expected_ip。
	// normalize 不再读取该字段。
	LockFirstIP bool
	// MaxPerIP 同一出口 IP 允许承载的账号数上限；0 = 不限制。
	MaxPerIP int
	// MaxPerIPAction 超过 MaxPerIP 时的动作：
	//   warn（默认）只在日志与面板标记；
	//   quarantine 超限账号直接不参与选号（同 IP 聚号是风控的经典靶心）。
	// 保留策略：按 uid 排序保留前 MaxPerIP 个，其余隔离——不是全隔离，否则
	// 一个 IP 上挂了 5 个号会直接把 5 个号一起打死，等于自伤。
	MaxPerIPAction string
	// MinInterval 同一账号两次出站之间的最小间隔（节流）。0 = 关闭。
	//
	// 为什么要节流：高频同号请求本身就是限流与风控的触发源（上游 6004 模型级
	// 配额与账号级频率限制都按时间窗计数）。每条代理链接对应一个住宅 IP，
	// 「一个住宅 IP 每秒发 5 条对话」本身就是不自然的流量形态。
	// 代价：并发高时请求会排队（等待时间最坏 = 并发数 × 间隔），所以默认关闭且
	// 配置上限 5s（normalize 校验）。
	MinInterval time.Duration
}

// AccountProxyEntry 单账号代理绑定（落盘结构）。
type AccountProxyEntry struct {
	// Proxy 代理链接。支持 socks5://[user:pass@]host:port、socks5h://、
	// http://[user:pass@]host:port、https://。
	Proxy string `json:"proxy"`
	// ExpectedIP 声明的出口 IP。空 = 首次实测后自动锁定（推荐填：住宅代理通常
	// 每个端口对应一个固定 IP，填了才能立刻发现「出口不是它」）。
	ExpectedIP string `json:"expected_ip,omitempty"`
	Enabled bool `json:"enabled"`
	// Auto 自动绑定标记：true = 由订阅池自动选择/换绑（当前链接失败或劫持时
	// 自动切到池里下一条稳定链接）；false = 用户手动指定，永不自动改动。
	Auto bool `json:"auto,omitempty"`
	// Label 备注（如「美国-洛杉矶-住宅-01」），仅面板展示。
	Label string `json:"label,omitempty"`
	// Note 自由备注。
	Note string `json:"note,omitempty"`
}

// accountProxyDoc 绑定表落盘结构。
type accountProxyDoc struct {
	Version  int                           `json:"version"`
	Accounts map[string]*AccountProxyEntry `json:"accounts"`
}

// proxyRuntime 单个账号的实测快照（写 data/proxy_state.json）。
type proxyRuntime struct {
	IP         string `json:"ip,omitempty"`
	ExpectedIP string `json:"expected_ip,omitempty"`
	DirectIP   string `json:"direct_ip,omitempty"`
	Country    string `json:"country,omitempty"`
	CountryCode string `json:"country_code,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
	ASN        string `json:"asn,omitempty"`
	State      string `json:"state"`
	Message    string `json:"message,omitempty"`
	Leak       string `json:"leak,omitempty"`
	LatencyMS  int64  `json:"latency_ms,omitempty"`
	CheckedAt  int64  `json:"checked_at,omitempty"`
	Fails      int    `json:"fails,omitempty"`
	// CallIP 调用链路出口最近一次采样（SampleLoop；轮换池会随时间变化）。
	CallIP     string `json:"call_ip,omitempty"`
	CallAt     int64  `json:"call_at,omitempty"`
}

// AccountProxyStatus 面板 / 日志消费的一个账号代理状态。
type AccountProxyStatus struct {
	UID       string `json:"uid"`
	Nickname  string `json:"nickname,omitempty"`
	Proxy     string `json:"proxy,omitempty"` // 脱敏后的链接（不含密码）
	ProxyHost string `json:"proxy_host,omitempty"`
	Scheme    string `json:"scheme,omitempty"`
	Enabled   bool   `json:"enabled"`
	Label     string `json:"label,omitempty"`
	Note      string `json:"note,omitempty"`

	ExpectedIP string `json:"expected_ip,omitempty"`
	ActualIP   string `json:"actual_ip,omitempty"`
	DirectIP   string `json:"direct_ip,omitempty"`
	Country    string `json:"country,omitempty"`
	CountryCode string `json:"country_code,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
	ASN        string `json:"asn,omitempty"`

	// Effective 代理确实改变了出口（出口 != 本机直连出口）。
	Effective bool `json:"effective"`
	// Match 实测出口 == 声明/锁定 IP。
	Match  bool   `json:"match"`
	State  string `json:"state"`
	Message string `json:"message,omitempty"`
	Leak   string `json:"leak,omitempty"`

	LatencyMS int64 `json:"latency_ms"`
	CheckedAt int64 `json:"checked_at"`
	Fails     int   `json:"fails"`
	SharedBy  int   `json:"shared_by,omitempty"` // 同一出口 IP 上还有几个别的账号
	// SharedBlocked 因「同出口 IP 承载账号超限」被隔离（max_per_ip_action=quarantine）。
	SharedBlocked bool `json:"shared_blocked,omitempty"`
}

// atomicString 并发安全的字符串持有者。
//
// sync/atomic 没有 String 类型（只有 Bool/Int*/Uint*/Pointer/Value），用
// Pointer[string] 包一层，把「nil 判空」收在一处，调用点直接 Load/Store。
type atomicString struct{ p atomic.Pointer[string] }

// Store 写入新值。
func (a *atomicString) Store(s string) { a.p.Store(&s) }

// Load 读取当前值（从未写入时返回空串）。
func (a *atomicString) Load() string {
	if v := a.p.Load(); v != nil {
		return *v
	}
	return ""
}

// accountBinding 一个账号的代理运行态。
type accountBinding struct {
	uid   string
	spec  *AccountProxyEntry
	entry *proxyEntry // 复用既有出口统计结构（fails/deadUntil + client 对）

	state   atomic.Int32
	msg     atomicString
	probeIP atomicString
	directIP atomicString
	// lockedIP 固定模式下锁定的出口 IP；rotate 模式下为最近一次实测值。
	lockedIP atomicString
	checked  atomic.Int64
	lastOK   atomic.Int64
	country  atomicString
	countryC atomicString
	tz       atomicString
	asn      atomicString
	latency  atomic.Int64
	leak     atomicString

	// lastReq 上一次出站时刻（unix nano），节流排队用（见 AccountProxy.Wait）。
	lastReq atomic.Int64

	// callIP / callAt 最近一次「调用链路出口采样」（SampleLoop 定时实测）。
	// 一致性验证用：显示的出口（probeIP = 校验侧全源实测）与
	// callIP（调用链路单源定时采样）比对。
	callIP atomicString
	callAt atomic.Int64
	// sharedBlocked 该账号因「同出口 IP 承载账号超限」被隔离（MaxPerIPAction=quarantine）。
	sharedBlocked atomic.Bool
}

// AccountProxy 账号代理守卫。
type AccountProxy struct {
	opts AccountProxyOptions

	mu       sync.RWMutex
	doc      *accountProxyDoc
	bindings map[string]*accountBinding

	// direct 直连对照 client：判定「代理有没有真的改变出口」的基准。
	direct *http.Client
	// client 出站 Client 引用（订阅池所在；SetClient 注入）。
	// 自动绑定/换绑用它查池里最稳定的条目（Client.BestProxyFor）。
	client *Client
	// directIPCache 直连出口 IP（每次扫都重测成本高，缓存到 check 周期粒度）。
	directIPCache atomicString
	// 直连出口的地理与运营方（与 directIPCache 同一次探测取得）。
	// 未绑代理的账号出口就是它，面板账号池要显示，所以必须缓存住。
	directCountry   atomicString
	directCountryC  atomicString
	directASN       atomicString
	directChecked   atomic.Int64

	checkMu sync.Mutex // 串行化校验，避免面板手触发与周期校验并发打同一批代理
	runs    atomic.Uint64
	shared  atomic.Pointer[map[string]int] // ip → 账号数（上次扫描结果）
}

// NewAccountProxy 构建守卫；File 为空时等同关闭（所有查询返回未绑定）。
func NewAccountProxy(opts AccountProxyOptions) *AccountProxy {
	if opts.OnMismatch == "" {
		opts.OnMismatch = MismatchFallback
	}
	if opts.Quorum <= 0 {
		opts.Quorum = defaultEgressQuorum
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 20 * time.Second
	}
	tr := newTransport()
	m := &AccountProxy{
		opts:     opts,
		doc:      &accountProxyDoc{Version: 1, Accounts: map[string]*AccountProxyEntry{}},
		bindings: map[string]*accountBinding{},
		direct:   &http.Client{Timeout: 30 * time.Second, Transport: tr},
	}
	return m
}

// Active 守卫是否已启用（绑定表路径非空）。
func (m *AccountProxy) Active() bool { return m != nil && m.opts.File != "" }

// ---------------------------------------------------------------------------
// 绑定表读写
// ---------------------------------------------------------------------------

// Load 载入绑定表并完成首次构建。文件不存在不算错（等价于没有任何绑定）。
func (m *AccountProxy) Load() error {
	if m == nil || m.opts.File == "" {
		return nil
	}
	raw, err := os.ReadFile(m.opts.File)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("读取代理绑定表 %s: %w", m.opts.File, err)
	}
	var doc accountProxyDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("解析代理绑定表 %s: %w", m.opts.File, err)
	}
	if doc.Accounts == nil {
		doc.Accounts = map[string]*AccountProxyEntry{}
	}
	m.mu.Lock()
	m.doc = &doc
	m.rebuildLocked()
	m.mu.Unlock()
	m.loadRuntime()
	return nil
}

// saveLocked 落盘绑定表（调用方持写锁）。
func (m *AccountProxy) saveLocked() error {
	if m.opts.File == "" {
		return nil
	}
	raw, err := json.MarshalIndent(m.doc, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(m.opts.File)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("创建目录 %s: %w", dir, err)
		}
	}
	tmp := m.opts.File + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return fmt.Errorf("写入代理绑定表 %s: %w", tmp, err)
	}
	// 同 main.saveConfig：挂载点不能被 rename 覆盖时回退为写回。
	if err := os.Rename(tmp, m.opts.File); err != nil {
		data, rerr := os.ReadFile(tmp)
		_ = os.Remove(tmp)
		if rerr != nil {
			return fmt.Errorf("替换代理绑定表 %s: %w", m.opts.File, err)
		}
		if werr := os.WriteFile(m.opts.File, data, 0o600); werr != nil {
			return fmt.Errorf("写回代理绑定表 %s: %w", m.opts.File, werr)
		}
	}
	return nil
}

// SetEntry 写入 / 更新一个账号的代理绑定并立即重建 client。
func (m *AccountProxy) SetEntry(uid string, e AccountProxyEntry) error {
	if m == nil || m.opts.File == "" {
		return fmt.Errorf("账号代理未启用（account_proxy.file 未配置）")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return fmt.Errorf("uid 不能为空")
	}
	e.Proxy = strings.TrimSpace(e.Proxy)
	if e.Proxy == "" {
		return fmt.Errorf("代理链接不能为空")
	}
	if err := validateProxyURL(e.Proxy); err != nil {
		return err
	}
	if e.ExpectedIP != "" && net.ParseIP(e.ExpectedIP) == nil {
		return fmt.Errorf("expected_ip 不是合法 IP: %s", e.ExpectedIP)
	}
	cp := e
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doc.Accounts[uid] = &cp
	if err := m.saveLocked(); err != nil {
		return err
	}
	return m.rebuildOneLocked(uid)
}

// Remove 删除一个账号的代理绑定。
// SetClient 注入出站 Client（订阅池所在），启用自动绑定/故障换绑。
func (m *AccountProxy) SetClient(c *Client) {
	if m != nil {
		m.client = c
	}
}

// probeCandidate 候选链接预检：单源实测出口可达（避免把账号绑到整条挂掉的
// 线路上——resin 订阅刷新中该线路全部节点会暂时失效）。
func (m *AccountProxy) probeCandidate(raw string) bool {
	e, err := newProxyEntry(raw)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	p := probeEgress(ctx, e.http, 1)
	return p.IP != ""
}

// AutoBindAccount 自动绑定：无绑定账号粘住池里最稳定链接；auto 绑定的链接
// 连续校验失败（fails>=2）时换绑到下一条稳定链接。手动绑定（auto=false）
// 永不改动。换绑保留原 Enabled/Label/Note，清掉旧出口采样。
//
// 设计语义（用户定稿）：**不是随机轮询**——账号粘住一条稳定出口，只有它坏了
// 才切换；切换后重新粘住。与池轮询（pickProxy 分散负载）是两种模式。
func (m *AccountProxy) AutoBindAccount(uid, realm string) {
	if m == nil || m.client == nil {
		return
	}
	// 严禁持锁做网络 IO（曾因此卡死整个服务）：锁外读快照 → 锁外探活/预检 →
	// 短临界区提交。所有探活都在锁外完成。
	m.mu.RLock()
	b := m.bindings[uid]
	var curRaw string
	var isAuto, isEnabled bool
	if b != nil {
		curRaw = b.spec.Proxy
		isAuto = b.spec.Auto
		isEnabled = b.spec.Enabled
	}
	m.mu.RUnlock()

	if b != nil && (!isAuto || !isEnabled) {
		return // 手动绑定 / 已停用：不碰
	}

	// ── 无绑定：首次自动绑定 ─────────────────────────────────────
	// 逐个探活候选（按稳定度排序），用第一个真正可用的；全部不可用 → 保持直连。
	if b == nil {
		if m.bindFirstUsable(uid, realm) {
			log.Printf("[autobind] %s 自动绑定稳定出口 uid=%s", realm, logfmt.UID8(uid))
		}
		return
	}

	// ── 有 auto 绑定：先主动探活当前链接（1 分钟粒度发现失效）────
	// 必须无条件探活：不能用「fails>0 就跳探活」短路——那样链路恢复了计数还在涨，
	// 会误判成持续失效而错误换绑。
	if m.probeCandidate(curRaw) {
		b.entry.fails.Store(0) // 探活成功清零（抗抖动：瞬时不可用不累积）
		return
	}
	b.entry.noteFail()

	// 连续失败未达阈值：继续观察（避免网络抖动导致频繁换绑）
	const failThreshold = 3
	if b.entry.fails.Load() < failThreshold {
		return
	}

	// ── 阈值达到：换绑（逐个探活候选）────────────────────────────
	// 池里没有可用候选（全探活失败）→ 解绑回落直连（订阅恢复后自动重绑）。
	if m.bindFirstUsable(uid, realm) {
		log.Printf("[autobind] 绑定出口连续失败 %d 次，已换绑 uid=%s",
			failThreshold, logfmt.UID8(uid))
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if nb := m.bindings[uid]; nb == nil || nb.spec.Proxy != curRaw {
		return // 并发：已变化
	}
	delete(m.doc.Accounts, uid)
	delete(m.bindings, uid)
	_ = m.saveLocked()
	log.Printf("[autobind] %s 池内全部候选不可用，解绑回落直连 uid=%s", realm, logfmt.UID8(uid))
}

// bindFirstUsable 按稳定度逐个探活候选，把第一个可用的绑到 uid。
// 返回是否成功绑定。全部不可用返回 false（调用方决定保持直连还是解绑）。
// 网络 IO 全在锁外；仅在确定候选后用短临界区写入。
func (m *AccountProxy) bindFirstUsable(uid, realm string) bool {
	m.mu.RLock()
	cur := ""
	if b := m.bindings[uid]; b != nil {
		cur = b.spec.Proxy
	}
	m.mu.RUnlock()

	for _, cand := range m.client.CandidateProxies(realm, cur) {
		if !m.probeCandidate(cand) {
			continue // 这条不可用，试下一条
		}
		m.mu.Lock()
		e := &AccountProxyEntry{Proxy: cand, Enabled: true, Auto: true}
		nb, err := newAccountBinding(uid, e)
		if err != nil {
			m.mu.Unlock()
			continue
		}
		// 407 = 绑定凭据失效（resin 强刷轮换 token）：立即解绑该账号回落直连。
		// 绑定 entry 与池条目是独立实例，池的 onAuthFail 覆盖不到这里，必须单独注入。
		nb.entry.onAuthFail = func() {
			m.mu.Lock()
			if b := m.bindings[uid]; b != nil && b.spec.Proxy == nb.entry.raw {
				delete(m.doc.Accounts, uid)
				delete(m.bindings, uid)
				_ = m.saveLocked()
				log.Printf("[autobind] 绑定出口凭据失效（407），解绑 uid=%s 回落直连", logfmt.UID8(uid))
			}
			m.mu.Unlock()
		}
		// 保留用户手填的声明 IP / 备注（若有）
		if ob := m.bindings[uid]; ob != nil {
			nb.spec.ExpectedIP = ob.spec.ExpectedIP
			nb.spec.Label = ob.spec.Label
			nb.spec.Note = ob.spec.Note
			e.ExpectedIP = ob.spec.ExpectedIP
			e.Label = ob.spec.Label
			e.Note = ob.spec.Note
		}
		m.doc.Accounts[uid] = e
		m.bindings[uid] = nb
		_ = m.saveLocked()
		m.mu.Unlock()
		return true
	}
	return false
}

// Remove 从绑定表移除一个账号的代理绑定（落盘同步）。
func (m *AccountProxy) Remove(uid string) error {
	if m == nil || m.opts.File == "" {
		return fmt.Errorf("账号代理未启用")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.doc.Accounts[uid]; !ok {
		return nil
	}
	delete(m.doc.Accounts, uid)
	delete(m.bindings, uid)
	return m.saveLocked()
}

// Entries 返回全部绑定（副本）。
func (m *AccountProxy) Entries() map[string]AccountProxyEntry {
	out := map[string]AccountProxyEntry{}
	if m == nil {
		return out
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for uid, e := range m.doc.Accounts {
		if e == nil {
			continue
		}
		out[uid] = *e
	}
	return out
}

// rebuildLocked 按当前绑定表重建全部入口检察 entry（丢弃旧 client 与其统计）。
func (m *AccountProxy) rebuildLocked() {
	keep := map[string]*accountBinding{}
	for uid, spec := range m.doc.Accounts {
		if spec == nil || strings.TrimSpace(spec.Proxy) == "" {
			continue
		}
		if old := m.bindings[uid]; old != nil && accountProxyUnchanged(old, spec) {
			old.spec = spec
			keep[uid] = old
			continue
		}
		if b, err := newAccountBinding(uid, spec); err == nil {
			keep[uid] = b
		}
	}
	m.bindings = keep
}

// rebuildOneLocked 重建单个账号（调用方持写锁）。
func (m *AccountProxy) rebuildOneLocked(uid string) error {
	spec := m.doc.Accounts[uid]
	if spec == nil || strings.TrimSpace(spec.Proxy) == "" {
		delete(m.bindings, uid)
		return nil
	}
	b, err := newAccountBinding(uid, spec)
	if err != nil {
		return err
	}
	m.bindings[uid] = b
	return nil
}

// accountProxyUnchanged 判定绑定是否需要重建 client（链接没变就复用，保住失败统计）。
func accountProxyUnchanged(b *accountBinding, spec *AccountProxyEntry) bool {
	return b.spec != nil && b.spec.Proxy == spec.Proxy
}

// newAccountBinding 由绑定项构造运行态。
func newAccountBinding(uid string, spec *AccountProxyEntry) (*accountBinding, error) {
	cp := *spec
	e, err := newProxyEntry(cp.Proxy)
	if err != nil {
		return nil, err
	}
	b := &accountBinding{uid: uid, spec: &cp, entry: e}
	if !cp.Enabled {
		b.state.Store(int32(proxyStateDisabled))
	} else {
		b.state.Store(int32(proxyStateUnchecked))
	}
	if exp := expectedIPOf(&cp); exp != "" {
		b.lockedIP.Store(exp)
	}
	return b, nil
}

// expectedIPOf 返回**显式声明**的出口 IP（未填返回空）。
//
// 【为什么不再从代理链接 host 推导】曾把「host 是 IP 字面量」自动当作声明出口，
// 假设是"住宅代理入口即出口"。对 resin 这类聚合代理是错的：host 是代理网关
// （甚至就是用户自己的 VPS IP），实际出口由背后节点决定，入口≠出口是常态——
// 用户配置正确的代理被误判 mismatch。声明只能显式给；不填靠 LockFirstIP
// 首次实测锁定，锁的本来就是实测出口，语义正确。
func expectedIPOf(e *AccountProxyEntry) string {
	return strings.TrimSpace(e.ExpectedIP)
}

// validateProxyURL 校验代理链接形态（复用 newProxyEntry 的解析，出错即刻反馈给面板）。
func validateProxyURL(raw string) error {
	e, err := newProxyEntry(raw)
	if err != nil {
		return err
	}
	_ = e
	return nil
}

// SyncAccounts 与 auth 目录对齐：账号被删除时移除其绑定配置（避免僵尸条目堆积）。
func (m *AccountProxy) SyncAccounts(auths []*auth.Auth) {
	if m == nil {
		return
	}
	live := map[string]bool{}
	for _, a := range auths {
		if a != nil && a.UID != "" {
			live[a.UID] = true
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for uid := range m.doc.Accounts {
		if !live[uid] {
			delete(m.doc.Accounts, uid)
			delete(m.bindings, uid)
			changed = true
		}
	}
	if changed {
		_ = m.saveLocked()
	}
}

// ---------------------------------------------------------------------------
// 出站 client 选取（热路径：每请求调用，必须无阻塞读）
// ---------------------------------------------------------------------------

// HTTPFor 返回该账号的代理 client；第二个返回值为 false 表示「没有可用账号代理」，
// 调用方应回落到既定路径（realm 默认出口 / 直连）。
func (m *AccountProxy) HTTPFor(uid string) (*http.Client, bool) {
	if m == nil {
		return nil, false
	}
	b := m.bindingOf(uid)
	if b == nil || b.entry == nil {
		return nil, false
	}
	if !b.spec.Enabled {
		return nil, false
	}
	if !m.usableState(proxyState(b.state.Load())) {
		return nil, false
	}
	if b.entry.deadUntil.Load() > time.Now().UnixNano() {
		return nil, false
	}
	// 节流：按配置的每号最小间隔排队（关闭时是空操作，见 Wait）。
	m.Wait(uid)
	return b.entry.http, true
}

// ChatHTTPFor 同上，取流式（无总超时）client。
func (m *AccountProxy) ChatHTTPFor(uid string) (*http.Client, bool) {
	if m == nil {
		return nil, false
	}
	b := m.bindingOf(uid)
	if b == nil || b.entry == nil {
		return nil, false
	}
	if !b.spec.Enabled {
		return nil, false
	}
	if !m.usableState(proxyState(b.state.Load())) {
		return nil, false
	}
	if b.entry.deadUntil.Load() > time.Now().UnixNano() {
		return nil, false
	}
	// 节流同上。
	m.Wait(uid)
	return b.entry.chat, true
}

// Wait 按 MinInterval 给「同一账号的相邻两次出站」排队。
//
// 语义：不是「每秒最多 N 次」的令牌桶，而是「两次之间至少隔 MinInterval」——
// 后者对风控更友好（上游看到的请求间隔是均匀的，而不是批量突发后静默）。
//
// 排队用 CAS 推进 lastReq 实现：并发请求各自领到一个不重叠的时间槽，不会惊群
// （不是 sleep 完一起冲出去）。等待超过 3×MinInterval 时放弃等待直接放行：
// 排队过长说明并发远超配置预期，此时继续堆 goroutine 只会把延迟转成超时，
// 宁可放行让上游自己去限流。
func (m *AccountProxy) Wait(uid string) {
	if m == nil || m.opts.MinInterval <= 0 {
		return
	}
	b := m.bindingOf(uid)
	if b == nil {
		return
	}
	step := int64(m.opts.MinInterval)
	maxWait := 3 * step
	for {
		now := time.Now().UnixNano()
		last := b.lastReq.Load()
		if last == 0 {
			if b.lastReq.CompareAndSwap(0, now) {
				return
			}
			continue
		}
		if delta := now - last; delta >= step {
			if b.lastReq.CompareAndSwap(last, now) {
				return
			}
			continue
		} else {
			next := last + step
			wait := next - now
			if wait > maxWait {
				return // 队太长了，放弃排队（说明并发远超配置预期）
			}
			if !b.lastReq.CompareAndSwap(last, next) {
				continue
			}
			time.Sleep(time.Duration(wait))
			return
		}
	}
}

// usableState 该状态的账号代理能否被使用。
func (m *AccountProxy) usableState(s proxyState) bool {
	switch s {
	case proxyStateOK, proxyStateUnchecked:
		// 未校验也放行：容器刚起来时不该因为还没跑完首轮校验就全站回落，
		// 后台校验会很快给出结论（届时按状态处置）。
		return true
	case proxyStateDisabled:
		return false
	default:
		return m.opts.OnMismatch == MismatchIgnore
	}
}

// Usable 该账号能否参与服务（供 pool 选号闸门）：quarantine 策略下代理失配即隔离。
// 未绑定代理的账号恒 true（本模块不介入它们的命运）。
func (m *AccountProxy) Usable(uid string) bool {
	if m == nil {
		return true
	}
	b := m.bindingOf(uid)
	if b == nil || !b.spec.Enabled {
		return true
	}
	// 同出口 IP 承载账号超限（MaxPerIPAction=quarantine）→ 隔离。
	if b.sharedBlocked.Load() {
		return false
	}
	s := proxyState(b.state.Load())
	if s == proxyStateOK || s == proxyStateUnchecked || s == proxyStateDisabled {
		return true
	}
	return m.opts.OnMismatch != MismatchQuarantine
}

// bindingOf 无阻塞取绑定（RWMutex 读锁，热路径开销 ~几十 ns）。
func (m *AccountProxy) bindingOf(uid string) *accountBinding {
	if uid == "" {
		return nil
	}
	m.mu.RLock()
	b := m.bindings[uid]
	m.mu.RUnlock()
	return b
}

// ---------------------------------------------------------------------------
// 校验
// ---------------------------------------------------------------------------

// CheckAll 串行校验所有已启用绑定，返回状态列表。
func (m *AccountProxy) CheckAll(ctx context.Context) []AccountProxyStatus {
	if m == nil {
		return nil
	}
	m.checkMu.Lock()
	defer m.checkMu.Unlock()
	m.runs.Add(1)

	m.mu.RLock()
	uids := make([]string, 0, len(m.bindings))
	for uid := range m.bindings {
		uids = append(uids, uid)
	}
	m.mu.RUnlock()
	sort.Strings(uids)

	// 每轮扫描重测一次直连出口作为对照基准（每账号都测太贵）。
	m.refreshDirect(ctx)

	out := make([]AccountProxyStatus, 0, len(uids))
	for _, uid := range uids {
		out = append(out, m.checkOne(ctx, uid))
	}
	m.recomputeShared()
	m.saveRuntime()
	return out
}

// Check 校验单个账号（面板「校验」按钮）。
func (m *AccountProxy) Check(ctx context.Context, uid string) (AccountProxyStatus, error) {
	if m == nil {
		return AccountProxyStatus{}, fmt.Errorf("账号代理未启用")
	}
	if m.bindingOf(uid) == nil {
		return AccountProxyStatus{}, fmt.Errorf("账号 %s 未配置代理", uid)
	}
	m.checkMu.Lock()
	defer m.checkMu.Unlock()
	st := m.checkOne(ctx, uid)
	m.recomputeShared()
	m.saveRuntime()
	return st, nil
}

// checkOne 单个账号的完整校验（调用方持 checkMu）。
func (m *AccountProxy) checkOne(ctx context.Context, uid string) AccountProxyStatus {
	b := m.bindingOf(uid)
	if b == nil {
		return AccountProxyStatus{UID: uid, State: proxyStateUnchecked.String()}
	}
	if !b.spec.Enabled {
		b.state.Store(int32(proxyStateDisabled))
		return b.toStatus(m)
	}
	pctx, cancel := context.WithTimeout(ctx, m.opts.ProbeTimeout)
	defer cancel()

	// ① 走代理实测出口
	p := probeEgress(pctx, b.entry.http, m.opts.Quorum)
	b.latency.Store(p.LatencyMS)
	if p.XFF != "" {
		b.leak.Store(p.XFF)
	}
	if p.CountryCode != "" {
		b.country.Store(p.Country)
		b.countryC.Store(p.CountryCode)
		b.tz.Store(p.Timezone)
		b.asn.Store(p.ASN)
	}
	b.checked.Store(time.Now().UnixMilli())

	switch {
	case p.IP == "":
		if strings.Contains(p.Error, "不一致") {
			// 多源各不相同 = 轮换池在校验窗口内换了出口。轮换是聚合代理的
			// 正常行为，不判死：取任一实测源作为「当前出口」，状态仍 ok，
			// msg 说明轮换事实。真正的失败只有两种：连不上（unreachable）、
			// 出口=直连（not_effective）。
			fallbackIP := ""
			for _, v := range p.Sources {
				if v != "" {
					fallbackIP = v
					break
				}
			}
			if fallbackIP == "" {
				b.setState(proxyStateUnreachable, "出口探测失败：全部回显源无结果")
				b.entry.noteFail()
				b.probeIP.Store("")
				return b.toStatus(m)
			}
			b.probeIP.Store(fallbackIP)
			b.setState(proxyStateOK, "出口轮换中（多源不一致），当前实测 "+fallbackIP)
			b.lastOK.Store(time.Now().UnixMilli())
			return b.toStatus(m)
		}
		b.setState(proxyStateUnreachable, firstNonEmpty(p.Error, "出口探测失败"))
		b.entry.noteFail()
		b.probeIP.Store("")
		return b.toStatus(m)
	}
	b.probeIP.Store(p.IP)
	b.entry.noteSuccess()

	// ② 直连对照：代理到底有没有改变出口
	directIP := m.directIPCache.Load()
	if directIP == "" {
		dpctx, dcancel := context.WithTimeout(pctx, 15*time.Second)
		dp := probeEgress(dpctx, m.direct, m.opts.Quorum)
		dcancel()
		directIP = dp.IP
		if directIP != "" {
			m.directIPCache.Store(directIP)
		}
	}
	b.directIP.Store(directIP)
	if directIP != "" && p.IP == directIP {
		b.setState(proxyStateNotEffective,
			fmt.Sprintf("代理未生效：走代理与直连出口同为 %s（拨号器没接上 / 代理绕回本机）", p.IP))
		return b.toStatus(m)
	}

		// ③ 出口一致性验证。
	//
	// 语义：验证的是「调用确实走这条代理链路」，而**不是**「出口 IP 恒定不变」。
	// 聚合代理（resin 等）的出口会实时轮换，轮换是正常行为，绝不因漂移判
	// mismatch——那是把轮换池判死刑。
	//
	// 因此只有一种 mismatch：用户**显式填写**了 expected_ip 且实测与之不符
	//（固定 IP 商家场景：用户明知出口该是多少，填进来就是要求强校验）。
	// 未声明时：记录最新实测出口（lockedIP 字段名沿用，语义=最近一次实测值），
	// 状态一律 ok。
	prev := b.lockedIP.Load()
	b.lockedIP.Store(p.IP) // 每次校验都刷新为最新实测出口
	if prev != "" && prev != p.IP {
		log.Printf("[account-proxy] 出口轮换（正常）uid=%s 旧=%s 新=%s", logfmt.UID8(b.uid), prev, p.IP)
	}
	if declared := strings.TrimSpace(b.spec.ExpectedIP); declared != "" && p.IP != declared {
		b.setState(proxyStateMismatch,
			fmt.Sprintf("出口 IP 与声明不符：声明 %s，实测 %s", declared, p.IP))
		return b.toStatus(m)
	}

// ④ 来源泄漏
	if p.XFF != "" && !strings.Contains(p.XFF, p.IP) {
		b.setState(proxyStateLeak, fmt.Sprintf("代理注入了来源头（%s），真实来源可能随请求到达上游", truncate(p.XFF, 120)))
		return b.toStatus(m)
	}

	b.setState(proxyStateOK, "")
	b.lastOK.Store(time.Now().UnixMilli())
	return b.toStatus(m)
}

// setState 记录状态与说明。
func (b *accountBinding) setState(s proxyState, msg string) {
	b.state.Store(int32(s))
	b.msg.Store(msg)
}

// CallSampleInterval 调用链路出口的采样周期（固定值，不进配置——
// 验收口径：定时测一次即可，不需要频繁；每轮每个绑定只发 1 个单源请求）。
const CallSampleInterval = 5 * time.Minute

// SampleLoop 后台定时采样「调用链路出口」。
//
// 与校验（CheckAll：全源投票、CheckInterval 周期、产出 probeIP）分开：
// 本循环只做轻量单源采样、走与 chat 完全相同的 client（b.entry.chat），
// 产出的 callIP 与 probeIP 比对即「显示的出口与调用实际出口是否一致」。
// 轮换池出口变了 → callIP != probeIP → 面板如实显示「不一致」。
//
// interval 固定 5 分钟（CallSampleInterval）；首轮在启动后 15 秒跑一次，
// 让面板很快有 call_ip 可看，不用等第一个 tick。
func (m *AccountProxy) SampleLoop(ctx context.Context) {
	if m == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
			m.sampleAll(ctx)
		}
	}()
	t := time.NewTicker(CallSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.sampleAll(ctx)
		}
	}
}

// sampleAll 采样一轮：对每个启用的绑定，用它的 chat client 做单源出口探测。
func (m *AccountProxy) sampleAll(ctx context.Context) {
	type job struct {
		uid string
		b   *accountBinding
	}
	m.mu.RLock()
	jobs := make([]job, 0, len(m.bindings))
	for uid, b := range m.bindings {
		if b.spec.Enabled && b.entry != nil {
			jobs = append(jobs, job{uid, b})
		}
	}
	m.mu.RUnlock()
	if len(jobs) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(uid string, b *accountBinding) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			// b.entry.chat = 与 chat 调用同一 client（同代理、同拨号器、同连接池），
			// 采到的出口就是调用实际走的出口。
			if ip := probeEgressQuick(cctx, b.entry.chat); ip != "" {
				b.callIP.Store(ip)
				b.callAt.Store(time.Now().UnixMilli())
			}
		}(j.uid, j.b)
	}
	wg.Wait()
	m.saveRuntime() // 采样结果落 runtime 快照，重启不丢
}

// Run 后台周期校验循环（阻塞，main 里开 goroutine；interval<=0 时不循环）。// Run 后台周期校验循环（阻塞，main 里开 goroutine；interval<=0 时不循环）。
func (m *AccountProxy) Run(ctx context.Context) {
	if m == nil || m.opts.CheckInterval <= 0 {
		return
	}
	t := time.NewTicker(m.opts.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sts := m.CheckAll(ctx)
			for _, s := range sts {
				if s.State == proxyStateOK.String() || s.State == proxyStateDisabled.String() {
					continue
				}
				logProxyWarn(s)
			}
		}
	}
}

// logProxyWarn 失配告警（UID 走 logfmt.UID8 短码，代理链接只出现在 Message 之外）。
func logProxyWarn(s AccountProxyStatus) {
	log.Printf("[account-proxy] 出口异常: uid=%s state=%s desc=%s actual=%s declared=%s direct=%s",
		logfmt.UID8(s.UID), s.State, s.Message, s.ActualIP, s.ExpectedIP, s.DirectIP)
}

// ProbeDirect 探测本机直连出口（IP + 地区 + ASN）并缓存。
//
// 为什么必须独立于「账号代理」存在：**未绑代理的账号出口就是本机直连出口**，
// 面板账号池同样要显示它的 IP 与地区。原先只在 CheckAll（有绑定才跑）里顺带
// 探测，导致没绑代理的账号永远拿不到出口信息。
//
// 返回 true 表示本次探测成功（IP 非空）。
func (m *AccountProxy) ProbeDirect(ctx context.Context) bool {
	if m == nil {
		return false
	}
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	dp := probeEgress(dctx, m.direct, m.opts.Quorum)
	if dp.IP == "" {
		return false
	}
	m.directIPCache.Store(dp.IP)
	m.directCountry.Store(dp.Country)
	m.directCountryC.Store(dp.CountryCode)
	m.directASN.Store(dp.ASN)
	m.directChecked.Store(time.Now().UnixMilli())
	return true
}

// refreshDirect 兼容既有调用点（CheckAll 内每轮扫一次），语义同 ProbeDirect。
func (m *AccountProxy) refreshDirect(ctx context.Context) {
	_ = m.ProbeDirect(ctx)
}

// DirectEgress 返回本机直连出口的当前缓存（IP/地区/ASN/探测时刻）。
// IP 为空表示尚未探测成功（调用方应显示「探测中」而非空白）。
func (m *AccountProxy) DirectEgress() (ip, country, cc, asn string, at int64) {
	if m == nil {
		return "", "", "", "", 0
	}
	return m.directIPCache.Load(), m.directCountry.Load(), m.directCountryC.Load(),
		m.directASN.Load(), m.directChecked.Load()
}

// ---------------------------------------------------------------------------
// 状态输出
// ---------------------------------------------------------------------------

// Statuses 返回全部账号代理状态（含未配置代理的账号由调用方补）。
func (m *AccountProxy) Statuses() []AccountProxyStatus {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	uids := make([]string, 0, len(m.bindings))
	for uid := range m.bindings {
		uids = append(uids, uid)
	}
	m.mu.RUnlock()
	sort.Strings(uids)
	out := make([]AccountProxyStatus, 0, len(uids))
	for _, uid := range uids {
		if b := m.bindingOf(uid); b != nil {
			out = append(out, b.toStatus(m))
		}
	}
	return out
}

// Status 单账号状态。
func (m *AccountProxy) Status(uid string) (AccountProxyStatus, bool) {
	if b := m.bindingOf(uid); b != nil {
		return b.toStatus(m), true
	}
	return AccountProxyStatus{}, false
}

// toStatus 由运行态构造对外状态。
func (b *accountBinding) toStatus(m *AccountProxy) AccountProxyStatus {
	s := AccountProxyStatus{
		UID:        b.uid,
		Enabled:    b.spec.Enabled,
		Label:      b.spec.Label,
		Note:       b.spec.Note,
		ExpectedIP: strings.TrimSpace(b.spec.ExpectedIP),
		ActualIP:   b.probeIP.Load(),
		DirectIP:   b.directIP.Load(),
		Country:    b.country.Load(),
		CountryCode: b.countryC.Load(),
		Timezone:   b.tz.Load(),
		ASN:        b.asn.Load(),
		State:      proxyState(b.state.Load()).String(),
		Message:    b.msg.Load(),
		Leak:       b.leak.Load(),
		LatencyMS:  b.latency.Load(),
		CheckedAt:  b.checked.Load(),
	}
	if u, err := url.Parse(b.spec.Proxy); err == nil {
		s.Scheme = strings.ToLower(u.Scheme)
		if u.Host != "" {
			s.ProxyHost = u.Host
		}
		if u.User != nil {
			user := u.User.Username()
			masked := b.spec.Proxy
			if user != "" {
				masked = strings.Replace(masked, user+"@", user+"@" /* 保留用户名便于识别 */, 1)
			}
			// 密码必须脱敏（面板/日志都会读这个值）。
			if pw, ok := u.User.Password(); ok && pw != "" {
				masked = strings.Replace(masked, ":"+pw+"@", ":***@", 1)
			}
			s.Proxy = masked
		} else {
			s.Proxy = b.spec.Proxy
		}
	}
	s.Effective = s.ActualIP != "" && s.DirectIP != "" && s.ActualIP != s.DirectIP
	s.Match = s.ActualIP != "" && s.ExpectedIP != "" && s.ActualIP == s.ExpectedIP
	if b.entry != nil {
		s.Fails = int(b.entry.fails.Load())
	}
	if n := m.sharedCount(s.ActualIP); n > 0 {
		s.SharedBy = n
	}
	s.SharedBlocked = b.sharedBlocked.Load()
	return s
}

// CountryCode 返回该账号当前出口 IP 的实测国家/地区码（两位大写）。
// 第二个返回值为 false 表示「不知道」（未绑代理 / 尚未校验 / 回显源没给 geo），
// 调用方必须回落既有行为，不能拿空串当 "CN"。
//
// 用途：出站 Accept-Language 与出口所在地对齐（见 headers.go）。
func (m *AccountProxy) CountryCode(uid string) (string, bool) {
	if m == nil {
		return "", false
	}
	b := m.bindingOf(uid)
	if b == nil || !b.spec.Enabled {
		return "", false
	}
	if cc := b.countryC.Load(); cc != "" {
		return cc, true
	}
	return "", false
}

// UsingProxy 报告该账号当前是否真的在走账号代理（供出站头决策：
// 走代理时不透传客户端 IP）。
func (m *AccountProxy) UsingProxy(uid string) bool {
	if m == nil {
		return false
	}
	_, ok := m.HTTPFor(uid)
	return ok
}

// EgressView 返回该账号出口信息的扁平视图（供 pool 注入给面板账号池列表）。
//
// 为什么不让 pool 直接读 AccountProxyStatus：那是本包的对外完整结构（含脱敏代理
// 链接、校验延迟等），而列表只需要「出口在哪、可不可信」几个字段。刻意在这里做
// 一次窄化转换，让 pool 侧的定义保持稳定，不受本包内部结构调整影响。
//
// 返回 nil 表示该账号没有启用中的账号级代理（面板显示「直连」）。
func (m *AccountProxy) EgressView(uid string) *EgressViewInfo {
	b := m.bindingOf(uid)
	if b == nil || !b.spec.Enabled {
		// 未绑代理：出口就是本机直连出口——IP 与地区照常显示，让列表一整列
		// 都能看出「这个号从哪出去」，不留空。一致性字段留空（没有代理链接
		// IP 可比），由前端按「未声明」呈现。
		ip, country, cc, asn, at := m.DirectEgress()
		return &EgressViewInfo{
			Source:      EgressSourceDirect,
			IP:          ip,
			Country:     country,
			CountryCode: cc,
			ASN:         asn,
			CheckedAt:   at,
			State:       "direct",
		}
	}
	v := &EgressViewInfo{
		Source:    EgressSourceProxy,
		IP:        b.probeIP.Load(),
		CallIP:    b.callIP.Load(),
		// Declared 只回显**显式声明**的出口 IP。lockedIP 现在的语义是「最近一次
		// 实测出口」（出口轮换池每次校验都会变），若当 declared 回显，前端会
		// 把「IP vs 自己」永远判成一致——假的一致比不一致更误导。
		Declared:  strings.TrimSpace(b.spec.ExpectedIP),
		DirectIP:  b.directIP.Load(),
		Country:   b.country.Load(),
		CountryCode: b.countryC.Load(),
		ASN:       b.asn.Load(),
		State:     proxyState(b.state.Load()).String(),
		Label:     b.spec.Label,
		CheckedAt: b.checked.Load(),
	}
	if u, err := url.Parse(b.spec.Proxy); err == nil {
		v.ProxyHost = u.Host
	}
	if n := m.sharedCount(v.IP); n > 0 {
		v.SharedBy = n
	}
	return v
}

// 出口来源标识。
const (
	// EgressSourceProxy 账号级代理出口。
	EgressSourceProxy = "proxy"
	// EgressSourceDirect 本机直连出口（未绑代理的账号）。
	EgressSourceDirect = "direct"
)

// EgressViewInfo 出口信息扁平视图（见 EgressView）。
type EgressViewInfo struct {
	// Source 出口来源：proxy（账号代理）/ direct（未绑，走本机直连）。
	Source string
	// CallIP 最近一次「调用时实测出口」（NoteCall 采样；空 = 尚未采样）。
	CallIP string
	IP          string
	Declared    string
	DirectIP    string
	Country     string
	CountryCode string
	ASN         string
	State       string
	ProxyHost   string
	Label       string
	SharedBy    int
	CheckedAt   int64
}

// NoteResult 由外部通报一次出站成败（供参考；proxyRT 已自统计）。
func (m *AccountProxy) NoteResult(uid string, ok bool) {
	b := m.bindingOf(uid)
	if b == nil || b.entry == nil {
		return
	}
	if ok {
		b.entry.noteSuccess()
	} else {
		b.entry.noteFail()
	}
}

// recomputeShared 统计每个出口 IP 上承载的账号数（同 IP 多号 = 风控高危信号），
// 并按 MaxPerIPAction 决定是只告警还是把超限账号隔离出选号池。
func (m *AccountProxy) recomputeShared() {
	tally := map[string]int{}
	byIP := map[string][]string{}
	m.mu.RLock()
	for uid, b := range m.bindings {
		if !b.spec.Enabled {
			continue
		}
		if ip := b.probeIP.Load(); ip != "" {
			tally[ip]++
			byIP[ip] = append(byIP[ip], uid)
		}
	}
	m.mu.RUnlock()
	cp := tally
	m.shared.Store(&cp)

	if m.opts.MaxPerIP <= 0 {
		return
	}
	// quarantine：每个超限 IP 上按 uid 排序保留前 MaxPerIP 个，其余隔离。
	// 保留规则必须确定（排序而非 map 遍历顺序），否则每次扫描隔离的账号都不一样，
	// 表现为「账号随机不可用」。
	blocked := map[string]bool{}
	for ip, uids := range byIP {
		if len(uids) <= m.opts.MaxPerIP {
			continue
		}
		sort.Strings(uids)
		for _, u := range uids[m.opts.MaxPerIP:] {
			blocked[u] = true
		}
		logProxyWarn(AccountProxyStatus{
			UID: "*", State: "shared_ip", ActualIP: ip,
			Message: fmt.Sprintf("出口 IP %s 承载 %d 个账号，超过上限 %d（%s：隔离 %d 个）",
				ip, len(uids), m.opts.MaxPerIP, m.opts.MaxPerIPAction, len(uids)-m.opts.MaxPerIP),
		})
	}
	if m.opts.MaxPerIPAction == MaxPerIPQuarantine {
		m.mu.RLock()
		for uid, b := range m.bindings {
			b.sharedBlocked.Store(blocked[uid])
		}
		m.mu.RUnlock()
		return
	}
	// warn：只展示，不隔离（先清掉可能由上一次 quarantine 配置留下的标记）。
	m.mu.RLock()
	for _, b := range m.bindings {
		b.sharedBlocked.Store(false)
	}
	m.mu.RUnlock()
}

// sharedCount 该 IP 上共有几个账号（含自己）。
func (m *AccountProxy) sharedCount(ip string) int {
	if ip == "" {
		return 0
	}
	if p := m.shared.Load(); p != nil {
		return (*p)[ip]
	}
	return 0
}

// ---------------------------------------------------------------------------
// 运行期快照持久化（重启后面板立刻看得见上次实测结论）
// ---------------------------------------------------------------------------

// saveRuntime 写 data/proxy_state.json。
func (m *AccountProxy) saveRuntime() {
	if m == nil || m.opts.StateFile == "" {
		return
	}
	m.mu.RLock()
	out := map[string]proxyRuntime{}
	for uid, b := range m.bindings {
		out[uid] = proxyRuntime{
			IP:          b.probeIP.Load(),
			ExpectedIP:  b.lockedIP.Load(),
			DirectIP:    b.directIP.Load(),
			Country:     b.country.Load(),
			CountryCode: b.countryC.Load(),
			Timezone:    b.tz.Load(),
			ASN:         b.asn.Load(),
			State:       proxyState(b.state.Load()).String(),
			Message:     b.msg.Load(),
			Leak:        b.leak.Load(),
			LatencyMS:   b.latency.Load(),
			CheckedAt:   b.checked.Load(),
			Fails:       int(b.entry.fails.Load()),
			CallIP:      b.callIP.Load(),
			CallAt:      b.callAt.Load(),
		}
	}
	m.mu.RUnlock()
	raw, err := json.MarshalIndent(map[string]any{"version": 1, "accounts": out}, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(m.opts.StateFile)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := m.opts.StateFile + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return
	}
	if err := os.Rename(tmp, m.opts.StateFile); err != nil {
		if data, rerr := os.ReadFile(tmp); rerr == nil {
			_ = os.WriteFile(m.opts.StateFile, data, 0o600)
		}
		_ = os.Remove(tmp)
	}
}

// loadRuntime 载入上次实测快照（仅用于展示，不参与放行判定）。
func (m *AccountProxy) loadRuntime() {
	if m == nil || m.opts.StateFile == "" {
		return
	}
	raw, err := os.ReadFile(m.opts.StateFile)
	if err != nil {
		return
	}
	var doc struct {
		Accounts map[string]proxyRuntime `json:"accounts"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return
	}
	m.mu.RLock()
	for uid, rt := range doc.Accounts {
		b := m.bindings[uid]
		if b == nil {
			continue
		}
		b.probeIP.Store(rt.IP)
		if rt.ExpectedIP != "" {
			b.lockedIP.Store(rt.ExpectedIP)
		}
		b.directIP.Store(rt.DirectIP)
		b.country.Store(rt.Country)
		b.countryC.Store(rt.CountryCode)
		b.tz.Store(rt.Timezone)
		b.asn.Store(rt.ASN)
		b.msg.Store(rt.Message)
		b.leak.Store(rt.Leak)
		b.latency.Store(rt.LatencyMS)
		b.checked.Store(rt.CheckedAt)
		b.callIP.Store(rt.CallIP)
		b.callAt.Store(rt.CallAt)
		switch rt.State {
		case proxyStateOK.String():
			b.state.Store(int32(proxyStateOK))
		case proxyStateMismatch.String():
			b.state.Store(int32(proxyStateMismatch))
		case proxyStateNotEffective.String():
			b.state.Store(int32(proxyStateNotEffective))
		case proxyStateRotating.String():
			b.state.Store(int32(proxyStateRotating))
		case proxyStateLeak.String():
			b.state.Store(int32(proxyStateLeak))
		case proxyStateDisabled.String():
			b.state.Store(int32(proxyStateDisabled))
		case proxyStateUnreachable.String():
			b.state.Store(int32(proxyStateUnreachable))
		default:
			b.state.Store(int32(proxyStateUnchecked))
		}
	}
	m.mu.RUnlock()
}
