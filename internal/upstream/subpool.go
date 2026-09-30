package upstream

import (
	"context"
	"io"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// SubPool 订阅链接池：每个 realm 一组订阅 URL，定时拉取解析出代理链接，
// 喂给出站池（SetRealmProxy）。realm 隔离——global 与 cn 的订阅互不互通。
//
// 安全模型（与既有 TLS 劫持熔断衔接）：
//   - 拉取出来的链接进入 realm 池后，健康度由 proxyRT/proxyEntry 追踪
//     （失败降权 / TLS 劫持长熔断），坏出口自动被 pickProxy 绕开。
//   - 订阅刷新是**替换**语义：新解析结果整体覆盖该 realm 旧池（含健康度清零，
//     因为出口列表可能已经变了，旧统计不再可信）。
type SubPool struct {
	mu sync.RWMutex

	subs map[string][]string // realm → 订阅 URL 列表
	file string              // data/subpool.json

	refreshedAt map[string]int64 // realm → 最近刷新 unix ms
	lastErr     map[string]string

	interval time.Duration
	client   *http.Client // 拉订阅用（直连即可，订阅地址一般可达）
	onChange func(realm, joined string) error
}

// subPoolDoc 落盘结构。
type subPoolDoc struct {
	Version int              `json:"version"`
	Realms  map[string][]string `json:"realms"` // realm → 订阅 URL 列表
}

// SubPoolStatus 面板展示用。
type SubPoolStatus struct {
	Realm       string   `json:"realm"`
	Subs        []string `json:"subs"`
	RefreshedAt int64    `json:"refreshed_at"`
	LastErr     string   `json:"last_error,omitempty"`
	Entries     int      `json:"entries"`
}

// NewSubPool 从文件加载订阅配置。interval<=0 用默认 6h。
func NewSubPool(file string, interval time.Duration, onChange func(realm, joined string) error) *SubPool {
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	m := &SubPool{
		subs:        map[string][]string{},
		file:        file,
		refreshedAt: map[string]int64{},
		lastErr:     map[string]string{},
		interval:    interval,
		client:      &http.Client{Timeout: 20 * time.Second},
		onChange:    onChange,
	}
	if b, err := os.ReadFile(file); err == nil {
		var doc subPoolDoc
		if json.Unmarshal(b, &doc) == nil && doc.Realms != nil {
			for r, urls := range doc.Realms {
				clean := make([]string, 0, len(urls))
				for _, u := range urls {
					if u = strings.TrimSpace(u); u != "" {
						clean = append(clean, u)
					}
				}
				if len(clean) > 0 {
					m.subs[r] = clean
				}
			}
		}
	}
	return m
}

// Subs 返回指定 realm 的订阅 URL 列表副本。
func (m *SubPool) Subs(realm string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := append([]string(nil), m.subs[realm]...)
	return out
}

// SetSubs 覆盖指定 realm 的订阅 URL 列表并落盘；立即触发一次刷新。
func (m *SubPool) SetSubs(realm string, urls []string) error {
	clean := make([]string, 0, len(urls))
	seen := map[string]bool{}
	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" || seen[u] {
			continue
		}
		if _, err := url.Parse(u); err != nil {
			return fmt.Errorf("订阅链接无效 %q: %w", u, err)
		}
		seen[u] = true
		clean = append(clean, u)
	}
	m.mu.Lock()
	if len(clean) == 0 {
		delete(m.subs, realm)
	} else {
		m.subs[realm] = clean
	}
	m.mu.Unlock()
	if err := m.save(); err != nil {
		return err
	}
	// 清空订阅 = 同时清空该 realm 的池（否则旧池残留，与新语义矛盾）。
	if len(clean) == 0 && m.onChange != nil {
		_ = m.onChange(realm, "")
		return nil
	}
	return m.Refresh(realm)
}

// Status 面板展示。
func (m *SubPool) Status() []SubPoolStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	realms := []string{"global", "cn"}
	out := make([]SubPoolStatus, 0, len(realms))
	for _, r := range realms {
		out = append(out, SubPoolStatus{
			Realm:       r,
			Subs:        append([]string(nil), m.subs[r]...),
			RefreshedAt: m.refreshedAt[r],
			LastErr:     m.lastErr[r],
			Entries:     int(m.refreshedAt[r+"|entries"]),
		})
	}
	return out
}

// save 落盘（不含敏感解析结果，只存 URL 列表）。
func (m *SubPool) save() error {
	m.mu.RLock()
	doc := subPoolDoc{Version: 1, Realms: map[string][]string{}}
	for r, urls := range m.subs {
		doc.Realms[r] = append([]string(nil), urls...)
	}
	m.mu.RUnlock()
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(m.file, b, 0o600)
}

// Refresh 拉取并应用指定 realm 的所有订阅（失败订阅跳过，成功的合并生效）。
// 全部失败时保留旧池不动（可用性优先）。
func (m *SubPool) Refresh(realm string) error {
	m.mu.RLock()
	urls := append([]string(nil), m.subs[realm]...)
	m.mu.RUnlock()
	if len(urls) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 输入行分类（用户两种粘贴方式都支持）：
	//   直接代理链接（socks5(h)://... 或带凭据的 http(s)://user:pass@host）→ 直接入池；
	//   订阅地址（无凭据的 http(s)://...）→ HTTP 拉取后解析。
	// 区分依据：代理链接带 userinfo（@），订阅地址不带；socks scheme 无法 GET，
	// 必是代理链接。这样 resin 线路链接（http://Link...@host:port）可以直接粘贴。
	seen := map[string]bool{}
	var proxies []string
	var subURLs []string
	for _, line := range urls {
		if isDirectProxyLine(line) {
			if !seen[line] {
				seen[line] = true
				proxies = append(proxies, line)
			}
			continue
		}
		subURLs = append(subURLs, line)
	}
	var errs []string
	okSubs := 0
	for _, u := range subURLs {
		lines, err := m.fetchOne(ctx, u)
		if err != nil {
			errs = append(errs, shortSubURL(u)+": "+err.Error())
			continue
		}
		okSubs++
		for _, line := range lines {
			if !seen[line] {
				seen[line] = true
				proxies = append(proxies, line)
			}
		}
	}

	m.mu.Lock()
	m.refreshedAt[realm] = time.Now().UnixMilli()
	if len(errs) > 0 {
		m.lastErr[realm] = strings.Join(errs, " | ")
	} else {
		delete(m.lastErr, realm)
	}
	m.mu.Unlock()
	// 条目数记录在 errors map 旁（Status 里展示实际生效的池条数）
	if len(proxies) > 0 {
		m.mu.Lock()
		m.refreshedAt[realm+"|entries"] = int64(len(proxies))
		m.mu.Unlock()
	}

	if okSubs == 0 && len(proxies) == 0 {
		return errors.New("订阅拉取全部失败（直连代理行也没有）: " + strings.Join(errs, " | "))
	}
	if len(proxies) == 0 {
		return errors.New("订阅里没有可用的 http/socks 代理链接（vmess/vless 等暂不支持），保留旧池")
	}
	if m.onChange != nil {
		if err := m.onChange(realm, strings.Join(proxies, ",")); err != nil {
			return err
		}
	}
	log.Printf("[subpool] %s 订阅刷新完成: %d 个订阅源 → %d 条代理链接", realm, okSubs, len(proxies))
	return nil
}

// fetchOne 拉取单个订阅并解析出代理链接列表。
// 支持两种格式（自动识别）：
//  1. base64（v2ray 订阅标准形态）
//  2. 纯文本行（每行一条代理 URL）
// 只接受网关能用 scheme：http/https/socks5/socks5h；vmess/vless 等跳过并计数。
func (m *SubPool) fetchOne(ctx context.Context, subURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, subURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	raw, err := readAllLimited(resp.Body, 1<<20)
	if err != nil {
		return nil, err
	}
	text := strings.TrimSpace(string(raw))
	// 尝试 base64（标准/URL-safe，去 padding 宽容处理）
	if !strings.Contains(text, "://") {
		if dec, err := base64.StdEncoding.DecodeString(text); err == nil {
			text = string(dec)
		} else if dec, err := base64.RawStdEncoding.DecodeString(text); err == nil {
			text = string(dec)
		} else if dec, err := base64.URLEncoding.DecodeString(text); err == nil {
			text = string(dec)
		} else if dec, err := base64.RawURLEncoding.DecodeString(text); err == nil {
			text = string(dec)
		}
	}
	var out []string
	skipped := 0
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		schemeEnd := strings.Index(line, "://")
		if schemeEnd <= 0 {
			continue
		}
		scheme := strings.ToLower(line[:schemeEnd])
		switch scheme {
		case "http", "https", "socks5", "socks5h":
			out = append(out, line)
		default:
			skipped++ // vmess/vless/trojan 等暂不支持
		}
	}
	if skipped > 0 {
		log.Printf("[subpool] %s 跳过 %d 条不支持的链接（vmess/vless 等非 http/socks 形态）", shortSubURL(subURL), skipped)
	}
	if len(out) == 0 {
		return nil, errors.New("未解析出 http/socks 代理链接")
	}
	return out, nil
}

// Run 定时刷新循环（阻塞；main 里开 goroutine）。
// 启动后 10s 先刷一轮，之后按 interval 周期刷新。
func (m *SubPool) Run(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			for realm := range m.allRealms() {
				if err := m.Refresh(realm); err != nil {
					log.Printf("[subpool] %s 首轮刷新失败: %v", realm, err)
				}
			}
		}
	}()
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for realm := range m.allRealms() {
				if err := m.Refresh(realm); err != nil {
					log.Printf("[subpool] %s 定时刷新失败: %v", realm, err)
				}
			}
		}
	}
}

// allRealms 当前配置了订阅的 realm 集合。
func (m *SubPool) allRealms() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]bool{}
	for r, urls := range m.subs {
		if len(urls) > 0 {
			out[r] = true
		}
	}
	return out
}

// isDirectProxyLine 判断一行输入是否本身就是代理链接（而非订阅地址）。
//   - socks5/socks5h scheme：HTTP GET 无法访问，必是代理链接；
//   - http/https 且带 userinfo（@ 在第一个 / 之前）：代理链接带凭据，订阅地址不带。
func isDirectProxyLine(line string) bool {
	i := strings.Index(line, "://")
	if i <= 0 {
		return false
	}
	switch strings.ToLower(line[:i]) {
	case "socks5", "socks5h":
		return true
	case "http", "https":
		rest := line[i+3:]
		at := strings.Index(rest, "@")
		slash := strings.Index(rest, "/")
		return at >= 0 && (slash < 0 || at < slash)
	}
	return false
}

// readAllLimited 限量读 body（防超大响应吃内存）。
func readAllLimited(r interface{ Read([]byte) (int, error) }, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, limit))
}

// shortSubURL 订阅地址脱敏（query 里的 token 不进日志）。
func shortSubURL(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return "(bad url)"
	}
	return pu.Host + pu.Path
}
