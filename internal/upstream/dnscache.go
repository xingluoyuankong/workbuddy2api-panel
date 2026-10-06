// dnscache.go 出站 DNS 结果缓存。
//
// 背景：网关每个上游请求都新建一条代理 CONNECT 隧道（一请求一连接，无复用），
// 而 Go 的 net.Dialer 每次拨号都重新做 DNS 查询。实测代理域名单次查询
// 220ms~2.5s，全部落在 TTFB 关键路径上。
//
// 本文件在 net.Dialer 外包一层按主机名的 DNS 缓存：
//   - 命中且未过期 → 直接拨缓存 IP，跳过 DNS；
//   - 未命中/过期 → 正常解析并回填缓存；
//   - 缓存 IP 建连失败 → 驱逐该条目，用新鲜 DNS 重试一次（防钉死已失效 IP）。
//
// 缓存实例与代理条目（proxyEntry）/ Transport 同生命周期：
// 代理链接不变 → 同一实例 → DNS 一直复用；
// 代理链接变更 → 建新实例 → 新缓存 → 自然走新鲜 DNS。
// dnsCacheTTL 是兜底：域名真实换 IP 时至多延迟一个 TTL 生效。
//
// TLS 安全说明：http.Transport 的 SNI/证书校验取自请求 URL 的主机名，
// 与 DialContext 实际拨的 IP 无关，故拨缓存 IP 不影响 TLS。
package upstream

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// dnsCacheTTL DNS 缓存有效期（兜底，见包注释）。var 而非 const，供测试改写。
var dnsCacheTTL = 10 * time.Minute

type dnsCacheEntry struct {
	ips     []net.IP
	expires time.Time
}

// cachedDialer 带 DNS 缓存的拨号器，DialContext 签名与 *net.Dialer 兼容，
// 可直接挂到 http.Transport.DialContext 或作为 socks5Dialer.forward。
type cachedDialer struct {
	d      *net.Dialer
	lookup func(ctx context.Context, host string) ([]net.IP, error)

	mu    sync.RWMutex
	cache map[string]*dnsCacheEntry
}

// newCachedDialer 构造缓存拨号器，实际解析走 d 自带的 Resolver
// （d.Resolver 为 nil 时用系统默认解析器）。
func newCachedDialer(d *net.Dialer) *cachedDialer {
	c := &cachedDialer{d: d, cache: make(map[string]*dnsCacheEntry)}
	c.lookup = func(ctx context.Context, host string) ([]net.IP, error) {
		r := d.Resolver
		if r == nil {
			r = net.DefaultResolver
		}
		addrs, err := r.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		ips := make([]net.IP, 0, len(addrs))
		for _, a := range addrs {
			ips = append(ips, a.IP)
		}
		return ips, nil
	}
	return c
}

// flush 清空全部缓存。常规路径靠换链接即新实例天然隔离，
// 此方法供代理配置热变更等需要立即失效的场景手动调用。
func (c *cachedDialer) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*dnsCacheEntry)
}

func (c *cachedDialer) get(host string) []net.IP {
	c.mu.RLock()
	e, ok := c.cache[host]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expires) {
		return nil
	}
	return e.ips
}

func (c *cachedDialer) set(host string, ips []net.IP) {
	c.mu.Lock()
	c.cache[host] = &dnsCacheEntry{ips: ips, expires: time.Now().Add(dnsCacheTTL)}
	c.mu.Unlock()
}

func (c *cachedDialer) evict(host string) {
	c.mu.Lock()
	delete(c.cache, host)
	c.mu.Unlock()
}

// dialIPs 按序拨候选 IP；network 为 tcp4/tcp6 时过滤地址族。
func (c *cachedDialer) dialIPs(ctx context.Context, network, port string, ips []net.IP) (net.Conn, error) {
	var firstErr error
	for _, ip := range ips {
		if network == "tcp4" && ip.To4() == nil {
			continue
		}
		if network == "tcp6" && ip.To4() != nil {
			continue
		}
		conn, err := c.d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("dnscache: no suitable address to dial")
	}
	return nil, firstErr
}

// DialContext 实现 ContextDialer。IP 字面量直接透传（无 DNS 可省）。
func (c *cachedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return c.d.DialContext(ctx, network, addr)
	}
	if net.ParseIP(host) != nil {
		return c.d.DialContext(ctx, network, addr)
	}

	if ips := c.get(host); ips != nil {
		if conn, err := c.dialIPs(ctx, network, port, ips); err == nil {
			return conn, nil
		}
		c.evict(host) // 缓存 IP 全挂：驱逐，走新鲜 DNS 重试一次
	}

	ips, err := c.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	c.set(host, ips)
	return c.dialIPs(ctx, network, port, ips)
}
