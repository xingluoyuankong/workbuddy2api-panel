// socks5.go 零依赖 SOCKS5 出站拨号（RFC 1928）。
//
// 为什么需要：国际版（global）上游对网关**出口 IP** 做 WAF 风控，10 秒内两个
// 不同账号都撞 403 WAF 拦截页即判定 IP 级封锁，整片请求 503——换账号完全无效
// （同 IP）。解法是让 global 域走代理出口（本机 warp/gost 提供 SOCKS5）。
//
// 为什么不引 golang.org/x/net/proxy：本项目出站依赖只有 redis 与 x/sys，
// 为一个 60 行的握手协议引模块不划算；而且标准库 http.Transport 的 Proxy
// 只认 http/https 代理，socks5:// 必须自建 DialContext 才能接。
package upstream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// socks5HandshakeTimeout 握手阶段（方法协商 + CONNECT）的总上限。
// 只约束握手，建立后清除 deadline——长流式响应不受影响。
const socks5HandshakeTimeout = 10 * time.Second

// socks5Dialer 通过 SOCKS5 代理建立 TCP 连接。
type socks5Dialer struct {
	addr    string // 代理地址 host:port
	user    string // 空 = 无认证
	pass    string
	// forward 建连到代理用的拨号器（复用网关统一的 10s 建连上限 + keepalive）。
	forward *net.Dialer
}

// DialContext 实现 ContextDialer，可直接挂到 http.Transport.DialContext。
//
// 目标地址一律用**域名形式**发给代理（ATYP=0x03），由代理侧解析 DNS：
// 网关所在机器的 DNS 可能被污染/被墙，交给出口侧解析更可靠。
func (d *socks5Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("socks5: unsupported network %q", network)
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("socks5: bad addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("socks5: bad port %q", portStr)
	}

	c, err := d.forward.DialContext(ctx, "tcp", d.addr)
	if err != nil {
		return nil, fmt.Errorf("socks5: dial proxy %s: %w", d.addr, err)
	}
	// 握手必须有独立 deadline：ctx 的 deadline 只覆盖到「连上代理」，
	// 代理不回包时会让请求挂死到 http.Client 的 Timeout（可能是 0=无限）。
	deadline := time.Now().Add(socks5HandshakeTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = c.SetDeadline(deadline)

	if err := d.handshake(c, host, port); err != nil {
		_ = c.Close()
		return nil, err
	}
	_ = c.SetDeadline(time.Time{}) // 握手完成，解除——流式响应可能持续很久
	return c, nil
}

// handshake 完成方法协商 + 认证 + CONNECT。
func (d *socks5Dialer) handshake(c net.Conn, host string, port int) error {
	// ── 1) 方法协商 ────────────────────────────────────────────────
	methods := []byte{0x00} // 0x00 = 无需认证
	if d.user != "" {
		methods = append(methods, 0x02) // 0x02 = 用户名/密码
	}
	req := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := c.Write(req); err != nil {
		return fmt.Errorf("socks5: write greeting: %w", err)
	}
	var greet [2]byte
	if _, err := io.ReadFull(c, greet[:]); err != nil {
		return fmt.Errorf("socks5: read greeting: %w", err)
	}
	if greet[0] != 0x05 {
		return fmt.Errorf("socks5: bad version 0x%02x in greeting", greet[0])
	}
	switch greet[1] {
	case 0x00:
		// 代理接受无认证
	case 0x02:
		if err := d.auth(c); err != nil {
			return err
		}
	case 0xFF:
		return errors.New("socks5: proxy rejected all offered auth methods")
	default:
		return fmt.Errorf("socks5: unsupported auth method 0x%02x", greet[1])
	}

	// ── 2) CONNECT（域名形式）─────────────────────────────────────
	hb := []byte(host)
	if len(hb) == 0 || len(hb) > 255 {
		return fmt.Errorf("socks5: host length %d out of range", len(hb))
	}
	req = []byte{0x05, 0x01, 0x00, 0x03, byte(len(hb))}
	req = append(req, hb...)
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := c.Write(req); err != nil {
		return fmt.Errorf("socks5: write connect: %w", err)
	}

	// ── 3) 应答：VER REP RSV ATYP BND.ADDR BND.PORT ───────────────
	var head [4]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return fmt.Errorf("socks5: read connect reply: %w", err)
	}
	if head[0] != 0x05 {
		return fmt.Errorf("socks5: bad version 0x%02x in connect reply", head[0])
	}
	if head[1] != 0x00 {
		return fmt.Errorf("socks5: connect rejected: %s", socks5ErrText(head[1]))
	}
	var skip int
	switch head[3] {
	case 0x01:
		skip = 4 + 2 // IPv4 + port
	case 0x03:
		var l [1]byte
		if _, err := io.ReadFull(c, l[:]); err != nil {
			return fmt.Errorf("socks5: read bound addr len: %w", err)
		}
		skip = int(l[0]) + 2
	case 0x04:
		skip = 16 + 2 // IPv6 + port
	default:
		return fmt.Errorf("socks5: bad ATYP 0x%02x in connect reply", head[3])
	}
	if _, err := io.CopyN(io.Discard, c, int64(skip)); err != nil {
		return fmt.Errorf("socks5: read bound addr: %w", err)
	}
	return nil
}

// auth 用户名/密码认证（RFC 1929）。
func (d *socks5Dialer) auth(c net.Conn) error {
	if len(d.user) > 255 || len(d.pass) > 255 {
		return errors.New("socks5: credential too long")
	}
	req := []byte{0x01, byte(len(d.user))}
	req = append(req, d.user...)
	req = append(req, byte(len(d.pass)))
	req = append(req, d.pass...)
	if _, err := c.Write(req); err != nil {
		return fmt.Errorf("socks5: write auth: %w", err)
	}
	var resp [2]byte
	if _, err := io.ReadFull(c, resp[:]); err != nil {
		return fmt.Errorf("socks5: read auth reply: %w", err)
	}
	if resp[1] != 0x00 {
		return errors.New("socks5: authentication failed")
	}
	return nil
}

// socks5ErrText REP 字段的人类可读文案（排障用）。
func socks5ErrText(code byte) string {
	switch code {
	case 1:
		return "general SOCKS server failure"
	case 2:
		return "connection not allowed by ruleset"
	case 3:
		return "network unreachable"
	case 4:
		return "host unreachable"
	case 5:
		return "connection refused"
	case 6:
		return "TTL expired"
	case 7:
		return "command not supported"
	case 8:
		return "address type not supported"
	default:
		return "unknown code " + strconv.Itoa(int(code))
	}
}
