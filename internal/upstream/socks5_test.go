package upstream

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeSocks5 起一个最小 SOCKS5 服务端，记录收到的 CONNECT 目标，用于验证
// 客户端的握手字节序正确（这是最容易写错、且错了只表现为"连不上"的地方）。
func fakeSocks5(t *testing.T, reply byte) (addr string, got *string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	target := new(string)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var greet [3]byte
		if _, err := io.ReadFull(c, greet[:]); err != nil {
			return
		}
		// 只接受无认证
		if _, err := c.Write([]byte{0x05, 0x00}); err != nil {
			return
		}
		var head [5]byte
		if _, err := io.ReadFull(c, head[:]); err != nil {
			return
		}
		var host string
		if head[3] == 0x03 {
			buf := make([]byte, head[4])
			if _, err := io.ReadFull(c, buf); err != nil {
				return
			}
			host = string(buf)
		}
		var portBuf [2]byte
		if _, err := io.ReadFull(c, portBuf[:]); err != nil {
			return
		}
		*target = host + ":" + string(rune('0'+binary.BigEndian.Uint16(portBuf[:])/10000))
		if reply != 0x00 {
			_, _ = c.Write([]byte{0x05, reply, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			return
		}
		_, _ = c.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x1f, 0x90})
		// 保持连接直到测试结束
		_, _ = io.Copy(io.Discard, c)
	}()
	return ln.Addr().String(), target
}

func TestSocks5DialerHandshake(t *testing.T) {
	addr, got := fakeSocks5(t, 0x00)
	d := &socks5Dialer{addr: addr, forward: newDialer()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := d.DialContext(ctx, "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	// 域名必须以 ATYP=0x03 原样透传给代理（由出口侧解析 DNS）。
	if !strings.HasPrefix(*got, "example.com:") {
		t.Errorf("proxy saw %q, want example.com:*", *got)
	}
}

func TestSocks5DialerReject(t *testing.T) {
	addr, _ := fakeSocks5(t, 0x05) // 0x05 = connection refused
	d := &socks5Dialer{addr: addr, forward: newDialer()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := d.DialContext(ctx, "tcp", "example.com:443")
	if err == nil {
		t.Fatal("want error for rejected connect")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v, want readable REP text", err)
	}
}

func TestSocks5DialerBadAddr(t *testing.T) {
	d := &socks5Dialer{addr: "127.0.0.1:1", forward: newDialer()}
	if _, err := d.DialContext(context.Background(), "tcp", "no-port"); err == nil {
		t.Error("addr without port must error")
	}
	if _, err := d.DialContext(context.Background(), "udp", "h:1"); err == nil {
		t.Error("udp must error")
	}
}

func TestSetGlobalProxy(t *testing.T) {
	c := New()
	if c.GlobalProxyActive() {
		t.Error("no proxy by default")
	}
	// 非法 scheme 必须报错（否则会静默直连、global 域继续被 WAF 拦）
	if err := c.SetGlobalProxy("ftp://x:1"); err == nil {
		t.Error("unsupported scheme must error")
	}
	if err := c.SetGlobalProxy("socks5://nohost"); err == nil {
		t.Error("missing port must error")
	}
	if err := c.SetGlobalProxy("socks5://warp:1080"); err != nil {
		t.Fatalf("valid socks5 url rejected: %v", err)
	}
	if !c.GlobalProxyActive() {
		t.Error("proxy should be active")
	}
	// realm 分流：global 走代理 client，cn 走直连 client。
	if c.chatHTTPFor("global") == c.chatHTTP() {
		t.Error("global must use the proxied client")
	}
	if c.chatHTTPFor("cn") != c.chatHTTP() {
		t.Error("cn must keep the direct client")
	}
	if c.httpFor("global") != c.HTTPGlobal {
		t.Error("httpFor(global) must return the proxied client")
	}
	if c.httpFor("cn") != c.HTTP {
		t.Error("httpFor(cn) must return the direct client")
	}
	// 清除
	if err := c.SetGlobalProxy(""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if c.GlobalProxyActive() || c.chatHTTPFor("global") != c.chatHTTP() {
		t.Error("clearing proxy must fall back to direct")
	}
}
