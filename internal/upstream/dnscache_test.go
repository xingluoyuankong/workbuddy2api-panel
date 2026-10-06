package upstream

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// testListener 起一个本地 TCP 监听器，返回地址与关闭函数。
func testListener(t *testing.T) (addr string, close func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func testCachedDialer(lookup func(ctx context.Context, host string) ([]net.IP, error)) *cachedDialer {
	c := newCachedDialer(&net.Dialer{Timeout: 5 * time.Second})
	c.lookup = lookup
	return c
}

// 命中缓存后不再调 lookup：拨 fake 域名两次，lookup 只应被调用一次。
func TestCachedDialerHitSkipsLookup(t *testing.T) {
	srv, closeSrv := testListener(t)
	defer closeSrv()
	_, port, _ := net.SplitHostPort(srv)

	var calls int64
	c := testCachedDialer(func(ctx context.Context, host string) ([]net.IP, error) {
		atomic.AddInt64(&calls, 1)
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		conn, err := c.DialContext(ctx, "tcp", net.JoinHostPort("fake.invalid", port))
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conn.Close()
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("lookup called %d times, want 1 (second dial should hit cache)", got)
	}
}

// TTL 过期后重新解析。
func TestCachedDialerExpiry(t *testing.T) {
	old := dnsCacheTTL
	dnsCacheTTL = 50 * time.Millisecond
	defer func() { dnsCacheTTL = old }()

	srv, closeSrv := testListener(t)
	defer closeSrv()
	_, port, _ := net.SplitHostPort(srv)

	var calls int64
	c := testCachedDialer(func(ctx context.Context, host string) ([]net.IP, error) {
		atomic.AddInt64(&calls, 1)
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	})

	ctx := context.Background()
	addr := net.JoinHostPort("fake.invalid", port)
	conn, err := c.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	time.Sleep(80 * time.Millisecond) // 等缓存过期
	conn, err = c.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("lookup called %d times, want 2 (expired entry must re-resolve)", got)
	}
}

// 缓存 IP 建连失败 → 驱逐并用新鲜 DNS 重试。
func TestCachedDialerEvictOnDialFailure(t *testing.T) {
	srv, closeSrv := testListener(t)
	defer closeSrv()
	_, port, _ := net.SplitHostPort(srv)

	// 127.0.0.2 无监听：建连被拒，速度快
	ips := []net.IP{net.ParseIP("127.0.0.2")}
	var calls int64
	c := testCachedDialer(func(ctx context.Context, host string) ([]net.IP, error) {
		atomic.AddInt64(&calls, 1)
		return ips, nil
	})

	ctx := context.Background()
	addr := net.JoinHostPort("fake.invalid", port)
	if _, err := c.DialContext(ctx, "tcp", addr); err == nil {
		t.Fatal("expected dial failure with 127.0.0.2")
	}
	// 换成有效 IP，第二次拨号应走新鲜 DNS（lookup 第二次被调）且成功
	ips = []net.IP{net.ParseIP("127.0.0.1")}
	conn, err := c.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("retry after evict: %v", err)
	}
	conn.Close()
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("lookup called %d times, want 2 (failed cached IP must evict + re-resolve)", got)
	}
}

// flush 清空后重新解析；IP 字面量不走 lookup。
func TestCachedDialerFlushAndIPLiteral(t *testing.T) {
	srv, closeSrv := testListener(t)
	defer closeSrv()
	_, port, _ := net.SplitHostPort(srv)

	var calls int64
	c := testCachedDialer(func(ctx context.Context, host string) ([]net.IP, error) {
		atomic.AddInt64(&calls, 1)
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	})

	ctx := context.Background()
	addr := net.JoinHostPort("fake.invalid", port)
	for i := 0; i < 2; i++ {
		conn, err := c.DialContext(ctx, "tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	c.flush()
	conn, err := c.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("lookup called %d times, want 2 (flush must invalidate)", got)
	}

	// IP 字面量：不查 DNS
	conn, err = c.DialContext(ctx, "tcp", srv)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("lookup called %d times after IP literal dial, want still 2", got)
	}
}
