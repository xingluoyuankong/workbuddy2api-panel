package session

import (
	"testing"
	"time"
)

// newTestRouter 构造一个注入了固定可用账号集的路由器（测试专用）。
func newTestRouter(uids ...string) *Router {
	r := New(Config{
		TTL:        time.Hour,
		GCInterval: time.Hour,
		Available:  func() []string { return uids },
	})
	return r
}

func TestListReturnsBindingsSortedByActivity(t *testing.T) {
	r := newTestRouter("u1", "u2")
	r.Bind("sess-a", "u1")
	time.Sleep(2 * time.Millisecond)
	r.Bind("sess-b", "u2")

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("list = %d, want 2", len(list))
	}
	// 最近活跃的排在前面
	if list[0].Key != "sess-b" {
		t.Errorf("first = %q, want sess-b (most recent)", list[0].Key)
	}
	if list[0].UID != "u2" || list[1].UID != "u1" {
		t.Errorf("uids = %q/%q, want u2/u1", list[0].UID, list[1].UID)
	}
	if list[0].Idle == "" {
		t.Error("idle duration should be rendered")
	}
}

func TestClearRemovesAllBindings(t *testing.T) {
	r := newTestRouter("u1")
	r.Bind("s1", "u1")
	r.Bind("s2", "u1")
	r.Bind("s3", "u1")
	if got := r.Count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
	n := r.Clear()
	if n != 3 {
		t.Errorf("Clear() = %d, want 3", n)
	}
	if got := r.Count(); got != 0 {
		t.Errorf("count after clear = %d, want 0", got)
	}
	// 清空后 Resolve 应能重新分配（而不是残留旧绑定）
	if uid, ok := r.Resolve("s1"); !ok || uid != "u1" {
		t.Errorf("Resolve after clear = (%q,%v), want (u1,true)", uid, ok)
	}
}

// TestClearRemovesFromStore 清空必须同时清 redisstore 镜像。
// 否则重启后 LoadFromStore 会把旧绑定恢复回来，用户会以为"重置没生效"。
func TestClearRemovesFromStore(t *testing.T) {
	store := &memStore{binds: map[string]string{}}
	r := New(Config{TTL: time.Hour, GCInterval: time.Hour, Store: store,
		Available: func() []string { return []string{"u1"} }})
	r.Bind("s1", "u1")
	if len(store.binds) != 1 {
		t.Fatalf("store binds = %d, want 1 after Bind", len(store.binds))
	}
	r.Clear()
	if len(store.binds) != 0 {
		t.Errorf("store binds = %d after Clear, want 0 (restart would restore them)", len(store.binds))
	}
}

func TestClearUIDOnlyUnbindsThatAccount(t *testing.T) {
	r := newTestRouter("u1", "u2")
	r.Bind("s1", "u1")
	r.Bind("s2", "u2")
	r.Bind("s3", "u1")

	n := r.ClearUID("u1")
	if n != 2 {
		t.Errorf("ClearUID(u1) = %d, want 2", n)
	}
	if got := r.Count(); got != 1 {
		t.Errorf("count = %d, want 1 (s2 remains)", got)
	}
	if _, ok := r.entries["s2"]; !ok {
		t.Error("s2 (bound to u2) must survive ClearUID(u1)")
	}
	if n := r.ClearUID(""); n != 0 {
		t.Errorf("ClearUID(\"\") = %d, want 0 (empty uid is a no-op)", n)
	}
}

func TestGCNowRemovesOnlyExpired(t *testing.T) {
	r := newTestRouter("u1")
	r.Bind("fresh", "u1")
	r.Bind("stale", "u1")
	// 把 stale 的最后活跃时间推到 TTL 之前
	r.mu.Lock()
	r.entries["stale"] = entry{uid: "u1", lastActive: time.Now().Add(-2 * time.Hour)}
	r.mu.Unlock()

	n := r.GCNow()
	if n != 1 {
		t.Errorf("GCNow() = %d, want 1 (only stale)", n)
	}
	if _, ok := r.entries["fresh"]; !ok {
		t.Error("fresh binding must survive GC")
	}
	if _, ok := r.entries["stale"]; ok {
		t.Error("stale binding must be removed")
	}
}

// memStore 记录绑定镜像的最小 redisstore 实现（验证 Clear 是否双清）。
type memStore struct {
	binds map[string]string
}

func (m *memStore) SetBind(key, uid string, ttl time.Duration) { m.binds[key] = uid }
func (m *memStore) DelBind(key string)                         { delete(m.binds, key) }
func (m *memStore) LoadBinds() map[string]string {
	out := map[string]string{}
	for k, v := range m.binds {
		out[k] = v
	}
	return out
}
func (m *memStore) SaveState(data []byte)     {}
func (m *memStore) LoadState() ([]byte, bool) { return nil, false }
func (m *memStore) Close() error              { return nil }
