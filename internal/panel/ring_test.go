package panel

import (
	"strings"
	"testing"
	"time"
)

func TestClassifyLine(t *testing.T) {
	cases := map[string]string{
		"| #001 | glm-5.2 | stream | 200 | uid=c8a3e793 | TTFB=120ms |": ChChat,
		"school c8a3e793: ★ 分享任务完成":                                     ChTask,
		"streak-bonus 5c162cc9: 🎊 新手礼包 +100c":                           ChTask,
		"blackcat c8a3e793: 完成 3 次夜间对话":                                 ChTask,
		"checkin 5c162cc9: 已签到":                                         ChTask,
		"panel: 任务动作 uid=x code=chat_5":                                 ChTask,
		"panel: 队列启动：6 项（并发 2）":                                         ChTask,
		"panel: revive uid=x":                                           ChSys,
		"workbuddy2api listening on :7863":                              ChSys,
		"scheduler: 余额后台刷新每 5m0s":                                       ChSys,
	}
	for line, want := range cases {
		if got := classifyLine(line); got != want {
			t.Errorf("classifyLine(%q)=%q want %q", line, got, want)
		}
	}
}

func TestRingWriteStripsTimestamp(t *testing.T) {
	r := NewRing(4)
	if _, err := r.Write([]byte("2026/09/14 00:12:34 school x: done\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("| #002 | glm | stream | 200 | ok |")); err != nil {
		t.Fatal(err)
	}
	es := r.Snapshot()
	if len(es) != 2 {
		t.Fatalf("entries=%d want 2", len(es))
	}
	if strings.HasPrefix(es[0].Text, "2026/") {
		t.Errorf("timestamp not stripped: %q", es[0].Text)
	}
	if es[0].Ch != ChTask || es[1].Ch != ChChat {
		t.Errorf("channels: %q %q", es[0].Ch, es[1].Ch)
	}
	if time.Since(es[0].TS) > 5*time.Second {
		t.Errorf("stale ts: %v", es[0].TS)
	}
}

// TestClassifyLevel 级别判定用**线上真实日志行**做样本——这些是排障时真正
// 会看到的行。chat 表格行的状态码列是最高频的报错信号（503/502），
// 必须正确定级为 err；tok=169 这类数字列不能被误当状态码。
func TestClassifyLevel(t *testing.T) {
	cases := []struct {
		ch, line, want string
	}{
		// chat 表格行：状态码列定级
		{ChChat, "| #016 | 19:52:38 | global:hy4-preview-f | stream | 200 | uid=26f5fd27 | TTFB=18373ms | tok=169 | 8.7tok/s | cr=0 | total=19.3s |", LvlInfo},
		{ChChat, "| #033 | 20:49:34 | cn:glm-5.3-flash | sync | 502 | uid=abb0a664 | TTFB=- | tok=- | -tok/s | cr=- | total=3.4s |", LvlErr},
		{ChChat, "| #009 | 20:46:49 | cn:glm-5.3-flash | sync | 503 | uid=- | TTFB=- | tok=- | -tok/s | cr=- | total=0.0s |", LvlErr},
		{ChChat, "| #001 | glm-5.2 | stream | 400 | uid=c8a3e793 | TTFB=120ms |", LvlWarn},
		{ChChat, "| #002 | glm | stream | 200 | ok |", LvlInfo},
		// 行首强信号
		{ChSys, "ERR: [upstream] chat_stream uid=26f5fd27 realm=global model=hy4: transport error: tls: failed to verify certificate: x509: certificate is not valid", LvlErr},
		{ChSys, "SECURITY: global 代理出口 resin-proxy.x:2268 疑似 TLS 劫持（证书校验失败），熔断 30m0s（第 1 次）", LvlErr},
		{ChSys, "panic: runtime error: index out of range", LvlErr},
		{ChSys, "WARN: 账号代理绑定表加载失败（出站不受影响）", LvlWarn},
		// 行内弱信号
		{ChSys, "[autobind] cn 出口 IP 104.28.156.209 与 cab8093b 冲突，换绑 uid=d98bf163", LvlWarn},
		{ChSys, "[autobind] cn 池内全部候选不可用，解绑回落直连 uid=cab8093b", LvlWarn},
		{ChSys, "[account-proxy] 出口异常: uid=* state=shared_ip desc=出口 IP 104.28.156.211 承载 2 个账号，超过上限 1", LvlWarn},
		{ChSys, "pool: state.json 落盘失败: permission denied", LvlErr},
		{ChSys, "[autobind] cn 自动绑定稳定出口 uid=d98bf163", LvlInfo},
		{ChSys, "[account-proxy] 出口轮换（正常）uid=26f5fd27 旧=3.120.109.26 新=83.217.9.140", LvlInfo},
		{ChSys, "[subpool] cn 订阅刷新完成: 0 个订阅源 → 13 条代理链接", LvlInfo},
		// task 频道
		{ChTask, "balance 26f5fd27-86e5-4089: Post \"https://www.workbuddy.ai/billing/meter/get-user-resource\": Too Many Requests", LvlWarn},
		{ChTask, "checkin 5c162cc9: 已签到", LvlInfo},
		{ChTask, "school c8a3e793: ★ 分享任务完成", LvlInfo},
	}
	for _, c := range cases {
		if got := classifyLevel(c.ch, c.line); got != c.want {
			t.Errorf("classifyLevel(%s, %q)=%q want %q", c.ch, c.line, got, c.want)
		}
	}
}

// TestRingChatQuota 对话流量再大也不能把 task/sys 的报错行挤出去。
// 线上事故（2026-10-02 全站死锁）里，500 行环在几十秒内就被对话行冲光，
// 排障时 sys 里已经找不到任何报错痕迹。
func TestRingChatQuota(t *testing.T) {
	r := NewRing(100) // chatCap = 60
	if r.chatCap != 60 {
		t.Fatalf("chatCap=%d want 60", r.chatCap)
	}
	for i := 0; i < 300; i++ {
		if _, err := r.Write([]byte("| #" + itoa(i) + " | m | stream | 200 | ok |")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Write([]byte("ERR: chat_stream uid=x: upstream error")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("checkin x: 已签到")); err != nil {
		t.Fatal(err)
	}
	// 再灌一批对话，报错行必须还活着
	for i := 0; i < 100; i++ {
		if _, err := r.Write([]byte("| #f" + itoa(i) + " | m | stream | 200 | ok |")); err != nil {
			t.Fatal(err)
		}
	}
	_, st := r.Filter(SnapshotOpts{})
	if st.ByChannel[ChChat] > r.chatCap {
		t.Errorf("chat 行数 %d 超过配额 %d", st.ByChannel[ChChat], r.chatCap)
	}
	if st.ByLevel[LvlErr] != 1 {
		t.Errorf("错误行被对话冲掉了：by_level=%v", st.ByLevel)
	}
	if st.ByChannel[ChTask] != 1 {
		t.Errorf("task 行被冲掉：by_channel=%v", st.ByChannel)
	}
	if st.LastErr == nil || !strings.Contains(st.LastErr.Text, "upstream error") {
		t.Errorf("last_error 应指向报错行，got %+v", st.LastErr)
	}
}

// TestRingFilter 过滤与统计口径：条目按条件过滤，stats 始终统计全环
// （否则「筛出 3 条 → 显示 3 个错误」是自欺数字）。
func TestRingFilter(t *testing.T) {
	r := NewRing(100)
	lines := []string{
		"| #1 | m | stream | 200 | ok |",
		"ERR: a failed",
		"WARN: cooling down",
		"panel: 任务动作 uid=x code=chat_5",
		"| #2 | m | sync | 503 | uid=- |",
	}
	for _, l := range lines {
		if _, err := r.Write([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}
	// 按级别过滤
	es, st := r.Filter(SnapshotOpts{Level: LvlErr})
	if len(es) != 2 {
		t.Fatalf("err 过滤应命中 2 条，got %d: %+v", len(es), es)
	}
	if st.Total != 5 || st.ByLevel[LvlErr] != 2 || st.ByLevel[LvlWarn] != 1 {
		t.Fatalf("stats 必须统计全环：total=%d by_level=%v", st.Total, st.ByLevel)
	}
	// 组合过滤：chat + err
	if es, _ := r.Filter(SnapshotOpts{Channel: ChChat, Level: LvlErr}); len(es) != 1 {
		t.Fatalf("chat+err 应命中 1 条，got %d", len(es))
	}
	// 子串（大小写不敏感）
	if es, _ := r.Filter(SnapshotOpts{Query: "FAILED"}); len(es) != 1 {
		t.Fatalf("大小写不敏感搜索应命中 1 条，got %d", len(es))
	}
	// limit 取最近 N 条
	if es, _ := r.Filter(SnapshotOpts{Limit: 2}); len(es) != 2 ||
		es[0].Text != "panel: 任务动作 uid=x code=chat_5" || es[1].Text != "| #2 | m | sync | 503 | uid=- |" {
		t.Fatalf("limit=2 应取最近 2 条，got %+v", es)
	}
	// 空查询字符串不得当作"过滤空文本"（全部命中）
	if es, _ := r.Filter(SnapshotOpts{Query: ""}); len(es) != 5 {
		t.Fatalf("空 query 应命中全部，got %d", len(es))
	}
}

// itoa 避免 fmt 依赖（测试内联的小工具）。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
