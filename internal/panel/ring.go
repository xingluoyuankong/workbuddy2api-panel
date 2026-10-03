// ring.go 固定容量的结构化日志环形缓冲（并发安全，实现 io.Writer）。main 把
// log 包输出与 chat 表格日志经 MultiWriter 镜像进来，面板 /panel/api/logs 读取
// 过滤后的快照；超出容量的旧行按 FIFO 淘汰。
//
// 每行入环时做两件分类：
//   - 频道（chat=对话请求表格行 / task=任务动作 / sys=系统与其它）
//   - 级别（err / warn / info）——**入环时定级一次**，读取侧直接按级别过滤。
//     旧版在前端用 `error|失败|错误` 正则现判，正则弱、且每个订阅周期重复算；
//     服务端定级后还能覆盖 chat 表格行的状态码列（5xx=err / 4xx=warn），
//     这是前端正则做不到的——排障最高频的信号恰恰是 chat 行里的 503/502。
//
// 容量与配额：对话行是流量大头（每请求一行），不设限会把 sys/task 的报错全部
// 挤出去——线上实测高 QPS 时 500 行环只够几十秒。所以给 chat 一个硬上限
// （容量的 60%），保证 task/sys 至少占 40%；task/sys 之间不互限（它们才是
// 排障要看的），总量超限按 FIFO 淘汰最旧。
package panel

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

// 日志频道。
const (
	ChChat = "chat"
	ChTask = "task"
	ChSys  = "sys"
)

// 日志级别（入环时定级，读取侧按此过滤与着色）。
const (
	LvlInfo = "info"
	LvlWarn = "warn"
	LvlErr  = "err"
)

// LogEntry 单条日志（时间戳取写入时刻；log 包行的行首日期时间已被剥离）。
type LogEntry struct {
	TS   time.Time `json:"ts"`
	Ch   string    `json:"ch"`
	Lvl  string    `json:"lvl"`
	Text string    `json:"text"`
}

// taskPrefixes 任务动作日志的行首标识（scheduler 与 panel 的既有口径）。
var taskPrefixes = []string{
	"school ", "streak-bonus ", "travel ", "blackcat ", "lottery ",
	"checkin ", "activity ", "keepalive ", "balance ", "user-resource ",
	"panel: 任务", "panel: 一键", "panel: checkin", "panel: 手动",
	"panel: 队列", "panel: 开学季",
}

// tsPrefixRe log 包默认 flags（日期 时间）产生的行首时间戳。
var tsPrefixRe = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `)

// chatRowStatusRe chat 表格行的 HTTP 状态码列。
// 列数在两种形态间不一致（带/不带时间列），所以用惰性列匹配逐列推进，
// 命中"第一个纯 3 位数字列"——不能用 [^\n]* 贪心跨列，否则会捕到
// tok=169 的 169 这类数字列。
//
//	`| #016 | 19:52:38 | global:hy4 | stream | 200 | uid=... | TTFB=... | tok=169 | ...`
var chatRowStatusRe = regexp.MustCompile(`^\| #\d+ \|(?:\s*[^|]*\|)*?\s*(\d{3})\s*\|`)

// 级别判定信号。行首强信号（前缀）最可信，优先判定；行内弱信号按本项目
// 实际日志用语收敛。**宁缺勿滥**：把正常行误判成错误会淹没真正的报错，
// 比漏判一行错误危害大——所以像"异常"这类宽泛词只放 warn 不放 err。
var (
	errPrefixes  = []string{"ERR:", "ERROR:", "ERROR ", "SECURITY:", "panic:", "PANIC:", "FATAL:"}
	warnPrefixes = []string{"WARN:", "WARN "}
	errSubstr    = []string{
		"失败", "错误", "劫持", "无法",
		"x509", "certificate", "proxy error", "connection refused",
		"落盘失败", "session dead",
	}
	warnSubstr = []string{
		"冷却", "熔断", "降权", "降级", "重试", "异常", "不可用",
		"超上限", "超过上限", "回落", "skip ", "自动换绑", "冲突",
		"Too Many Requests",
	}
)

// classifyLine 按行首特征归类频道。
func classifyLine(line string) string {
	if strings.HasPrefix(line, "| #") { // chat 表格日志（server/logging.go logChatRow）
		return ChChat
	}
	for _, p := range taskPrefixes {
		if strings.HasPrefix(line, p) {
			return ChTask
		}
	}
	return ChSys
}

// classifyLevel 定级别。err 优先于 warn（一行同时含两类信号时按更严重算）。
func classifyLevel(ch, text string) string {
	for _, p := range errPrefixes {
		if strings.HasPrefix(text, p) {
			return LvlErr
		}
	}
	for _, p := range warnPrefixes {
		if strings.HasPrefix(text, p) {
			return LvlWarn
		}
	}
	// chat 表格行：状态码列是权威信号（5xx=err / 4xx=warn），不看文案——
	// 表格行里出现"失败/错误"字样的概率极低，反而 model 名可能带干扰词。
	if ch == ChChat {
		if m := chatRowStatusRe.FindStringSubmatch(text); m != nil {
			switch m[1][0] {
			case '5':
				return LvlErr
			case '4':
				return LvlWarn
			default:
				return LvlInfo
			}
		}
		return LvlInfo
	}
	for _, s := range errSubstr {
		if strings.Contains(text, s) {
			return LvlErr
		}
	}
	for _, s := range warnSubstr {
		if strings.Contains(text, s) {
			return LvlWarn
		}
	}
	return LvlInfo
}

// Ring 日志环形缓冲。
type Ring struct {
	mu      sync.Mutex
	entries []LogEntry
	cap     int
	chatCap int // chat 频道容量上限：对话流量再大也挤不掉 task/sys 的报错行
}

// NewRing 构建容量为 capacity 的日志环（非正值回退 2000；下限 100）。
func NewRing(capacity int) *Ring {
	if capacity <= 0 {
		capacity = 2000
	}
	if capacity < 100 {
		capacity = 100
	}
	return &Ring{cap: capacity, chatCap: capacity * 3 / 5}
}

// Write 按 \n 切分入环（实现 io.Writer）。空行丢弃；超容量淘汰最旧行。
func (r *Ring) Write(p []byte) (int, error) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(string(p), "\r\n"), "\n") {
		if line == "" {
			continue
		}
		text := tsPrefixRe.ReplaceAllString(line, "")
		ch := classifyLine(text)
		// chat 超配额：先挤出最旧的 chat 行再追加——保证报错行的存活窗口
		// 不受对话 QPS 影响（这是本环与"纯 FIFO 大数组"的核心差别）。
		if ch == ChChat && r.countLocked(ChChat) >= r.chatCap {
			r.evictOldestLocked(ChChat)
		}
		r.entries = append(r.entries, LogEntry{TS: now, Ch: ch, Lvl: classifyLevel(ch, text), Text: text})
		if overflow := len(r.entries) - r.cap; overflow > 0 {
			r.entries = r.entries[overflow:]
		}
	}
	return len(p), nil
}

// Snapshot 按写入顺序返回缓冲内全部条目（拷贝，调用方可安全持有）。
func (r *Ring) Snapshot() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LogEntry, len(r.entries))
	copy(out, r.entries)
	return out
}

// SnapshotOpts 读取过滤条件（零值 = 不过滤）。
type SnapshotOpts struct {
	Channel string // 频道；空 = 全部
	Level   string // 级别；空 = 全部
	Query   string // 子串（大小写不敏感）；空 = 不过滤
	Limit   int    // 只返回最近 N 条；<=0 = 不限
}

// Stats 环内概况。计数**始终统计全环**（不受过滤条件影响），面板头部
// 的徽标因此永远反映真实报错存量，而不是"当前筛出来的条数"。
type Stats struct {
	Cap       int            `json:"cap"`
	ChatCap   int            `json:"chat_cap"`
	Total     int            `json:"total"`
	ByChannel map[string]int `json:"by_channel"`
	ByLevel   map[string]int `json:"by_level"`
	LastErr   *LogEntry      `json:"last_error,omitempty"`
}

// Filter 返回过滤后的条目（按写入顺序、拷贝）与全环统计。
func (r *Ring) Filter(o SnapshotOpts) ([]LogEntry, Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()

	st := Stats{
		Cap:       r.cap,
		ChatCap:   r.chatCap,
		Total:     len(r.entries),
		ByChannel: map[string]int{},
		ByLevel:   map[string]int{},
	}
	q := strings.ToLower(o.Query)
	for i := range r.entries {
		e := &r.entries[i]
		st.ByChannel[e.Ch]++
		st.ByLevel[e.Lvl]++
		if e.Lvl == LvlErr {
			le := *e
			st.LastErr = &le
		}
	}
	out := make([]LogEntry, 0, len(r.entries))
	for i := range r.entries {
		e := &r.entries[i]
		if o.Channel != "" && e.Ch != o.Channel {
			continue
		}
		if o.Level != "" && e.Lvl != o.Level {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(e.Text), q) {
			continue
		}
		out = append(out, *e)
	}
	if o.Limit > 0 && len(out) > o.Limit {
		out = out[len(out)-o.Limit:]
	}
	return out, st
}

// countLocked 统计某频道当前条数（调用方必须已持锁）。
func (r *Ring) countLocked(ch string) int {
	n := 0
	for i := range r.entries {
		if r.entries[i].Ch == ch {
			n++
		}
	}
	return n
}

// evictOldestLocked 挤出最旧的一条指定频道日志（调用方必须已持锁）。
func (r *Ring) evictOldestLocked(ch string) {
	for i := range r.entries {
		if r.entries[i].Ch == ch {
			r.entries = append(r.entries[:i], r.entries[i+1:]...)
			return
		}
	}
}
