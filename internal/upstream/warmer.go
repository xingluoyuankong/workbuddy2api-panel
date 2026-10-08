// warmer.go 出站连接保温器：把「账号 → 代理隧道 → 上游 TLS 会话」维持在热态，
// 消除对话冷启动时 1.2~4s 的端到端 TLS 握手开销（2026-10-08 实测：
// resin 同机网关 TCP 9ms，但经隧道到 workbuddy.ai 的 TLS 握手 1.2~4s，
// 占 TTFB p50 4.9s 的大头；idleConnTimeout=30s，间隔一超连接即弃、重付握手）。
//
// 【为什么不打 ipify 等回显源没用】SampleLoop 的出口采样虽然 5 分钟一次，
// 但它连的是 api.ipify.org——与 workbuddy.ai 不共享连接池，对上游 TLS 会话
// 保温为零。保温必须打**上游同域**端点，连接池命中才有意义。
//
// 【保温目标的选择】workbuddy.ai 对未认证 GET / 也返回 200 首页（实测 curl
// 经完整代理链接 ttfb 2~4.8s code=200），轻量端点用 GET / 即可；打不出认证
// 调用（不消耗配额、不触发风控计数），唯一效果是 Transport 连接池里常驻一条
// 热隧道。cn 域打 https://copilot.tencent.com。
//
// 【节奏】25s 一轮（idleConnTimeout=30s 内），每账号独立 goroutine 随绑定
// 生死启停。请求 6s 超时，失败静默（保温失败不影响判定，真实校验在 CheckAll）。
package upstream

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// warmerKeepAlive 保温间隔：必须 < idleConnTimeout(30s)，否则连接先被池回收。
const warmerKeepAlive = 25 * time.Second

// warmerTimeout 单次保温请求上限。超时只说明这条隧道此刻不通，
// 下一轮自然重试；绝不能在这里换绑（保温器无任何绑定决策）。
const warmerTimeout = 6 * time.Second

// warmerWarmPath 保温端点。GET / 最轻、无副作用、200 即达（TLS+隧道全链路打通）。
const warmerWarmPath = "/"

// warmer 上游连接保温器。
type warmer struct {
	mu     sync.Mutex
	cancel map[string]context.CancelFunc // uid → 保温循环取消函数
}

// newWarmer 构造保温器。
func newWarmer(c *Client) *warmer {
	return &warmer{cancel: map[string]context.CancelFunc{}}
}

// Start 开始保温某账号（已有循环则跳过）。b.entry.chat 是与真实对话同一个
// *http.Client——同代理、同 Transport、同连接池，保温产生的热连接对话直接命中。
func (w *warmer) Start(uid, realm string, chat *http.Client) {
	if w == nil || chat == nil || uid == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.cancel[uid]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel[uid] = cancel
	go w.loop(ctx, uid, realm, chat)
}

// Stop 停止某账号的保温（解绑/禁用时调用）。
func (w *warmer) Stop(uid string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if cancel, ok := w.cancel[uid]; ok {
		cancel()
		delete(w.cancel, uid)
	}
}

// StopAll 停止全部（进程退出/测试收尾）。
func (w *warmer) StopAll() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cancel := range w.cancel {
		cancel()
	}
	w.cancel = map[string]context.CancelFunc{}
}

// loop 单账号保温循环：立即打一发热身（覆盖「绑定刚建好就来对话」的冷启动），
// 之后每 25s 一发，直到 ctx 取消。
func (w *warmer) loop(ctx context.Context, uid, realm string, chat *http.Client) {
	// 首发错开 0~3s：8 个账号同时预热会对同一代理网关瞬时开 8 条隧道。
	jitter := time.Duration(time.Now().UnixNano()%int64(3*time.Second))
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}
	w.ping(ctx, uid, realm, chat)
	t := time.NewTicker(warmerKeepAlive)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.ping(ctx, uid, realm, chat)
		}
	}
}

// warmBase 按 realm 返回保温目标：global 打 workbuddy.ai（对话实际上游），
// cn 打 copilot.tencent.com（与 client.go 的 ChatBaseCN 默认一致）。
func warmBase(realm string) string {
	if realm == "cn" {
		return "https://copilot.tencent.com"
	}
	return "https://www.workbuddy.ai"
}

// ping 打一发轻量 GET。全程静默：失败只计数不打日志（保温是尽力而为，
// 真链路问题由 CheckAll/巡检负责发现与处置）。
func (w *warmer) ping(ctx context.Context, uid, realm string, chat *http.Client) {
	pctx, cancel := context.WithTimeout(ctx, warmerTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, warmBase(realm)+warmerWarmPath, nil)
	if err != nil {
		return
	}
	// 保温只关心「握手+首包」，不读 body；禁压缩减解码开销。
	req.Header.Set("Accept", "text/html")
	resp, err := chat.Do(req)
	if err != nil {
		return
	}
	// 排干 body 让 keep-alive 连接回池复用（直接 Close 会把暖热的连接一起带走）。
	drainAndClose(resp)
}

// drainAndClose 排干 body 后关闭——保留 keep-alive 连接回池（保温的意义所在）。
func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	buf := make([]byte, 32*1024)
	for {
		if _, err := resp.Body.Read(buf); err != nil {
			break
		}
	}
	resp.Body.Close()
}
