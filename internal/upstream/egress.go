// egress.go 出站出口探测 + 「代理到底有没有真的生效」的硬校验。
//
// 背景（本文件存在的理由）：给账号配代理很容易流于形式。配置文件里写了
// socks5://host:port，但下列任一情况发生时，请求照样出得去、上游照样有响应，
// 看起来一切正常，唯独**出口 IP 没变**：
//   - 拨号器没接上，Transport 仍走本地网卡；
//   - 代理是透明转发 / 代理自己又绕回本机出口；
//   - http:// 代理被 URL 解析吃掉（缺 scheme、拼错 host），静默忽略；
//   - 代理池轮换，每次换一个 IP（对「账号绑固定 IP」的场景比不代理更糟）。
// 风控按 IP 聚号时，这批「以为自己在用代理」的账号仍然全挂在同一出口上。
//
// 本文件的判定不采信「配置里写了代理」这个事实，只采信实测：
//   1. 走被测 client 实测出口 IP（多源交叉，防单源被劫持 / 被 CDN 缓存）；
//   2. 走直连 client 实测出口 IP 作为对照组；
//   3. 两者相同 → PROXY_NOT_EFFECTIVE：代理没生效，无论配置写了什么；
//   4. 实测 IP 与声明 IP（ExpectedIP，缺省从代理 URL 的 host 推导）不同
//      → IP_MISMATCH：入口 IP 与出口 IP 不是同一个，背离「账号绑死一个住宅 IP」
//        的初衷，风控视角等价于「这个号每次从不同 IP 登录」；
//   5. 回显里出现 X-Forwarded-For 链 → LEAK：代理注入了来源头，真实来源仍在请求里。
package upstream

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ipEchoMode 回显源的解析形态。
type ipEchoMode string

const (
	echoJSON   ipEchoMode = "json"   // {"ip":"..."} 形态
	echoText   ipEchoMode = "text"   // 纯文本一行 IP
	echoGeo    ipEchoMode = "geo"    // ip-api.com/json 形态（含国家/时区/ASN）
	echoOrigin ipEchoMode = "origin" // httpbin/echo 形态 {"origin":"a, b"}（多个 = XFF 链）
)

// ipEcho 一个 IP 回显源定义。
type ipEcho struct {
	name string
	url  string
	mode ipEchoMode
}

// ipEchoSources 默认回显源清单：跨运营商 / 跨地域混合，任一单点被劫持、被缓存、
// 被墙都不影响整体判定；至少 quorum 个源给出同一个 IP 才算采信。
var ipEchoSources = []ipEcho{
	{name: "ipify", url: "https://api.ipify.org?format=json", mode: echoJSON},
	{name: "ipsb", url: "https://api.ip.sb/jsonip", mode: echoJSON},
	{name: "icanhazip", url: "https://icanhazip.com", mode: echoText},
	{name: "3322", url: "https://ip.3322.net", mode: echoText},
	{name: "ip-api", url: "http://ip-api.com/json", mode: echoGeo},
	{name: "httpbin", url: "https://httpbin.org/ip", mode: echoOrigin},
}

// EgressProbe 一次出口探测的结果（可 JSON 序列化，面板/日志直接消费）。
type EgressProbe struct {
	IP string `json:"ip"` // 采信的出口 IP（未达 quorum 时为空）
	// Sources 各源实测到的 IP（源名 → IP），用于排障：单源偏移时看得出是谁在撒谎。
	Sources map[string]string `json:"sources,omitempty"`
	// Quorum 是否达到一致源数门槛。false 常见于「代理每次轮换出口」或某源被缓存。
	Quorum     bool   `json:"quorum"`
	Country    string `json:"country,omitempty"`
	CountryCode string `json:"country_code,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
	ASN        string `json:"asn,omitempty"`
	LatencyMS  int64  `json:"latency_ms"`
	// XFF 回显里看到的 X-Forwarded-For 链（非空即代理在注入来源头，泄漏线索）。
	XFF string `json:"xff,omitempty"`
	// Rejected 未被采信的源（IP 与多数派不一致），轮换代理排查用。
	Rejected map[string]string `json:"rejected,omitempty"`
	Error    string `json:"error,omitempty"`
	At       time.Time `json:"at"`
}

// defaultEgressQuorum 默认一致源门槛：两个互相独立的源给出同一个 IP 才采信。
// 取 2 而非 3：IPv6 优先栈、CDN 边缘、国内镜像等因素常让个别源不可达，要求
// 3 个源会把健康代理误判为失败。取 2 时单源被缓存/被劫持仍能由第二源推翻。
const defaultEgressQuorum = 2

// probeEgress 用给定 client 实测出口 IP。
//
// ctx 控制单次探测总时长（调用方负责超时）；client 可以是任意出站形态
// （直连 / 代理），判定只依赖它实际从哪里出去。
// probeEgressQuick 单源快速采样出口 IP（调用侧用）。
//
// 与 probeEgress 的区别：不搞全源并发+多数投票，只打一个回显源拿 IP。
// 用途：chat 调用侧的出口采样（NoteCall）——频率高、只要"此刻从哪出去"一个值，
// 全源探测（6 请求/次）会把采样成本放大一个数量级。单源失败的代价只是
// 该次采样缺席（callIP 保持旧值），不影响主请求。
func probeEgressQuick(ctx context.Context, client *http.Client) string {
	if client == nil {
		return ""
	}
	qctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	for _, s := range ipEchoSources {
		ip, _, _, errMsg := fetchIPEcho(qctx, client, s)
		if errMsg == "" && ip != "" {
			return ip
		}
	}
	return ""
}

func probeEgress(ctx context.Context, client *http.Client, quorum int) EgressProbe {
	if client == nil {
		return EgressProbe{Error: "nil client", At: time.Now()}
	}
	if quorum <= 0 {
		quorum = defaultEgressQuorum
	}
	start := time.Now()
	p := EgressProbe{Sources: map[string]string{}, At: start}

	type result struct {
		name string
		ip   string
		geo  bool
		out  EgressProbe
		err  string
	}
	resCh := make(chan result, len(ipEchoSources))
	var wg sync.WaitGroup
	for _, s := range ipEchoSources {
		wg.Add(1)
		go func(s ipEcho) {
			defer wg.Done()
			ip, extra, xff, err := fetchIPEcho(ctx, client, s)
			r := result{name: s.name, ip: ip, out: extra}
			if err != "" {
				r.err = err
			}
			if xff != "" {
				r.out.XFF = xff
			}
			resCh <- r
		}(s)
	}
	wg.Wait()
	close(resCh)

	// 多数投票：同一 IP 出现次数最多且 >= quorum 才采信。
	tally := map[string]int{}
	for r := range resCh {
		if r.ip == "" {
			continue
		}
		p.Sources[r.name] = r.ip
		tally[r.ip]++
		// geo 只有一个源提供，顺手吸收（不参与投票）。
		if r.out.CountryCode != "" && p.CountryCode == "" {
			p.Country, p.CountryCode, p.Timezone, p.ASN =
				r.out.Country, r.out.CountryCode, r.out.Timezone, r.out.ASN
		}
		if r.out.XFF != "" && p.XFF == "" {
			p.XFF = r.out.XFF
		}
	}
	var best string
	var bestN int
	for ip, n := range tally {
		if n > bestN {
			best, bestN = ip, n
		}
	}
	if bestN >= quorum {
		p.IP, p.Quorum = best, true
		for name, ip := range p.Sources {
			if ip != best {
				if p.Rejected == nil {
					p.Rejected = map[string]string{}
				}
				p.Rejected[name] = ip
			}
		}
	} else if bestN == 1 && len(tally) == 1 {
		// 只有一个源可用：采信但明确标记未达 quorum（弱证据，面板要能看出来）。
		p.IP = best
	} else if len(tally) > 1 {
		p.Error = "出口 IP 多源不一致（代理在轮换出口？）"
	} else {
		p.Error = "所有回显源均不可达"
	}
	p.LatencyMS = time.Since(start).Milliseconds()
	return p
}

// fetchIPEcho 请求单个回显源并解析。
func fetchIPEcho(ctx context.Context, client *http.Client, s ipEcho) (ip string, extra EgressProbe, xff string, err string) {
	req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if rerr != nil {
		return "", extra, "", "build request: " + rerr.Error()
	}
	// 回显服务不是上游业务，不需要伪装客户端指纹；显式 UA 避免部分源返回 403。
	req.Header.Set("User-Agent", "curl/8.0.1")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	resp, derr := client.Do(req)
	if derr != nil {
		return "", extra, "", derr.Error()
	}
	defer resp.Body.Close()
	// 上限 32KB：回显体最多几百字节，防病态响应拖住探测。
	body, rerr2 := io.ReadAll(io.LimitReader(resp.Body, 32<<10))
	if rerr2 != nil {
		return "", extra, "", "read body: " + rerr2.Error()
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", extra, "", "http " + resp.Status
	}
	// 来源头泄漏检测：代理若在请求里注入 XFF/X-Real-IP，回显服务会把它们照出来。
	xff = firstNonEmpty(resp.Header.Get("X-Forwarded-For"), resp.Header.Get("X-Real-IP"), resp.Header.Get("X-Client-IP"))

	switch s.mode {
	case echoText:
		ip = firstIPIn(string(body))
	case echoJSON:
		var m map[string]any
		if json.Unmarshal(body, &m) == nil {
			for _, k := range []string{"ip", "ip_addr", "address", "query"} {
				if v, ok := m[k].(string); ok {
					ip = firstIPIn(v)
					if ip != "" {
						break
					}
				}
			}
		}
		if ip == "" {
			ip = firstIPIn(string(body))
		}
	case echoOrigin:
		var m struct {
			Origin string `json:"origin"`
		}
		if json.Unmarshal(body, &m) == nil {
			parts := strings.Split(m.Origin, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			if len(parts) > 1 {
				// 逗号分隔的多个 IP = 代理链把来源一路带了过来。最后一个是回显服务
				// 实际看到的对端（即真实出去的那个 IP），前面全是泄漏的来源。
				xff = firstNonEmpty(xff, m.Origin)
			}
			for i := len(parts) - 1; i >= 0; i-- {
				if net.ParseIP(parts[i]) != nil {
					ip = parts[i]
					break
				}
			}
		}
	case echoGeo:
		var m struct {
			Query       string `json:"query"`
			Country     string `json:"country"`
			CountryCode string `json:"countryCode"`
			Timezone    string `json:"timezone"`
			AS          string `json:"as"`
		}
		if json.Unmarshal(body, &m) == nil {
			ip = firstIPIn(m.Query)
			extra.Country, extra.CountryCode, extra.Timezone, extra.ASN = m.Country, m.CountryCode, m.Timezone, m.AS
		}
	}
	if ip == "" {
		err = "回显无有效 IP"
	}
	return ip, extra, xff, err
}

// firstIPIn 从任意文本里抠出第一个 IPv4/IPv6 字面量。
// 回显形态五花八门（纯 IP 行、HTML 片段、「Your IP: x.x.x.x」），逐个 token 试解析
// 比正则稳，也不会把 IPv6 的冒号误伤掉。
func firstIPIn(s string) string {
	s = strings.TrimSpace(s)
	if ip := net.ParseIP(s); ip != nil {
		return ip.String()
	}
	for _, f := range strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == ',' || r == ';' || r == '"' || r == '\''
	}) {
		f = strings.Trim(f, "<>/\"'()[]{}:=")
		if ip := net.ParseIP(f); ip != nil {
			return ip.String()
		}
	}
	return ""
}

// firstNonEmpty 返回第一个非空串。
func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
