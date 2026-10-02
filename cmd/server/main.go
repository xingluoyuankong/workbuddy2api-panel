// main.go workbuddy2api 入口：加载配置、构建 pool、起调度器与 HTTP 服务。
package main

import (
	"sync"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/livecfg"
	"github.com/linguo2625469/workbuddy2api-panel/internal/modelmeta"
	"github.com/linguo2625469/workbuddy2api-panel/internal/panel"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
	"github.com/linguo2625469/workbuddy2api-panel/internal/redisstore"
	"github.com/linguo2625469/workbuddy2api-panel/internal/scheduler"
	"github.com/linguo2625469/workbuddy2api-panel/internal/server"
	"github.com/linguo2625469/workbuddy2api-panel/internal/session"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
	"github.com/linguo2625469/workbuddy2api-panel/internal/usage"
)

// appVersion 网关版本（fork 版：面板 + 任务体系），透出到 /panel/api/overview。
const appVersion = "1.10.0-panel"

// usagePathFor 由 state 文件路径推出用量文件路径：同目录、文件名 usage.json。
// 这样 config 里改 state_file 时用量数据跟着走，不需要额外配置项。
func usagePathFor(stateFile string) string { return stateSibling(stateFile, "usage.json") }

// stateSibling 返回与 state 文件同目录的指定文件名路径（相对路径场景回落当前目录）。
// usage.json（用量记录）与 output_probes.json（模型上限探测）共用本规则。
func stateSibling(stateFile, name string) string {
	dir := filepath.Dir(stateFile)
	if dir == "" || dir == "." {
		return name
	}
	return filepath.Join(dir, name)
}

func main() {
	cfgPath := flag.String("config", "config.json", "配置文件路径（默认当前目录 config.json；不存在时自动生成推荐配置）")
	flag.Parse()

	cfg, err := Load(*cfgPath)
	if err != nil {
		// errors.Is 才能看穿 Load 里 fmt.Errorf("%w") 的包装；os.IsNotExist 不行。
		if errors.Is(err, fs.ErrNotExist) {
			// 首次运行：目录下没有配置 → 自动落一份推荐配置（含随机 api_key）再加载。
			// 双击 exe / 裸跑 docker 即开，无需先手工复制样例。
			if key, werr := WriteDefault(*cfgPath); werr == nil {
				log.Printf("config %s 不存在，已生成推荐配置（api_key=%s，记录在该文件里，可自行修改）", *cfgPath, key)
				cfg, err = Load(*cfgPath)
			}
			if err != nil {
				// 生成失败（目录只读等）：退回纯默认 + env（旧行为兜底），不阻塞启动。
				log.Printf("config %s not found (auto-generate failed), using defaults+env: %v", *cfgPath, err)
				cfg, err = Load("")
			}
		}
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
	}

	auths, err := auth.LoadDir(cfg.AuthDir)
	if err != nil {
		log.Fatalf("load auths: %v", err)
	}
	log.Printf("loaded %d account(s) from %s", len(auths), cfg.AuthDir)

	// redisstore：未配置/连接失败 → Noop（纯内存模式，一切功能照常）。
	store := redisstore.New(cfg.Upstash.URL, cfg.Upstash.Token)

	p := pool.New(cfg.StateFile)
	// 停机序：先 pool.Close()（最后一次 Flush → SaveState 已提交到 store），
	// 再 store.Close() 排空在途异步写（最后一笔 Redis 镜像必须写完才关连接）。
	defer func() {
		p.Close()
		_ = store.Close()
	}()
	p.SetStore(store)
	p.RestoreFromSnapshot() // 择新恢复：Redis 快照比本地新才采用，否则本地优先
	p.SyncToDir(auths)      // 与 auths 目录对齐：新账号加入、已删除文件账号剔除（状态保留）

	// 熔断器 + 在途上限（含 global 分档）+ 连败降权 + 三因子加权调优（从 config 注入，
	// 非正值回退默认）。
	p.SetBreaker(cfg.Pool.BreakerThreshold, cfg.BreakerCooldownDur, cfg.BreakerCooldownMaxD)
	p.SetMaxInFlight(cfg.Pool.MaxInFlight)
	p.SetMaxInFlightGlobal(cfg.Pool.MaxInFlightGlobal) // global 域 WAF 风控分档（P1-1）
	p.SetDegrade(cfg.Pool.DegradeThreshold, cfg.DegradeCooldownDur, cfg.DegradeCooldownMaxD)
	p.SetSoftRateMax(cfg.SoftRateMaxDur) // 软冷却指数退避封顶（soft_rate_max，默认 2h）
	p.SetWeights(cfg.Pool.IdleWeightPerHour, cfg.Pool.IdleWeightMax)

	// 会话粘性路由（可配关闭）。
	var sessRouter *session.Router
	redisMode := "noop"
	if _, ok := store.(redisstore.Noop); !ok {
		redisMode = "upstash"
	}
	if cfg.SessionSticky.Enabled {
		sessRouter = session.New(session.Config{
			TTL:        cfg.SessionTTL,
			GCInterval: cfg.SessionGCInterval,
			Store:      store,
			Available:  p.AvailableUIDs,
			// realm 感知闭包：带前缀模型名按 realm 过滤可用账号（跨 realm 不泄漏）；
			// 裸名走 cn（现状零回归）。闭包内部 resolveModel 剥前缀，再按 realm 过滤。
			AvailableForModel: realmAwareAvailableForModel(p),
		})
		sessRouter.LoadFromStore() // 启动时从 Redis 恢复粘性（读操作仅此处）
		sessRouter.StartGC()
		defer sessRouter.StopGC()
	}
	sessCount := func() int {
		if sessRouter != nil {
			return sessRouter.Count()
		}
		return 0
	}

	up := upstream.New()
	// global 域代理出口：绕开国际版上游对网关出口 IP 的 WAF 风控。
	// 配置非法直接 fail-fast（拼错的代理地址会让 global 域全部静默失败，
	// 不如启动时就报错）；代理暂时不可达不算错——首次请求才会暴露。
	// 订阅链接池（realm 隔离）：global/cn 各自独立的订阅 URL 列表，
	// 定时拉取解析出代理链接并整体替换对应 realm 的出站池。
	// 订阅生效后覆盖 proxy_global（订阅是动态来源，优先于静态配置）。
	sub := upstream.NewSubPool("data/subpool.json", 6*time.Hour, func(realm, joined string) error {
		return up.SetRealmProxy(realm, joined)
	})
	go sub.Run(context.Background())
	// 池出口采样：无绑定账号的面板视图数据源（5 分钟一轮，随订阅刷新更新出口）。
	go func() {
		time.Sleep(15 * time.Second) // 等 subpool 首刷建池
		up.SampleRealmEgress("global")
		up.SampleRealmEgress("cn")
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for range t.C {
			up.SampleRealmEgress("global")
			up.SampleRealmEgress("cn")
		}
	}()

	if err := up.SetGlobalProxy(cfg.Upstream.ProxyGlobal); err != nil {
		log.Fatalf("upstream.proxy_global 配置无效: %v", err)
	}
	if up.GlobalProxyActive() {
		// 用 ProxySummary 而不是原始配置：代理 URL 里带 userinfo（resin 的
		// proxy token），明文打进日志等于把凭据写进 docker logs 与面板日志区。
		log.Printf("[upstream] global 域出站代理已启用: %s", strings.Join(up.ProxySummary("global"), ", "))
	}

	// ── 账号级出站代理（每账号一条固定出口）────────────────────────────
	//
	// proxy_global 是「全局共用一个出口池」；这里是「一账号一出口」。风控按出口
	// IP 聚号时前者只是把风险平移，后者才是真隔离。绑定表缺省与 state.json 同目录，
	// 一个账号都没配时本段不影响任何出站行为（没有绑定就没有变化）。
	if cfg.AccountProxy.Enabled {
		apFile := strings.TrimSpace(cfg.AccountProxy.File)
		if apFile == "" {
			apFile = stateSibling(cfg.StateFile, "proxy.json")
		}
		ap := upstream.NewAccountProxy(upstream.AccountProxyOptions{
			File:          apFile,
			StateFile:     stateSibling(cfg.StateFile, "proxy_state.json"),
			CheckInterval: cfg.AccountProxyCheckInterval,
			ProbeTimeout:  cfg.AccountProxyProbeTimeout,
			Quorum:        cfg.AccountProxy.Quorum,
			OnMismatch:    cfg.AccountProxy.OnMismatch,
			LockFirstIP:   cfg.AccountProxy.LockFirstIP,
			MaxPerIP:      cfg.AccountProxy.MaxPerIP,
			MaxPerIPAction: cfg.AccountProxy.MaxPerIPAction,
			MinInterval:    cfg.AccountProxyMinInterval,
		})
		if err := ap.Load(); err != nil {
			// 绑定表损坏不该拖垮启动：出站回落既有路径，面板会显示错误原因。
			log.Printf("WARN: 账号代理绑定表加载失败（出站不受影响）: %v", err)
		}
		ap.SyncAccounts(auths) // 账号已删除的绑定一并清理，防僵尸条目堆积
		ap.SetClient(up)
		// 自动绑定循环：每分钟巡检——无绑定账号粘住池里最稳定链接；auto 绑定
		// 连续校验失败自动换绑下一条（非随机轮询，故障才切）。realm 从 auths 取。
		go func() {
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for range t.C {
				// 动态账号列表：auths 是启动快照，运行时新增的账号不在里面
				//（实测新号永远没被自动绑定）。每 tick 从池取当前全量。
				type ar struct{ uid, realm string }
				list := make([]ar, 0, 8)
				for _, st := range p.List() {
					list = append(list, ar{st.UID, st.Realm})
				}
				// 并发巡检：每个账号的探活最长 8s，串行会拖过一个 tick。
				var wg sync.WaitGroup
				for _, a := range list {
					wg.Add(1)
					go func(uid, realm string) {
						defer wg.Done()
						ap.AutoBindAccount(uid, realm)
					}(a.uid, a.realm)
				}
				wg.Wait()
			}
		}()
		up.AccountProxy = ap
		// 代理闸门：quarantine 策略下出口不可信的账号不参与选号（其余策略恒放行）。
		p.SetProxyGate(ap.Usable)
		// 出口信息注入账号池：面板「账号池」列表直接显示每号实测出口 IP / 地区 /
		// 状态，不必切到代理页。pool 不反向依赖 upstream，故用闭包注入。
		p.SetEgressProvider(func(uid string) *pool.EgressInfo {
			v := ap.EgressView(uid)
			if v == nil {
				return nil
			}
			// 无绑定账号：出站实际走 realm 订阅池（若池非空），不是直连——
			// EgressView 的「未绑定=直连」是旧假设，直连 IP 显示会误导。
			// 用池出口采样视图覆盖（realm 从账号档案取）。
			if v.Source == upstream.EgressSourceDirect {
				if a := p.AuthByUID(uid); a != nil {
					if ev := up.RealmEgressView(a.Realm()); ev != nil {
						v = ev
					}
				}
			}
			return &pool.EgressInfo{
				Source: v.Source, CallIP: v.CallIP,
				IP: v.IP, Declared: v.Declared, DirectIP: v.DirectIP,
				Country: v.Country, CountryCode: v.CountryCode, ASN: v.ASN,
				State: v.State, ProxyHost: v.ProxyHost, Label: v.Label,
				SharedBy: v.SharedBy, CheckedAt: v.CheckedAt,
			}
		})
		if n := len(ap.Entries()); n > 0 {
			log.Printf("[account-proxy] 已加载 %d 个账号代理绑定 file=%s interval=%s on_mismatch=%s",
				n, apFile, cfg.AccountProxy.CheckInterval, cfg.AccountProxy.OnMismatch)
			// 首轮校验异步跑：容器网络刚就绪时同步探测会拖慢启动；结论出来前
			// 账号代理按「未校验」放行（见 AccountProxy.usableState）。
			go func() {
				time.Sleep(3 * time.Second)
				sts := ap.CheckAll(context.Background())
				bad := 0
				for _, s := range sts {
					if s.State != "ok" && s.State != "disabled" {
						bad++
						log.Printf("[account-proxy] 出口异常 uid=%s state=%s actual=%s declared=%s desc=%s",
							s.UID, s.State, s.ActualIP, s.ExpectedIP, s.Message)
					}
				}
				log.Printf("[account-proxy] 首轮出口校验完成: %d 个绑定，%d 个异常", len(sts), bad)
			}()
			go ap.Run(context.Background())
		}
		// 调用链路出口定时采样（固定 5 分钟一轮，首轮 15s 后）：与校验分开，
		// 产出 callIP 供面板比对「显示出口 vs 调用实际出口」。不依赖是否有绑定。
		go ap.SampleLoop(context.Background())
	}

	// 本机直连出口探测：**不依赖有没有账号代理绑定**。
	// 未绑代理的账号出口就是这个 IP，面板账号池每一行都要显示 IP + 地区，
	// 所以这里无条件启动一次首轮探测 + 周期刷新（与账号代理的 check_interval
	// 共用节奏；interval=0 时只做启动那一次）。
	if ap := up.AccountProxy; ap != nil {
		directInterval := cfg.AccountProxyCheckInterval
		if directInterval <= 0 {
			directInterval = 30 * time.Minute
		}
		go func() {
			time.Sleep(2 * time.Second) // 等容器网络就绪
			if ap.ProbeDirect(context.Background()) {
				ip, country, cc, asn, _ := ap.DirectEgress()
				log.Printf("[egress-direct] 本机直连出口: ip=%s country=%s(%s) asn=%s", ip, country, cc, asn)
			} else {
				log.Printf("WARN: [egress-direct] 本机直连出口探测失败（未绑代理的账号出口列将显示「探测中」）")
			}
			// 周期刷新：进程存活期间一直跑（与账号代理 Run 同性质，
			// 由进程退出回收）。本机出口在容器生命周期内基本不变，
			// 但要能感知「换了出口 / 出口被墙」，所以保留周期复测。
			t := time.NewTicker(directInterval)
			defer t.Stop()
			for range t.C {
				_ = ap.ProbeDirect(context.Background())
			}
		}()
	}
	// 短 RPC 总时长上限（refresh/checkin/balance/FetchModels），语义不变。
	up.HTTP.Timeout = time.Duration(cfg.Upstream.TimeoutSeconds) * time.Second
	// 聊天 SSE 首字节前（响应头）上限：cfg 已 normalize（缺省回落 timeout_seconds）。
	up.HeaderTimeout = time.Duration(cfg.Upstream.HeaderTimeoutSeconds) * time.Second
	if tr, ok := up.ChatHTTP.Transport.(*http.Transport); ok {
		tr.ResponseHeaderTimeout = up.HeaderTimeout
	}
	// 聊天 SSE 流中空闲上限（S3 空闲监控读取）。
	up.IdleTimeout = time.Duration(cfg.Upstream.IdleTimeoutSeconds) * time.Second
	up.SanitizeFingerprints = cfg.Features.SanitizeBlacklistFingerprints
	// 出站 UA 与归属头（issue #42 + 上游同步）：
	// UserAgent 非空则完全覆盖；ClientVersion/CliVersion 缺省对齐官方形态；
	// ClientName 非空时 chat 路径注入 X-IDE-* 四头（用量归因对齐官方桌面端）。
	up.UserAgent = cfg.Upstream.UserAgent
	up.ClientVersion = cfg.Upstream.ClientVersion
	up.CliVersion = cfg.Upstream.CliVersion
	up.ClientName = cfg.Upstream.ClientName
	up.DeviceToken = cfg.Upstream.DeviceToken
	up.DeviceTokenFile = cfg.Upstream.DeviceTokenFile
	up.PassthroughIP = cfg.Upstream.PassthroughIP
	// global realm 路由（config global 段）：上游侧开关（第一道闸）+ base 覆盖；
	// auth 侧开关（auth.SetGlobalEnabled）是第二道闸，两者同 config global.enabled。
	up.GlobalEnabled = cfg.Global.Enabled
	up.ChatBaseGlobal = cfg.Global.ChatBase
	up.BillingBaseGlobal = cfg.Global.BillingBase
	auth.SetGlobalEnabled(cfg.Global.Enabled)
	// model.json 本地缓存接线（context_length/max_output_tokens 四级查找链第 3 级）：
	// 数据目录与 state.json 同风格（Docker volume 持久化路径）。首次缺失/损坏自动
	// 回落仓库内嵌种子；models.dev 按需拉取成功后原子写回。
	upstream.SetModelCatalogPath(stateSibling(cfg.StateFile, "model.json"))

	sch := scheduler.New(scheduler.Config{
		Pool:           p,
		Upstream:       up,
		CheckinHours:   cfg.Schedule.CheckinHours,
		TravelHours:    cfg.Schedule.TravelHours,
		ActivityHours:  cfg.Schedule.ActivityHours,
		KeepaliveHours: cfg.Schedule.KeepaliveHours,
		BlackcatHours:  cfg.Schedule.BlackcatHours,
		// 快过期积分优先消耗：签到/余额刷新按此窗口分桶（issue:积分过期）。
		ExpiringSoonWindow: cfg.ExpiringSoonDur,
		CheckinDisabled:    !cfg.Schedule.CheckinEnabled,
		TravelDisabled:     !cfg.Schedule.TravelEnabled,
		ActivityDisabled:   !cfg.Schedule.ActivityEnabled,
		KeepaliveDisabled:  !cfg.Schedule.KeepaliveEnabled,
		BlackcatDisabled:   !cfg.Schedule.BlackcatEnabled,
	})
	switch {
	case !cfg.Schedule.CheckinEnabled:
		log.Printf("签到已禁用（schedule.checkin_enabled=false）")
	default:
		log.Printf("签到已启用：%v 点（签到 + 余额查询解冻）", cfg.Schedule.CheckinHours)
	}
	switch {
	case !cfg.Schedule.TravelEnabled:
		log.Printf("猫猫旅行已禁用（schedule.travel_enabled=false）")
	default:
		log.Printf("猫猫旅行已启用：%v 点（独立排程：领养 / 派出 / 领奖）", cfg.Schedule.TravelHours)
	}
	switch {
	case !cfg.Schedule.ActivityEnabled:
		log.Printf("活跃上报已禁用（schedule.activity_enabled=false）")
	default:
		log.Printf("活跃上报已启用：%v 点（每日 1 次，点亮连登 + 解锁 first_buddy）", cfg.Schedule.ActivityHours)
	}
	if !cfg.Schedule.KeepaliveEnabled {
		log.Printf("token 保活已禁用（schedule.keepalive_enabled=false）")
	} else {
		log.Printf("token 保活已启用：%v 点", cfg.Schedule.KeepaliveHours)
	}
	switch {
	case !cfg.Schedule.BlackcatEnabled:
		log.Printf("夜猫子已禁用（schedule.blackcat_enabled=false）")
	default:
		log.Printf("夜猫子已启用：%v 点（23:00–08:00 窗口 glm-5.2 对话补足）", cfg.Schedule.BlackcatHours)
	}
	switch {
	case !cfg.Schedule.BalanceRefreshEnabled:
		log.Printf("余额后台刷新已禁用（schedule.balance_refresh_enabled=false）")
	case cfg.BalanceRefreshInterval > 0:
		log.Printf("余额后台刷新：每 %s（签到时点照常额外刷新）", cfg.BalanceRefreshInterval)
	}

	// 管理面板日志镜像：标准 log（stderr）与 chat 表格日志（stdout）双路复制进
	// 面板环形缓冲，供 /panel/api/logs 读取；控制台输出行为完全不变。
	// live 承载可热改字段（api_key/soft_rate/脱敏开关），面板保存配置时在线替换。
	live := livecfg.New(livecfg.Snapshot{
		APIKey:               cfg.APIKey,
		SoftCooldown:         cfg.SoftRateDur,
		SanitizeFingerprints: cfg.Features.SanitizeBlacklistFingerprints,
	})
	// 用量记录器：与 state 文件同目录，随 state_file 配置一起搬移。
	// datapath 由 state 文件路径推出，避免再加一个配置项。
	usagePath := usagePathFor(cfg.StateFile)
	rec := usage.New(usagePath)
	rec.Start()
	defer rec.Stop()
	log.Printf("[usage] 逐请求用量记录已启用: %s (%s)", usagePath, rec.Describe())

	// chatHandler 前置声明：panel 的 SaveConfig 闭包要拿到 handler 以热应用
	// server.max_body_mb，而 handler 的 Config.Panel 又依赖 pn——装配循环用
	// 变量前置 + saveConfig 内 nil 保护解开（SaveConfig 只在请求期被调，彼时
	// handler 必已就位）。
	var chatHandler *server.Handler

	// 模型元数据仓库（上游真实名 / 验证状态 / 备注 / 手动思考档）：
	// 与 state、usage 同目录，缺省 data/model_meta.json。
	modelStore := modelmeta.New(stateSibling(cfg.StateFile, "model_meta.json"))
	modelVerifier := modelmeta.NewVerifier(modelStore)
	// 验证探针：经网关回路发起真实调用（走完整下游链路，结论=客户端实际体验）。
	modelProbe := modelProbeFunc(cfg.Listen, cfg.APIKey)

	pn := panel.New(panel.Config{
		Pool:        p,
		Usage:       rec,
		Upstream:    up,
		SubPool:     sub,
		Scheduler:   sch,
		AuthDir:     cfg.AuthDir,
		APIKey:      cfg.APIKey,
		RedisMode:   redisMode,
		StickyCount: sessCount,
		// 粘性会话路由器：面板「重置/清理粘性会话」用（未启用时 nil，接口返回 501）。
		Session: sessRouter,
		Version: appVersion,
		Live:        live,
		// 模型上限探测数据（scripts/probe_max_tokens.py --panel-out 写入）：
		// 与 state 文件同目录，缺省 data/output_probes.json。
		ProbeFile:  stateSibling(cfg.StateFile, "output_probes.json"),
		ConfigPath: *cfgPath,
		LoadConfig: func() (any, error) {
			return Load(*cfgPath)
		},
		SaveConfig: func(raw []byte) ([]string, error) {
			return saveConfig(raw, *cfgPath, live, p, up, sch, chatHandler)
		},
		// 模型管理与映射：元数据仓库、验证器、回路探针。
		ModelMeta:     modelStore,
		ModelVerifier: modelVerifier,
		ModelProbe:    modelProbe,
		// 面板「模型与映射」页直接消费 /v1/models 同口径数据（handler 装配后注入，
		// 见下方 pn.SetModelLister），保证面板与客户端看到的永不漂移。
		ListModels: func() []map[string]any {
			if chatHandler == nil {
				return nil
			}
			return chatHandler.ModelList()
		},
	})
	log.SetOutput(io.MultiWriter(os.Stderr, pn.Logs()))
	server.SetChatLogOutput(io.MultiWriter(os.Stdout, pn.Logs()))

	h := server.NewHandler(server.Config{
		Pool:         p,
		Upstream:     up,
		APIKey:       cfg.APIKey,
		Session:      sessRouter,
		StickyCount:  sessCount,
		RedisMode:    redisMode,
		SoftCooldown: cfg.SoftRateDur,
		Panel:        pn,
		Live:         live,
		Usage:        rec,
		PromptMode:   cfg.Prompt.Mode,
		PromptText:   cfg.PromptText,
		// handler 侧第三道闸（global realm）：false（显式逃生门）时不列 global: 模型名。
		GlobalEnabled: cfg.Global.Enabled,
		MaxBodyBytes:  int64(cfg.Server.MaxBodyMB) << 20, // MB → 字节
		// 模型元数据：/v1/models 透出上游真实名/验证状态/备注，并支持
		// only_verified 过滤与手动思考档回调。
		ModelMeta:    modelStore,
		OnlyVerified: cfg.Models.OnlyVerified,
		ModelProbe:   modelProbe,
	})
	chatHandler = h

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go sch.Run(ctx)
	sch.StartBalanceRefresh(ctx, cfg.BalanceRefreshInterval)

	// 每天 00:00 自动刷新模型目录（credits 倍率）与实测消耗（per1k）。
	if dailyRefreshEnabled(cfg) {
		go runDailyModelRefresh(ctx, h, modelStore, modelVerifier, modelProbe)
		log.Printf("[models] 每日刷新已启用（下次 %s）", nextMidnight(time.Now()).Format("2006-01-02 15:04"))
	} else {
		log.Printf("[models] 每日刷新已禁用（models.daily_refresh=false）")
	}

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		// ReadTimeout 覆盖整个请求读取（含 body）：防慢速 body 拖死连接。
		// 取值大于 MaxBodyMB 在常规带宽下的上传耗时；聊天请求体上限默认 8MB。
		ReadTimeout: 60 * time.Second,
		// IdleTimeout keep-alive 空闲连接回收：配合 chat 出站 ctx 传播防连接泄漏堆积。
		// 注意：SSE 流式响应期间连接非空闲，不受此项掐断；不设全局 WriteTimeout
		// （长流式生成合法时长可达数分钟，全局 WriteTimeout 会误杀在途 SSE）。
		IdleTimeout: 120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		p.Flush() // 信号触发：先落盘再做优雅停机
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("workbuddy2api listening on %s (api_key=%v)，管理面板 http://127.0.0.1%s/panel/", cfg.Listen, cfg.APIKey != "", panelListenPath(cfg.Listen))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
	log.Printf("bye")
}

// loopbackBase 把监听地址转成回环 base（"0.0.0.0:7863" / ":7863" → "http://127.0.0.1:7863"）。
// 模型验证走回环而非外网域名：不经 nginx/DNS，避免把验证流量打到公网绕一圈。
func loopbackBase(listen string) string {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		// 无端口的裸地址（如 ":7863" 已被 SplitHostPort 正常拆开）→ 原样拼。
		return "http://127.0.0.1" + listen
	}
	if port == "" {
		return "http://127.0.0.1"
	}
	return "http://127.0.0.1:" + port
}

// modelProbeFunc 构造模型可调用性验证探针：经网关自身回环发起一次真实
// /v1/chat/completions 请求。
//
// 走完整下游链路（鉴权 → realm 前缀解析 → 选号 → 上游 → 计费），因此验证结论
// 就是客户端实际会遇到的结果——能抓出三类目录看不出的坑：
//  1. 别名：deep-model 上游回显 glm-5.3（回显名即真实映射名）；
//  2. 幽灵：目录里有但 11102 service info not found（cn:default-model）；
//  3. 隐藏：目录里没有但调用成功（cn:hunyuan-2.0-instruct）。
func modelProbeFunc(listen, apiKey string) modelmeta.ProbeFunc {
	base := loopbackBase(listen)
	client := &http.Client{Timeout: 150 * time.Second}
	return func(ctx context.Context, full string) modelmeta.ProbeResult {
		body, err := json.Marshal(map[string]any{
			"model":      full,
			"messages":   []map[string]string{{"role": "user", "content": "ping"}},
			"max_tokens": 16,
			"stream":     false,
		})
		if err != nil {
			return modelmeta.ProbeResult{ErrCode: "probe_error", ErrMsg: err.Error()}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return modelmeta.ProbeResult{ErrCode: "probe_error", ErrMsg: err.Error()}
		}
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			return modelmeta.ProbeResult{ErrCode: "probe_error", ErrMsg: err.Error(), Latency: time.Since(start)}
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		lat := time.Since(start)

		var out struct {
			Model   string `json:"model"`
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
			Usage struct {
				Credit float64 `json:"credit"`
			} `json:"usage"`
			Error *struct {
				Code    any    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &out)
		if resp.StatusCode != http.StatusOK {
			code := fmt.Sprintf("http_%d", resp.StatusCode)
			msg := strings.TrimSpace(string(raw))
			if out.Error != nil {
				if c := fmt.Sprintf("%v", out.Error.Code); c != "" && c != "<nil>" {
					code = c
				}
				if out.Error.Message != "" {
					msg = out.Error.Message
				}
			}
			if len(msg) > 400 {
				msg = msg[:400]
			}
			return modelmeta.ProbeResult{ErrCode: code, ErrMsg: msg, Latency: lat}
		}
		if out.Error != nil {
			code := fmt.Sprintf("%v", out.Error.Code)
			if code == "" || code == "<nil>" {
				code = "upstream_error"
			}
			return modelmeta.ProbeResult{ErrCode: code, ErrMsg: out.Error.Message, Latency: lat}
		}
		return modelmeta.ProbeResult{OK: true, UpstreamModel: out.Model, Credit: out.Usage.Credit, Latency: lat}
	}
}

// dailyRefreshEnabled 是否启用每日模型刷新（未配置 → 默认启用）。
func dailyRefreshEnabled(c *Config) bool {
	return c.Models.DailyRefresh == nil || *c.Models.DailyRefresh
}

// nextMidnight 返回下一个本地时间 00:00（当天 00:00 已过则取次日）。
func nextMidnight(now time.Time) time.Time {
	y, m, d := now.Date()
	n := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
	if !n.After(now) {
		n = n.AddDate(0, 0, 1)
	}
	return n
}

// runDailyModelRefresh 每天 00:00 刷新模型目录与实测消耗。
//
// 为什么需要：
//   - **倍率**（credits）是上游定价，随时调整（限免开始/结束、调价），目录缓存
//     只有 1 小时但面板可能整天没被访问，隔天看到的仍是旧价格；
//   - **实测消耗**（每千 token 积分）只有真实请求发生后才更新，冷门模型会永远
//     停留在旧值——而"这个模型到底多少钱"正是选模型时最该看准的数字。
//
// 流程：清目录缓存 → 立即触发一次拉取（面板不必等下一个客户端请求）→
// 对全部在册且未删除的模型跑一轮实测 → 结果落盘 data/model_meta.json。
//
// 成本：每模型一次极短调用（max_tokens=16，约 500 token prompt），
// 并发 3、总超时 30 分钟；不想承担这部分开销时设 models.daily_refresh=false。
func runDailyModelRefresh(ctx context.Context, h *server.Handler, store *modelmeta.Store,
	verifier *modelmeta.Verifier, probe modelmeta.ProbeFunc) {
	for {
		timer := time.NewTimer(time.Until(nextMidnight(time.Now())))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		// 1) 目录缓存失效并立即重拉：拿最新 credits 倍率与上下架状态。
		h.InvalidateModelCache()
		h.ModelList()

		// 2) 实测消耗刷新：只跑在册且未删除的模型。
		refs := make([]modelmeta.ModelRef, 0, 128)
		for _, v := range store.All() {
			if v.Removed {
				continue
			}
			refs = append(refs, modelmeta.ModelRef{Realm: v.Realm, ID: v.ID, Full: v.FullID})
		}
		if len(refs) == 0 {
			log.Printf("[models] 每日刷新：在册模型为空，跳过实测")
			continue
		}
		vctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		views, err := verifier.Verify(vctx, probe, modelmeta.VerifyOption{Only: refs, Concurrency: 3})
		cancel()
		ok := 0
		for _, v := range views {
			if v.Verified {
				ok++
			}
		}
		log.Printf("[models] 每日刷新完成：%d/%d 可调用 err=%v（下次 %s）",
			ok, len(views), err, nextMidnight(time.Now()).Format("2006-01-02 15:04"))
	}
}

// panelListenPath 从 listen 地址提取 ":port" 形式，用于启动日志拼面板 URL
// （":7863" 或 "0.0.0.0:7863" → ":7863"；异常输入原样返回）。
func panelListenPath(listen string) string {
	for i := len(listen) - 1; i >= 0; i-- {
		if listen[i] == ':' {
			return listen[i:]
		}
	}
	return listen
}

// saveConfig 面板保存配置：校验 → 落盘 → 热应用 → 返回需重启的字段列表。
//
// 热生效范围（设计取舍）：
//   - api_key / cooldown.soft_rate / features.sanitize_blacklist_fingerprints → livecfg 快照
//   - pool.* → pool.SetBreaker/SetMaxInFlight/SetSoftRateMax/SetWeights
//   - schedule.* → scheduler.Reconfigure/SetBalanceInterval
//   - server.max_body_mb → handler.SetMaxBodyBytes（issue #17：面板改完即时生效，不再"静默不生效还重启也不提示"）
//
// 需重启（涉及监听地址、HTTP client 超时、auth_dir 等装配期依赖）：
//   - listen / auth_dir / state_file / upstream.* / upstash.* / session_sticky.*（TTL 类）
//
// 落盘用"先写 tmp 再 rename"原子替换，且优先保留磁盘上的原始 JSON 结构（只改
// 面板表单覆盖到的键），避免把用户手写的注释性字段/未知键洗掉——这里直接整体
// 序列化校验后的配置，未知键在 json.Unmarshal 时已丢失，故先合并原始 map。
func saveConfig(raw []byte, path string, live *livecfg.Holder, p *pool.Pool, up *upstream.Client, sch *scheduler.Scheduler, srv *server.Handler) ([]string, error) {
	// 1) 解析原始 JSON 为 map（保留用户手写的未知键），再叠加面板提交的键。
	oldRaw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read current config: %w", err)
	}
	var cur, incoming map[string]any
	if err := json.Unmarshal(oldRaw, &cur); err != nil {
		cur = map[string]any{}
	}
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return nil, fmt.Errorf("parse submitted config: %w", err)
	}
	merged := mergeConfigMaps(cur, incoming)

	// 2) 校验（与启动同一套 Default+normalize），失败直接返回、不落盘。
	newCfg, err := ParseConfig(mergedJSON(merged))
	if err != nil {
		return nil, err
	}

	// 3) 落盘（原子替换）。
	out, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return nil, fmt.Errorf("write config: %w", err)
	}
	// Docker 单文件 bind mount（`- ./config.json:/app/config.json`）下不能直接
	// rename 覆盖挂载点，会报 "device or resource busy"。回退为把 tmp 内容写回
	// 目标文件：原子性降级（截断写），但保存一定能成功——面板存不下配置比极端
	// 情况下丢半截文件严重得多。
	if err := os.Rename(tmp, path); err != nil {
		if cerr := copyFileContents(tmp, path); cerr != nil {
			_ = os.Remove(tmp)
			return nil, fmt.Errorf("replace config: %w（回退写回也失败: %v）", err, cerr)
		}
		_ = os.Remove(tmp)
		log.Printf("config: 挂载点无法 rename，已回退为直接写回 %s", path)
	}

	// 4) 热应用：能立即生效的字段全部应用，并列出仍需重启的字段。
	live.Store(livecfg.Snapshot{
		APIKey:               newCfg.APIKey,
		SoftCooldown:         newCfg.SoftRateDur,
		SanitizeFingerprints: newCfg.Features.SanitizeBlacklistFingerprints,
	})
	up.SanitizeFingerprints = newCfg.Features.SanitizeBlacklistFingerprints
	p.SetBreaker(newCfg.Pool.BreakerThreshold, newCfg.BreakerCooldownDur, newCfg.BreakerCooldownMaxD)
	p.SetMaxInFlight(newCfg.Pool.MaxInFlight)
	p.SetMaxInFlightGlobal(newCfg.Pool.MaxInFlightGlobal)
	p.SetDegrade(newCfg.Pool.DegradeThreshold, newCfg.DegradeCooldownDur, newCfg.DegradeCooldownMaxD)
	p.SetSoftRateMax(newCfg.SoftRateMaxDur)
	p.SetWeights(newCfg.Pool.IdleWeightPerHour, newCfg.Pool.IdleWeightMax)
	sch.Reconfigure(
		newCfg.Schedule.CheckinHours, newCfg.Schedule.TravelHours,
		newCfg.Schedule.ActivityHours, newCfg.Schedule.KeepaliveHours, newCfg.Schedule.BlackcatHours,
		!newCfg.Schedule.CheckinEnabled, !newCfg.Schedule.TravelEnabled,
		!newCfg.Schedule.ActivityEnabled, !newCfg.Schedule.KeepaliveEnabled, !newCfg.Schedule.BlackcatEnabled)
	sch.SetBalanceInterval(newCfg.BalanceRefreshInterval)
	// srv 为 nil 仅出现在装配未完成的窗口（SaveConfig 只在请求期被调，理论不可达），
	// 跳过热应用即可——下次重启仍会从落盘的 config.json 读到新值。
	if srv != nil {
		srv.SetMaxBodyBytes(int64(newCfg.Server.MaxBodyMB) << 20)
	}

	return restartRequiredFields(newCfg), nil
}

// restartRequiredFields 返回本次改动中无法热生效、需要重启进程的字段名。
// 恒返回完整清单中的"与当前进程装配期依赖相关"的项——面板据此提示用户。
// copyFileContents 把 src 内容整体写回 dst（rename 的降级替代）。
// 先读全量再写，避免边读边写把 dst 截断成空（src 与 dst 不同路径，无此风险，
// 但保持实现直白）；写失败时 dst 可能已被截断——调用方需自行判断是否需要备份。
func copyFileContents(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	st, err := os.Stat(dst)
	mode := os.FileMode(0o600)
	if err == nil {
		mode = st.Mode().Perm()
	}
	return os.WriteFile(dst, data, mode)
}

func restartRequiredFields(c *Config) []string {
	var out []string
	// 这些字段在进程内被监听地址/HTTP client/目录句柄等装配期对象捕获。
	if c.Listen != "" {
		out = append(out, "listen")
	}
	if c.AuthDir != "" {
		out = append(out, "auth_dir")
	}
	if c.StateFile != "" {
		out = append(out, "state_file")
	}
	out = append(out, "upstream.timeout_seconds", "upstream.header_timeout_seconds", "upstream.idle_timeout_seconds")
	if c.Upstash.URL != "" || c.Upstash.Token != "" {
		out = append(out, "upstash")
	}
	out = append(out, "session_sticky.ttl", "session_sticky.gc_interval")
	return out
}

// mergeConfigMaps 把 incoming 深合并进 cur（原地），返回 cur。
// 对嵌套对象逐键覆盖而不是整体替换：面板表单只提交它管理的键，
// 未提交的兄弟键（含用户手写的未知键）保持原样。
func mergeConfigMaps(cur, incoming map[string]any) map[string]any {
	for k, v := range incoming {
		if inMap, ok := v.(map[string]any); ok {
			if curMap, ok := cur[k].(map[string]any); ok {
				cur[k] = mergeConfigMaps(curMap, inMap)
				continue
			}
		}
		cur[k] = v
	}
	return cur
}

// mergedJSON 把合并后的 map 序列化回 JSON（供 ParseConfig 校验）。
func mergedJSON(m map[string]any) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		return []byte("{}")
	}
	return b
}
