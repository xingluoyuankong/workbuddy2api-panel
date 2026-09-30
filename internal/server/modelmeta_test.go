package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/modelmeta"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

// resetModelsCache 清掉进程级模型缓存（测试间隔离，与 TestModelsDynamic 同手法）。
func resetModelsCache(t *testing.T) {
	t.Helper()
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = nil
	dynamicModelsCache.fetched = time.Time{}
	dynamicModelsCache.lastFail = time.Time{}
	dynamicModelsCache.Unlock()
	t.Cleanup(resetModelsCacheUnlocked)
}

func resetModelsCacheUnlocked() {
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = nil
	dynamicModelsCache.fetched = time.Time{}
	dynamicModelsCache.lastFail = time.Time{}
	dynamicModelsCache.Unlock()
}

// fakeCatalogUpstream 返回固定目录的假上游（3 个模型 + 一个 sg 区域变体）。
func fakeCatalogUpstream(t *testing.T) *upstream.Client {
	t.Helper()
	catalog := `{"code":0,"data":{"models":[
		{"id":"deepseek-v4.1-flash","name":"DS-Flash","maxInputTokens":1000000,"maxOutputTokens":128000,"credits":"x0.03","descriptionZh":"限时免费"},
		{"id":"deep-model","name":"Deep","maxInputTokens":176000,"maxOutputTokens":24000,"credits":"x3.33","descriptionZh":"深度推理"},
		{"id":"hy3","name":"HY3","maxInputTokens":128000,"maxOutputTokens":8192,"credits":"x0.00","descriptionZh":"混元"}
	]}}`
	return newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, catalog, false
	})
}

// listModels 取 /v1/models 条目列表。
func listModels(t *testing.T, h *Handler, query string) []map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models"+query, nil))
	if rec.Code != 200 {
		t.Fatalf("GET /v1/models%s code=%d body=%s", query, rec.Code, rec.Body)
	}
	var resp struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	return resp.Data
}

func indexByID(rows []map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, r := range rows {
		if id, ok := r["id"].(string); ok {
			out[id] = r
		}
	}
	return out
}

// --- 元数据透出 ---

func TestModelsEnrichedWithMeta(t *testing.T) {
	resetModelsCache(t)
	store := modelmeta.New("")
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  fakeCatalogUpstream(t),
		ModelMeta: store,
	})
	rows := indexByID(listModels(t, h, ""))
	if len(rows) != 3 {
		t.Fatalf("want 3 models, got %d", len(rows))
	}
	// deep-model：CN 域别名为 glm-5.3（静态知识库 + 未验证状态）。
	dm := rows["cn:deep-model"]
	if dm == nil {
		t.Fatal("cn:deep-model missing")
	}
	if got := dm["upstream_model"]; got != "glm-5.3" {
		t.Errorf("cn:deep-model upstream_model = %v, want glm-5.3", got)
	}
	if got := dm["is_alias"]; got != true {
		t.Errorf("is_alias = %v, want true", got)
	}
	if got := dm["status"]; got != "unverified" {
		t.Errorf("status = %v, want unverified (never claim verified without probe)", got)
	}
	if got := dm["verified"]; got != false {
		t.Errorf("verified = %v, want false", got)
	}
	if cat := dm["category"]; cat != string(modelmeta.CatAlias) {
		t.Errorf("category = %v, want %v", cat, modelmeta.CatAlias)
	}
	if note, _ := dm["note"].(string); note == "" {
		t.Error("deep-model should carry an explanatory note")
	}
	// 免费模型：note 说明限免（静态知识库）。
	if note, _ := rows["cn:deepseek-v4.1-flash"]["note"].(string); !strings.Contains(note, "免费") {
		t.Errorf("flash note = %q, want 免费", note)
	}
}

// TestModelsRemovesRegionVariantAndLegacy 删除规则：区域变体（-sg）与遗留名单
// 不得出现在 /v1/models（Removed 优先于一切筛选，仅「已删除」视图可见）。
func TestModelsRemovesRegionVariantAndLegacy(t *testing.T) {
	resetModelsCache(t)
	catalog := `{"code":0,"data":{"models":[
		{"id":"deepseek-v4.1-flash-sg","name":"DS-SG","maxInputTokens":1000000,"maxOutputTokens":128000},
		{"id":"default-1.2","name":"Claude-4.0-Sonnet","maxInputTokens":200000,"maxOutputTokens":24000},
		{"id":"hunyuan-2.0-instruct","name":"Hunyuan-2.0-Instruct","maxInputTokens":128000,"maxOutputTokens":16000},
		{"id":"glm-4.6","name":"GLM-4.6","maxInputTokens":168000,"maxOutputTokens":32000},
		{"id":"hy3","name":"HY3","maxInputTokens":128000,"maxOutputTokens":8192}
	]}}`
	up := newFakeUpstream(t, func(authz string) (int, string, bool) { return 200, catalog, false })
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  up,
		ModelMeta: modelmeta.New(""),
	})
	rows := indexByID(listModels(t, h, ""))
	if len(rows) != 1 || rows["cn:hy3"] == nil {
		t.Fatalf("only hy3 should survive removal, got %v", rows)
	}
	for _, gone := range []string{"cn:deepseek-v4.1-flash-sg", "cn:default-1.2", "cn:hunyuan-2.0-instruct", "cn:glm-4.6"} {
		if _, ok := rows[gone]; ok {
			t.Errorf("%s must be removed", gone)
		}
	}
	// 即使显式要求全量（only_verified=0）也不能复活。
	if all := indexByID(listModels(t, h, "?only_verified=0")); len(all) != 1 {
		t.Errorf("removal must survive only_verified=0, got %v", all)
	}
}

func TestModelsWithoutMetaIsBackwardCompatible(t *testing.T) {
	resetModelsCache(t)
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: fakeCatalogUpstream(t),
	})
	rows := indexByID(listModels(t, h, ""))
	if len(rows) != 3 {
		t.Fatalf("want 3 models (no filtering when meta disabled), got %d", len(rows))
	}
	for id, r := range rows {
		for _, k := range []string{"upstream_model", "status", "verified", "note", "category", "is_alias"} {
			if _, ok := r[k]; ok {
				t.Errorf("%s: field %q must be omitted when ModelMeta is nil", id, k)
			}
		}
	}
}

// --- 只展示已验证模型 ---

func TestModelsOnlyVerifiedFilter(t *testing.T) {
	resetModelsCache(t)
	store := modelmeta.New("")
	verifier := modelmeta.NewVerifier(store)
	// 实测：hy3 / deep-model 通过，sg 失败（11102 not found）。
	_, _ = verifier.Verify(context.Background(), func(ctx context.Context, full string) modelmeta.ProbeResult {
		switch full {
		case "cn:hy3":
			return modelmeta.ProbeResult{OK: true, Credit: 0, Latency: 100 * time.Millisecond}
		case "cn:deep-model":
			return modelmeta.ProbeResult{OK: true, UpstreamModel: "glm-5.3", Credit: 0.09, Latency: 200 * time.Millisecond}
		default:
			return modelmeta.ProbeResult{OK: false, ErrCode: "no_healthy_account", ErrMsg: "model service info not found"}
		}
	}, modelmeta.VerifyOption{Only: []modelmeta.ModelRef{
		{Realm: "cn", ID: "hy3", Full: "cn:hy3"},
		{Realm: "cn", ID: "deep-model", Full: "cn:deep-model"},
		{Realm: "cn", ID: "default-model", Full: "cn:default-model"},
	}})

	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  fakeCatalogUpstream(t),
		ModelMeta: store,
	})
	// 实测后：only_verified=1 只留通过的两个，失败与未验证的一律剔除。
	rows := indexByID(listModels(t, h, "?only_verified=1"))
	if len(rows) != 2 {
		t.Fatalf("only_verified=1 = %d, want 2: %v", len(rows), rows)
	}
	if _, ok := rows["cn:hy3"]; !ok {
		t.Error("cn:hy3 verified but filtered out")
	}
	if _, ok := rows["cn:deep-model"]; !ok {
		t.Error("cn:deep-model verified but filtered out")
	}
	if _, ok := rows["cn:default-model"]; ok {
		t.Error("cn:default-model failed verification but still listed")
	}
	// 实测别名：deep-model 的上游真实名被记录为 glm-5.3。
	if got := rows["cn:deep-model"]["upstream_model"]; got != "glm-5.3" {
		t.Errorf("upstream_model = %v, want glm-5.3 (measured)", got)
	}
	if got := rows["cn:deep-model"]["verified"]; got != true {
		t.Errorf("verified = %v, want true", got)
	}
	// 默认（未开开关）仍全量可见——兼容性不破。
	if all := listModels(t, h, ""); len(all) != 3 {
		t.Errorf("default should keep all 3, got %d", len(all))
	}
	// only_verified=0 显式关闭 → 同样全量。
	if all := listModels(t, h, "?only_verified=0"); len(all) != 3 {
		t.Errorf("only_verified=0 = %d, want 3", len(all))
	}
}

// TestModelsOnlyVerifiedStaticConfig 静态配置 OnlyVerified=true 时默认查询即过滤。
func TestModelsOnlyVerifiedStaticConfig(t *testing.T) {
	resetModelsCache(t)
	store := modelmeta.New("")
	verifier := modelmeta.NewVerifier(store)
	_, _ = verifier.Verify(context.Background(), func(ctx context.Context, full string) modelmeta.ProbeResult {
		return modelmeta.ProbeResult{OK: full == "cn:hy3"}
	}, modelmeta.VerifyOption{Only: []modelmeta.ModelRef{{Realm: "cn", ID: "hy3", Full: "cn:hy3"}}})

	h := NewHandler(Config{
		Pool:         testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:     fakeCatalogUpstream(t),
		ModelMeta:    store,
		OnlyVerified: true,
	})
	if rows := listModels(t, h, ""); len(rows) != 1 || rows[0]["id"] != "cn:hy3" {
		t.Fatalf("static OnlyVerified not applied: %v", rows)
	}
	// 查询参数可单次反向覆盖。
	if rows := listModels(t, h, "?only_verified=0"); len(rows) != 3 {
		t.Errorf("override to 0 = %d, want 3", len(rows))
	}
}

// TestModelsUnverifiedFilteredOutWhenOnlyVerified 从未验证过的模型在 only_verified 下被剔除
// （保守语义：只留确认能用的，避免客户端选到 11102 的幽灵模型）。
func TestModelsUnverifiedFilteredOutWhenOnlyVerified(t *testing.T) {
	resetModelsCache(t)
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  fakeCatalogUpstream(t),
		ModelMeta: modelmeta.New(""),
	})
	if rows := listModels(t, h, "?only_verified=1"); len(rows) != 0 {
		t.Errorf("unverified models must be filtered out, got %d: %v", len(rows), rows)
	}
}

// --- 隐藏 ---

func TestModelsHiddenExcluded(t *testing.T) {
	resetModelsCache(t)
	store := modelmeta.New("")
	store.SetHidden("cn", "deep-model", true)
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  fakeCatalogUpstream(t),
		ModelMeta: store,
	})
	rows := indexByID(listModels(t, h, ""))
	if len(rows) != 2 {
		t.Fatalf("hidden model should be excluded, got %d: %v", len(rows), rows)
	}
	if _, ok := rows["cn:deep-model"]; ok {
		t.Error("cn:deep-model is hidden but still listed")
	}
}

// --- realm / 关键字过滤 ---

func TestModelsRealmAndKeywordFilter(t *testing.T) {
	resetModelsCache(t)
	store := modelmeta.New("")
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  fakeCatalogUpstream(t),
		ModelMeta: store,
	})
	if rows := listModels(t, h, "?realm=global"); len(rows) != 0 {
		t.Errorf("realm=global should exclude CN models, got %d", len(rows))
	}
	if rows := listModels(t, h, "?realm=cn"); len(rows) != 3 {
		t.Errorf("realm=cn = %d, want 3", len(rows))
	}
	// 关键字按上游真实名命中：搜 glm-5.3 应命中别名 deep-model。
	rows := indexByID(listModels(t, h, "?q=glm-5.3"))
	if _, ok := rows["cn:deep-model"]; !ok {
		t.Errorf("keyword should match upstream_model; got %v", rows)
	}
	if rows2 := indexByID(listModels(t, h, "?q=hy3")); len(rows2) != 1 {
		t.Errorf("q=hy3 = %d, want 1", len(rows2))
	}
	if rows3 := listModels(t, h, "?q=nope"); len(rows3) != 0 {
		t.Errorf("q=nope = %d, want 0", len(rows3))
	}
}

// --- 手动思考档位回调上游 ---

func TestInjectEffortIfAbsent(t *testing.T) {
	cases := []struct {
		name, body, effort, want string
	}{
		{"absent", `{"model":"m","messages":[]}`, "high", `{"messages":[],"model":"m","reasoning_effort":"high"}`},
		{"client snake wins", `{"model":"m","reasoning_effort":"low"}`, "high", `{"model":"m","reasoning_effort":"low"}`},
		{"client camel wins", `{"model":"m","reasoningEffort":"low"}`, "high", `{"model":"m","reasoningEffort":"low"}`},
		{"empty effort noop", `{"model":"m"}`, "", `{"model":"m"}`},
		{"invalid json noop", `not-json`, "high", `not-json`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(injectEffortIfAbsent([]byte(c.body), c.effort))
			var a, b map[string]any
			if json.Unmarshal([]byte(got), &a) != nil || json.Unmarshal([]byte(c.want), &b) != nil {
				if got != c.want {
					t.Fatalf("got %q want %q", got, c.want)
				}
				return
			}
			for k, v := range b {
				if !reflect.DeepEqual(a[k], v) {
					t.Fatalf("field %s = %#v, want %#v (got %s)", k, a[k], v, got)
				}
			}
			if len(a) != len(b) {
				t.Fatalf("got %s want %s", got, c.want)
			}
		})
	}
}

// TestChatUsesManualEffort 面板手动指定的思考档位会回调上游；客户端显式档位优先。
func TestChatUsesManualEffort(t *testing.T) {
	cases := []struct {
		name, body  string
		manual      string
		wantEffort  string
		wantMissing bool
	}{
		{"manual applied", `{"model":"cn:hy3","messages":[]}`, "max", "max", false},
		{"client wins", `{"model":"cn:hy3","messages":[],"reasoning_effort":"low"}`, "max", "low", false},
		{"no manual", `{"model":"cn:hy3","messages":[]}`, "", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var gotBody []byte
			up := &upstream.Client{
				HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					gotBody, _ = io.ReadAll(r.Body)
					return &http.Response{
						StatusCode: 200,
						Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
						Body:       io.NopCloser(strings.NewReader(sseOK)),
					}, nil
				})},
				ChatBaseCN:    "https://fake.example",
				BillingBaseCN: "https://fake.example",
			}
			store := modelmeta.New("")
			if c.manual != "" {
				store.SetEffort("cn", "hy3", c.manual)
			}
			h := NewHandler(Config{
				Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
				Upstream:  up,
				ModelMeta: store,
			})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(c.body)))
			if rec.Code != 200 {
				t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
			}
			var sent map[string]any
			if err := json.Unmarshal(gotBody, &sent); err != nil {
				t.Fatalf("outbound body not json: %v (%s)", err, gotBody)
			}
			got, _ := sent["reasoning_effort"].(string)
			if c.wantMissing && got != "" {
				t.Errorf("reasoning_effort = %q, want absent", got)
			}
			if !c.wantMissing && got != c.wantEffort {
				t.Errorf("reasoning_effort = %q, want %q", got, c.wantEffort)
			}
			// realm 前缀必须已剥离（上游不认 "cn:hy3"）。
			if m, _ := sent["model"].(string); m != "hy3" {
				t.Errorf("outbound model = %q, want hy3 (bare)", m)
			}
		})
	}
}

// TestManualEffortNotSetWithoutMeta ModelMeta 为 nil 时档位注入完全不生效（零回归）。
func TestManualEffortNotSetWithoutMeta(t *testing.T) {
	var gotBody []byte
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotBody, _ = io.ReadAll(r.Body)
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(sseOK)),
			}, nil
		})},
		ChatBaseCN:    "https://fake.example",
		BillingBaseCN: "https://fake.example",
	}
	h := NewHandler(Config{Pool: testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}), Upstream: up})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"hy3","messages":[]}`)))
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	var sent map[string]any
	_ = json.Unmarshal(gotBody, &sent)
	if _, ok := sent["reasoning_effort"]; ok {
		t.Errorf("nil ModelMeta must not inject effort: %s", gotBody)
	}
}

// ensure time import used
var _ = time.Second

// TestChatModelBlockedReturns404 上游 11102「该后端无此模型」必须报 404 model_not_found。
//
// 回归保护：此前该错误掉进默认分支被报成 503 no_healthy_account —— 用户以为
// "账号全挂了"，客户端还会不停换号重试（换号对幽灵模型毫无作用）。
// 幽灵模型的正确语义是「这个模型不存在」，客户端应立刻换模型而不是重试。
func TestChatModelBlockedReturns404(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 400, `{"code":11102,"msg":"model [auto-chat] service info not found","requestId":"r1"}`, false
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"cn:auto-chat","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404 (body=%s)", rec.Code, rec.Body)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	e, _ := resp["error"].(map[string]any)
	if e == nil {
		t.Fatalf("no error object: %s", rec.Body)
	}
	if e["code"] != "model_not_found" {
		t.Errorf("error.code = %v, want model_not_found", e["code"])
	}
	if !strings.Contains(fmt.Sprint(e["message"]), "auto-chat") {
		t.Errorf("message should name the model, got %v", e["message"])
	}
	// 404 不是「稍后重试」语义：不得带 Retry-After（否则客户端会退避重打）。
	if ra := rec.Header().Get("Retry-After"); ra != "" {
		t.Errorf("404 must not carry Retry-After, got %q", ra)
	}
}
