package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// upstreamModelsRequestCount 统计假上游被请求的次数。
var testUpstreamModelsHits atomic.Int32

// modelsMockUpstream 返回一个按 hits 计数的假 get_detail_param 上游。
func modelsMockUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testUpstreamModelsHits.Add(1)
		if !strings.HasSuffix(r.URL.Path, EpModels) {
			http.Error(w, "wrong path "+r.URL.Path, 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// 返回一个上游模型 + 一个 __max 变体（验证 modelsFromUpstream 展开）
		_, _ = w.Write([]byte(`{"config_info_list":[
			{"config_name":"glm-5.2","display_config":{"display_name":"GLM-5.2","max_mode":true},
			 "display_contact_config":"{\"consumption_rate\":{\"data\":{\"rate\":2.0}}}",
			 "context_window_tokens":{"dev":131072,"max":200000},
			 "reasoning_effort_config":{"support_thinking":true,"default_level":"medium","options":["low","medium","high"]},
			 "model_detail_list":[
			   {"model_name":"glm-5.2__dev","prompt_max_tokens":100000,"max_tokens":8192},
			   {"model_name":"glm-5.2__max","prompt_max_tokens":180000,"max_tokens":16384}]}
		]}`))
	}))
	t.Cleanup(func() {
		srv.Close()
		dynamicModelsCache.Store(nil)
	})
	return srv
}

func TestModelForAuthSyncsFromUpstream(t *testing.T) {
	modelsTrackingEnabled()
	// 关闭自动后台探测是全局的（TestMain 已关），这里显式打开以便驱动 refreshDynamicModels
	srv := modelsMockUpstream(t)
	orig := defaultClient.Load()
	fake := &Client{HTTP: srv.Client(), AgentHost: srv.URL, UgHost: srv.URL, OAuthHost: srv.URL, ClientID: ClientID}
	setDefaultClient(fake)
	t.Cleanup(func() { setDefaultClient(orig) })

	// 首次 model.for_auth：无缓存 → 返回静态 + 后台触发 refresh
	testUpstreamModelsHits.Store(0)
	body, _ := json.Marshal(pluginapi.AuthModelRequest{StorageJSON: []byte(storageOf())})
	raw, err := handleMethodGuarded("model.for_auth", body)
	if err != nil {
		t.Fatalf("model.for_auth: %v", err)
	}
	var resp pluginapi.ModelResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw), &resp)
	if resp.Provider != providerName {
		t.Fatalf("provider=%q", resp.Provider)
	}
	// 无缓存命中 → 首查回退静态（后台异步，尚未写缓存）
	if len(resp.Models) != len(staticModelIDs) {
		t.Fatalf("expected static fallback on first call, got %d", len(resp.Models))
	}

	// 后台刷新：直接以同步形式驱动 probeAndStore 等价物 → refreshDynamicModels
	sa, _ := parseStored(storageOf())
	refreshDynamicModels(sa)

	// 现在缓存命中了动态表（含 -max 变体）
	body2, _ := json.Marshal(pluginapi.AuthModelRequest{StorageJSON: []byte(storageOf())})
	raw2, _ := handleMethodGuarded("model.for_auth", body2)
	var resp2 pluginapi.ModelResponse
	_ = json.Unmarshal(mustDecodeResult(t, raw2), &resp2)
	foundDev, foundMax := false, false
	for _, m := range resp2.Models {
		if m.ID == "glm-5.2" {
			foundDev = true
			if m.ContextLength != 131072 {
				t.Errorf("glm-5.2 context=%d", m.ContextLength)
			}
			if m.Thinking == nil || len(m.Thinking.Levels) != 3 {
				t.Errorf("glm-5.2 thinking=%+v", m.Thinking)
			}
		}
		if m.ID == "glm-5.2-max" {
			foundMax = true
			if m.ContextLength != 200000 {
				t.Errorf("glm-5.2-max context=%d", m.ContextLength)
			}
		}
	}
	if !foundDev || !foundMax {
		t.Fatalf("missing dynamic model(s): dev=%v max=%v ; models=%d", foundDev, foundMax, len(resp2.Models))
	}
	// 缓存命中不应再打上游
	if testUpstreamModelsHits.Load() != 1 {
		t.Errorf("upstream hits=%d want 1 (cache should short-circuit)", testUpstreamModelsHits.Load())
	}
}

func TestModelForAuthNegativeCacheNoRepeatedFetch(t *testing.T) {
	testUpstreamModelsHits.Store(0)
	// 直接写一个失败缓存
	storeDynamicModelsFail()
	m, ok := cachedDynamicModels()
	if !ok || m != nil {
		t.Fatalf("fresh negative cache should hit with empty models: ok=%v m=%v", ok, m)
	}
	sa, _ := parseStored(storageOf())
	fetchDynamicModels(sa) // 不应发起任何上游请求（负缓存 TTL 内不算 miss）
	if got := testUpstreamModelsHits.Load(); got != 0 {
		t.Errorf("negative cache should prevent upstream fetch, got %d hits", got)
	}
	dynamicModelsCache.Store(nil)
}

func TestRefreshDynamicModelsRejectsEmptyList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"config_info_list":[]}`))
	}))
	defer srv.Close()
	orig := defaultClient.Load()
	fake := &Client{HTTP: srv.Client(), AgentHost: srv.URL, UgHost: srv.URL, OAuthHost: srv.URL, ClientID: ClientID}
	setDefaultClient(fake)
	defer setDefaultClient(orig)
	defer dynamicModelsCache.Store(nil)

	sa, _ := parseStored(storageOf())
	refreshDynamicModels(sa)
	e := dynamicModelsCache.Load()
	if e == nil || e.ok {
		t.Fatalf("empty list should record negative cache, got ok=%v e=%+v", e == nil || e.ok, e)
	}
	t.Log("empty list → negative cache recorded")
}

func TestRefreshDynamicModelsErrorNow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"err":"boom"}`))
	}))
	defer srv.Close()
	orig := defaultClient.Load()
	fake := &Client{HTTP: srv.Client(), AgentHost: srv.URL, UgHost: srv.URL, OAuthHost: srv.URL, ClientID: ClientID}
	setDefaultClient(fake)
	defer setDefaultClient(orig)
	defer dynamicModelsCache.Store(nil)

	sa, _ := parseStored(storageOf())
	refreshDynamicModels(sa)
	e := dynamicModelsCache.Load()
	if e == nil || e.ok {
		t.Fatalf("error should record negative cache, got ok=%v", e == nil || e.ok)
	}
	t.Log("error → negative cache recorded")
}

func TestCachedDynamicModelsTTL(t *testing.T) {
	dynamicModelsCache.Store(&dynamicModelsEntry{models: []pluginapi.ModelInfo{{ID: "m"}}, fetched: time.Now(), ok: true})
	if m, ok := cachedDynamicModels(); !ok || len(m) != 1 {
		t.Fatalf("fresh cache should hit: ok=%v len=%d", ok, len(m))
	}
	// 过期成功缓存 → miss
	dynamicModelsCache.Store(&dynamicModelsEntry{models: []pluginapi.ModelInfo{{ID: "m"}}, fetched: time.Now().Add(-(dynamicModelsSuccessTTL + time.Minute)), ok: true})
	if _, ok := cachedDynamicModels(); ok {
		t.Fatal("expired success cache should miss")
	}
	// 未过期负缓存 → hit（但不返回数据）
	dynamicModelsCache.Store(&dynamicModelsEntry{fetched: time.Now(), ok: false})
	m, ok := cachedDynamicModels()
	if !ok || m != nil {
		t.Fatalf("fresh negative cache should hit: ok=%v m=%v", ok, m)
	}
	dynamicModelsCache.Store(nil)
}

// TestConsumptionRateCarried 验证 consumption_rate 元数据在有对应字段时被带上。
// pluginapi.ModelInfo 无 consumption_rate 字段，因此该值不落入宿主模型——本测试
// 确认解析层（client.FetchModels）仍能取到该值，动态表沿用消费倍率不丢。
func TestConsumptionRateCarriedIntoUpstream(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"config_info_list":[
			{"config_name":"glm-5.3","display_config":{"display_name":"GLM-5.3"},
			 "display_contact_config":"{\"consumption_rate\":{\"data\":{\"rate\":3.0}}}",
			 "context_window_tokens":{"dev":131072},
			 "model_detail_list":[{"model_name":"glm-5.3__dev","prompt_max_tokens":100000,"max_tokens":8192}]}
		]}`), nil
	})
	infos, err := c.FetchModels(fakeAuth())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(infos) != 1 || infos[0].ConsumptionRate != 3.0 {
		t.Fatalf("consumption rate not parsed: %+v", infos)
	}
}