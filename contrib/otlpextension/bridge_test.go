package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------- fakes

type fakeSink struct {
	mu        sync.Mutex
	memories  []MemoryReq
	entities  []Entity
	relations []Relation
	recalls   []RecallReq
	recallRes *RecallResp
	failMem   bool
}

func (f *fakeSink) SaveMemory(_ context.Context, r MemoryReq) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failMem {
		return io.ErrUnexpectedEOF
	}
	f.memories = append(f.memories, r)
	return nil
}
func (f *fakeSink) UpsertEntities(_ context.Context, e []Entity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entities = append(f.entities, e...)
	return nil
}
func (f *fakeSink) UpsertRelations(_ context.Context, r []Relation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.relations = append(f.relations, r...)
	return nil
}
func (f *fakeSink) Recall(_ context.Context, r RecallReq) (*RecallResp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recalls = append(f.recalls, r)
	if f.recallRes != nil {
		return f.recallRes, nil
	}
	return &RecallResp{}, nil
}

func (f *fakeSink) mems() []MemoryReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]MemoryReq(nil), f.memories...)
}

func (f *fakeSink) waitMems(t *testing.T, n int) []MemoryReq {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m := f.mems(); len(m) >= n {
			return m
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("want %d memories, have %d", n, len(f.mems()))
	return nil
}

func (f *fakeSink) waitRelations(t *testing.T, n int) []Relation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		r := append([]Relation(nil), f.relations...)
		f.mu.Unlock()
		if len(r) >= n {
			return r
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("want %d relations", n)
	return nil
}

func testCfg() Config {
	return Config{
		TenantAttr: "tenant.id", DefaultTenant: "acme", DefaultEnabled: true,
		SlowSpan: 2 * time.Second, MinLogSeverity: 13, Cooldown: time.Minute, MemoryTTL: time.Hour,
		MaxIncidents: 100, EdgeRefresh: time.Hour, MetricWarmup: 10, MetricZ: 4, MaxMetricSeries: 100,
		QueueSize: 64, Workers: 1,
	}
}

func newTestLearner(cfg Config) (*Learner, *fakeSink, *Writer) {
	sink := &fakeSink{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := NewWriter(cfg.QueueSize, cfg.Workers, log)
	t, _ := NewTenants(cfg.DefaultEnabled, "")
	return NewLearner(cfg, sink, w, t, log), sink, w
}

// ------------------------------------------------------------ builders

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func res(service, tenant string) *resourcepb.Resource {
	attrs := []*commonpb.KeyValue{str("service.name", service)}
	if tenant != "" {
		attrs = append(attrs, str("tenant.id", tenant))
	}
	return &resourcepb.Resource{Attributes: attrs}
}

func span(id byte, parent byte, name string, d time.Duration, code tracepb.Status_StatusCode, msg string) *tracepb.Span {
	s := &tracepb.Span{
		TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{id}, 8), Name: name,
		StartTimeUnixNano: 1_000_000_000, EndTimeUnixNano: 1_000_000_000 + uint64(d),
		Status: &tracepb.Status{Code: code, Message: msg},
	}
	if parent != 0 {
		s.ParentSpanId = bytes.Repeat([]byte{parent}, 8)
	}
	return s
}

func traceReq(rs ...*tracepb.ResourceSpans) *coltrace.ExportTraceServiceRequest {
	return &coltrace.ExportTraceServiceRequest{ResourceSpans: rs}
}

func rspans(r *resourcepb.Resource, spans ...*tracepb.Span) *tracepb.ResourceSpans {
	return &tracepb.ResourceSpans{Resource: r, ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}}}
}

// ---------------------------------------------------------------- tests

func TestTemplateCollapsesVariableParts(t *testing.T) {
	a := Template(`timeout after 5012ms calling 10.0.3.7:5432 for user "alice" id 550e8400-e29b-41d4-a716-446655440000`)
	b := Template(`timeout after 87ms calling 10.0.9.1:5432 for user "bob" id 123e4567-e89b-12d3-a456-426614174000`)
	if a != b {
		t.Fatalf("templates differ:\n%s\n%s", a, b)
	}
	if strings.Contains(a, "alice") || strings.Contains(a, "10.0") {
		t.Fatalf("variable parts leaked: %s", a)
	}
	if Fingerprint("x", a) != Fingerprint("x", b) || Fingerprint("a", "b") == Fingerprint("ab", "") {
		t.Fatal("fingerprint must be stable and separator-safe")
	}
}

func TestErrorSpanBecomesOneMemoryWithGraph(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())

	sp := span(2, 0, "POST /pay", 300*time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, "pool exhausted after 8200ms")
	l.Traces(traceReq(rspans(res("checkout", "acme"), sp)))

	m := sink.waitMems(t, 1)[0]
	if m.MemoryID == "" || !strings.HasPrefix(m.MemoryID, "otel:acme:") {
		t.Fatalf("memory id %q", m.MemoryID)
	}
	if m.Scope != "global" || m.Namespace != "otel-acme" {
		t.Fatalf("scope/namespace = %q/%q", m.Scope, m.Namespace)
	}
	if m.Grade != "verified" || m.GradeWhy == "" {
		t.Fatalf("grade %q why %q (verified needs evidence)", m.Grade, m.GradeWhy)
	}
	if m.Metadata["trace_id"] != strings.Repeat("01", 16) {
		t.Fatalf("trace id not kept: %v", m.Metadata["trace_id"])
	}
	if _, bad := m.Metadata["kind"]; bad {
		t.Fatal("metadata key 'kind' collides with CortexDB's own")
	}
	if _, has := m.Metadata["sample"]; has {
		t.Fatal("raw text stored without BRIDGE_INCLUDE_SAMPLES")
	}
	if m.TTLSeconds != 3600 {
		t.Fatalf("ttl = %d", m.TTLSeconds)
	}
	if len(m.Entities) != 2 || len(m.Relations) != 1 || m.Relations[0].Type != "has_error" {
		t.Fatalf("graph around incident: %+v %+v", m.Entities, m.Relations)
	}
	// the relation must name entities that are sent in the same call
	ids := map[string]bool{}
	for _, e := range m.Entities {
		ids[e.ID] = true
	}
	if !ids[m.Relations[0].From] || !ids[m.Relations[0].To] {
		t.Fatalf("relation endpoints are not among the entities: %+v", m)
	}
}

func TestSameIncidentRewritesWithinCooldown(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }

	send := func(msg string) {
		l.Traces(traceReq(rspans(res("checkout", "acme"), span(2, 0, "POST /pay", time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, msg))))
	}
	send("timeout after 100ms")
	sink.waitMems(t, 1)
	send("timeout after 250ms") // same template, inside cooldown
	time.Sleep(50 * time.Millisecond)
	if n := len(sink.mems()); n != 1 {
		t.Fatalf("cooldown ignored: %d writes", n)
	}
	now = now.Add(2 * time.Minute)
	send("timeout after 900ms")
	m := sink.waitMems(t, 2)
	if m[1].MemoryID != m[0].MemoryID {
		t.Fatal("same incident must reuse its memory id")
	}
	if m[1].Metadata["count"] != 3 {
		t.Fatalf("count = %v, want 3", m[1].Metadata["count"])
	}
}

func TestSlowSpanAndHealthySpan(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	l.Traces(traceReq(rspans(res("api", "acme"),
		span(2, 0, "GET /fast", 10*time.Millisecond, tracepb.Status_STATUS_CODE_OK, ""),
		span(3, 0, "GET /slow", 3*time.Second, tracepb.Status_STATUS_CODE_OK, ""))))
	m := sink.waitMems(t, 1)
	time.Sleep(30 * time.Millisecond)
	if len(sink.mems()) != 1 || m[0].Metadata["incident_kind"] != "slow" {
		t.Fatalf("want exactly one slow incident, got %+v", sink.mems())
	}
}

func TestServiceMapFromParentChildAndClientSpans(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())

	caller := span(2, 0, "call", time.Millisecond, tracepb.Status_STATUS_CODE_OK, "")
	callee := span(3, 2, "handle", time.Millisecond, tracepb.Status_STATUS_CODE_OK, "")
	db := span(4, 3, "SELECT", time.Millisecond, tracepb.Status_STATUS_CODE_OK, "")
	db.Kind = tracepb.Span_SPAN_KIND_CLIENT
	db.Attributes = []*commonpb.KeyValue{str("db.system", "postgresql"), str("db.name", "orders")}

	l.Traces(traceReq(
		rspans(res("web", "acme"), caller),
		rspans(res("orders", "acme"), callee, db)))

	rels := sink.waitRelations(t, 2)
	got := map[string]bool{}
	for _, r := range rels {
		got[r.From+" "+r.Type+" "+r.To] = true
	}
	for _, want := range []string{"acme__web calls acme__orders", "acme__orders uses acme__postgresql-orders"} {
		if !got[want] {
			t.Fatalf("missing %q in %v", want, got)
		}
	}
}

func TestEdgesAreNotResentWithinRefresh(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	send := func() {
		l.Traces(traceReq(
			rspans(res("web", "acme"), span(2, 0, "c", time.Millisecond, 0, "")),
			rspans(res("orders", "acme"), span(3, 2, "h", time.Millisecond, 0, ""))))
	}
	send()
	sink.waitRelations(t, 1)
	send()
	time.Sleep(50 * time.Millisecond)
	if n := len(sink.waitRelations(t, 1)); n != 1 {
		t.Fatalf("edge re-sent: %d", n)
	}
}

func TestDisabledTenantLearnsNothing(t *testing.T) {
	cfg := testCfg()
	cfg.DefaultEnabled = false
	l, sink, w := newTestLearner(cfg)
	defer w.Close(context.Background())

	bad := span(2, 0, "op", time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, "boom")
	l.Traces(traceReq(rspans(res("svc", "acme"), bad)))
	time.Sleep(50 * time.Millisecond)
	if len(sink.mems()) != 0 {
		t.Fatal("disabled tenant was written")
	}
	if err := l.tenants.Set("acme", true); err != nil {
		t.Fatal(err)
	}
	l.Traces(traceReq(rspans(res("svc", "acme"), bad)))
	sink.waitMems(t, 1)
}

func TestTenantsPersist(t *testing.T) {
	file := t.TempDir() + "/state.json"
	a, _ := NewTenants(false, file)
	if err := a.Set("acme", true); err != nil {
		t.Fatal(err)
	}
	b, err := NewTenants(false, file)
	if err != nil || !b.Enabled("acme") || b.Enabled("other") {
		t.Fatalf("not restored: %v", err)
	}
}

func TestLogsFilterBySeverityAndDedupByTemplate(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	rec := func(sev logspb.SeverityNumber, body string) *logspb.LogRecord {
		return &logspb.LogRecord{
			SeverityNumber: sev, TraceId: bytes.Repeat([]byte{9}, 16),
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
		}
	}
	l.Logs(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: res("billing", "acme"),
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{
			rec(logspb.SeverityNumber_SEVERITY_NUMBER_INFO, "all good"),
			rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "payment 4711 declined for card ending 1234"),
			rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "payment 9999 declined for card ending 5678"),
		}}},
	}}})
	m := sink.waitMems(t, 1)
	time.Sleep(30 * time.Millisecond)
	if len(sink.mems()) != 1 {
		t.Fatalf("want 1 memory, got %d", len(sink.mems()))
	}
	if m[0].Metadata["incident_kind"] != "log" || m[0].Metadata["count"] != 2 && m[0].Metadata["count"] != 1 {
		t.Fatalf("meta %v", m[0].Metadata)
	}
	if strings.Contains(m[0].Content, "4711") || strings.Contains(m[0].Content, "1234") {
		t.Fatalf("raw ids leaked into memory: %s", m[0].Content)
	}
}

func TestBaselineFlagsSpikeAndAdoptsNewLevel(t *testing.T) {
	var b baseline
	for i := 0; i < 50; i++ {
		if bad, _, _, _ := b.observe(100+float64(i%3), 10, 4); bad {
			t.Fatalf("steady value flagged at %d", i)
		}
	}
	if bad, _, _, z := b.observe(500, 10, 4); !bad || z <= 4 {
		t.Fatalf("spike not flagged (z=%v)", z)
	}
	// a lasting shift stops alerting once it has been seen adoptAfter times
	flagged := 0
	for i := 0; i < adoptAfter+5; i++ {
		if bad, _, _, _ := b.observe(500, 10, 4); bad {
			flagged++
		}
	}
	if flagged >= adoptAfter+5 {
		t.Fatal("new level never adopted as the baseline")
	}
}

func TestMetricAnomalyBecomesAssertedMemory(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	send := func(v float64) {
		l.Metrics(&colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: res("api", "acme"),
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
				Name: "queue.depth", Unit: "1",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{
					{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}},
				}}},
			}}}},
		}}})
	}
	for i := 0; i < 30; i++ {
		send(10 + float64(i%2))
	}
	send(900)
	m := sink.waitMems(t, 1)[0]
	if m.Grade != "asserted" || m.Metadata["incident_kind"] != "metric" {
		t.Fatalf("anomaly must be graded asserted, got %q / %v", m.Grade, m.Metadata["incident_kind"])
	}
}

func TestCumulativeCounterIsDifferenced(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	send := func(v int64) {
		l.Metrics(&colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: res("api", "acme"),
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
				Name: "requests",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					IsMonotonic: true, AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					DataPoints: []*metricspb.NumberDataPoint{{Value: &metricspb.NumberDataPoint_AsInt{AsInt: v}}},
				}},
			}}}},
		}}})
	}
	// a steadily growing total is a flat rate of 10 per interval: no anomaly
	for i := 1; i <= 60; i++ {
		send(int64(i * 10))
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(sink.mems()); n != 0 {
		t.Fatalf("a flat rate was flagged %d times (counter not differenced)", n)
	}
}

func TestSeenBeforeIsLookedUpAndRecorded(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	hit := RecallHit{Coverage: 0.9}
	hit.Memory.ID = "otel:acme:resolution:abc"
	hit.Memory.Content = "Fix for: pool exhausted\nResolution: raise pool size to 50"
	old := RecallHit{Coverage: 0.7}
	old.Memory.ID = "otel:acme:deadbeef"
	weak := RecallHit{Coverage: 0.1}
	weak.Memory.ID = "otel:acme:noise"
	sink.recallRes = &RecallResp{Memories: []RecallHit{hit, old, weak}}

	l.Traces(traceReq(rspans(res("checkout", "acme"),
		span(2, 0, "POST /pay", time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, "pool exhausted"))))
	m := sink.waitMems(t, 1)[0]

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.recalls) != 1 || sink.recalls[0].Namespace != "otel-acme" || !sink.recalls[0].DisableKnowledge {
		t.Fatalf("recall request %+v", sink.recalls)
	}
	if fixes, _ := m.Metadata["known_fixes"].([]string); len(fixes) != 1 || !strings.Contains(fixes[0], "raise pool size") {
		t.Fatalf("fix not attached: %v", m.Metadata["known_fixes"])
	}
	if sim, _ := m.Metadata["similar_to"].([]string); len(sim) != 1 || sim[0] != "otel:acme:deadbeef" {
		t.Fatalf("weak match must be left out: %v", m.Metadata["similar_to"])
	}
}

func TestFailedWriteIsRetriedOnNextSighting(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	sink.failMem = true
	send := func() {
		l.Traces(traceReq(rspans(res("svc", "acme"), span(2, 0, "op", time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, "boom"))))
	}
	send()
	deadline := time.Now().Add(2 * time.Second)
	for w.Failed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	sink.mu.Lock()
	sink.failMem = false
	sink.mu.Unlock()
	send() // inside the cooldown, but the first write never landed
	sink.waitMems(t, 1)
}

func TestQueueFullDropsInsteadOfBlocking(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := NewWriter(1, 1, log)
	release := make(chan struct{})
	started := make(chan struct{})
	w.Submit(func(context.Context) error { close(started); <-release; return nil })
	<-started
	w.Submit(func(context.Context) error { return nil }) // fills the queue
	done := make(chan bool)
	go func() { done <- w.Submit(func(context.Context) error { return nil }) }()
	select {
	case ok := <-done:
		if ok || w.Dropped.Load() != 1 {
			t.Fatalf("ok=%v dropped=%d", ok, w.Dropped.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("Submit blocked on a full queue")
	}
	close(release)
	w.Close(context.Background())
}

func TestOTLPHTTPProtobufJSONAndAuth(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	srv := httptest.NewServer(newOTLPHTTP(l, "sekret"))
	defer srv.Close()

	req := traceReq(rspans(res("svc", "acme"), span(2, 0, "op", time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, "boom")))
	body, _ := proto.Marshal(req)

	post := func(token, ct string, b []byte) int {
		r, _ := http.NewRequest("POST", srv.URL+"/v1/traces", bytes.NewReader(b))
		r.Header.Set("Content-Type", ct)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := post("", "application/x-protobuf", body); c != http.StatusUnauthorized {
		t.Fatalf("no token: %d", c)
	}
	if c := post("wrong", "application/x-protobuf", body); c != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", c)
	}
	if c := post("sekret", "text/plain", body); c != http.StatusUnsupportedMediaType {
		t.Fatalf("bad content type: %d", c)
	}
	if c := post("sekret", "application/x-protobuf", []byte("not protobuf")); c != http.StatusBadRequest {
		t.Fatalf("garbage body: %d", c)
	}
	if c := post("sekret", "application/x-protobuf", body); c != http.StatusOK {
		t.Fatalf("protobuf: %d", c)
	}
	sink.waitMems(t, 1)
}

func TestAdminSwitchAndAuth(t *testing.T) {
	cfg := testCfg()
	cfg.DefaultEnabled = false
	l, _, w := newTestLearner(cfg)
	defer w.Close(context.Background())
	srv := httptest.NewServer(newAdmin(l, w, l.tenants, "adm"))
	defer srv.Close()

	do := func(method, path, token, body string) (int, map[string]any) {
		r, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	if c, _ := do("GET", "/status", "", ""); c != http.StatusUnauthorized {
		t.Fatalf("status without token: %d", c)
	}
	if c, _ := do("GET", "/healthz", "", ""); c != http.StatusOK {
		t.Fatalf("healthz must be open: %d", c)
	}
	if c, _ := do("PUT", "/tenants/acme", "adm", `{}`); c != http.StatusBadRequest {
		t.Fatalf("missing enabled: %d", c)
	}
	if l.tenants.Enabled("acme") {
		t.Fatal("tenant on before being enabled")
	}
	if c, _ := do("PUT", "/tenants/acme", "adm", `{"enabled":true}`); c != http.StatusOK {
		t.Fatalf("enable: %d", c)
	}
	if !l.tenants.Enabled("acme") {
		t.Fatal("enable had no effect")
	}
	if c, out := do("GET", "/status", "adm", ""); c != http.StatusOK || out["tenants"].(map[string]any)["acme"] != true {
		t.Fatalf("status: %d %v", c, out)
	}
}

func TestResolveStoresFixUnderTheIncidentsWording(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	l.Traces(traceReq(rspans(res("checkout", "acme"), span(2, 0, "POST /pay", time.Millisecond, tracepb.Status_STATUS_CODE_ERROR, "pool exhausted"))))
	m := sink.waitMems(t, 1)[0]
	fp, _ := m.Metadata["fingerprint"].(string)

	if err := l.Resolve(context.Background(), "acme", fp, "raise pool size to 50", ""); err != nil {
		t.Fatal(err)
	}
	got := sink.mems()[1]
	if got.MemoryID != "otel:acme:resolution:"+fp || !strings.Contains(got.Content, "pool exhausted") || !strings.Contains(got.Content, "raise pool size") {
		t.Fatalf("resolution %+v", got)
	}
	if err := l.Resolve(context.Background(), "acme", "unknown", "x", ""); err == nil {
		t.Fatal("unknown incident without text must be refused")
	}
}

func TestOTLPJSONHexIDsKeepTheirValue(t *testing.T) {
	l, sink, w := newTestLearner(testCfg())
	defer w.Close(context.Background())
	srv := httptest.NewServer(newOTLPHTTP(l, ""))
	defer srv.Close()

	// JSON as the OTLP spec writes it: ids are hex, times are strings.
	body := `{"resourceSpans":[{"resource":{"attributes":[
	  {"key":"service.name","value":{"stringValue":"checkout"}},
	  {"key":"tenant.id","value":{"stringValue":"acme"}}]},
	 "scopeSpans":[{"spans":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708",
	   "name":"POST /pay","startTimeUnixNano":"1000000000","endTimeUnixNano":"1300000000",
	   "status":{"code":2,"message":"boom"}}]}]}]}`
	resp, err := http.Post(srv.URL+"/v1/traces", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	m := sink.waitMems(t, 1)[0]
	if got := m.Metadata["trace_id"]; got != "0102030405060708090a0b0c0d0e0f10" {
		t.Fatalf("trace_id = %v, want the hex id that was sent", got)
	}
	if got := m.Metadata["span_id"]; got != "0102030405060708" {
		t.Fatalf("span_id = %v", got)
	}
}
