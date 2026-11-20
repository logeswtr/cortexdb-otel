package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Learner turns telemetry into things worth remembering. Raw spans, log lines
// and metric points never reach CortexDB; what does is
//
//   - the service dependency graph (services, databases and queues as
//     entities, "calls"/"uses" as relations),
//   - one memory per distinct incident (error span, slow span, WARN/ERROR log
//     template, metric anomaly), rewritten with a running count, and
//   - for each new incident, a recall of earlier similar incidents and the
//     fixes recorded for them.
type Learner struct {
	cfg     Config
	sink    Sink
	w       *Writer
	tenants *Tenants
	log     *slog.Logger
	now     func() time.Time

	mu        sync.Mutex
	incidents map[string]*incState
	edges     map[string]time.Time
	baselines map[string]*baseline

	Stats struct {
		Spans, Logs, Points, Incidents, Anomalies, Skipped atomic.Int64
	}
}

func NewLearner(cfg Config, sink Sink, w *Writer, t *Tenants, log *slog.Logger) *Learner {
	return &Learner{
		cfg: cfg, sink: sink, w: w, tenants: t, log: log, now: time.Now,
		incidents: map[string]*incState{},
		edges:     map[string]time.Time{},
		baselines: map[string]*baseline{},
	}
}

type incState struct {
	count               int
	first, sent         time.Time
	looked              bool
	similar, fixes      []string
	text, service, kind string
}

type incident struct {
	tenant, service, kind, fp string
	text, query               string
	importance                float64
	grade, why                string
	meta                      map[string]any
	entities                  []Entity
	relations                 []Relation
}

func namespaceFor(tenant string) string { return "otel-" + tenant }
func memoryID(tenant, fp string) string { return "otel:" + tenant + ":" + fp }

// resource resolves a Resource to its tenant and service, and reports whether
// that tenant is switched on.
func (l *Learner) resource(res *resourcepb.Resource) (tenant, service string, ok bool) {
	attrs := res.GetAttributes()
	tenant = sanitizeTenant(attrStr(attrs, l.cfg.TenantAttr))
	if tenant == "" {
		tenant = sanitizeTenant(l.cfg.DefaultTenant)
	}
	if !l.tenants.Enabled(tenant) {
		return "", "", false
	}
	service = strings.TrimSpace(attrStr(attrs, "service.name"))
	if service == "" {
		service = "unknown"
	}
	return tenant, service, true
}

// ---------------------------------------------------------------- traces

type graphBatch struct {
	entities  map[string]Entity
	relations map[string]Relation
	marks     []string
}

func newBatch() *graphBatch {
	return &graphBatch{entities: map[string]Entity{}, relations: map[string]Relation{}}
}

type spanRef struct{ tenant, service string }

func (l *Learner) Traces(req *coltrace.ExportTraceServiceRequest) {
	type enabled struct {
		tenant, service string
		rs              *tracepb.ResourceSpans
	}
	var list []enabled
	owners := map[string]spanRef{}
	for _, rs := range req.GetResourceSpans() {
		tenant, service, ok := l.resource(rs.GetResource())
		if !ok {
			l.Stats.Skipped.Add(1)
			continue
		}
		list = append(list, enabled{tenant, service, rs})
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				owners[string(sp.GetTraceId())+string(sp.GetSpanId())] = spanRef{tenant, service}
			}
		}
	}

	batches := map[string]*graphBatch{}
	batch := func(tenant string) *graphBatch {
		b := batches[tenant]
		if b == nil {
			b = newBatch()
			batches[tenant] = b
		}
		return b
	}

	for _, e := range list {
		l.addService(batch(e.tenant), e.tenant, e.service, "service")
		for _, ss := range e.rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				l.Stats.Spans.Add(1)

				// A parent in another service is a call into this one. A
				// parent that arrived in an earlier batch is not seen here;
				// the edge is picked up from any later span whose parent is
				// in the same batch.
				if pid := sp.GetParentSpanId(); len(pid) > 0 {
					if p, ok := owners[string(sp.GetTraceId())+string(pid)]; ok && p.tenant == e.tenant && p.service != e.service {
						l.addEdge(batch(e.tenant), e.tenant, p.service, "service", e.service, "service", "calls")
					}
				}
				if sp.GetKind() == tracepb.Span_SPAN_KIND_CLIENT {
					if name, typ, rel := peerOf(sp.GetAttributes()); name != "" && name != e.service {
						l.addEdge(batch(e.tenant), e.tenant, e.service, "service", name, typ, rel)
					}
				}

				dur := time.Duration(int64(sp.GetEndTimeUnixNano()) - int64(sp.GetStartTimeUnixNano()))
				switch {
				case sp.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR:
					l.record(l.errorIncident(e.tenant, e.service, sp, dur))
				case dur >= l.cfg.SlowSpan && l.cfg.SlowSpan > 0:
					l.record(l.slowIncident(e.tenant, e.service, sp, dur))
				}
			}
		}
	}
	l.flush(batches)
}

// peerOf names what a client span called, from the semantic-convention
// attributes: another service, a database or a message broker.
func peerOf(attrs []*commonpb.KeyValue) (name, typ, rel string) {
	if v := attrStr(attrs, "peer.service"); v != "" {
		return v, "service", "calls"
	}
	if sys := attrStr(attrs, "db.system"); sys != "" {
		if db := attrStr(attrs, "db.name"); db != "" {
			return sys + ":" + db, "database", "uses"
		}
		return sys, "database", "uses"
	}
	if sys := attrStr(attrs, "messaging.system"); sys != "" {
		return sys, "queue", "uses"
	}
	return "", "", ""
}

func entityOf(tenant, name, typ string) Entity {
	return Entity{ID: entityKey(tenant, name), Name: name, Type: typ, Metadata: map[string]string{"tenant": tenant}}
}

func (l *Learner) addService(b *graphBatch, tenant, name, typ string) {
	if key := "s|" + tenant + "|" + name; l.due(key, b) {
		b.entities[entityKey(tenant, name)] = entityOf(tenant, name, typ)
	}
}

func (l *Learner) addEdge(b *graphBatch, tenant, from, fromTyp, to, toTyp, rel string) {
	key := "e|" + tenant + "|" + from + "|" + rel + "|" + to
	if !l.due(key, b) {
		return
	}
	b.entities[entityKey(tenant, from)] = entityOf(tenant, from, fromTyp)
	b.entities[entityKey(tenant, to)] = entityOf(tenant, to, toTyp)
	b.relations[key] = Relation{
		From: entityKey(tenant, from), To: entityKey(tenant, to), Type: rel, Weight: 1,
		Metadata: map[string]string{"tenant": tenant, "source": "otlpextension"},
	}
}

// due reports whether key has not been sent within EdgeRefresh, and marks it
// sent. The mark goes on the batch so a failed write can clear it.
func (l *Learner) due(key string, b *graphBatch) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.edges[key]; ok && now.Sub(t) < l.cfg.EdgeRefresh {
		return false
	}
	if len(l.edges) >= l.cfg.MaxIncidents*4 {
		l.edges = map[string]time.Time{} // worst case: edges are re-sent once
	}
	l.edges[key] = now
	b.marks = append(b.marks, key)
	return true
}

func (l *Learner) unmark(keys []string) {
	l.mu.Lock()
	for _, k := range keys {
		delete(l.edges, k)
	}
	l.mu.Unlock()
}

func (l *Learner) flush(batches map[string]*graphBatch) {
	for _, b := range batches {
		if len(b.entities) == 0 && len(b.relations) == 0 {
			continue
		}
		b := b
		ents := make([]Entity, 0, len(b.entities))
		for _, e := range b.entities {
			ents = append(ents, e)
		}
		rels := make([]Relation, 0, len(b.relations))
		for _, r := range b.relations {
			rels = append(rels, r)
		}
		ok := l.w.Submit(func(ctx context.Context) error {
			// Entities first: CortexDB rejects an edge whose endpoint node
			// does not exist yet.
			if err := l.sink.UpsertEntities(ctx, ents); err != nil {
				l.unmark(b.marks)
				return err
			}
			if len(rels) > 0 {
				if err := l.sink.UpsertRelations(ctx, rels); err != nil {
					l.unmark(b.marks)
					return err
				}
			}
			return nil
		})
		if !ok {
			l.unmark(b.marks)
		}
	}
}

func (l *Learner) errorIncident(tenant, service string, sp *tracepb.Span, dur time.Duration) incident {
	var excType, excMsg string
	for _, ev := range sp.GetEvents() {
		if ev.GetName() == "exception" {
			excType = attrStr(ev.GetAttributes(), "exception.type")
			excMsg = attrStr(ev.GetAttributes(), "exception.message")
			break
		}
	}
	msg := excMsg
	if msg == "" {
		msg = sp.GetStatus().GetMessage()
	}
	tmpl := Template(msg)
	fp := Fingerprint(tenant, service, "error", sp.GetName(), excType, tmpl)

	what := firstNonEmpty(excType, "error")
	text := fmt.Sprintf("Error in service %s, operation %q: %s", service, sp.GetName(), what)
	if tmpl != "" {
		text += ": " + tmpl
	}
	if code := firstNonEmpty(attrStr(sp.GetAttributes(), "http.response.status_code"), attrStr(sp.GetAttributes(), "http.status_code")); code != "" {
		text += ". HTTP status " + code
	}
	text += ". Duration " + dur.Round(time.Millisecond).String() + "."

	meta := map[string]any{
		"trace_id":  hex.EncodeToString(sp.GetTraceId()),
		"span_id":   hex.EncodeToString(sp.GetSpanId()),
		"operation": sp.GetName(),
		"template":  tmpl,
	}
	if l.cfg.IncludeSamples && msg != "" {
		meta["sample"] = truncate(msg, 500)
	}
	errEnt := Entity{
		ID: entityKey(tenant, "err-"+fp), Name: truncate(what+" in "+sp.GetName(), 80), Type: "error",
		Metadata: map[string]string{"tenant": tenant},
	}
	return incident{
		tenant: tenant, service: service, kind: "error", fp: fp, text: text,
		query:      strings.Join([]string{service, sp.GetName(), excType, tmpl}, " "),
		importance: 0.7, grade: "verified", why: "observed in telemetry, trace " + hex.EncodeToString(sp.GetTraceId()),
		meta:      meta,
		entities:  []Entity{entityOf(tenant, service, "service"), errEnt},
		relations: []Relation{{From: entityKey(tenant, service), To: errEnt.ID, Type: "has_error", Weight: 1}},
	}
}

func (l *Learner) slowIncident(tenant, service string, sp *tracepb.Span, dur time.Duration) incident {
	fp := Fingerprint(tenant, service, "slow", sp.GetName())
	return incident{
		tenant: tenant, service: service, kind: "slow", fp: fp,
		text: fmt.Sprintf("Slow operation in service %s: %q took %s (threshold %s).",
			service, sp.GetName(), dur.Round(time.Millisecond), l.cfg.SlowSpan),
		query:      service + " " + sp.GetName() + " slow latency",
		importance: 0.5, grade: "verified", why: "measured span duration, trace " + hex.EncodeToString(sp.GetTraceId()),
		meta: map[string]any{
			"trace_id": hex.EncodeToString(sp.GetTraceId()), "span_id": hex.EncodeToString(sp.GetSpanId()),
			"operation": sp.GetName(), "duration_ms": dur.Milliseconds(),
		},
		entities: []Entity{entityOf(tenant, service, "service")},
	}
}

// ------------------------------------------------------------------ logs

func (l *Learner) Logs(req *collogs.ExportLogsServiceRequest) {
	for _, rl := range req.GetResourceLogs() {
		tenant, service, ok := l.resource(rl.GetResource())
		if !ok {
			l.Stats.Skipped.Add(1)
			continue
		}
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				l.Stats.Logs.Add(1)
				sev := int32(lr.GetSeverityNumber())
				if sev == 0 && lr.GetSeverityText() != "" {
					sev = severityFromName(lr.GetSeverityText())
				}
				if sev < l.cfg.MinLogSeverity {
					continue
				}
				body := bodyText(lr.GetBody())
				if body == "" {
					continue
				}
				l.record(l.logIncident(tenant, service, lr, sev, body))
			}
		}
	}
}

func (l *Learner) logIncident(tenant, service string, lr *logspb.LogRecord, sev int32, body string) incident {
	level := "WARN"
	imp := 0.4
	if sev >= 17 {
		level, imp = "ERROR", 0.7
	}
	tmpl := Template(body)
	fp := Fingerprint(tenant, service, "log", tmpl)
	meta := map[string]any{"template": tmpl, "level": level}
	if len(lr.GetTraceId()) > 0 {
		meta["trace_id"] = hex.EncodeToString(lr.GetTraceId())
	}
	if l.cfg.IncludeSamples {
		meta["sample"] = truncate(body, 500)
	}
	why := "observed in logs"
	if t, ok := meta["trace_id"].(string); ok {
		why += ", trace " + t
	}
	return incident{
		tenant: tenant, service: service, kind: "log", fp: fp,
		text:       fmt.Sprintf("%s log in service %s: %s", level, service, tmpl),
		query:      service + " " + tmpl,
		importance: imp, grade: "verified", why: why, meta: meta,
		entities: []Entity{entityOf(tenant, service, "service")},
	}
}

// bodyText reads a log body: a string as is, or the message field of a
// structured body.
func bodyText(v *commonpb.AnyValue) string {
	if s, ok := v.GetValue().(*commonpb.AnyValue_StringValue); ok {
		return strings.TrimSpace(s.StringValue)
	}
	if kv := v.GetKvlistValue(); kv != nil {
		for _, key := range []string{"message", "msg", "error", "err"} {
			if s := strings.TrimSpace(attrStr(kv.GetValues(), key)); s != "" {
				return s
			}
		}
	}
	return ""
}

// --------------------------------------------------------------- metrics

// baseline is a running mean and variance (Welford) for one metric series,
// plus the previous cumulative reading when the series needs differencing.
type baseline struct {
	n         int
	mean, m2  float64
	consec    int
	prevSum   float64
	prevCount uint64
	hasPrev   bool
}

// adoptAfter is how many anomalous readings in a row make the new level the
// baseline: a real shift (a deploy, a scale-out) stops alerting.
const adoptAfter = 10

func (b *baseline) observe(x float64, warmup int, zLimit float64) (anomalous bool, mean, std, z float64) {
	if b.n >= warmup && b.n > 1 {
		std = math.Sqrt(b.m2 / float64(b.n-1))
		if floor := math.Max(math.Abs(b.mean)*0.05, 1e-9); std < floor {
			std = floor
		}
		mean, z = b.mean, (x-b.mean)/std
		if math.Abs(z) >= zLimit {
			b.consec++
			if b.consec >= adoptAfter {
				*b = baseline{n: 1, mean: x}
			}
			return true, mean, std, z
		}
	}
	b.consec = 0
	b.n++
	d := x - b.mean
	b.mean += d / float64(b.n)
	b.m2 += d * (x - b.mean)
	return false, 0, 0, 0
}

func (l *Learner) Metrics(req *colmetrics.ExportMetricsServiceRequest) {
	for _, rm := range req.GetResourceMetrics() {
		tenant, service, ok := l.resource(rm.GetResource())
		if !ok {
			l.Stats.Skipped.Add(1)
			continue
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if !l.metricAllowed(m.GetName()) {
					continue
				}
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					for _, dp := range d.Gauge.GetDataPoints() {
						l.point(tenant, service, m, dp.GetAttributes(), m.GetName(), numberValue(dp), dp.GetTimeUnixNano(), false, false)
					}
				case *metricspb.Metric_Sum:
					cumulative := d.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
					for _, dp := range d.Sum.GetDataPoints() {
						l.point(tenant, service, m, dp.GetAttributes(), m.GetName(), numberValue(dp), dp.GetTimeUnixNano(),
							d.Sum.GetIsMonotonic() && cumulative, false)
					}
				case *metricspb.Metric_Histogram:
					cumulative := d.Histogram.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
					for _, dp := range d.Histogram.GetDataPoints() {
						if dp.Sum == nil {
							continue
						}
						l.histogramPoint(tenant, service, m, dp, cumulative)
					}
				}
			}
		}
	}
}

func (l *Learner) metricAllowed(name string) bool {
	if len(l.cfg.MetricAllow) == 0 {
		return true
	}
	for _, p := range l.cfg.MetricAllow {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func numberValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return math.NaN()
}

func seriesKey(tenant, service, name string, attrs []*commonpb.KeyValue) string {
	parts := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		parts = append(parts, kv.GetKey()+"="+attrStr(attrs, kv.GetKey()))
	}
	sort.Strings(parts)
	return tenant + "|" + service + "|" + name + "|" + strings.Join(parts, ",")
}

// baselineFor finds or creates the baseline of a series, or nil when the
// series cap is reached.
func (l *Learner) baselineFor(key string) *baseline {
	b := l.baselines[key]
	if b == nil {
		if len(l.baselines) >= l.cfg.MaxMetricSeries {
			return nil
		}
		b = &baseline{}
		l.baselines[key] = b
	}
	return b
}

func (l *Learner) point(tenant, service string, m *metricspb.Metric, attrs []*commonpb.KeyValue, name string, x float64, _ uint64, differenced, _ bool) {
	l.Stats.Points.Add(1)
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return
	}
	l.mu.Lock()
	b := l.baselineFor(seriesKey(tenant, service, name, attrs))
	if b == nil {
		l.mu.Unlock()
		return
	}
	if differenced { // cumulative counter: learn the per-interval increase
		prev, had := b.prevSum, b.hasPrev
		b.prevSum, b.hasPrev = x, true
		if !had || x < prev {
			l.mu.Unlock()
			return
		}
		x -= prev
	}
	anomalous, mean, std, z := b.observe(x, l.cfg.MetricWarmup, l.cfg.MetricZ)
	l.mu.Unlock()
	if anomalous {
		l.anomaly(tenant, service, name, m.GetUnit(), x, mean, std, z)
	}
}

func (l *Learner) histogramPoint(tenant, service string, m *metricspb.Metric, dp *metricspb.HistogramDataPoint, cumulative bool) {
	l.Stats.Points.Add(1)
	sum, count := dp.GetSum(), dp.GetCount()
	name := m.GetName() + " (mean)"
	l.mu.Lock()
	b := l.baselineFor(seriesKey(tenant, service, name, dp.GetAttributes()))
	if b == nil {
		l.mu.Unlock()
		return
	}
	if cumulative {
		pSum, pCount, had := b.prevSum, b.prevCount, b.hasPrev
		b.prevSum, b.prevCount, b.hasPrev = sum, count, true
		if !had || count <= pCount || sum < pSum {
			l.mu.Unlock()
			return
		}
		sum, count = sum-pSum, count-pCount
	}
	if count == 0 {
		l.mu.Unlock()
		return
	}
	x := sum / float64(count)
	if math.IsNaN(x) || math.IsInf(x, 0) {
		l.mu.Unlock()
		return
	}
	anomalous, mean, std, z := b.observe(x, l.cfg.MetricWarmup, l.cfg.MetricZ)
	l.mu.Unlock()
	if anomalous {
		l.anomaly(tenant, service, name, m.GetUnit(), x, mean, std, z)
	}
}

func (l *Learner) anomaly(tenant, service, name, unit string, x, mean, std, z float64) {
	l.Stats.Anomalies.Add(1)
	dir := "high"
	if z < 0 {
		dir = "low"
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'g', 4, 64) }
	l.record(incident{
		tenant: tenant, service: service, kind: "metric", fp: Fingerprint(tenant, service, "metric", name, dir),
		text: fmt.Sprintf("Metric %s on service %s is unusually %s: %s %s against a baseline of %s ± %s (%.1f standard deviations).",
			name, service, dir, f(x), unit, f(mean), f(std), math.Abs(z)),
		query:      service + " " + name + " " + dir,
		importance: 0.6, grade: "asserted", why: "statistical deviation from a learned baseline, not a checked fault",
		meta:     map[string]any{"metric": name, "value": x, "baseline_mean": mean, "baseline_std": std, "z": z},
		entities: []Entity{entityOf(tenant, service, "service")},
	})
}

// ------------------------------------------------------------- incidents

func (l *Learner) record(inc incident) {
	key := inc.tenant + "|" + inc.fp
	now := l.now()

	l.mu.Lock()
	st := l.incidents[key]
	if st == nil {
		if len(l.incidents) >= l.cfg.MaxIncidents {
			l.evictLocked(now)
		}
		if len(l.incidents) >= l.cfg.MaxIncidents {
			l.mu.Unlock()
			l.Stats.Skipped.Add(1)
			return
		}
		st = &incState{first: now, text: inc.text, service: inc.service, kind: inc.kind}
		l.incidents[key] = st
	}
	st.count++
	st.text = inc.text
	if !st.sent.IsZero() && now.Sub(st.sent) < l.cfg.Cooldown {
		l.mu.Unlock()
		return
	}
	st.sent = now
	count, first, looked := st.count, st.first, st.looked
	similar, fixes := st.similar, st.fixes
	l.mu.Unlock()

	failed := func() {
		l.mu.Lock()
		st.sent = time.Time{}
		l.mu.Unlock()
	}
	ok := l.w.Submit(func(ctx context.Context) error {
		if !looked {
			similar, fixes = l.lookup(ctx, inc)
			l.mu.Lock()
			st.looked, st.similar, st.fixes = true, similar, fixes
			l.mu.Unlock()
			if len(similar)+len(fixes) > 0 {
				l.log.Info("seen before", "tenant", inc.tenant, "service", inc.service, "kind", inc.kind,
					"fingerprint", inc.fp, "similar", similar, "known_fixes", len(fixes))
			}
		}
		if err := l.sink.SaveMemory(ctx, l.memoryReq(inc, count, first, now, similar, fixes)); err != nil {
			failed()
			return err
		}
		l.Stats.Incidents.Add(1)
		return nil
	})
	if !ok {
		failed()
	}
}

// evictLocked drops incidents that have been quiet for twice the cooldown.
func (l *Learner) evictLocked(now time.Time) {
	for k, st := range l.incidents {
		if now.Sub(st.sent) > 2*l.cfg.Cooldown {
			delete(l.incidents, k)
		}
	}
}

// lookup asks CortexDB for earlier incidents like inc and for fixes recorded
// against them. It is best effort: a failed recall must not stop the incident
// from being stored.
func (l *Learner) lookup(ctx context.Context, inc incident) (similar, fixes []string) {
	resp, err := l.sink.Recall(ctx, RecallReq{
		Query: inc.query, Scope: "global", Namespace: namespaceFor(inc.tenant),
		TopKMemories: 5, DisableKnowledge: true,
	})
	if err != nil {
		l.log.Debug("recall failed", "err", err)
		return nil, nil
	}
	own := memoryID(inc.tenant, inc.fp)
	for _, h := range resp.Memories {
		id := h.Memory.ID
		if id == own || !(h.Coverage >= 0.5 || h.Similarity >= 0.8) {
			continue
		}
		if strings.Contains(id, ":resolution:") {
			fixes = append(fixes, truncate(h.Memory.Content, 300))
		} else {
			similar = append(similar, id)
		}
	}
	return similar, fixes
}

func (l *Learner) memoryReq(inc incident, count int, first, last time.Time, similar, fixes []string) MemoryReq {
	content := fmt.Sprintf("%s\nOccurrences: %d (first seen %s, last seen %s).",
		inc.text, count, first.UTC().Format(time.RFC3339), last.UTC().Format(time.RFC3339))
	// The keys "kind", "scope" and "namespace" are CortexDB's own, so the
	// incident's kind is stored as incident_kind.
	meta := map[string]any{
		"source": "otlpextension", "tenant": inc.tenant, "service": inc.service, "incident_kind": inc.kind,
		"fingerprint": inc.fp, "count": count,
		"first_seen": first.UTC().Format(time.RFC3339), "last_seen": last.UTC().Format(time.RFC3339),
	}
	for k, v := range inc.meta {
		meta[k] = v
	}
	if len(similar) > 0 {
		meta["similar_to"] = similar
	}
	if len(fixes) > 0 {
		meta["known_fixes"] = fixes
	}
	return MemoryReq{
		MemoryID: memoryID(inc.tenant, inc.fp), Scope: "global", Namespace: namespaceFor(inc.tenant),
		Content: content, Metadata: meta, Importance: inc.importance,
		TTLSeconds: int(l.cfg.MemoryTTL / time.Second),
		Grade:      inc.grade, GradeWhy: inc.why,
		Entities: inc.entities, Relations: inc.relations,
	}
}

// Resolve records how an incident was fixed. The note is stored together with
// the incident's own wording, so a later recall for the same kind of incident
// finds the fix. text is used when the incident is no longer in memory.
func (l *Learner) Resolve(ctx context.Context, tenant, fp, note, text string) error {
	tenant = sanitizeTenant(tenant)
	l.mu.Lock()
	st := l.incidents[tenant+"|"+fp]
	var service string
	if st != nil {
		service = st.service
		if text == "" {
			text = st.text
		}
	}
	l.mu.Unlock()
	if strings.TrimSpace(note) == "" {
		return fmt.Errorf("note is required")
	}
	if text == "" {
		return fmt.Errorf("incident %s is not known to this process; pass its text", fp)
	}
	return l.sink.SaveMemory(ctx, MemoryReq{
		MemoryID: "otel:" + tenant + ":resolution:" + fp, Scope: "global", Namespace: namespaceFor(tenant),
		Content:    "Fix for: " + text + "\nResolution: " + strings.TrimSpace(note),
		Metadata:   map[string]any{"source": "otlpextension", "tenant": tenant, "fingerprint": fp, "service": service, "incident_kind": "resolution"},
		Importance: 0.9, Grade: "verified", GradeWhy: "recorded by an operator",
	})
}

// Suggest answers "what do we know about this service or problem": earlier
// incidents, fixes, and the graph facts around the service.
func (l *Learner) Suggest(ctx context.Context, tenant, service, query string) (*RecallResp, error) {
	tenant = sanitizeTenant(tenant)
	req := RecallReq{
		Query: firstNonEmpty(query, service+" incident error"), Scope: "global", Namespace: namespaceFor(tenant),
		TopKMemories: 8, DisableKnowledge: true,
	}
	if service != "" {
		req.EntityNames = []string{entityKey(tenant, service)}
	}
	return l.sink.Recall(ctx, req)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
