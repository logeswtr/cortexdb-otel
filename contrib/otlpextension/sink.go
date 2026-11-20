package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// The structs below mirror the CortexDB REST bodies (pkg/cortexdb json tags).

type Entity struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name"`
	Type        string            `json:"type,omitempty"`
	Description string            `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type Relation struct {
	From     string            `json:"from"`
	To       string            `json:"to"`
	Type     string            `json:"type,omitempty"`
	Weight   float64           `json:"weight,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type MemoryReq struct {
	MemoryID   string         `json:"memory_id"`
	Scope      string         `json:"scope,omitempty"`
	Namespace  string         `json:"namespace,omitempty"`
	Role       string         `json:"role,omitempty"`
	Content    string         `json:"content"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	Importance float64        `json:"importance,omitempty"`
	TTLSeconds int            `json:"ttl_seconds,omitempty"`
	Entities   []Entity       `json:"entities,omitempty"`
	Relations  []Relation     `json:"relations,omitempty"`
	Grade      string         `json:"grade,omitempty"`
	GradeWhy   string         `json:"grade_why,omitempty"`
}

type RecallReq struct {
	Query            string   `json:"query"`
	Scope            string   `json:"scope,omitempty"`
	Namespace        string   `json:"namespace,omitempty"`
	TopKMemories     int      `json:"top_k_memories,omitempty"`
	EntityNames      []string `json:"entity_names,omitempty"`
	DisableKnowledge bool     `json:"disable_knowledge,omitempty"`
}

type RecallHit struct {
	Memory struct {
		ID       string         `json:"id"`
		Content  string         `json:"content"`
		Metadata map[string]any `json:"metadata,omitempty"`
	} `json:"memory"`
	Score      float64 `json:"score"`
	Similarity float64 `json:"similarity,omitempty"`
	Coverage   float64 `json:"coverage,omitempty"`
}

type GraphFact struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
}

type RecallResp struct {
	Memories    []RecallHit `json:"memories,omitempty"`
	GraphFacts  []GraphFact `json:"graph_facts,omitempty"`
	ContextPack struct {
		Text string `json:"text"`
	} `json:"context_pack"`
	Degraded []string `json:"degraded,omitempty"`
}

// Sink is what the learner needs from CortexDB. The REST client implements it;
// tests use a fake.
type Sink interface {
	SaveMemory(ctx context.Context, r MemoryReq) error
	UpsertEntities(ctx context.Context, e []Entity) error
	UpsertRelations(ctx context.Context, r []Relation) error
	Recall(ctx context.Context, r RecallReq) (*RecallResp, error)
}

// RESTSink talks to cortexdb-grpc's REST API (CORTEXDB_HTTP_ADDR).
type RESTSink struct {
	base, token string
	hc          *http.Client
}

func NewRESTSink(base, token string) *RESTSink {
	return &RESTSink{base: base, token: token, hc: &http.Client{Timeout: 15 * time.Second}}
}

func (s *RESTSink) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("cortexdb %s: %s: %s", path, resp.Status, bytes.TrimSpace(raw[:min(len(raw), 300)]))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (s *RESTSink) SaveMemory(ctx context.Context, r MemoryReq) error {
	return s.post(ctx, "/v1/memory", r, nil)
}

func (s *RESTSink) UpsertEntities(ctx context.Context, e []Entity) error {
	return s.post(ctx, "/v1/graph/entities", map[string]any{"entities": e}, nil)
}

func (s *RESTSink) UpsertRelations(ctx context.Context, r []Relation) error {
	return s.post(ctx, "/v1/graph/relations", map[string]any{"relations": r}, nil)
}

func (s *RESTSink) Recall(ctx context.Context, r RecallReq) (*RecallResp, error) {
	var out RecallResp
	if err := s.post(ctx, "/v1/recall", r, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Writer runs CortexDB writes off the receive path. The Collector's exporter
// has already acknowledged the batch by the time a job runs, and a full queue
// drops work rather than waiting: telemetry the bridge cannot keep up with is
// not worth slowing the pipeline that also feeds Instana.
type Writer struct {
	q       chan func(context.Context) error
	wg      sync.WaitGroup
	log     *slog.Logger
	Dropped atomic.Int64
	Failed  atomic.Int64
	Done    atomic.Int64
	lastErr atomic.Int64 // unix nanos of the last logged failure
}

func NewWriter(size, workers int, log *slog.Logger) *Writer {
	w := &Writer{q: make(chan func(context.Context) error, size), log: log}
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for job := range w.q {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := job(ctx); err != nil {
					w.Failed.Add(1)
					w.logFailure(err)
				} else {
					w.Done.Add(1)
				}
				cancel()
			}
		}()
	}
	return w
}

// logFailure logs at most one failure per ten seconds; a CortexDB that is down
// would otherwise write a line per queued job.
func (w *Writer) logFailure(err error) {
	now := time.Now().UnixNano()
	last := w.lastErr.Load()
	if now-last < int64(10*time.Second) || !w.lastErr.CompareAndSwap(last, now) {
		return
	}
	w.log.Warn("cortexdb write failed", "err", err, "failed_total", w.Failed.Load())
}

// Submit queues job, or counts it dropped when the queue is full.
func (w *Writer) Submit(job func(context.Context) error) bool {
	select {
	case w.q <- job:
		return true
	default:
		w.Dropped.Add(1)
		return false
	}
}

func (w *Writer) QueueLen() int { return len(w.q) }

// Close stops accepting work and waits for queued jobs, up to ctx.
func (w *Writer) Close(ctx context.Context) {
	close(w.q)
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}
