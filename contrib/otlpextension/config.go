package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is read from BRIDGE_* environment variables. Every field has a
// default that works for a local trial; only the CortexDB URL and token
// normally need setting.
type Config struct {
	GRPCAddr  string // OTLP/gRPC receiver
	HTTPAddr  string // OTLP/HTTP receiver
	AdminAddr string // enable/disable + status; keep on loopback or set AdminToken

	CortexURL   string // base URL of cortexdb-grpc's REST API (CORTEXDB_HTTP_ADDR)
	CortexToken string // bearer token (CORTEXDB_GRPC_TOKEN)

	TenantAttr     string   // resource attribute naming the customer
	DefaultTenant  string   // used when the attribute is absent
	DefaultEnabled bool     // tenants not listed explicitly are on/off
	StateFile      string   // persists the per-tenant switches across restarts
	AdminToken     string   // bearer token required by the admin API when set
	ReceiverToken  string   // bearer token the Collector must send to the OTLP receivers; empty = open
	MetricAllow    []string // metric name prefixes to learn baselines for; empty = every metric

	SlowSpan        time.Duration // spans at least this long are "slow" incidents
	MinLogSeverity  int32         // OTLP severity number; 13 = WARN, 17 = ERROR
	Cooldown        time.Duration // an incident is rewritten at most this often
	MemoryTTL       time.Duration // how long incident memories live; 0 = forever
	IncludeSamples  bool          // store raw log/status text (may hold personal data)
	MaxIncidents    int           // distinct fingerprints tracked in memory
	EdgeRefresh     time.Duration // a service-to-service edge is re-sent at most this often
	MetricWarmup    int           // points seen before a baseline can flag anything
	MetricZ         float64       // deviation, in standard deviations, that counts as anomalous
	MaxMetricSeries int           // distinct metric series tracked in memory

	QueueSize int // pending CortexDB writes; excess is dropped, never blocks the Collector
	Workers   int // concurrent CortexDB writers
}

func loadConfig() (Config, error) {
	c := Config{
		GRPCAddr:        env("BRIDGE_GRPC_ADDR", "0.0.0.0:4317"),
		HTTPAddr:        env("BRIDGE_HTTP_ADDR", "0.0.0.0:4318"),
		AdminAddr:       env("BRIDGE_ADMIN_ADDR", "127.0.0.1:4319"),
		CortexURL:       strings.TrimRight(env("BRIDGE_CORTEXDB_URL", "http://127.0.0.1:8080"), "/"),
		CortexToken:     env("BRIDGE_CORTEXDB_TOKEN", os.Getenv("CORTEXDB_GRPC_TOKEN")),
		TenantAttr:      env("BRIDGE_TENANT_ATTR", "tenant.id"),
		DefaultTenant:   env("BRIDGE_DEFAULT_TENANT", "default"),
		StateFile:       env("BRIDGE_STATE_FILE", ""),
		AdminToken:      env("BRIDGE_ADMIN_TOKEN", ""),
		ReceiverToken:   env("BRIDGE_RECEIVER_TOKEN", ""),
		MinLogSeverity:  13,
		MetricZ:         4,
		MetricWarmup:    30,
		MaxIncidents:    50000,
		MaxMetricSeries: 10000,
		QueueSize:       4096,
		Workers:         4,
	}
	var err error
	parse := func(name string, set func(string) error) {
		if v := os.Getenv(name); v != "" && err == nil {
			if e := set(v); e != nil {
				err = fmt.Errorf("%s=%q: %w", name, v, e)
			}
		}
	}
	dur := func(dst *time.Duration, def time.Duration) func(string) error {
		*dst = def
		return func(v string) (e error) { *dst, e = time.ParseDuration(v); return }
	}
	num := func(dst *int) func(string) error {
		return func(v string) (e error) { *dst, e = strconv.Atoi(v); return }
	}

	for _, p := range strings.Split(os.Getenv("BRIDGE_METRIC_ALLOW"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			c.MetricAllow = append(c.MetricAllow, p)
		}
	}
	parse("BRIDGE_DEFAULT_ENABLED", func(v string) (e error) { c.DefaultEnabled, e = strconv.ParseBool(v); return })
	parse("BRIDGE_INCLUDE_SAMPLES", func(v string) (e error) { c.IncludeSamples, e = strconv.ParseBool(v); return })
	parse("BRIDGE_MIN_LOG_SEVERITY", func(v string) error { c.MinLogSeverity = severityFromName(v); return nil })
	parse("BRIDGE_METRIC_Z", func(v string) (e error) { c.MetricZ, e = strconv.ParseFloat(v, 64); return })
	parse("BRIDGE_METRIC_WARMUP", num(&c.MetricWarmup))
	parse("BRIDGE_MAX_INCIDENTS", num(&c.MaxIncidents))
	parse("BRIDGE_MAX_METRIC_SERIES", num(&c.MaxMetricSeries))
	parse("BRIDGE_QUEUE_SIZE", num(&c.QueueSize))
	parse("BRIDGE_WORKERS", num(&c.Workers))

	c.SlowSpan, c.Cooldown, c.MemoryTTL, c.EdgeRefresh = 2*time.Second, 5*time.Minute, 30*24*time.Hour, time.Hour
	parse("BRIDGE_SLOW_SPAN", dur(&c.SlowSpan, c.SlowSpan))
	parse("BRIDGE_COOLDOWN", dur(&c.Cooldown, c.Cooldown))
	parse("BRIDGE_MEMORY_TTL", dur(&c.MemoryTTL, c.MemoryTTL))
	parse("BRIDGE_EDGE_REFRESH", dur(&c.EdgeRefresh, c.EdgeRefresh))
	if err != nil {
		return c, err
	}
	if c.Workers < 1 || c.QueueSize < 1 {
		return c, fmt.Errorf("BRIDGE_WORKERS and BRIDGE_QUEUE_SIZE must be at least 1")
	}
	return c, nil
}

func env(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

// severityFromName maps a level name to its OTLP severity number; a number is
// taken as is.
func severityFromName(s string) int32 {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TRACE":
		return 1
	case "DEBUG":
		return 5
	case "INFO":
		return 9
	case "WARN", "WARNING":
		return 13
	case "ERROR":
		return 17
	case "FATAL":
		return 21
	}
	if n, err := strconv.Atoi(s); err == nil {
		return int32(n)
	}
	return 13
}
