// Command otlpextension receives OTLP traces, logs and metrics from an
// OpenTelemetry Collector and teaches CortexDB from them: a service
// dependency graph, one memory per distinct incident, metric baselines, and a
// "seen before" recall for every new incident. It sits beside the Collector's
// existing exporter (Instana, say), not in place of it.
//
// See README.md for the Collector config and the BRIDGE_* settings.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-health" {
		os.Exit(healthCheck())
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)

	cfg, err := loadConfig()
	if err != nil {
		log.Error("bad configuration", "err", err)
		os.Exit(2)
	}
	tenants, err := NewTenants(cfg.DefaultEnabled, cfg.StateFile)
	if err != nil {
		log.Error("cannot read state file", "file", cfg.StateFile, "err", err)
		os.Exit(2)
	}

	writer := NewWriter(cfg.QueueSize, cfg.Workers, log)
	learner := NewLearner(cfg, NewRESTSink(cfg.CortexURL, cfg.CortexToken), writer, tenants, log)

	grpcLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Error("listen otlp/grpc", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}
	grpcSrv := newGRPCServer(learner, cfg.ReceiverToken)
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: newOTLPHTTP(learner, cfg.ReceiverToken), ReadHeaderTimeout: 10 * time.Second}
	adminSrv := &http.Server{Addr: cfg.AdminAddr, Handler: newAdmin(learner, writer, tenants, cfg.AdminToken), ReadHeaderTimeout: 10 * time.Second}

	if cfg.AdminToken == "" {
		log.Warn("BRIDGE_ADMIN_TOKEN is empty: the admin API is open to anyone who can reach it", "addr", cfg.AdminAddr)
	}
	if cfg.ReceiverToken == "" {
		log.Warn("BRIDGE_RECEIVER_TOKEN is empty: the OTLP receivers accept any sender")
	}
	log.Info("otlpextension starting", "otlp_grpc", cfg.GRPCAddr, "otlp_http", cfg.HTTPAddr, "admin", cfg.AdminAddr,
		"cortexdb", cfg.CortexURL, "tenant_attr", cfg.TenantAttr, "default_enabled", cfg.DefaultEnabled)

	errc := make(chan error, 3)
	go func() { errc <- grpcSrv.Serve(grpcLn) }()
	go func() { errc <- httpSrv.ListenAndServe() }()
	go func() { errc <- adminSrv.ListenAndServe() }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-stop:
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server stopped", "err", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	_ = httpSrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
	writer.Close(ctx) // let queued CortexDB writes finish
	log.Info("otlpextension stopped", "written", writer.Done.Load(), "failed", writer.Failed.Load(), "dropped", writer.Dropped.Load())
}

// healthCheck probes the admin /healthz on loopback and returns the process
// exit code. It is what the container HEALTHCHECK runs, so the image needs no
// curl: like cortexdb-grpc, the binary is its own probe.
func healthCheck() int {
	_, port, err := net.SplitHostPort(env("BRIDGE_ADMIN_ADDR", "127.0.0.1:4319"))
	if err != nil {
		return 1
	}
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
