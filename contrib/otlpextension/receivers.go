package main

import (
	"compress/gzip"
	"context"
	"crypto/subtle"
	"io"
	"net/http"
	"strings"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxBody bounds one OTLP/HTTP request after decompression.
const maxBody = 32 << 20

// tokenOK compares a presented bearer token with the configured one. An empty
// configured token means the receiver is open.
func tokenOK(want, got string) bool {
	if want == "" {
		return true
	}
	got = strings.TrimPrefix(got, "Bearer ")
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}

// ------------------------------------------------------------- OTLP/gRPC

type logsReceiver struct {
	l *Learner
	collogs.UnimplementedLogsServiceServer
}

func (g *logsReceiver) Export(_ context.Context, r *collogs.ExportLogsServiceRequest) (*collogs.ExportLogsServiceResponse, error) {
	g.l.Logs(r)
	return &collogs.ExportLogsServiceResponse{}, nil
}

type metricsReceiver struct {
	l *Learner
	colmetrics.UnimplementedMetricsServiceServer
}

func (g *metricsReceiver) Export(_ context.Context, r *colmetrics.ExportMetricsServiceRequest) (*colmetrics.ExportMetricsServiceResponse, error) {
	g.l.Metrics(r)
	return &colmetrics.ExportMetricsServiceResponse{}, nil
}

type traceReceiver struct {
	l *Learner
	coltrace.UnimplementedTraceServiceServer
}

func (g *traceReceiver) Export(_ context.Context, r *coltrace.ExportTraceServiceRequest) (*coltrace.ExportTraceServiceResponse, error) {
	g.l.Traces(r)
	return &coltrace.ExportTraceServiceResponse{}, nil
}

func newGRPCServer(l *Learner, token string) *grpc.Server {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxBody),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if token != "" {
				md, _ := metadata.FromIncomingContext(ctx)
				v := ""
				if a := md.Get("authorization"); len(a) > 0 {
					v = a[0]
				}
				if !tokenOK(token, v) {
					return nil, status.Error(codes.Unauthenticated, "bad or missing bearer token")
				}
			}
			return h(ctx, req)
		}),
	)
	coltrace.RegisterTraceServiceServer(srv, &traceReceiver{l: l})
	collogs.RegisterLogsServiceServer(srv, &logsReceiver{l: l})
	colmetrics.RegisterMetricsServiceServer(srv, &metricsReceiver{l: l})
	return srv
}

// ------------------------------------------------------------- OTLP/HTTP

// newOTLPHTTP serves POST /v1/traces, /v1/logs and /v1/metrics in protobuf or
// JSON, optionally gzip-compressed, as the Collector's otlphttp exporter
// sends them.
func newOTLPHTTP(l *Learner, token string) http.Handler {
	mux := http.NewServeMux()
	handle := func(path string, newReq func() proto.Message, newResp func() proto.Message, apply func(proto.Message)) {
		mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
			if !tokenOK(token, r.Header.Get("Authorization")) {
				w.Header().Set("WWW-Authenticate", "Bearer")
				http.Error(w, "bad or missing bearer token", http.StatusUnauthorized)
				return
			}
			var body io.Reader = http.MaxBytesReader(w, r.Body, maxBody)
			if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
				zr, err := gzip.NewReader(body)
				if err != nil {
					http.Error(w, "bad gzip body", http.StatusBadRequest)
					return
				}
				defer zr.Close()
				body = io.LimitReader(zr, maxBody)
			}
			raw, err := io.ReadAll(body)
			if err != nil {
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			req := newReq()
			ct := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
			switch ct {
			case "application/json":
				if raw, err = fixOTLPJSONIDs(raw); err == nil {
					err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, req)
				}
			case "application/x-protobuf", "application/protobuf", "":
				err = proto.Unmarshal(raw, req)
				ct = "application/x-protobuf"
			default:
				http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
				return
			}
			if err != nil {
				http.Error(w, "cannot decode request: "+err.Error(), http.StatusBadRequest)
				return
			}
			apply(req)

			var out []byte
			if ct == "application/json" {
				out, _ = protojson.Marshal(newResp())
			} else {
				out, _ = proto.Marshal(newResp())
			}
			w.Header().Set("Content-Type", ct)
			_, _ = w.Write(out)
		})
	}
	handle("/v1/traces",
		func() proto.Message { return &coltrace.ExportTraceServiceRequest{} },
		func() proto.Message { return &coltrace.ExportTraceServiceResponse{} },
		func(m proto.Message) { l.Traces(m.(*coltrace.ExportTraceServiceRequest)) })
	handle("/v1/logs",
		func() proto.Message { return &collogs.ExportLogsServiceRequest{} },
		func() proto.Message { return &collogs.ExportLogsServiceResponse{} },
		func(m proto.Message) { l.Logs(m.(*collogs.ExportLogsServiceRequest)) })
	handle("/v1/metrics",
		func() proto.Message { return &colmetrics.ExportMetricsServiceRequest{} },
		func() proto.Message { return &colmetrics.ExportMetricsServiceResponse{} },
		func(m proto.Message) { l.Metrics(m.(*colmetrics.ExportMetricsServiceRequest)) })
	return mux
}
