package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// newAdmin serves the control surface a product UI calls:
//
//	GET  /status                          counters, queue depth, tenant switches
//	GET  /tenants                         explicit switches and the default
//	PUT  /tenants/{id}   {"enabled":true} the "customer enables CortexDB" switch
//	POST /resolve   {"tenant","fingerprint","note"}   record how an incident was fixed
//	POST /suggest   {"tenant","service","query"}      earlier incidents, fixes, graph facts
//	GET  /healthz
//
// Everything except /healthz needs the admin bearer token when one is set.
// With no token, bind it to loopback (the default address does).
func newAdmin(l *Learner, w *Writer, t *Tenants, token string) http.Handler {
	mux := http.NewServeMux()
	guard := func(h http.HandlerFunc) http.HandlerFunc {
		return func(rw http.ResponseWriter, r *http.Request) {
			if !tokenOK(token, r.Header.Get("Authorization")) {
				rw.Header().Set("WWW-Authenticate", "Bearer")
				writeJSON(rw, http.StatusUnauthorized, map[string]string{"error": "bad or missing bearer token"})
				return
			}
			h(rw, r)
		}
	}

	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, _ *http.Request) {
		writeJSON(rw, http.StatusOK, map[string]bool{"ok": true})
	})

	mux.HandleFunc("GET /status", guard(func(rw http.ResponseWriter, _ *http.Request) {
		explicit, def := t.Snapshot()
		writeJSON(rw, http.StatusOK, map[string]any{
			"received": map[string]int64{
				"spans": l.Stats.Spans.Load(), "logs": l.Stats.Logs.Load(), "metric_points": l.Stats.Points.Load(),
				"skipped_disabled_or_capped": l.Stats.Skipped.Load(),
			},
			"learned": map[string]int64{"incidents_written": l.Stats.Incidents.Load(), "metric_anomalies": l.Stats.Anomalies.Load()},
			"cortexdb_writes": map[string]any{
				"queued": w.QueueLen(), "done": w.Done.Load(), "failed": w.Failed.Load(), "dropped": w.Dropped.Load(),
			},
			"default_enabled": def, "tenants": explicit,
		})
	}))

	mux.HandleFunc("GET /tenants", guard(func(rw http.ResponseWriter, _ *http.Request) {
		explicit, def := t.Snapshot()
		list := make([]map[string]any, 0, len(explicit))
		for _, k := range sortedKeys(explicit) {
			list = append(list, map[string]any{"id": k, "enabled": explicit[k]})
		}
		writeJSON(rw, http.StatusOK, map[string]any{"default_enabled": def, "tenants": list})
	}))

	mux.HandleFunc("PUT /tenants/{id}", guard(func(rw http.ResponseWriter, r *http.Request) {
		id := sanitizeTenant(r.PathValue("id"))
		var body struct {
			Enabled *bool `json:"enabled"`
		}
		if err := decode(r, &body); err != nil || body.Enabled == nil {
			writeJSON(rw, http.StatusBadRequest, map[string]string{"error": `body must be {"enabled": true|false}`})
			return
		}
		if err := t.Set(id, *body.Enabled); err != nil {
			writeJSON(rw, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(rw, http.StatusOK, map[string]any{"id": id, "enabled": *body.Enabled})
	}))

	mux.HandleFunc("POST /resolve", guard(func(rw http.ResponseWriter, r *http.Request) {
		var body struct{ Tenant, Fingerprint, Note, Text string }
		if err := decode(r, &body); err != nil || strings.TrimSpace(body.Fingerprint) == "" {
			writeJSON(rw, http.StatusBadRequest, map[string]string{"error": "tenant, fingerprint and note are required"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if err := l.Resolve(ctx, firstNonEmpty(body.Tenant, l.cfg.DefaultTenant), body.Fingerprint, body.Note, body.Text); err != nil {
			writeJSON(rw, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(rw, http.StatusOK, map[string]bool{"ok": true})
	}))

	mux.HandleFunc("POST /suggest", guard(func(rw http.ResponseWriter, r *http.Request) {
		var body struct{ Tenant, Service, Query string }
		if err := decode(r, &body); err != nil {
			writeJSON(rw, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		resp, err := l.Suggest(ctx, firstNonEmpty(body.Tenant, l.cfg.DefaultTenant), body.Service, body.Query)
		if err != nil {
			writeJSON(rw, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(rw, http.StatusOK, resp)
	}))
	return mux
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
