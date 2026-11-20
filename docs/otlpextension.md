# otlpextension

Receives OTLP **traces, logs and metrics** from an OpenTelemetry Collector and
teaches CortexDB from them. It runs **beside** your existing exporter
(Instana), not instead of it.

```
Apps ─OTLP─▶ OTel Collector ─┬─▶ Instana          (unchanged)
                             └─▶ otlpextension ─REST─▶ cortexdb-grpc
```

It is a standalone Go service, so OpenTelemetry and gRPC stay out of the
CortexDB module itself.

## What it learns

Raw spans, log lines and metric points are **not** stored. The bridge reduces
them first:

| Input | Rule | Written to CortexDB |
| --- | --- | --- |
| Traces | A parent span in another service is a call into this one. A client span with `peer.service`, `db.system` or `messaging.system` is a call to that peer. | Entities (`service`, `database`, `queue`) and relations (`calls`, `uses`): the dependency map. |
| Traces | A span with error status, or one at least `BRIDGE_SLOW_SPAN` long. | One memory per distinct incident, graded `verified`, with `trace_id`. |
| Logs | Records at `BRIDGE_MIN_LOG_SEVERITY` or above (default WARN). | One memory per message template. Ids, numbers, IPs and quoted values are replaced, so "payment 4711 declined" and "payment 9999 declined" are one incident. |
| Metrics | A running mean and deviation per series. A reading beyond `BRIDGE_METRIC_Z` deviations (default 4), after `BRIDGE_METRIC_WARMUP` readings, is flagged. Cumulative counters are differenced first. | One memory per anomaly, graded `asserted` (a statistical deviation is not a checked fault). A shift that lasts 10 readings becomes the new baseline. |
| Any new incident | Recall in CortexDB for earlier similar incidents and recorded fixes. | `similar_to` and `known_fixes` on the new memory, and an `INFO seen before` log line. |

An incident that repeats is rewritten (running count, last seen) at most once
per `BRIDGE_COOLDOWN`, not once per occurrence.

### Learning from fixes

```bash
curl -X POST localhost:4319/resolve -H "Authorization: Bearer $BRIDGE_ADMIN_TOKEN" \
  -d '{"tenant":"acme","fingerprint":"<from the memory metadata>","note":"raised pool size to 50"}'
```

The fix is stored with the incident's own wording. The next time a similar
incident appears, recall finds it and attaches it as `known_fixes`.

### Asking what is known

```bash
curl -X POST localhost:4319/suggest -H "Authorization: Bearer $BRIDGE_ADMIN_TOKEN" \
  -d '{"tenant":"acme","service":"checkout","query":"connection pool exhausted"}'
```

Returns the recall response: earlier incidents, fixes, graph facts and a
ready-made context pack for an LLM or a UI.

## Enabling it per customer

Tenants are identified by a resource attribute (`tenant.id` by default,
`BRIDGE_TENANT_ATTR` to change it). Telemetry from a tenant that is switched
off is received, acknowledged and discarded.

```bash
# the switch your UI calls
curl -X PUT localhost:4319/tenants/acme -H "Authorization: Bearer $BRIDGE_ADMIN_TOKEN" \
  -d '{"enabled": true}'
curl localhost:4319/tenants -H "Authorization: Bearer $BRIDGE_ADMIN_TOKEN"
curl localhost:4319/status  -H "Authorization: Bearer $BRIDGE_ADMIN_TOKEN"
```

Set `BRIDGE_STATE_FILE` so switches survive a restart. `BRIDGE_DEFAULT_ENABLED`
decides tenants that were never set (default off).

Each tenant gets its own CortexDB namespace, `otel-<tenant>`, and its own
entity ids, so one customer's incidents never appear in another's recall.

## Collector config

Add one exporter and list it beside the Instana one:

```yaml
exporters:
  otlp/cortexdb:
    endpoint: otlpextension:4317
    tls: { insecure: true }              # or configure TLS
    headers: { authorization: "Bearer ${env:BRIDGE_RECEIVER_TOKEN}" }
    sending_queue: { enabled: true }
    retry_on_failure: { enabled: true }
  # otlphttp/cortexdb works too: endpoint http://otlpextension:4318

processors:
  resource/tenant:                       # if the tenant is not already an attribute
    attributes:
      - { key: tenant.id, value: acme, action: upsert }

service:
  pipelines:
    traces:  { receivers: [otlp], processors: [resource/tenant, batch], exporters: [otlp/instana, otlp/cortexdb] }
    logs:    { receivers: [otlp], processors: [resource/tenant, batch], exporters: [otlp/instana, otlp/cortexdb] }
    metrics: { receivers: [otlp], processors: [resource/tenant, batch], exporters: [otlp/instana, otlp/cortexdb] }
```

A bridge that is down or slow cannot hurt the Instana path: it has its own
exporter queue, and the bridge itself never blocks the Collector (see below).

## Run

### Container

```bash
docker build -t otlpextension:local .

docker run -d --name otlpextension -v otlpextension-data:/data \
  -p 4317:4317 -p 4318:4318 -p 127.0.0.1:4319:4319 \
  -e BRIDGE_CORTEXDB_URL=http://cortexdb:8080 -e BRIDGE_CORTEXDB_TOKEN=... \
  -e BRIDGE_RECEIVER_TOKEN=... -e BRIDGE_ADMIN_TOKEN=... \
  otlpextension:local
```

The image is a static binary on Alpine (about 32 MB on disk, 9 MB compressed) running as a non-root
user. `/data` holds the tenant switches (`BRIDGE_STATE_FILE=/data/tenants.json`),
so mount a volume there or a restart turns every customer back off. The
healthcheck is the binary itself (`otlpextension -health`), which probes the
admin port. Publish the admin port on loopback only, as above, unless it is
behind your own authentication.

If CortexDB runs on the Docker host, use `http://host.docker.internal:8080`
for `BRIDGE_CORTEXDB_URL`; if both are containers, put them on one network and
use the service name.

### Binary

```bash
# CortexDB, with REST on
CORTEXDB_HTTP_ADDR=127.0.0.1:8080 CORTEXDB_GRPC_TOKEN=... cortexdb-grpc

# the bridge
go build -o otlpextension .
BRIDGE_CORTEXDB_URL=http://127.0.0.1:8080 BRIDGE_CORTEXDB_TOKEN=... \
BRIDGE_RECEIVER_TOKEN=... BRIDGE_ADMIN_TOKEN=... BRIDGE_STATE_FILE=./tenants.json \
./otlpextension
```

## Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `BRIDGE_GRPC_ADDR` / `BRIDGE_HTTP_ADDR` | `0.0.0.0:4317` / `0.0.0.0:4318` | OTLP/gRPC and OTLP/HTTP (protobuf or JSON, gzip ok) |
| `BRIDGE_ADMIN_ADDR` | `127.0.0.1:4319` | enable/disable, status, resolve, suggest |
| `BRIDGE_CORTEXDB_URL` / `BRIDGE_CORTEXDB_TOKEN` | `http://127.0.0.1:8080` / `$CORTEXDB_GRPC_TOKEN` | CortexDB REST API (`CORTEXDB_HTTP_ADDR` on the server) |
| `BRIDGE_RECEIVER_TOKEN` | empty (open) | bearer token the Collector must send |
| `BRIDGE_ADMIN_TOKEN` | empty (open) | bearer token for the admin API |
| `BRIDGE_TENANT_ATTR` / `BRIDGE_DEFAULT_TENANT` | `tenant.id` / `default` | where the customer id comes from |
| `BRIDGE_DEFAULT_ENABLED` / `BRIDGE_STATE_FILE` | `false` / none | tenants never set, and persistence of the switches |
| `BRIDGE_SLOW_SPAN` | `2s` | spans this long are slow incidents (`0` = off) |
| `BRIDGE_MIN_LOG_SEVERITY` | `WARN` | name or OTLP severity number |
| `BRIDGE_METRIC_Z` / `BRIDGE_METRIC_WARMUP` | `4` / `30` | anomaly threshold and readings before a baseline can flag |
| `BRIDGE_METRIC_ALLOW` | all | comma-separated metric name prefixes to learn |
| `BRIDGE_COOLDOWN` | `5m` | an incident is rewritten at most this often |
| `BRIDGE_MEMORY_TTL` | `720h` | how long incident memories live (`0` = forever) |
| `BRIDGE_EDGE_REFRESH` | `1h` | a dependency edge is re-sent at most this often |
| `BRIDGE_INCLUDE_SAMPLES` | `false` | also store one raw message per incident |
| `BRIDGE_QUEUE_SIZE` / `BRIDGE_WORKERS` | `4096` / `4` | pending CortexDB writes and writers |
| `BRIDGE_MAX_INCIDENTS` / `BRIDGE_MAX_METRIC_SERIES` | `50000` / `10000` | in-memory caps |

## Behaviour worth knowing

- **Never blocks the Collector.** The receiver acknowledges a batch as soon as
  it is classified. CortexDB writes happen on a bounded queue; when it is full
  the write is dropped and counted (`/status` → `cortexdb_writes.dropped`).
- **Personal data.** Messages are reduced to templates, so ids, numbers,
  addresses and quoted values are not stored. Free text can still contain
  personal data; `BRIDGE_INCLUDE_SAMPLES` is off for that reason.
- **Memory grades.** Incidents seen in telemetry are `verified` (with the trace
  id as evidence). Metric anomalies are `asserted`. Recall can filter on
  `min_grade`.
- **Recall without an embedder.** Matching works on keywords. Set an embedding
  endpoint on `cortexdb-grpc` (`OPENAI_BASE_URL`) and the same recall becomes
  semantic, so differently worded errors match. The bridge needs no change.
- **State is per process.** Counts, cooldowns and baselines are in memory and
  restart empty; the incident memories themselves live in CortexDB. Run one
  bridge replica per Collector group, or key Collectors to bridges by tenant.
- **A span whose parent arrived in an earlier batch** does not produce a
  service-to-service edge on its own; edges come from spans whose parent is in
  the same batch, plus client spans with a peer attribute. Use the Collector's
  `batch` processor (or tail sampling) so a trace's spans travel together.

## Tests

```bash
go vet ./... && go test -race ./...
```

The tests use a fake CortexDB. They check the bridge against the CortexDB API
as read from `pkg/httpapi` and `pkg/cortexdb`, not against a running server.
