# Run CortexDB + otlpextension locally

`otlpextension` receives OTLP traces, logs and metrics and teaches CortexDB
from them (service dependency map, incident memories, metric baselines,
"seen before" recall). It runs beside your existing exporter (Instana).

```
Apps -> OTel Collector -+-> Instana                (unchanged)
                        +-> otlpextension -REST-> cortexdb-grpc
```

| Port | Service | What |
| --- | --- | --- |
| 8080 | cortexdb | REST API, `/healthz`, `/metrics` |
| 47821 | cortexdb | gRPC (used by the web UI) |
| 4317 | otlpextension | OTLP/gRPC receiver |
| 4318 | otlpextension | OTLP/HTTP receiver |
| 4319 | otlpextension | admin API (loopback only) |

Settings keep the `BRIDGE_*` prefix (see `contrib/otlpextension/README.md`).

## 1. Get the code

```bash
git clone https://github.com/logeswtr/cortexdb-otel.git
cd cortexdb-otel
```

## 2. Build the images

```bash
docker build -f deploy/docker/Dockerfile -t cortexdb-grpc:local .
docker build -t otlpextension:local contrib/otlpextension
```

## 3. Run

Pick two secrets. The values below are for local use only.

```bash
docker network create otel-learn

docker run -d --name cortexdb --network otel-learn \
  -v cortexdb-data:/data \
  -p 127.0.0.1:8080:8080 -p 127.0.0.1:47821:47821 \
  -e CORTEXDB_HTTP_ADDR=0.0.0.0:8080 \
  -e CORTEXDB_GRPC_TOKEN=devtoken \
  cortexdb-grpc:local

docker run -d --name otlpextension --network otel-learn \
  -v otlpextension-data:/data \
  -p 4317:4317 -p 4318:4318 -p 127.0.0.1:4319:4319 \
  -e BRIDGE_CORTEXDB_URL=http://cortexdb:8080 \
  -e BRIDGE_CORTEXDB_TOKEN=devtoken \
  -e BRIDGE_RECEIVER_TOKEN=recvtoken \
  -e BRIDGE_ADMIN_TOKEN=admin \
  otlpextension:local

docker ps        # both should become "healthy" after a few seconds
```

- Without `CORTEXDB_HTTP_ADDR` CortexDB has no REST API and otlpextension
  cannot write to it.
- The `otlpextension-data` volume keeps the per-customer on/off switches
  across restarts.
- Publish 47821 only if you want the web UI (step 5).

## 4. Check that it works

```bash
A='Authorization: Bearer admin'

# both services are up
curl -s localhost:8080/healthz
curl -s localhost:4319/healthz                       # {"ok":true}

# switch a customer on (customers are off by default)
curl -s -X PUT localhost:4319/tenants/acme -H "$A" -d '{"enabled": true}'

# send one failing span (OTLP/JSON)
NOW=$(date +%s)000000000
curl -s -X POST localhost:4318/v1/traces \
  -H 'Authorization: Bearer recvtoken' -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[{"resource":{"attributes":[
        {"key":"service.name","value":{"stringValue":"checkout"}},
        {"key":"tenant.id","value":{"stringValue":"acme"}}]},
      "scopeSpans":[{"spans":[{
        "traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
        "name":"POST /pay","kind":2,
        "startTimeUnixNano":"'$NOW'","endTimeUnixNano":"'$NOW'",
        "status":{"code":2,"message":"connection pool exhausted"}}]}]}]}'
# -> {}   (an empty answer is the success response)

sleep 2
curl -s localhost:4319/status -H "$A"
# expect: "spans":1, "incidents_written":1, "cortexdb_writes":{"done":2,...}

# what CortexDB learned (namespace is otel-<tenant>)
curl -s -X POST localhost:8080/v1/recall \
  -H 'Authorization: Bearer devtoken' -H 'Content-Type: application/json' \
  -d '{"namespace":"otel-acme","scope":"global","query":"connection pool exhausted"}'

# ask through otlpextension (returns earlier incidents, fixes, context pack)
curl -s -X POST localhost:4319/suggest -H "$A" \
  -d '{"tenant":"acme","service":"checkout","query":"connection pool exhausted"}'

# record a fix; the next similar incident gets it as "known_fixes"
curl -s -X POST localhost:4319/resolve -H "$A" \
  -d '{"tenant":"acme","fingerprint":"<from the memory metadata>","note":"raised pool size to 50"}'
```

Wrong token -> `401`. A tenant that is switched off is accepted and discarded, so
`spans` stays 0 and `skipped_disabled_or_capped` grows.

## 5. Web UI (graph view)

Needs Go 1.25+ on the machine and port 47821 published (step 3).

```bash
CORTEXDB_REMOTE=127.0.0.1:47821 CORTEXDB_GRPC_TOKEN=devtoken \
  go run ./cmd/cortexdb-mcp-stdio --graph-3d
```

It prints a URL (default port 46377, loopback only) and opens the browser.
`--port N` changes the port, `--no-open` skips the browser. The graph shows
the services, databases and queues otlpextension learned, with `calls` and
`uses` edges.

## 6. Connect an OpenTelemetry Collector and send test metrics

```
metrics.py --OTLP--> otelcontribcol (:14317) --OTLP--> otlpextension (:4317) --> CortexDB
                     adds tenant.id=acme
```

Steps 1-4 must be done first: both containers running and tenant `acme`
switched on (`PUT /tenants/acme`).

### 6.1 Free the ports

The Collector below listens on 14317, 14318 and 13133; otlpextension on 4317.
Make sure nothing else holds them, such as an older Collector or an old
`otel-bridge` container:

```bash
lsof -nP -iTCP:4317 -iTCP:14317 -iTCP:13133 -sTCP:LISTEN
docker rm -f otel-bridge 2>/dev/null     # old container name, if it exists
```

### 6.2 Build the Collector (contrib)

```bash
git clone https://github.com/open-telemetry/opentelemetry-collector-contrib.git otelcol-contrib-1
cd otelcol-contrib-1
make otelcontribcol          # needs Go (version in go.mod) and make
```

The binary is `./bin/otelcontribcol_darwin_arm64 --config config.yaml`, for example
`otelcontribcol_darwin_arm64`. If you already have a Collector, skip this.

### 6.3 Collector config

Save as `config.yaml` in that folder. The exporter points at otlpextension on
`localhost:4317`; the `resource/tenant` processor stamps every signal with
`tenant.id=acme`.

```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: "0.0.0.0:14317" }
      http: { endpoint: "0.0.0.0:14318" }

processors:
  memory_limiter: { check_interval: 1s, limit_mib: 400 }
  batch: { timeout: 5s, send_batch_size: 1024 }
  resource/tenant:
    attributes:
      - { key: tenant.id, value: acme, action: upsert }

exporters:
  debug: { verbosity: basic }
  otlp/cortexdb:
    endpoint: localhost:4317
    tls: { insecure: true }
    # only if you set BRIDGE_RECEIVER_TOKEN on otlpextension (step 3 does):
    headers: { authorization: "Bearer recvtoken" }

extensions:
  health_check: { endpoint: "0.0.0.0:13133" }

service:
  extensions: [health_check]
  pipelines:
    metrics: { receivers: [otlp], processors: [memory_limiter, resource/tenant, batch], exporters: [debug, otlp/cortexdb] }
    traces:  { receivers: [otlp], processors: [memory_limiter, resource/tenant, batch], exporters: [debug, otlp/cortexdb] }
    logs:    { receivers: [otlp], processors: [memory_limiter, resource/tenant, batch], exporters: [debug, otlp/cortexdb] }
```

Step 3 sets `BRIDGE_RECEIVER_TOKEN=recvtoken`, so the `headers` line is needed.
If you leave that variable unset, drop the line (the receiver is then open and
logs a warning).

For production, add the same exporter beside your Instana exporter in each
pipeline (`exporters: [otlp/instana, otlp/cortexdb]`). Do not remove Instana.

### 6.4 Run the Collector

```bash
./bin/otelcontribcol_darwin_arm64 --config config.yaml
curl -s http://localhost:13133          # in another terminal: health check
```

### 6.5 Send test metrics

`metrics.py` sends synthetic OpenSearch metrics. It sends normal JVM old-gen
memory for `BASELINE_READINGS` exports (one every 5 s), then jumps to about
900 MB so the anomaly rule fires.

```bash
pip3 install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc

# through the Collector (full pipeline)
OTLP_ENDPOINT=localhost:14317 TENANT_ID=acme SEND_SECONDS=300 python3 metrics.py

# or straight to otlpextension (skips the Collector)
OTLP_ENDPOINT=localhost:4317 TENANT_ID=acme SEND_SECONDS=300 python3 metrics.py
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `OTLP_ENDPOINT` | `localhost:4317` | OTLP/gRPC endpoint to send to |
| `TENANT_ID` | `acme` | tenant set on the resource |
| `SEND_SECONDS` | `240` | how long to run |
| `BASELINE_READINGS` | `40` | normal exports before the anomaly |

otlpextension needs `BRIDGE_METRIC_WARMUP` readings (default 30) before it can
flag anything, so keep `BASELINE_READINGS` above that. With the defaults,
40 normal exports take about 200 s and the anomaly follows. To test faster,
start otlpextension with `-e BRIDGE_METRIC_WARMUP=10` and run the generator
with `BASELINE_READINGS=14 SEND_SECONDS=120`.

The metric names start with `sample_opensearch_`. If you set
`BRIDGE_METRIC_ALLOW`, include that prefix.

### 6.6 Check the result

```bash
# 1. Collector terminal: the debug exporter prints each batch it received.
# 2. Collector delivery: must show no "Exporting failed" lines.
# 3. otlpextension:
curl -s localhost:4319/status -H 'Authorization: Bearer admin'
```

Expected after the anomaly starts: `received.metric_points` in the
hundreds or more, `learned.metric_anomalies` above 0, `incidents_written`
above 0.

```bash
# 4. the stored anomaly in CortexDB
curl -s -X POST localhost:8080/v1/recall \
  -H 'Authorization: Bearer devtoken' -H 'Content-Type: application/json' \
  -d '{"namespace":"otel-acme","scope":"global","query":"anomaly sample_opensearch_jvm_mem_pool_used_bytes"}'
```

You should see a memory like: *Metric sample_opensearch_jvm_mem_pool_used_bytes
on service opensearch is unusually high: 9.179e+08 By against a baseline of
1.394e+08 ...*. In the web UI (step 5), search for `sample_opensearch`.

### 6.7 Troubleshooting

| Symptom | Check |
| --- | --- |
| `connection refused` on `:14317` | The Collector is not running, or `OTLP_ENDPOINT` is wrong. |
| `address already in use` | Another process holds 4317, 14317 or 13133 (step 6.1). |
| Collector logs `Exporting failed ... otlp/cortexdb` | otlpextension is not running or not on 4317. `Unauthenticated`: the `headers` token does not match `BRIDGE_RECEIVER_TOKEN`. |
| `Decompressor is not installed for grpc-encoding "gzip"` | Your otlpextension image is older than the gzip fix. Rebuild it from the current source (step 2). |
| `received.metric_points` is 0 | Nothing reached otlpextension. Check the Collector log, then the endpoint and token. |
| Points arrive but `skipped_disabled_or_capped` grows | Tenant `acme` is off. Run `PUT /tenants/acme` (step 4). |
| Metrics arrive, no anomaly | Warm-up not finished, or the metric name is filtered by `BRIDGE_METRIC_ALLOW`. |

### 6.8 Other notes

- The Collector must reach otlpextension. In Docker, put both on the
  `otel-learn` network and use `otlpextension:4317` as the endpoint.
- Use the `batch` processor so a trace's spans travel together; that is how
  service-to-service edges are found.

## 7. Stop and clean up

```bash
docker rm -f otlpextension cortexdb
docker network rm otel-learn
docker volume rm otlpextension-data cortexdb-data     # deletes everything learned
```

## Without Docker

```bash
go build -o cortexdb-grpc ./cmd/cortexdb-grpc
(cd contrib/otlpextension && go build -o otlpextension .)

CORTEXDB_PATH=./cortex.db CORTEXDB_HTTP_ADDR=127.0.0.1:8080 \
CORTEXDB_GRPC_TOKEN=devtoken ./cortexdb-grpc &

BRIDGE_CORTEXDB_URL=http://127.0.0.1:8080 BRIDGE_CORTEXDB_TOKEN=devtoken \
BRIDGE_RECEIVER_TOKEN=recvtoken BRIDGE_ADMIN_TOKEN=admin \
BRIDGE_STATE_FILE=./tenants.json ./contrib/otlpextension/otlpextension
```

Always set `CORTEXDB_PATH`; the default is `~/.cortexdb/cortexdb.db`.

## Known limits

- State inside otlpextension (counts, cooldowns, baselines) is in memory and
  restarts empty. The incident memories themselves live in CortexDB.
- Recall is keyword-based until an embedding endpoint is configured on
  `cortexdb-grpc` (`OPENAI_BASE_URL`).
- Run one otlpextension replica per Collector group.
- Do not reuse the dev tokens above outside your machine.
