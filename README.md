# CortexDB OTel

**Native OpenTelemetry integration for CortexDB — turning telemetry into semantic knowledge and diagnostic intelligence.**

[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-native-blue)](https://opentelemetry.io/)
[![CortexDB](https://img.shields.io/badge/Backend-CortexDB-green)](https://github.com/liliang-cn/cortexdb)
[![Language](https://img.shields.io/badge/Language-Go-00ADD8)](https://go.dev/)
[![Status](https://img.shields.io/badge/Status-Experimental-orange)](#project-status)

CortexDB OTel is an OpenTelemetry integration project designed to bring metrics, traces, and logs into [CortexDB](https://github.com/liliang-cn/cortexdb), enabling telemetry-driven knowledge graph construction, semantic correlation, and AI-powered diagnostics.

Unlike conventional telemetry exporters that simply transport and store observability data, CortexDB OTel aims to connect telemetry signals with a predefined ontology, resolve relationships between system entities, and enrich the knowledge graph with operational evidence.

The goal is to make telemetry not just observable, but understandable and actionable.

---

## Why CortexDB OTel?

Modern distributed systems generate enormous volumes of metrics, traces, and logs. Although traditional observability platforms help identify symptoms, understanding the relationships between components and determining the underlying cause of failures often requires additional correlation and domain knowledge.

CortexDB provides capabilities such as vector search, full-text search, knowledge graphs, and semantic retrieval.

CortexDB OTel aims to bridge OpenTelemetry and these capabilities.

### Key capabilities

- **Native OTLP integration:** Ingest OpenTelemetry metrics, traces, and logs.
- **Ontology-aware ingestion:** Map telemetry resource attributes to predefined semantic entities.
- **Knowledge graph enrichment:** Automatically create and update entities and relationships.
- **Cross-signal correlation:** Connect metrics, traces, logs, and operational events.
- **Diagnostic evidence:** Organize relevant telemetry into contextual evidence for troubleshooting.
- **AI-ready retrieval:** Enable diagnostic agents to retrieve telemetry using semantic and graph-based queries.
- **Multi-tenant awareness:** Preserve tenant identity and isolation across telemetry ingestion and retrieval.

## Architecture
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/2c7f8b87-f30f-4852-8414-8b128f21b6bd" />

## Design principles

### 1. Ontology first, telemetry second

CortexDB OTel is designed around the concept of a predefined ontology.

The ontology defines the semantic structure of the monitored environment, including:

- Entity types.
- Attributes and identifiers.
- Relationship types.
- Relationship constraints.
- Semantic mappings for telemetry attributes.

The ontology is established independently and installed into CortexDB.

Runtime telemetry ingestion does not regenerate the ontology. Instead, incoming telemetry is interpreted against the installed ontology.

### 2. Telemetry becomes graph-aware

Telemetry resources and attributes can be mapped to existing entities.

For example, consider an incoming OpenTelemetry resource:

```json
{
  "service.name": "presto",
  "k8s.pod.name": "presto-worker-01",
  "k8s.namespace.name": "analytics",
  "cloud.region": "us-south"
}
```

The ontology resolver identifies the corresponding semantic entities and relationships.

Illustrative graph representation:

```text
ComputeEngine:Presto
        |
        | CONTAINS
        v
Worker:presto-worker-01
        |
        | RUNS_IN
        v
KubernetesPod:presto-worker-01
        |
        | HOSTED_ON
        v
InfrastructureNode:node-01
```

Telemetry is associated with these entities, providing the context needed for multi-hop diagnostic queries.

### 3. Separate telemetry evidence from semantic knowledge

Not every telemetry datapoint needs to become a graph node.

CortexDB OTel follows a separation between:

| Data category | Purpose |
|---|---|
| Metric evidence | Time-dependent metric observations and aggregates |
| Trace evidence | Distributed execution and span relationships |
| Log evidence | Events, exceptions, and error context |
| Knowledge graph | Entities and their relationships |
| Diagnostic evidence | Correlated observations and incident context |

This separation helps avoid unnecessary graph growth while retaining the context required for diagnosis.

---

## Supported telemetry signals

The project targets the OpenTelemetry Protocol (OTLP).

| Signal | Planned support |
|---|---|
| Metrics | Gauge, Sum, Histogram |
| Traces | Spans, Span Events, Span Links |
| Logs | Structured log records |
| Resources | Resource attributes and identity |
| Exemplars | Metric-to-trace correlation |

Supported transport protocols:

- OTLP/gRPC
- OTLP/HTTP with Protobuf encoding

Actual implementation and compatibility will be documented as features become available.

## Ontology integration

CortexDB OTel is designed to consume a pre-installed ontology.

Ontology definitions can be authored and maintained independently using tools such as [NVIDIA Auto Ontology](https://github.com/NVIDIA/auto-ontology).

The intended workflow is:

1. Discover domain entities and relationships.
2. Generate and validate the ontology.
3. Certify and version the ontology.
4. Import the ontology into CortexDB.
5. Configure telemetry-to-ontology mappings.
6. Resolve incoming telemetry against the installed ontology.
7. Enrich the knowledge graph with runtime entities and relationships.

### Example: Distributed systems diagnostic ontology

An initial ontology could include:

| Entity | Relationships |
|---|---|
| Tenant | OWNS Cluster |
| Cluster | CONTAINS Engine |
| Engine | EXECUTES Query |
| Query | HAS_STAGE Stage |
| Stage | RUNS_ON Worker |
| Worker | RUNS_IN Pod |
| Pod | HOSTED_ON Node |
| Query | HAS_TRACE Trace |
| Trace | CONTAINS Span |
| Resource | EMITS Metric |
| Incident | AFFECTS Resource |

This model can subsequently be extended to Presto, Spark, distributed databases, Kubernetes, GPU infrastructure, cloud platforms and FinOps.

---

## Example diagnostic use case

Consider a failed distributed query.

A diagnostic agent receives the question:

> Why did query Q123 fail?

CortexDB OTel enables a workflow in which the agent can:

1. Identify the failed query.
2. Retrieve its associated execution trace.
3. Reconstruct the relevant execution stages.
4. Identify the affected workers and Kubernetes pods.
5. Retrieve metric evidence around the failure.
6. Correlate memory pressure, CPU utilization, GC activity, and worker errors.
7. Retrieve similar historical incidents.
8. Generate an evidence-backed root cause analysis.

Illustrative diagnostic context:

```text
Query: Q123
Status: FAILED

Findings:
  - Worker timeout detected.
  - Memory utilization increased before failure.
  - Elevated GC activity observed.
  - Worker pod associated with the failed execution.

Potential root cause:
  Worker resource exhaustion.

Evidence:
  - Trace: TR123
  - Worker: WK01
  - Pod: presto-worker-01
  - Metric: jvm.memory.used
  - Incident: INC-001
```

The RCA itself is generated by a diagnostic reasoning layer; CortexDB OTel provides the correlated evidence.

---

## Technology stack

| Component | Technology |
|---|---|
| Implementation language | Go |
| Telemetry protocol | OpenTelemetry Protocol |
| Telemetry ecosystem | OpenTelemetry Collector |
| Knowledge graph | CortexDB |
| Ontology discovery | NVIDIA Auto Ontology |
| AI reasoning | compatible LLMs |
| Agent integration | MCP |
| Deployment | Containers / Kubernetes |

## Getting started

1. Set up a CortexDB instance.
2. Establish a small predefined ontology.
3. Start an OpenTelemetry Collector.
4. Send distributed application metrics and traces through OTLP.
5. Resolve telemetry attributes against the ontology.
6. Store telemetry evidence and enrich the CortexDB knowledge graph.

Detailed installation and configuration instructions will be added as the initial implementation becomes available.

## Contributing

Contributions are welcome!

Areas of interest:

- OpenTelemetry integration.
- Go-based telemetry processing.
- Ontology and semantic modeling.
- Knowledge graph construction.
- Telemetry correlation.
- AI-powered observability and diagnostics.

Please open an issue to discuss significant architectural changes before submitting a pull request.

## Related projects

- [CortexDB](https://github.com/liliang-cn/cortexdb)
- [OpenTelemetry](https://opentelemetry.io/)
- [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector)
- [NVIDIA Auto Ontology](https://github.com/NVIDIA/auto-ontology)
- [Apache Ossie](https://github.com/apache/ossie)

## Project status

**Experimental — Architecture and initial implementation in progress.**

CortexDB OTel aims to explore a new approach to observability: transforming telemetry from isolated operational signals into semantically connected knowledge that AI agents can use for diagnosis and reasoning.

---

**From Telemetry to Knowledge. From Knowledge to Intelligence.**

## otlpextension

The Go service in this repository receives OTLP traces, logs and metrics from an
OpenTelemetry Collector and writes distilled incidents, a service map and metric
anomalies into CortexDB. See [docs/otlpextension.md](docs/otlpextension.md) for
the architecture, Collector config, settings and how to run it.
