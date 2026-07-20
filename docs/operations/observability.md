---
license: AGPL-3.0-or-later
title: Observability
description: Metrics, traces, logs, and alerting for the Kanban backend.
category: operations
order: 3
nav_order: 3
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Observability

The Kanban backend exposes Prometheus metrics, writes OpenTelemetry traces, and emits structured logs.

## Metrics

Metrics are served as Prometheus text on `/metrics` on the main HTTP port.

Key metrics:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `kanban_rpc_duration_seconds` | histogram | `service`, `method` | gRPC handler duration. |
| `kanban_rpc_total` | counter | `service`, `method`, `status` | Total gRPC requests by status. |
| `kanban_permission_check_total` | counter | `result` (`allow`, `deny`, `error`) | Permission check results. |
| `kanban_subscribe_active_streams` | gauge | — | Active board subscription streams. |
| `kanban_jet_stream_lag_seconds` | gauge | `board_id` | JetStream consumer lag per board. |
| `kanban_mirror_drift_ratio` | gauge | — | Fraction of `project_member_view` rows that differ from the permission backend. |

Configure Prometheus or a scraping agent to scrape pods with the annotation `prometheus.io/scrape: "true"` on port `8080`.

## Traces

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to a gRPC OTLP collector endpoint. The service initializes a `tracing-subscriber` layer that exports spans via OpenTelemetry.

Example:

```yaml
OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector.monitoring.svc.cluster.local:4317"
```

If the variable is unset, tracing initializes without an exporter and only emits logs.

## Logs

Logs are emitted as structured JSON when `RUST_LOG` enables JSON formatting, or as plain ANSI logs in development. Common log fields:

- `level` — log level.
- `target` — Rust module path.
- `span` — active tracing span, often including `board_id`, `card_id`, or `project_id`.
- `error` — error message and chain.

Useful log lines to watch for:

- `kanban service starting` — server is booting.
- `Postgres connected and migrations applied` — migrations succeeded.
- `JetStream stream bootstrapped` — NATS stream is ready.
- `Kanban permission namespace ensured` — permission namespace and OpenFGA model registered with the sso-gateway.
- `kanban shutdown complete` — graceful shutdown finished.

## Alerting suggestions

| Symptom | Metric / log | Severity |
| --- | --- | --- |
| High error rate | `kanban_rpc_total{status=~"5.."}` | page |
| Permission check failures | `kanban_permission_check_total{result="error"}` | page |
| Readiness probe failing | `/healthz/ready` != 200 | page |
| Mirror drift | `kanban_mirror_drift_ratio > 0.001` | warning |
| NATS consumer lag | `kanban_jet_stream_lag_seconds` p95 > 5s | warning |
