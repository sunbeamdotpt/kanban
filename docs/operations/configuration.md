---
license: AGPL-3.0-or-later
title: Configuration
description: Environment variables, CLI flags, ConfigMap, and Secret reference for the Kanban backend.
category: operations
order: 2
nav_order: 2
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Configuration

The Kanban backend is configured through **environment variables** and optional **command-line flags**. Every setting has an environment variable equivalent, so Kubernetes deployments can use a `ConfigMap` for non-sensitive values and a `Secret` for sensitive ones.

Command-line flags take precedence over environment variables. Run `kanban --help` for the full flag list.

## Required variables

| Variable | Example | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://user:pass@postgres:5432/kanban` | PostgreSQL connection string. |



## Server endpoints

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_HOST` | `--host` | `0.0.0.0` | Bind host. |
| `KANBAN_PORT` | `--port` | `8080` | HTTP/gRPC listen port. |
| `NATS_URL` | `--nats-url` | `nats://localhost:4222` | NATS server URL. JetStream must be enabled. |
| `NATS_AUTH_TOKEN` | `--nats-auth-token` | — | Optional NATS auth callout token. |
| `KETO_READ_ADDR` | `--keto-read-addr` | `http://localhost:4466` | Keto read API (gRPC/HTTP). |
| `KETO_WRITE_ADDR` | `--keto-write-addr` | `http://localhost:4467` | Keto write API (gRPC/HTTP). |
| `OPENSEARCH_URL` | `--opensearch-url` | `http://localhost:9200` | OpenSearch URL. |
| `KANBAN_OPENSEARCH_INDEX` | `--opensearch-index-name` | `sunbeam-kanban-cards-v1` | OpenSearch index for card search. |

Legacy aliases: `KETO_GRPC_URL` and `KETO_WRITE_GRPC_URL` are still accepted by integration test helpers, but production startup uses `KETO_READ_ADDR` and `KETO_WRITE_ADDR`.

## PostgreSQL pool

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_DATABASE_MAX_CONNECTIONS` | `--database-max-connections` | `20` | Max Postgres pool size. |
| `KANBAN_DATABASE_ACQUIRE_TIMEOUT_SECS` | `--database-acquire-timeout-secs` | `10` | Connection acquire timeout. |

## Hydra / OAuth2 introspection

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `HYDRA_INTROSPECTION_URL` | `--hydra-introspection-url` | `http://localhost:4445/oauth2/introspect` | Hydra OAuth2 introspection endpoint. Every Bearer token is introspected here. |
| `HYDRA_CLIENT_ID` | `--hydra-client-id` | `''` | OAuth2 client ID for introspection Basic auth. |
| `HYDRA_CLIENT_SECRET` | `--hydra-client-secret` | `''` | OAuth2 client secret for introspection Basic auth. |

## Object storage (attachments)

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `S3_ENDPOINT` | `--s3-endpoint` | — | S3-compatible endpoint. Required for attachments. |
| `S3_REGION` | `--s3-region` | `us-east-1` | S3 region. |
| `S3_ACCESS_KEY` | `--s3-access-key` | — | S3 access key. |
| `S3_SECRET_KEY` | `--s3-secret-key` | — | S3 secret key. |
| `S3_BUCKET` | `--s3-bucket` | `sunbeam-kanban` | Bucket used for card attachments. |

## Attachment presigned URLs

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_UPLOAD_EXPIRES_SECS` | `--upload-expires-secs` | `900` | Presigned PUT URL lifetime. |
| `KANBAN_DOWNLOAD_EXPIRES_SECS` | `--download-expires-secs` | `300` | Presigned GET URL lifetime. |

## NATS / JetStream

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_NATS_LEASE_DURATION_SECS` | `--nats-lease-duration-secs` | `30` | NATS consumer lease duration. |
| `KANBAN_NATS_REPLICAS` | `--stream-replicas` | `1` | JetStream stream replica count. |
| `KANBAN_STREAM_MAX_AGE_SECS` | `--stream-max-age-secs` | `86400` | JetStream stream max age. |
| `KANBAN_STREAM_MAX_MSGS_PER_SUBJECT` | `--stream-max-msgs-per-subject` | `10000` | Max messages retained per subject. |
| `KANBAN_STREAM_RETENTION` | `--stream-retention` | `limits` | Retention policy: `limits`, `interest`, or `work_queue`. |
| `KANBAN_STREAM_STORAGE` | `--stream-storage` | `file` | Storage backend: `file` or `memory`. |

## Outbox dispatcher

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_OUTBOX_POLL_INTERVAL_MS` | `--outbox-poll-interval-ms` | `250` | Polling interval for undispatched `event_log` rows. |
| `KANBAN_OUTBOX_BATCH_SIZE` | `--outbox-batch-size` | `256` | Rows drained per loop. |

The outbox also embeds `POD_NAME` in every emitted event envelope.

## Board subscriber registry

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_REGISTRY_BROADCAST_CAPACITY` | `--registry-broadcast-capacity` | `256` | Per-board broadcast channel capacity. |
| `KANBAN_REGISTRY_INACTIVE_THRESHOLD_SECS` | `--registry-inactive-threshold-secs` | `30` | Ephemeral consumer GC threshold. |

## Live subscription streams

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `KANBAN_HEARTBEAT_INTERVAL_MS` | `--heartbeat-interval-ms` | `15000` | Heartbeat interval for `SubscribeBoard` / `SubscribeAggregatedBoard`. |
| `KANBAN_KETO_RECHECK_INTERVAL_MS` | `--keto-recheck-interval-ms` | `30000` | Permission recheck interval during streaming. |
| `KANBAN_CUTOVER_SEEN_CAPACITY` | `--cutover-seen-capacity` | `1024` | Replay/live deduplication ring-buffer size. |

## Observability

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OpenTelemetry OTLP gRPC endpoint. If unset, tracing initializes without an exporter. |
| `RUST_LOG` | `info` | `tracing-subscriber` log filter. |
| `KANBAN_RPC_DURATION_BUCKETS_SECS` | `0.005,0.01,0.025,0.05,0.1,0.25,0.5,1.0,2.5,5.0` | Comma-separated Prometheus histogram buckets for RPC duration. |

## Kubernetes-specific

| Variable | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `POD_NAME` | `--pod-name` | random ULID | Stable pod identity used when naming NATS consumers and emitted event envelopes. In Kubernetes, set this from `metadata.name` via the downward API. |

## Example Kubernetes objects

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kanban-config
  namespace: kanban
data:
  KANBAN_PORT: "8080"
  NATS_URL: "nats://nats.nats.svc.cluster.local:4222"
  KETO_READ_ADDR: "http://keto-read.ory.svc.cluster.local:4466"
  KETO_WRITE_ADDR: "http://keto-write.ory.svc.cluster.local:4467"
  OPENSEARCH_URL: "http://opensearch.opensearch.svc.cluster.local:9200"
  S3_ENDPOINT: "http://seaweedfs-s3.storage.svc.cluster.local:8333"
  S3_REGION: "us-east-1"
  S3_BUCKET: "sunbeam-kanban"
```

### Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kanban-secrets
  namespace: kanban
stringData:
  DATABASE_URL: "postgres://kanban:<password>@postgres.postgres.svc.cluster.local:5432/kanban"
  HYDRA_CLIENT_ID: "<hydra-client-id>"
  HYDRA_CLIENT_SECRET: "<hydra-client-secret>"
  S3_ACCESS_KEY: "<access-key>"
  S3_SECRET_KEY: "<secret-key>"
```

In production, generate the Secret with a secrets manager (External Secrets Operator, Sealed Secrets, Vault, etc.) rather than committing it.
