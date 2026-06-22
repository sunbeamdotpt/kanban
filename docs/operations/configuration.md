---
license: AGPL-3.0-or-later
title: Configuration
description: Environment variables, ConfigMap, and Secret reference for the Kanban backend.
category: operations
order: 2
nav_order: 2
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Configuration

The Kanban backend is configured entirely through environment variables. In Kubernetes, put non-sensitive values in a `ConfigMap` and secrets in a `Secret`.

## Required variables

| Variable | Example | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://user:pass@postgres:5432/kanban` | PostgreSQL connection string. |
| `JWT_SECRET` | `<hydra-jwt-secret>` | Secret used to validate Bearer JWTs from Hydra. |

## Service endpoints

| Variable | Default | Purpose |
| --- | --- | --- |
| `KANBAN_PORT` | `8080` | HTTP/gRPC listen port. |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. JetStream must be enabled. |
| `VALKEY_URL` | `redis://localhost:6379` | Valkey / Redis URL for logout watermarks. |
| `KETO_READ_ADDR` | `http://localhost:4466` | Keto read API (gRPC/HTTP). |
| `KETO_WRITE_ADDR` | `http://localhost:4467` | Keto write API (gRPC/HTTP). |
| `OPENSEARCH_URL` | `http://localhost:9200` | OpenSearch URL. |

Aliases: `KETO_GRPC_URL` and `KETO_WRITE_GRPC_URL` are accepted as fallbacks for `KETO_READ_ADDR` and `KETO_WRITE_ADDR`.

## Object storage

| Variable | Default | Purpose |
| --- | --- | --- |
| `S3_ENDPOINT` | — | S3-compatible endpoint. Required for attachments. |
| `S3_REGION` | `us-east-1` | S3 region. |
| `S3_ACCESS_KEY` | — | S3 access key. |
| `S3_SECRET_KEY` | — | S3 secret key. |
| `S3_BUCKET` | `sunbeam-kanban` | Bucket used for card attachments. |

## Observability

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OpenTelemetry OTLP gRPC endpoint. If unset, tracing initializes without an exporter. |
| `RUST_LOG` | `info` | `tracing-subscriber` log filter. |

## Kubernetes-specific

| Variable | Default | Purpose |
| --- | --- | --- |
| `POD_NAME` | random UUID | Stable pod identity used when naming NATS consumers. In Kubernetes, set this from `metadata.name` via the downward API. |

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
  VALKEY_URL: "redis://valkey.valkey.svc.cluster.local:6379"
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
  JWT_SECRET: "<hydra-jwt-secret>"
  S3_ACCESS_KEY: "<access-key>"
  S3_SECRET_KEY: "<secret-key>"
```

In production, generate the Secret with a secrets manager (External Secrets Operator, Sealed Secrets, Vault, etc.) rather than committing it.
