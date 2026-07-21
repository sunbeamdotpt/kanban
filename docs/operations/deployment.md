---
license: AGPL-3.0-or-later
title: Deployment
description: How to build and deploy the Kanban backend to Kubernetes.
category: operations
order: 1
nav_order: 1
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Deployment

The Kanban backend is a stateless HTTP/gRPC service. It is deployed as a Kubernetes `Deployment` and exposes a single port (`8080` by default).

## Container image

The repository includes a multi-architecture `Dockerfile` that produces images for `linux/amd64` and `linux/arm64`.

```sh
# Single platform
docker buildx build \
  --platform linux/amd64 \
  -f Dockerfile \
  -t ghcr.io/sunbeamdotpt/kanban:latest \
  .

# Both platforms
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile \
  -t ghcr.io/sunbeamdotpt/kanban:latest \
  --push \
  .
```

The image is built in two stages:

1. A `rust:1.95-bookworm` builder with `tonistiigi/xx` for cross-compilation.
2. A `gcr.io/distroless/cc-debian12:nonroot` runtime image with OCI labels for GHCR autolinking.

Only the `kanban` server binary is included in the runtime image. The `permission-coverage` binary is CI-only and is not shipped.

## Release workflow and versioning

Releases use [Calendar Versioning](https://calver.org) (`YYYY.0M.PATCH`, e.g. `v2026.07.4`). The patch field counts releases within the month and resets to `0` on the first release of each new month. Pushing a tag matching `v*` triggers `.github/workflows/release.yml`, which builds and pushes a multi-arch image to GHCR. The workflow can also be triggered manually from the Actions tab.

Each release publishes multiple tags so deployments can choose their pinning level:

- `v2026.07.4` — the exact release.
- `v2026.07` — floats to the latest release in the month.
- `v2026` — floats to the latest release in the year.
- `latest` — floats to the most recent non-prerelease tag.

## Migrations

Migrations are applied by the default `kanban` binary at boot using `sqlx::migrate!("./migrations")`. No separate migration Job or binary is required.

Implications for Kubernetes:

- The first pod to start will run migrations. Subsequent pods will re-run the same migration set; `sqlx` handles this idempotently.
- Rolling updates are safe because migrations are additive and backward-compatible for already-running pods.
- If you ever need to roll back code to an older commit, **do not roll back the database schema**. The migrations are one-way. Contact the on-call DBA if a rollback is needed.

## Kubernetes Deployment design

A minimal Deployment looks like this:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kanban
  namespace: kanban
spec:
  replicas: 3
  selector:
    matchLabels:
      app: kanban
  template:
    metadata:
      labels:
        app: kanban
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
    spec:
      serviceAccountName: kanban
      containers:
        - name: kanban
          image: ghcr.io/sunbeamdotpt/kanban:latest
          imagePullPolicy: Always
          ports:
            - name: http
              containerPort: 8080
          envFrom:
            - configMapRef:
                name: kanban-config
            - secretRef:
                name: kanban-secrets
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          readinessProbe:
            httpGet:
              path: /healthz/ready
              port: http
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /healthz/live
              port: http
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              cpu: 2000m
              memory: 1Gi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
```

Key points:

- `POD_NAME` is injected from the pod name so each pod gets a stable identity for NATS consumer naming.
- `readinessProbe` hits `/healthz/ready`, which proxies the sso-gateway's `/health/ready`. If the gateway or its OpenFGA backend is unreachable or misconfigured, the pod is removed from the Service. Server boot additionally fails fast when the Kanban permission namespace cannot be provisioned.
- `livenessProbe` hits `/healthz/live`, which always returns 200.

## Supporting resources

You will also need:

- A `Service` on port `8080`.
- A `ServiceAccount` bound to the workload.
- A `HorizontalPodAutoscaler` keyed to CPU and memory.
- A `PodDisruptionBudget` with `minAvailable: 1` for availability during node drains.
- A `ConfigMap` for non-sensitive configuration and a `Secret` for credentials.

For the full list of environment variables, see [Configuration](configuration.md).

## Ingress

The frontend uses Connect-RPC over h2 (gRPC and gRPC-Web are also supported). For best performance, terminate TLS at an Ingress or gateway that supports HTTP/2.

## Rollout and rollback

Rollout:

```sh
kubectl rollout status deployment/kanban -n kanban
```

Rollback:

```sh
kubectl rollout undo deployment/kanban -n kanban
```

Remember that schema migrations are one-way. Rolling back the Deployment does **not** roll back the database. If a migration caused an issue, the forward fix must be applied as a new migration.

## Boot dependencies

Before the Deployment is rolled out, the following must already be available:

- PostgreSQL database and user.
- NATS with JetStream enabled.
- The sso-gateway. The Kanban permission namespace and OpenFGA model are provisioned automatically at boot from the embedded model (`.integration/openfga-model.json`).
- OpenSearch.
- S3-compatible object store with the attachments bucket created.
