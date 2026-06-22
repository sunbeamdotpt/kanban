#!/usr/bin/env bash
# Cross-platform integration-test runner for the Kanban backend.
#
# Detects the local container runtime (docker, podman, or Apple Container's
# `container`), starts Postgres + NATS (JetStream) + Valkey + Keto + MinIO on
# localhost ports, runs sqlx migrations, creates the MinIO bucket, exports the
# standard service env vars, and runs the Rust test suite.
#
# Usage:
#   ./test.sh                    # cargo nextest run (or cargo test fallback)
#   ./test.sh services::boards   # run a subset of tests
#   ./test.sh --coverage         # run with cargo-llvm-cov and print summary
#
# Override images via environment variables, e.g.:
#   KANBAN_TEST_POSTGRES_IMAGE=postgres:16-alpine ./test.sh

set -euo pipefail

COVERAGE=0
if [[ "${1:-}" == "--coverage" ]]; then
    COVERAGE=1
    shift
fi

# ── Configuration ───────────────────────────────────────────────────────────

KANBAN_TEST_POSTGRES_IMAGE="${KANBAN_TEST_POSTGRES_IMAGE:-mirror.gcr.io/library/postgres:16-alpine}"
KANBAN_TEST_NATS_IMAGE="${KANBAN_TEST_NATS_IMAGE:-nats:2.10-alpine}"
KANBAN_TEST_VALKEY_IMAGE="${KANBAN_TEST_VALKEY_IMAGE:-valkey/valkey:8.0.2-alpine}"
KANBAN_TEST_KETO_IMAGE="${KANBAN_TEST_KETO_IMAGE:-oryd/keto:v26.2.0}"
KANBAN_TEST_MINIO_IMAGE="${KANBAN_TEST_MINIO_IMAGE:-minio/minio:RELEASE.2025-02-28T09-55-16Z}"
KANBAN_TEST_OPENSEARCH_IMAGE="${KANBAN_TEST_OPENSEARCH_IMAGE:-opensearchproject/opensearch:2.18.0}"

POSTGRES_PORT="${KANBAN_TEST_POSTGRES_PORT:-5432}"
OPENSEARCH_PORT="${KANBAN_TEST_OPENSEARCH_PORT:-9200}"
NATS_PORT="${KANBAN_TEST_NATS_PORT:-4222}"
VALKEY_PORT="${KANBAN_TEST_VALKEY_PORT:-6379}"
KETO_READ_PORT="${KANBAN_TEST_KETO_READ_PORT:-4466}"
KETO_WRITE_PORT="${KANBAN_TEST_KETO_WRITE_PORT:-4467}"
MINIO_PORT="${KANBAN_TEST_MINIO_PORT:-9000}"

MINIO_USER="${KANBAN_TEST_MINIO_USER:-minioadmin}"
MINIO_PASSWORD="${KANBAN_TEST_MINIO_PASSWORD:-minioadmin}"
MINIO_BUCKET="${KANBAN_TEST_MINIO_BUCKET:-sunbeam-kanban}"

# ── Container runtime detection ─────────────────────────────────────────────

detect_runtime() {
    if [[ -n "${KANBAN_TEST_RUNTIME:-}" ]]; then
        echo "$KANBAN_TEST_RUNTIME"
        return
    fi
    if command -v docker >/dev/null 2>&1; then
        echo docker
    elif command -v podman >/dev/null 2>&1; then
        echo podman
    elif command -v container >/dev/null 2>&1; then
        echo container
    else
        echo "No supported container runtime found (docker, podman, or 'container')." >&2
        exit 1
    fi
}

RUNTIME="$(detect_runtime)"

# ── Helpers ─────────────────────────────────────────────────────────────────

container_run() {
    local name="$1"
    shift
    case "$RUNTIME" in
        docker|podman)
            "$RUNTIME" run -d --rm --name "$name" "$@"
            ;;
        container)
            container run -d --rm --name "$name" "$@"
            ;;
    esac
}

container_stop() {
    local name="$1"
    case "$RUNTIME" in
        docker|podman)
            "$RUNTIME" stop -t 5 "$name" >/dev/null 2>&1 || true
            ;;
        container)
            container stop "$name" >/dev/null 2>&1 || true
            ;;
    esac
}

wait_for_tcp() {
    local host="$1" port="$2" label="$3"
    local attempts=60
    echo -n "Waiting for $label on $host:$port "
    for ((i = 0; i < attempts; i++)); do
        if python3 -c "import socket; socket.create_connection(('$host', $port), timeout=1).close()" 2>/dev/null; then
            echo "ok"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo " timeout"
    return 1
}

wait_for_postgres() {
    local url="$1" label="$2"
    local attempts=60
    echo -n "Waiting for $label to accept connections "
    for ((i = 0; i < attempts; i++)); do
        if psql "$url" -c 'SELECT 1' >/dev/null 2>&1; then
            echo "ok"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo " timeout"
    return 1
}

find_llvm_tool() {
    local tool="$1"
    local path

    # 1. Explicit env override.
    local env_var="LLVM_${tool^^}"
    env_var="${env_var//-/_}"
    if [[ -n "${!env_var:-}" && -x "${!env_var}" ]]; then
        echo "${!env_var}"
        return 0
    fi

    # 2. rustup-managed llvm-tools-preview.
    local sysroot
    sysroot="$(rustc --print sysroot 2>/dev/null || true)"
    if [[ -n "$sysroot" ]]; then
        path="$(find "$sysroot/lib/rustlib" -name "$tool" -type f -executable 2>/dev/null | head -n1)"
        if [[ -n "$path" ]]; then
            echo "$path"
            return 0
        fi
    fi

    # 3. Tool on PATH.
    path="$(command -v "$tool" 2>/dev/null || true)"
    if [[ -n "$path" ]]; then
        echo "$path"
        return 0
    fi

    # 4. Common Homebrew LLVM location.
    for brew_prefix in /opt/homebrew/opt/llvm /usr/local/opt/llvm; do
        if [[ -x "$brew_prefix/bin/$tool" ]]; then
            echo "$brew_prefix/bin/$tool"
            return 0
        fi
    done

    return 1
}

ensure_llvm_cov() {
    local cov profdata
    cov="$(find_llvm_tool llvm-cov)"
    profdata="$(find_llvm_tool llvm-profdata)"

    if [[ -z "$cov" || -z "$profdata" ]]; then
        echo "llvm-cov / llvm-profdata not found." >&2
        echo "Please install LLVM tools, for example:" >&2
        echo "  rustup component add llvm-tools-preview" >&2
        echo "  or" >&2
        echo "  brew install llvm" >&2
        exit 1
    fi

    export LLVM_COV="$cov"
    export LLVM_PROFDATA="$profdata"
    echo "Using llvm-cov: $LLVM_COV"
    echo "Using llvm-profdata: $LLVM_PROFDATA"
}

# ── Setup temp directory for Keto config ────────────────────────────────────

TMPDIR="${TMPDIR:-/tmp}"
TEST_DIR="$(mktemp -d "$TMPDIR/kanban-test.XXXXXX")"
trap 'rm -rf "$TEST_DIR"' EXIT

mkdir -p "$TEST_DIR/keto"
cat > "$TEST_DIR/keto/keto.yml" <<'YAML'
dsn: memory
namespaces:
  - id: 0
    name: KanbanProject
  - id: 1
    name: KanbanBoard
  - id: 2
    name: KanbanCard
  - id: 3
    name: KanbanAggregatedBoard
  - id: 4
    name: _kanban_health
serve:
  read:
    host: 0.0.0.0
    port: 4466
  write:
    host: 0.0.0.0
    port: 4467
  metrics:
    host: 0.0.0.0
    port: 4468
YAML

# ── Start containers ────────────────────────────────────────────────────────

echo "Using container runtime: $RUNTIME"

container_run kanban-test-postgres \
    -p "$POSTGRES_PORT:5432" \
    -e POSTGRES_USER=sunbeam \
    -e POSTGRES_PASSWORD=sunbeam \
    -e POSTGRES_DB=kanban \
    "$KANBAN_TEST_POSTGRES_IMAGE"
sleep 0.5

container_run kanban-test-nats \
    -p "$NATS_PORT:4222" \
    "$KANBAN_TEST_NATS_IMAGE" -js
sleep 0.5

container_run kanban-test-valkey \
    -p "$VALKEY_PORT:6379" \
    "$KANBAN_TEST_VALKEY_IMAGE"
sleep 0.5

container_run kanban-test-keto \
    -p "$KETO_READ_PORT:4466" \
    -p "$KETO_WRITE_PORT:4467" \
    --mount "type=bind,source=$TEST_DIR/keto,target=/etc/keto,readonly" \
    "$KANBAN_TEST_KETO_IMAGE" serve -c /etc/keto/keto.yml
sleep 0.5

container_run kanban-test-minio \
    -p "$MINIO_PORT:9000" \
    -e MINIO_ROOT_USER="$MINIO_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_PASSWORD" \
    "$KANBAN_TEST_MINIO_IMAGE" server /data
sleep 0.5

container_run kanban-test-opensearch \
    -p "$OPENSEARCH_PORT:9200" \
    -e discovery.type=single-node \
    -e plugins.security.disabled=true \
    -e OPENSEARCH_INITIAL_ADMIN_PASSWORD=KanbanTest123! \
    -e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \
    "$KANBAN_TEST_OPENSEARCH_IMAGE"

# ── Wait for readiness ──────────────────────────────────────────────────────

wait_for_tcp 127.0.0.1 "$POSTGRES_PORT" "Postgres TCP"
wait_for_postgres "postgres://sunbeam:sunbeam@127.0.0.1:$POSTGRES_PORT/kanban" "Postgres"
wait_for_tcp 127.0.0.1 "$NATS_PORT" "NATS"
wait_for_tcp 127.0.0.1 "$VALKEY_PORT" "Valkey"
wait_for_tcp 127.0.0.1 "$KETO_READ_PORT" "Keto read"
wait_for_tcp 127.0.0.1 "$KETO_WRITE_PORT" "Keto write"
wait_for_tcp 127.0.0.1 "$MINIO_PORT" "MinIO"
wait_for_tcp 127.0.0.1 "$OPENSEARCH_PORT" "OpenSearch"

# ── Initialize Postgres ─────────────────────────────────────────────────────

echo "Running database migrations..."
DATABASE_URL="postgres://sunbeam:sunbeam@127.0.0.1:$POSTGRES_PORT/kanban"
sqlx migrate run --database-url "$DATABASE_URL"

# ── Initialize MinIO bucket ─────────────────────────────────────────────────

echo "Creating MinIO bucket..."
export AWS_ACCESS_KEY_ID="$MINIO_USER"
export AWS_SECRET_ACCESS_KEY="$MINIO_PASSWORD"
aws --endpoint-url "http://127.0.0.1:$MINIO_PORT" --region us-east-1 \
    s3 mb "s3://$MINIO_BUCKET" >/dev/null 2>&1 || true

# ── Export env vars for tests ───────────────────────────────────────────────

export DATABASE_URL
export NATS_URL="nats://127.0.0.1:$NATS_PORT"
export VALKEY_URL="redis://127.0.0.1:$VALKEY_PORT"
export KETO_READ_ADDR="http://127.0.0.1:$KETO_READ_PORT"
export KETO_WRITE_ADDR="http://127.0.0.1:$KETO_WRITE_PORT"
export S3_ENDPOINT="http://127.0.0.1:$MINIO_PORT"
export S3_REGION="us-east-1"
export S3_ACCESS_KEY="$MINIO_USER"
export S3_SECRET_KEY="$MINIO_PASSWORD"
export S3_BUCKET="$MINIO_BUCKET"
export OPENSEARCH_URL="http://127.0.0.1:$OPENSEARCH_PORT"

# ── Run tests ───────────────────────────────────────────────────────────────

TEST_STATUS=0
if [[ "$COVERAGE" == "1" ]]; then
    ensure_llvm_cov
    echo "Running: cargo llvm-cov test $*"
    # main.rs only contains the binary entry point (telemetry/runtime scaffolding);
    # it is not exercised by the test suite.
    cargo llvm-cov test --ignore-filename-regex 'src/main\.rs' "$@" || TEST_STATUS=$?
elif command -v cargo-nextest >/dev/null 2>&1 || cargo nextest --version >/dev/null 2>&1; then
    echo "Running: cargo nextest run $*"
    cargo nextest run "$@" || TEST_STATUS=$?
else
    echo "Running: cargo test -- --test-threads=1 $*"
    cargo test -- --test-threads=1 "$@" || TEST_STATUS=$?
fi

# ── Cleanup ─────────────────────────────────────────────────────────────────

container_stop kanban-test-minio
container_stop kanban-test-opensearch
container_stop kanban-test-keto
container_stop kanban-test-valkey
container_stop kanban-test-nats
container_stop kanban-test-postgres

exit "$TEST_STATUS"
