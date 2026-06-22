#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-or-later
# Integration-test runner for the Kanban backend.
#
# The Rust test harness in src/test_support.rs uses testcontainers to start
# Postgres, NATS (JetStream), Valkey, Ory Keto, and MinIO automatically. This
# script simply points testcontainers at the local Docker-compatible socket
# (socktainer, docker, or podman) and runs the test suite.
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

# ── Container socket detection ──────────────────────────────────────────────

if [[ -z "${DOCKER_HOST:-}" ]]; then
    if [[ -S "${HOME}/.socktainer/container.sock" ]]; then
        export DOCKER_HOST="unix://${HOME}/.socktainer/container.sock"
    elif [[ -S "/var/run/docker.sock" ]]; then
        export DOCKER_HOST="unix:///var/run/docker.sock"
    elif [[ -S "${HOME}/.docker/run/docker.sock" ]]; then
        export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
    fi
fi

# ── Image overrides ─────────────────────────────────────────────────────────

export KANBAN_TEST_POSTGRES_IMAGE="${KANBAN_TEST_POSTGRES_IMAGE:-mirror.gcr.io/library/postgres:16-alpine}"
export KANBAN_TEST_NATS_IMAGE="${KANBAN_TEST_NATS_IMAGE:-nats:2.10-alpine}"
export KANBAN_TEST_VALKEY_IMAGE="${KANBAN_TEST_VALKEY_IMAGE:-valkey/valkey:8.0.2-alpine}"
export KANBAN_TEST_KETO_IMAGE="${KANBAN_TEST_KETO_IMAGE:-oryd/keto:v26.2.0}"
export KANBAN_TEST_MINIO_IMAGE="${KANBAN_TEST_MINIO_IMAGE:-minio/minio:RELEASE.2025-02-28T09-55-16Z}"
export KANBAN_TEST_OPENSEARCH_IMAGE="${KANBAN_TEST_OPENSEARCH_IMAGE:-opensearchproject/opensearch:2.19.1}"

# ── LLVM tools for coverage ─────────────────────────────────────────────────

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

# ── Run tests ───────────────────────────────────────────────────────────────

TEST_STATUS=0
if [[ "$COVERAGE" == "1" ]]; then
    ensure_llvm_cov
    echo "Running: cargo llvm-cov test $*"
    # main.rs only contains the binary entry point (telemetry/runtime scaffolding);
    # it is not exercised by the test suite.
    cargo llvm-cov test --ignore-filename-regex 'src/main\.rs' "$@" || TEST_STATUS=$?
else
    echo "Running: cargo test $*"
    cargo test "$@" || TEST_STATUS=$?
fi

exit "$TEST_STATUS"
