#!/usr/bin/env sh
# Writes the synthetic readiness tuple. Idempotent. Called once at deploy time
# by the kanban kustomize manifest in Stage 7c.
set -e
keto relation-tuple create \
  --read-remote=:4466 \
  --write-remote=:4467 \
  --namespace=_kanban_health \
  --object=health \
  --relation=probe \
  --subject-id=user:_kanban_startup_probe
