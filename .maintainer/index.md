# kanban maintainer knowledge bundle

OKF-shaped knowledge base for the kanban maintainer. Start here, follow links.
Instructions live in [charter.md](charter.md); code conventions live in the
repo's `AGENTS.md`; this bundle holds *knowledge* — what is true and why.

## State

- [state.md](state.md) — what is in flight right now, updated at every handoff
- [log.md](log.md) — append-only decision journal

## Concepts

- [architecture.md](architecture.md) — service shape, realtime pipeline, authz model
- [testing.md](testing.md) — how to actually verify a change (and which "CI gates" don't exist)
- [interfaces.md](interfaces.md) — who consumes kanban, what kanban consumes
- [migrations-policy.md](migrations-policy.md) — one-way migrations and the v2026.07.0 rewrite
- [known-issues.md](known-issues.md) — stale docs, TODO clusters, small debt
- [fragile-areas.md](fragile-areas.md) — incident-adjacent zones: auth, realtime, migrations
