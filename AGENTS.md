# AGENTS.md — Sunbeam Kanban

Rust service + TypeScript SPA for real-time collaborative board management. Gated by Keto, streamed via NATS JetStream, live-synced across replicas with ≤30ms eventual consistency.

## Quick Start

**Dev URL:** `http://localhost:47823`

**Required compose services:** postgres, valkey, nats, keto, kratos, opensearch, seaweedfs.

```sh
sunbeam ops compose up kanban     # Start all services for local dev
sunbeam ops compose ps            # Verify they're running
```

The Rust server boots on `:8080` (internal); the Vite frontend dev server is `:47823` (proxied). Before the first run, seed the database:

```sh
cd apps/kanban && cargo run --bin kanban-db-init
```

## Architecture Sketch

Request path (all transports are Connect-RPC over h2 or h2-compatible SSE fallback):

```
┌─────────────┐
│  Frontend   │
│  React/TS   │
└──────┬──────┘
       │ Connect-Web (h2 or SSE)
       │ + Bearer JWT (from authStore)
       ▼
┌──────────────────────┐
│  kanban-server       │
│  (Rust Axum)         │
│  :8080               │
└──────┬───────────────┘
       │
       ├─ JwtLayer: validate JWT from Hydra
       │
       ├─ keto_dispatch (DispatchEntry matrix)
       │  · Extracts object ID from request
       │  · Calls KetoClient::check_permission(namespace, object_id, relation, subject)
       │  · 403 PermissionDenied if Keto rejects
       │  · Inserts Extension<CheckedObjectId> on success
       │
       └─ Handler (in services/{rpc}.rs)
          │
          ├─ Reads checked object ID from Extension<CheckedObjectId>
          │
          ├─ Mutates: write to postgres → event_log → outbox dispatcher → NATS
          │
          ├─ Reads: post-filter via keto_expand_objects() for ListProjects, SearchCards
          │
          └─ Streams: subscribe handlers publish to local registry; NATS feeds live tail
             and replay-from-resume-token (ephemeral pull consumer)
       ▼
┌──────────────────────────────┐
│  Postgres                    │
│  boards, cards, members, ... │
│  event_log, outbox           │
└──────────────────────────────┘
       ▼
┌──────────────────────────────┐
│  NATS JetStream              │
│  kanban.board.{id}.events    │
│  stream: KANBAN_BOARD_EVENTS │
└──────────────────────────────┘
       ▼
┌──────────────────────────────┐
│  Pod-local registry          │
│  (SubscribeBoardMap)         │
└──────────────────────────────┘
       │ (re-broadcast to all subscribers on this pod)
       ▼
   [All clients on this pod receive live events]
```

## Add a New RPC: Recipe

Suppose you're adding `UpdateCardColor(card_id, color) -> Card`.

### 1. Define the RPC in proto

```sh
# Edit proto/sunbeam/kanban/v1/cards.proto
message UpdateCardColorRequest {
  string card_id = 1;
  string color = 2;  // e.g., "gold", "purple"
}

service CardsService {
  rpc UpdateCardColor(UpdateCardColorRequest) returns (Card);
  // ... existing RPCs ...
}
```

### 2. Generate code

```sh
cd /Users/sienna/Development/sunbeam
buf lint proto && buf build proto
buf generate proto --template proto/buf.gen.kanban.yaml  # Rust

cd apps/kanban/ui
npm run proto:gen  # TypeScript
```

### 3. Add dispatch entry

Edit `apps/kanban/src/auth/keto_dispatch.rs`. Find the `DISPATCH_MATRIX` const and add:

```rust
DispatchEntry {
    method: "/sunbeam.kanban.v1.CardsService/UpdateCardColor",
    namespace: "KanbanCard",
    relation: "edit",
    object_id_source: ObjectIdSource::Header,  // Client sends `x-sunbeam-object-id: <card_id>`
},
```

### 4. Verify coverage

```sh
cd apps/kanban
cargo run --bin keto-coverage
# Must exit with code 0
# If a proto method is missing from DISPATCH_MATRIX, it panics
```

### 5. Implement handler

Create or edit `apps/kanban/src/services/cards.rs`:

```rust
pub async fn update_card_color(
    State(AppState { db, .. }): State<AppState>,
    Extension(checked_id): Extension<CheckedObjectId>,
    req: UpdateCardColorRequest,
) -> Result<Card, ApiError> {
    // The checked_id is the card_id that passed Keto Check on "edit"
    // Always use the checked_id, never the request body's card_id
    let card_id = checked_id.0;

    let card = sqlx::query_as::<_, Card>(
        "UPDATE cards SET color = $1, revision = revision + 1 WHERE id = $2 RETURNING *"
    )
    .bind(&req.color)
    .bind(&card_id)
    .fetch_one(&db)
    .await?;

    // Publish to NATS outbox
    outbox_dispatcher.publish_event(
        &format!("kanban.board.{}", card.board_id),
        BoardEvent {
            card_updated: Some(CardUpdated { card: Some(card.clone()) }),
            ..Default::default()
        },
    )
    .await?;

    Ok(card)
}
```

Wire it in `src/services/mod.rs`:

```rust
app.service(
    web::scope("/sunbeam.kanban.v1.CardsService")
        .route("/UpdateCardColor", web::post().to(update_card_color))
);
```

### 6. Write integration tests

Create `apps/kanban/src/services/cards_tests.rs` (or append to the existing test module). Test:
- Authorized user can update their own card.
- Unauthorized user gets 403.
- Color field is persisted.
- Event fires to NATS.

```sh
cargo test --test '*' -- --include-ignored  # Run all IT tests
```

No `#[ignore]` attributes — all tests run by default per `feedback_no_ignored_tests.md`.

### 7. Wire FE consumer

In `apps/kanban/ui/src/hooks`:

```typescript
import { useRpcMutation } from "@sunbeam/g2v";
import { CardsService } from "@buf/sunbeam_kanban.connectrpc_es";

export const useUpdateCardColor = () => {
  return useRpcMutation(CardsService.UpdateCardColor, {
    onSuccess: (card) => {
      // Re-fetch or update local cache; the stream will also deliver the event
      queryClient.setQueryData(["card", card.id], card);
    },
  });
};
```

Usage in a component:

```typescript
const updateColor = useUpdateCardColor();
<button onClick={() => updateColor.mutate({ cardId: "card-123", color: "gold" })}>
  Change Color
</button>
```

## Keto Namespace Evolution Recipe (MF-8 Scenario 5)

You want to add a new permission (e.g., rename `view` → `view_v2` with different semantics) without locking anyone out during deploy.

### Step 1: Deploy new relation

Edit `apps/kanban/.integration/keto-namespaces.config.ts`:

```typescript
const namespace = {
  // ... existing ...
  relations: {
    view: { /* ... */ },
    view_v2: { /* ... new definition ... */ },
  },
};
```

Deploy the namespace:

```sh
sunbeam apply kanban
```

Old code still uses `view` for permission checks. New tuples not written yet.

### Step 2: Dual-write both relations

Edit every handler that grants a relation. In `CreateCard`:

```rust
// After inserting the card row in SQL:
keto.write_relation("KanbanCard", &card_id, "view", &user_subject).await?;
keto.write_relation("KanbanCard", &card_id, "view_v2", &user_subject).await?;
```

Deploy:

```sh
sunbeam apply kanban
```

Now permission checks accept both `view` and `view_v2`. Old tokens still validate on `view`. New tuples are written to both.

### Step 3: Switch all checks to v2

Edit `apps/kanban/src/auth/keto_dispatch.rs`. Update the dispatch matrix:

```rust
DispatchEntry {
    method: "/sunbeam.kanban.v1.CardsService/GetCard",
    namespace: "KanbanCard",
    relation: "view_v2",  // Changed from "view"
    object_id_source: ObjectIdSource::Header,
},
// ... repeat for all GetCard, SubscribeBoard, etc. ...
```

Run `cargo run --bin keto-coverage` to verify.

Deploy:

```sh
sunbeam apply kanban
```

All checks now use `view_v2`. The old `view` relation is still written but not checked.

### Step 4: Drop old relation (v1 follow-up)

Once you've verified no regressions:

```typescript
// apps/kanban/.integration/keto-namespaces.config.ts
const namespace = {
  relations: {
    view_v2: { /* ... */ },
    // view: removed
  },
};
```

Stop dual-writing `view` in handlers.

**Why three steps?** Deploy at Step 2 allows old code (still running on old pods) to read/write `view` while new code writes both. At Step 3, all code accepts `view_v2` and the old relation stops being checked. The window where permission is checked on the old relation overlaps with the window where it's still written, so no universal-deny window appears.

## Token Revocation Window

When a user is removed from a project (their Keto tuple is deleted):

1. **Immediate (≤0s):** The tuple is gone from Keto.
2. **Per-yield JWT check (≤1ms):** The user's JWT is still valid (exp is far in future). Token check passes.
3. **Keto recheck (≤30s):** Every 30 seconds (or on Heartbeat at 15s if events are flowing), the stream calls `KetoClient::check_permission` again. On the recheck, Keto says "deny" and the stream closes with `Unauthenticated`.
4. **Known property:** Between tuple deletion and the next Keto recheck, a user can still read/write for ≤30 seconds. This is acceptable for v1.

**Mitigation:** If you need faster revocation, reduce the 30s constant in `apps/kanban/src/auth/keto_dispatch.rs::RECHECK_INTERVAL_SECS`. The tradeoff is higher load on Keto.

## Rollback Recipe

To roll back the service to a previous image:

```sh
sunbeam apply kanban --image-tag <previous_sha>
```

Example:

```sh
# Last deploy was abc123, current is def456, and def456 is broken
sunbeam apply kanban --image-tag abc123
sunbeam ops compose ps kanban  # Watch the rollback complete
sunbeam ops logs kanban -f     # Tail logs for ≥30s
```

**Schema migrations are one-way.** If you roll back the code to a commit that expects the old schema, but the database is already on the new schema, the service will fail to boot. In that case:

1. Identify which migration to undo (check `apps/kanban/migrations/` and the most recent `--down` script).
2. Run the downgrade manually (your DBA or a `sunbeam ops db-migrate` command if one is available).
3. Then roll back the code.

**For v1:** No schema downgrade scripts are shipped. If a code rollback is needed, open an incident and let the on-call DBA handle it.

## Mirror Reconciliation

The `project_members` table in PostgreSQL mirrors Keto tuples. When a user is granted/revoked project access, the handler writes to both Keto and PostgreSQL. Rarely, the SQL write succeeds but the Keto write fails (transient Keto unavailability). The hourly reconciliation cron detects and fixes drift:

```sh
sunbeam ops reconcile kanban-membership
```

(This command will be registered in Stage 7a. Until then, the cron runs automatically in the kanban deployment as a CronJob.)

**Drift metric:** `kanban_mirror_drift_ratio` — if it exceeds 0.1% for ≥1 hour, pages on-call.

The reconciliation queries Keto via `keto_expand_objects(KanbanProject, viewers, *)` and compares the results to `project_members` table. If a row is missing from SQL but present in Keto, it's inserted. If it's in SQL but not in Keto (revoked), it's deleted. **Keto is the source of truth; SQL is rewritten to match.**

## Common Gotchas

### `node_modules/react` duplication

When `@sunbeam/g2v-fe` is installed via `file:../libs/sunbeam-g2v-fe`, it brings its own `node_modules/react`. Vite's deduplication + alias should handle it, but sometimes it doesn't. Symptom: two React instances, component state inconsistencies.

**Fix:**

```sh
cd apps/kanban/ui
rm -rf node_modules/@sunbeam/g2v-fe/node_modules/react*
npm install
```

### `kanban-db-init` must run before kanban-server boots

The `kanban-db-init` binary runs migrations and seeds the database. The `kanban-server` binary expects the schema to exist. In compose, this is managed by `depends_on` + `condition: service_healthy`. In Kubernetes, the `kanban-db-init` job must complete before the `kanban-server` Deployment starts.

Check `sunbeam.workspace.yaml::services.kanban-db-init` and `kustomize` overlays for the correct ordering.

### Keto namespace mount

Keto runs as a pod with a ConfigMap mount at `/etc/namespaces/`. The kanban namespace file is at `/etc/namespaces/kanban.config.ts` inside the pod. When you deploy a new namespace config, the ConfigMap is updated and Keto hot-reloads it. If the reload fails silently:

```sh
sunbeam ops logs keto -f  # Check for errors
sunbeam ops restart keto  # Force a restart
```

The deploy gate at Stage 7e includes a synthetic check (`_kanban_health` tuple) that fails readiness if the namespace didn't load correctly.

## Pointers

- **v2 plan:** `/Users/sienna/Development/sunbeam/.worktrees/feat-kanban/.omc/plans/kanban-plan-v2.md` (§Stage 7d references this).
- **Addendum 1 (vite port, streaming, git mv):** `/Users/sienna/Development/sunbeam/.worktrees/feat-kanban/.omc/plans/kanban-plan-v2-addendum-1.md`.
- **Mockup (UI/UX ground truth):** `/tmp/kanban-mockup-extracted/src/` (from Stage 0a salvage).
- **beam-ui:** `https://src.sunbeam.pt/sunbeam/beam-ui` (JSR `@sunbeam/beam-ui`).
- **g2v (auth + NATS + OTel):** `https://src.sunbeam.pt/sunbeam/sunbeam-g2v` (Cargo + npm).
- **Workspace rules:** `/Users/sienna/Development/sunbeam/CLAUDE.md` (git hosting, deploy gates, testing policy).
