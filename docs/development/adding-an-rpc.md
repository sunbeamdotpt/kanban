---
license: AGPL-3.0-or-later
title: Adding an RPC
description: Step-by-step recipe for adding a new gRPC RPC to Kanban.
category: development
order: 3
nav_order: 3
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Adding an RPC

This recipe adds a hypothetical `UpdateCardColor` RPC. Adapt it to the real service you are extending.

## 1. Define the RPC in proto

```proto
// proto/sunbeam/kanban/v1/cards.proto
message UpdateCardColorRequest {
  string card_id = 1;
  string color   = 2;
}

service CardsService {
  rpc UpdateCardColor(UpdateCardColorRequest) returns (Card);
  // ... existing RPCs ...
}
```

## 2. Generate code

```sh
cd /Users/sienna/Development/sunbeam
buf lint proto && buf build proto
buf generate proto --template proto/buf.gen.kanban.yaml

cd apps/kanban/ui
npm run proto:gen
```

## 3. Add the dispatch entry

Edit `src/auth/keto_dispatch.rs`:

```rust
DispatchEntry {
    method: "/sunbeam.kanban.v1.CardsService/UpdateCardColor",
    namespace: "KanbanCard",
    relation: "edit",
    object_id_source: ObjectIdSource::Header,
},
```

Run `cargo run --bin keto-coverage` and verify it exits 0.

## 4. Implement the handler

In `src/services/cards.rs`:

```rust
pub async fn update_card_color(
    State(AppState { db, .. }): State<AppState>,
    Extension(checked_id): Extension<CheckedObjectId>,
    req: UpdateCardColorRequest,
) -> Result<Card, ApiError> {
    let card_id = checked_id.0; // ALWAYS use checked_id, never req.card_id

    let card = sqlx::query_as::<_, Card>(
        "UPDATE cards SET color = $1, revision = revision + 1 WHERE id = $2 RETURNING *"
    )
    .bind(&req.color)
    .bind(&card_id)
    .fetch_one(&db)
    .await?;

    // Emit event via outbox
    infra.publish_event(
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

Wire the service in `src/services/mod.rs`.

## 5. Write tests

Add a `#[cfg(test)]` module at the bottom of `src/services/cards.rs`:

- Authorized user can update the color.
- Unauthorized user gets 403.
- The color is persisted.
- An event_log row is written.

The RPC is now available to any Connect-RPC client over HTTP/2 (or SSE fallback over HTTP/1.1).
