//! Shared test helpers for kanban integration tests.
//!
//! All helpers here are `#[cfg(test)]` — they are compiled only during
//! `cargo test`. The module is declared as `#[cfg(test)] pub(crate) mod
//! test_support` in `lib.rs` so every service module can re-use the seeders
//! without duplicating the FK-chain SQL.
//!
//! # Seeder contract
//!
//! Each `seed_*` function allocates fresh UUIDs so parallel test tasks
//! (nextest runs each test in isolation) never collide on unique constraints.
//!
//! # Teardown
//!
//! Tests are responsible for their own cleanup. The seeders do not run
//! inside a transaction that gets rolled back — we hit a shared dev
//! Postgres instance and rely on unique UUIDs to keep tests independent.

use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Seed a minimal project row. Returns the new `project_id`.
pub(crate) async fn seed_project(pool: &PgPool) -> Uuid {
    let project_id = Uuid::new_v4();
    let suffix = project_id.simple().to_string();

    sqlx::query(
        "INSERT INTO projects (id, name, slug, owner_id, created_at, updated_at)
         VALUES ($1, $2, $3, 'user:test', now(), now())",
    )
    .bind(project_id)
    .bind(format!("test-proj-{}", &suffix[0..8]))
    .bind(suffix[0..12].to_lowercase())
    .execute(pool)
    .await
    .expect("seed_project: INSERT failed");

    project_id
}

/// Seed a minimal board row under `project_id`. Returns the new `board_id`.
pub(crate) async fn seed_board(pool: &PgPool, project_id: Uuid) -> Uuid {
    let board_id = Uuid::new_v4();
    let suffix = board_id.simple().to_string();

    sqlx::query(
        "INSERT INTO boards (id, project_id, name, slug, created_at, updated_at)
         VALUES ($1, $2, $3, $4, now(), now())",
    )
    .bind(board_id)
    .bind(project_id)
    .bind(format!("test-board-{}", &suffix[0..8]))
    .bind(suffix[0..12].to_lowercase())
    .execute(pool)
    .await
    .expect("seed_board: INSERT failed");

    board_id
}

/// Seed a minimal column row under `board_id`. Returns the new `column_id`.
pub(crate) async fn seed_column(pool: &PgPool, board_id: Uuid) -> Uuid {
    let column_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO columns (id, board_id, title, position, created_at, updated_at)
         VALUES ($1, $2, 'todo', 0, now(), now())",
    )
    .bind(column_id)
    .bind(board_id)
    .execute(pool)
    .await
    .expect("seed_column: INSERT failed");

    column_id
}

/// Seed a minimal card row under `board_id` / `column_id` / `project_id`.
/// Returns the new `card_id`.
pub(crate) async fn seed_card(
    pool: &PgPool,
    board_id: Uuid,
    column_id: Uuid,
    project_id: Uuid,
) -> Uuid {
    let card_id = Uuid::new_v4();
    let card_suffix = card_id.simple().to_string();

    sqlx::query(
        "INSERT INTO cards \
         (id, board_id, column_id, project_id, ref, title, position, revision, \
          created_by, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, 'test-card', 0, 0, 'user:test', now(), now())",
    )
    .bind(card_id)
    .bind(board_id)
    .bind(column_id)
    .bind(project_id)
    .bind(format!("T{}", &card_suffix[0..6].to_uppercase()))
    .execute(pool)
    .await
    .expect("seed_card: INSERT failed");

    card_id
}

/// Seed a full project → board → column → card chain.
/// Returns `(project_id, board_id, column_id, card_id)`.
///
/// Each call is independent (fresh UUIDs) so parallel tests don't collide.
pub(crate) async fn seed_card_chain(pool: &PgPool) -> (Uuid, Uuid, Uuid, Uuid) {
    let project_id = seed_project(pool).await;
    let board_id = seed_board(pool, project_id).await;
    let column_id = seed_column(pool, board_id).await;
    let card_id = seed_card(pool, board_id, column_id, project_id).await;
    (project_id, board_id, column_id, card_id)
}

/// Insert a raw `event_log` row for testing the outbox dispatcher.
///
/// Returns the inserted row's `id` (UUID).
pub(crate) async fn seed_event_log(
    pool: &PgPool,
    board_id: Uuid,
    event_type: &str,
) -> Uuid {
    let row = sqlx::query(
        "INSERT INTO event_log (id, board_id, event_type, payload, created_at)
         VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, now())
         RETURNING id",
    )
    .bind(board_id)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("seed_event_log: INSERT failed");

    row.get("id")
}

/// Insert a raw `event_log` row that is already dispatched (nats_seq set).
/// Returns the inserted row's `id`.
pub(crate) async fn seed_event_log_dispatched(
    pool: &PgPool,
    board_id: Uuid,
    event_type: &str,
    nats_seq: i64,
) -> Uuid {
    let row = sqlx::query(
        "INSERT INTO event_log \
         (id, board_id, event_type, payload, nats_seq, dispatched_at, created_at)
         VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, $3, now(), now())
         RETURNING id",
    )
    .bind(board_id)
    .bind(event_type)
    .bind(nats_seq)
    .fetch_one(pool)
    .await
    .expect("seed_event_log_dispatched: INSERT failed");

    row.get("id")
}

/// Standard DATABASE_URL resolver for integration tests.
/// Falls back to a sensible dev-compose default.
pub(crate) fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sunbeam:sunbeam@localhost:5432/kanban".to_string())
}

/// Standard NATS URL resolver for integration tests.
pub(crate) fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}
