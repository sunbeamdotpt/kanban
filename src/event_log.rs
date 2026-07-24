// SPDX-License-Identifier: AGPL-3.0-or-later
//! Shared `event_log` (transactional outbox) helpers.
//!
//! Every mutation handler inserts exactly one `event_log` row in the same
//! database transaction as the entity write. These helpers centralise that
//! pattern so services don't each grow their own copy:
//!
//! - [`insert_board_event`] bumps `boards.revision` in the same transaction,
//!   merges the new revision into the payload as `"board_revision"`, inserts
//!   the row, and issues `pg_notify('kanban_event_log', '')` so the outbox
//!   dispatcher wakes immediately on commit (delivery happens on commit, so a
//!   rolled-back transaction never notifies).
//! - [`insert_aggregated_event`] does the same for `aggregated_boards`.
//! - [`insert_project_event`] inserts a project-scoped row; projects have no
//!   revision counter, but the notification is still issued.
//!
//! Uses the dynamic sqlx API (no macros) so the crate builds without a
//! live `DATABASE_URL`.

use connectrpc::ConnectError;
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};
use tracing::warn;

use crate::id::Id;

/// NOTIFY channel the outbox dispatcher LISTENs on.
pub(crate) const EVENT_LOG_NOTIFY_CHANNEL: &str = "kanban_event_log";

// ── Error helper ──────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    warn!(error = %err, "{msg}");
    ConnectError::internal(msg)
}

// ── Insert helpers ────────────────────────────────────────────────────────────

/// Insert a board-scoped outbox event.
///
/// Bumps `boards.revision` in the same transaction and merges the new value
/// into `payload` as `"board_revision"`; the dispatcher copies it into the
/// envelope's `board_revision` field.
pub(crate) async fn insert_board_event(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    board_id: Id,
    event_type: &str,
    payload: Value,
) -> Result<(), ConnectError> {
    let revision: i64 =
        sqlx::query("UPDATE boards SET revision = revision + 1 WHERE id = $1 RETURNING revision")
            .bind(board_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| internal("failed to bump board revision", e))?
            .get("revision");

    let payload = with_board_revision(payload, revision);
    insert_row(
        tx,
        tenant_id,
        Some(board_id),
        None,
        None,
        event_type,
        payload,
    )
    .await
}

/// Insert an aggregated-board-scoped outbox event.
///
/// Same contract as [`insert_board_event`], bumping `aggregated_boards.revision`.
pub(crate) async fn insert_aggregated_event(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    aggregated_board_id: Id,
    event_type: &str,
    payload: Value,
) -> Result<(), ConnectError> {
    let revision: i64 = sqlx::query(
        "UPDATE aggregated_boards SET revision = revision + 1 WHERE id = $1 RETURNING revision",
    )
    .bind(aggregated_board_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| internal("failed to bump aggregated board revision", e))?
    .get("revision");

    let payload = with_board_revision(payload, revision);
    insert_row(
        tx,
        tenant_id,
        None,
        Some(aggregated_board_id),
        None,
        event_type,
        payload,
    )
    .await
}

/// Insert a project-scoped outbox event.
///
/// Projects carry no revision counter; the row is routed to
/// `kanban.project.<id>.events` by the dispatcher.
pub(crate) async fn insert_project_event(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    project_id: Id,
    event_type: &str,
    payload: Value,
) -> Result<(), ConnectError> {
    insert_row(
        tx,
        tenant_id,
        None,
        None,
        Some(project_id),
        event_type,
        payload,
    )
    .await
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Merge `"board_revision"` into the payload JSON object.
fn with_board_revision(mut payload: Value, revision: i64) -> Value {
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("board_revision".to_string(), Value::from(revision));
    }
    payload
}

/// Insert the raw `event_log` row and NOTIFY the outbox dispatcher.
///
/// The notification is delivered on commit, never on rollback, so the
/// dispatcher can never observe a row that does not exist.
#[allow(clippy::too_many_arguments)]
async fn insert_row(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    board_id: Option<Id>,
    aggregated_board_id: Option<Id>,
    project_id: Option<Id>,
    event_type: &str,
    payload: Value,
) -> Result<(), ConnectError> {
    sqlx::query(
        "INSERT INTO event_log \
         (id, tenant_id, board_id, aggregated_board_id, project_id, event_type, payload, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())",
    )
    .bind(Id::new())
    .bind(tenant_id)
    .bind(board_id)
    .bind(aggregated_board_id)
    .bind(project_id)
    .bind(event_type)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .map_err(|e| internal("failed to insert event_log row", e))?;

    // Wake the outbox dispatcher; delivery happens on commit.
    sqlx::query("SELECT pg_notify($1, '')")
        .bind(EVENT_LOG_NOTIFY_CHANNEL)
        .execute(&mut **tx)
        .await
        .map_err(|e| internal("failed to pg_notify event_log insert", e))?;

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{containers, seed_card_chain, seed_project, setup_pool};

    /// A board event bumps `boards.revision` and merges it into the payload.
    #[tokio::test]
    async fn insert_board_event_bumps_revision_and_merges_payload() {
        let pool = setup_pool().await;
        let tenant_id = crate::test_support::test_tenant_id();
        let (project_id, board_id, _col, _card) = seed_card_chain(&pool).await;

        let mut tx = pool.begin().await.expect("begin tx");
        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "CardCreated",
            serde_json::json!({ "card_id": "c-1" }),
        )
        .await
        .expect("insert_board_event failed");
        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            serde_json::json!({ "card_id": "c-1" }),
        )
        .await
        .expect("insert_board_event failed");
        tx.commit().await.expect("commit");

        let revision: i64 = sqlx::query_scalar("SELECT revision FROM boards WHERE id = $1")
            .bind(board_id)
            .fetch_one(&pool)
            .await
            .expect("fetch revision");
        assert_eq!(revision, 2, "two events must bump revision twice");

        let payload: Value = sqlx::query_scalar(
            "SELECT payload FROM event_log WHERE board_id = $1 ORDER BY created_at LIMIT 1",
        )
        .bind(board_id)
        .fetch_one(&pool)
        .await
        .expect("fetch payload");
        assert_eq!(
            payload.get("board_revision").and_then(Value::as_i64),
            Some(1)
        );
        assert_eq!(
            payload.get("card_id").and_then(Value::as_str),
            Some("c-1"),
            "existing payload keys must be preserved"
        );

        let _ = sqlx::query("DELETE FROM boards WHERE id = $1")
            .bind(board_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(&pool)
            .await;
    }

    /// A project event is stored with `project_id` and no revision merge.
    #[tokio::test]
    async fn insert_project_event_stores_project_scoped_row() {
        let pool = setup_pool().await;
        let tenant_id = crate::test_support::test_tenant_id();
        let project_id = seed_project(&pool).await;

        let mut tx = pool.begin().await.expect("begin tx");
        insert_project_event(
            &mut tx,
            &tenant_id,
            project_id,
            "MemberAdded",
            serde_json::json!({ "project_id": project_id.to_string(), "subject": "user:a" }),
        )
        .await
        .expect("insert_project_event failed");
        tx.commit().await.expect("commit");

        let row = sqlx::query(
            "SELECT project_id, board_id, payload FROM event_log WHERE project_id = $1",
        )
        .bind(project_id)
        .fetch_one(&pool)
        .await
        .expect("fetch event row");
        let stored_project: Id = row.get("project_id");
        assert_eq!(stored_project, project_id);
        assert!(row.get::<Option<Id>, _>("board_id").is_none());
        let payload: Value = row.get("payload");
        assert!(
            payload.get("board_revision").is_none(),
            "project events must not carry a board revision"
        );

        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(&pool)
            .await;
    }

    /// A committed insert notifies listeners on the outbox channel.
    #[tokio::test]
    async fn committed_insert_notifies_kanban_event_log() {
        let infra = containers::setup().await;
        let pool = infra.pool.clone();
        let tenant_id = crate::test_support::test_tenant_id();
        let (project_id, board_id, _col, _card) = seed_card_chain(&pool).await;

        let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
            .await
            .expect("PgListener connect failed");
        listener
            .listen(EVENT_LOG_NOTIFY_CHANNEL)
            .await
            .expect("LISTEN failed");

        let mut tx = pool.begin().await.expect("begin tx");
        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "CardCreated",
            serde_json::json!({ "card_id": "c-1" }),
        )
        .await
        .expect("insert_board_event failed");
        tx.commit().await.expect("commit");

        let notification = tokio::time::timeout(std::time::Duration::from_secs(2), listener.recv())
            .await
            .expect("timed out waiting for pg_notify")
            .expect("recv failed");
        assert_eq!(notification.channel(), EVENT_LOG_NOTIFY_CHANNEL);

        let _ = sqlx::query("DELETE FROM boards WHERE id = $1")
            .bind(board_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(&pool)
            .await;
    }
}
