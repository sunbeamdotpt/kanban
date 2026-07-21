// SPDX-License-Identifier: AGPL-3.0-or-later
//! Search-index write path for cards.
//!
//! `SearchCards` reads from OpenSearch, but nothing indexed documents until
//! this module existed. Two producers use it:
//!
//! - the outbox dispatcher, which indexes/deletes documents as card events
//!   flow through `event_log` (near-real-time), and
//! - the `backfill_opensearch_cards` system migration, which indexes every
//!   existing card once at boot.
//!
//! Errors are logged and swallowed by callers: the index is a derived view
//! and can always be rebuilt by re-running the backfill migration.

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use tracing::warn;

use crate::id::Id;
use crate::integrations::opensearch::{CardDocument, OpenSearchClient};

/// Create the cards index if it does not exist (idempotent).
pub async fn ensure_cards_index(opensearch: &OpenSearchClient, index: &str) -> Result<()> {
    opensearch
        .create_cards_index(index)
        .await
        .with_context(|| format!("failed to ensure OpenSearch index {index}"))
}

/// Load a card and its label names / assignee subjects as a `CardDocument`.
/// Returns `Ok(None)` when the card does not exist (e.g. deleted between the
/// event and the index pass).
pub async fn card_document_from_db(
    pool: &PgPool,
    card_id: Id,
    tenant_id: &str,
) -> Result<Option<CardDocument>> {
    let row = sqlx::query(
        "SELECT id, tenant_id, board_id, project_id, ref, title, description, priority::text, completed_at \
         FROM cards WHERE id = $1 AND tenant_id = $2",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .context("failed to load card for search indexing")?;

    let Some(row) = row else {
        return Ok(None);
    };

    let labels: Vec<String> = sqlx::query_scalar(
        "SELECT l.name FROM labels l \
         JOIN card_labels cl ON cl.label_id = l.id \
         WHERE cl.card_id = $1 AND l.tenant_id = $2 \
         ORDER BY l.name",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .context("failed to load card labels for search indexing")?;

    let assignees: Vec<String> = sqlx::query_scalar(
        "SELECT subject FROM card_assignees WHERE card_id = $1 AND tenant_id = $2 ORDER BY assigned_at",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .context("failed to load card assignees for search indexing")?;

    let board_id: Id = row.get("board_id");
    let project_id: Id = row.get("project_id");
    let description: Option<String> = row.get("description");
    let completed_at: Option<chrono::DateTime<chrono::Utc>> = row.get("completed_at");

    Ok(Some(CardDocument {
        id: card_id.to_string(),
        tenant_id: tenant_id.to_string(),
        board_id: board_id.to_string(),
        project_id: project_id.to_string(),
        card_ref: row.get("ref"),
        title: row.get("title"),
        description: description.unwrap_or_default(),
        priority: row.get("priority"),
        labels,
        assignees,
        completed_at: completed_at.map(|ts| ts.to_rfc3339()),
    }))
}

/// Index one card by id. Missing cards are skipped (they may already be
/// deleted); indexing failures are logged, not propagated.
pub async fn index_card_by_id(
    opensearch: &OpenSearchClient,
    index: &str,
    pool: &PgPool,
    card_id: Id,
    tenant_id: &str,
) {
    match card_document_from_db(pool, card_id, tenant_id).await {
        Ok(Some(doc)) => {
            if let Err(e) = opensearch.index_card(index, &doc).await {
                warn!(card_id = %card_id, error = %e, "search indexing: index_card failed");
            }
        }
        Ok(None) => {
            // Card vanished between event and index pass; nothing to do.
        }
        Err(e) => {
            warn!(card_id = %card_id, error = %e, "search indexing: failed to load card");
        }
    }
}

/// Delete a card document. Failures are logged, not propagated.
pub async fn delete_card_doc(opensearch: &OpenSearchClient, index: &str, card_id: Id) {
    if let Err(e) = opensearch.delete_doc(index, &card_id.to_string()).await {
        warn!(card_id = %card_id, error = %e, "search indexing: delete_doc failed");
    }
}
