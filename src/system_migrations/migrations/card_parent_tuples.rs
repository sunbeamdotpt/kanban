// SPDX-License-Identifier: AGPL-3.0-or-later
//! Backfill `KanbanCard#parent@KanbanBoard` permission tuples.
//!
//! Cards created before v2026.07.3 have no permission tuples at all: the
//! dispatch middleware checks `KanbanCard:{id}#{view,edit,manage}` directly,
//! and the OpenFGA model computes those relations from the card's parent
//! board. This migration walks the `cards` table and writes the missing
//! parent tuples, one per card, grouped by tenant so each write lands in the
//! correct per-tenant store.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result};
use sqlx::Row;

use crate::auth::permission_client::PermissionClient;
use crate::auth::permission_retry::PermissionRetryExt;
use crate::id::Id;

use super::super::context::MigrationContext;
use super::super::migration::SystemMigration;

/// Permission namespace for card objects.
const PERMISSION_TYPE_CARD: &str = "KanbanCard";
/// Page size for the cards table scan.
const PAGE_SIZE: i64 = 500;

/// Backfill parent tuples for all existing cards.
pub struct CardParentTuples;

impl SystemMigration for CardParentTuples {
    fn name(&self) -> &'static str {
        "backfill_card_parent_tuples"
    }

    fn description(&self) -> &'static str {
        "Write KanbanCard#parent@KanbanBoard tuples for all existing cards"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a MigrationContext,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { backfill(ctx).await })
    }
}

async fn backfill(ctx: &MigrationContext) -> Result<()> {
    // One tenant-scoped client per tenant, built lazily as cards are seen.
    let mut tenant_clients: HashMap<String, PermissionClient> = HashMap::new();
    let mut scanned: u64 = 0;
    let mut last_id: Option<Id> = None;

    loop {
        let rows =
            match last_id {
                Some(after) => sqlx::query(
                    "SELECT id, tenant_id, board_id FROM cards WHERE id > $1 ORDER BY id LIMIT $2",
                )
                .bind(after)
                .bind(PAGE_SIZE)
                .fetch_all(&ctx.pool)
                .await,
                None => {
                    sqlx::query("SELECT id, tenant_id, board_id FROM cards ORDER BY id LIMIT $1")
                        .bind(PAGE_SIZE)
                        .fetch_all(&ctx.pool)
                        .await
                }
            }
            .context("failed to scan cards for parent-tuple backfill")?;

        if rows.is_empty() {
            break;
        }

        for row in &rows {
            let card_id: Id = row.get("id");
            let tenant_id: String = row.get("tenant_id");
            let board_id: Id = row.get("board_id");

            let client = match tenant_clients.get(&tenant_id) {
                Some(c) => c,
                None => {
                    let derived = ctx
                        .permission
                        .tenant_client(&tenant_id)
                        .await
                        .with_context(|| {
                            format!("failed to derive permission client for tenant {tenant_id}")
                        })?;
                    tenant_clients.entry(tenant_id.clone()).or_insert(derived)
                }
            };

            client
                .grant_with_retry(
                    PERMISSION_TYPE_CARD,
                    &card_id.to_string(),
                    "parent",
                    &format!("KanbanBoard:{board_id}"),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to backfill parent tuple for card {card_id} on board {board_id}"
                    )
                })?;

            scanned += 1;
            last_id = Some(card_id);
        }
    }

    tracing::info!(cards = scanned, "card parent-tuple backfill complete");
    Ok(())
}
