// SPDX-License-Identifier: AGPL-3.0-or-later
//! Backfill the OpenSearch cards index.
//!
//! Search indexing via the outbox dispatcher only covers cards changed after
//! v2026.07.3. This migration ensures the cards index exists and indexes
//! every card in the `cards` table, so `SearchCards` finds pre-existing data.

use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result};
use sqlx::Row;

use crate::id::Id;
use crate::search_indexing;

use super::super::context::MigrationContext;
use super::super::migration::SystemMigration;

/// Page size for the cards table scan.
const PAGE_SIZE: i64 = 500;

/// Index all existing cards into OpenSearch.
pub struct OpenSearchCards;

impl SystemMigration for OpenSearchCards {
    fn name(&self) -> &'static str {
        "backfill_opensearch_cards"
    }

    fn description(&self) -> &'static str {
        "Index all existing cards into the OpenSearch cards index"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a MigrationContext,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { backfill(ctx).await })
    }
}

async fn backfill(ctx: &MigrationContext) -> Result<()> {
    search_indexing::ensure_cards_index(&ctx.opensearch, &ctx.opensearch_index_name).await?;

    let mut indexed: u64 = 0;
    let mut last_id: Option<Id> = None;

    loop {
        let rows = match last_id {
            Some(after) => {
                sqlx::query("SELECT id, tenant_id FROM cards WHERE id > $1 ORDER BY id LIMIT $2")
                    .bind(after)
                    .bind(PAGE_SIZE)
                    .fetch_all(&ctx.pool)
                    .await
            }
            None => {
                sqlx::query("SELECT id, tenant_id FROM cards ORDER BY id LIMIT $1")
                    .bind(PAGE_SIZE)
                    .fetch_all(&ctx.pool)
                    .await
            }
        }
        .context("failed to scan cards for search backfill")?;

        if rows.is_empty() {
            break;
        }

        for row in &rows {
            let card_id: Id = row.get("id");
            let tenant_id: String = row.get("tenant_id");

            let doc = search_indexing::card_document_from_db(&ctx.pool, card_id, &tenant_id)
                .await
                .with_context(|| format!("failed to load card {card_id} for search backfill"))?;

            if let Some(doc) = doc {
                ctx.opensearch
                    .index_card(&ctx.opensearch_index_name, &doc)
                    .await
                    .with_context(|| format!("failed to index card {card_id}"))?;
                indexed += 1;
            }

            last_id = Some(card_id);
        }
    }

    tracing::info!(cards = indexed, "OpenSearch card backfill complete");
    Ok(())
}
