// SPDX-License-Identifier: AGPL-3.0-or-later
//! System migration: rewrite all existing UUID identifiers to deterministic ULIDs.
//!
//! The migration is idempotent. It skips any identifier that already looks like a
//! canonical ULID, and it uses each entity's `created_at` as the ULID timestamp
//! portion so migrated rows keep their original time-ordering.
//!
//! A single UUID→ULID mapping is built up front from Postgres and shared across
//! Postgres, Keto, and OpenSearch, so all three stores observe the same
//! deterministic translation.
//!
//! The same UUID + timestamp always produces the same ULID, so rerunning the
//! migration after a partial failure is safe.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::id::Id;
use crate::system_migrations::{MigrationContext, SystemMigration};

/// Batch size for Postgres rewrites. Large enough to amortise round-trips,
/// small enough to keep transaction size reasonable.
const POSTGRES_BATCH_SIZE: i64 = 1000;

/// Page size when listing Keto relation tuples.
const KETO_PAGE_SIZE: i32 = 500;

/// Page size when streaming OpenSearch documents.
const OPENSEARCH_PAGE_SIZE: i64 = 500;

pub struct UuidToUlidsMigration;

impl SystemMigration for UuidToUlidsMigration {
    fn name(&self) -> &'static str {
        "uuid_to_ulids"
    }

    fn description(&self) -> &'static str {
        "Rewrite existing UUID identifiers to ULIDs across Postgres, Keto, and OpenSearch"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a MigrationContext,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let mapping = load_or_build_mapping(&ctx.pool)
                .await
                .context("failed to load or build UUID→ULID mapping")?;

            if mapping.is_empty() {
                tracing::info!("uuid_to_ulids: no UUID identifiers found; nothing to migrate");
                return Ok(());
            }

            migrate_postgres(&ctx.pool, &mapping).await?;
            migrate_keto(ctx, &mapping).await?;
            migrate_opensearch(ctx, &mapping).await?;

            drop_mapping_backup(&ctx.pool)
                .await
                .context("failed to drop UUID→ULID mapping backup after successful migration")?;

            Ok(())
        })
    }
}

// ============================================================================
// Postgres
// ============================================================================

/// A single entity table to migrate, plus every column in other tables that
/// references it. Order matters: parents are migrated before children.
struct EntityMigration {
    table: &'static str,
    id_column: &'static str,
    created_at_column: &'static str,
    references: &'static [(&'static str, &'static str)],
}

const ENTITY_MIGRATIONS: &[EntityMigration] = &[
    EntityMigration {
        table: "projects",
        id_column: "id",
        created_at_column: "created_at",
        references: &[
            ("boards", "project_id"),
            ("cards", "project_id"),
            ("milestones", "project_id"),
            ("labels", "project_id"),
            ("board_templates", "project_id"),
            ("card_templates", "project_id"),
            ("project_members", "project_id"),
            ("project_ref_counter", "project_id"),
        ],
    },
    EntityMigration {
        table: "boards",
        id_column: "id",
        created_at_column: "created_at",
        references: &[
            ("columns", "board_id"),
            ("cards", "board_id"),
            ("event_log", "board_id"),
            ("presence", "board_id"),
            ("aggregated_board_sources", "board_id"),
        ],
    },
    EntityMigration {
        table: "columns",
        id_column: "id",
        created_at_column: "created_at",
        references: &[("cards", "column_id")],
    },
    EntityMigration {
        table: "cards",
        id_column: "id",
        created_at_column: "created_at",
        references: &[
            ("card_attachments", "card_id"),
            ("card_labels", "card_id"),
            ("card_assignees", "card_id"),
            ("comments", "card_id"),
            ("checklist_items", "card_id"),
            ("github_links", "card_id"),
            ("card_dependencies", "card_id"),
            ("card_dependencies", "depends_on_card_id"),
            ("idempotency_keys", "response_card_id"),
        ],
    },
    EntityMigration {
        table: "milestones",
        id_column: "id",
        created_at_column: "created_at",
        references: &[("cards", "milestone_id")],
    },
    EntityMigration {
        table: "labels",
        id_column: "id",
        created_at_column: "created_at",
        references: &[("card_labels", "label_id")],
    },
    EntityMigration {
        table: "comments",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
    EntityMigration {
        table: "checklist_items",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
    EntityMigration {
        table: "card_attachments",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
    EntityMigration {
        table: "github_links",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
    EntityMigration {
        table: "board_templates",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
    EntityMigration {
        table: "card_templates",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
    EntityMigration {
        table: "aggregated_boards",
        id_column: "id",
        created_at_column: "created_at",
        references: &[
            ("aggregated_board_sources", "aggregated_board_id"),
            ("aggregated_board_members", "aggregated_board_id"),
            ("event_log", "aggregated_board_id"),
        ],
    },
    EntityMigration {
        table: "event_log",
        id_column: "id",
        created_at_column: "created_at",
        references: &[],
    },
];

async fn migrate_postgres(pool: &PgPool, mapping: &HashMap<String, String>) -> Result<()> {
    drop_foreign_keys(pool)
        .await
        .context("failed to drop foreign keys before UUID→ULID rewrite")?;

    for entity in ENTITY_MIGRATIONS {
        migrate_entity(pool, entity, mapping)
            .await
            .with_context(|| format!("failed to migrate Postgres entity {}", entity.table))?;
    }

    create_foreign_keys(pool)
        .await
        .context("failed to recreate foreign keys after UUID→ULID rewrite")?;

    Ok(())
}

async fn drop_foreign_keys(pool: &PgPool) -> Result<()> {
    let constraints = [
        ("boards", "boards_project_id_fkey"),
        ("columns", "columns_board_id_fkey"),
        ("cards", "cards_project_id_fkey"),
        ("cards", "cards_column_id_fkey"),
        ("cards", "cards_board_id_fkey"),
        ("card_attachments", "card_attachments_card_id_fkey"),
        ("milestones", "milestones_project_id_fkey"),
        ("labels", "labels_project_id_fkey"),
        ("card_labels", "card_labels_card_id_fkey"),
        ("card_labels", "card_labels_label_id_fkey"),
        ("card_assignees", "card_assignees_card_id_fkey"),
        ("comments", "comments_card_id_fkey"),
        ("checklist_items", "checklist_items_card_id_fkey"),
        ("github_links", "github_links_card_id_fkey"),
        ("board_templates", "board_templates_project_id_fkey"),
        ("card_templates", "card_templates_project_id_fkey"),
        (
            "aggregated_board_sources",
            "aggregated_board_sources_aggregated_board_id_fkey",
        ),
        (
            "aggregated_board_sources",
            "aggregated_board_sources_board_id_fkey",
        ),
        (
            "aggregated_board_members",
            "aggregated_board_members_aggregated_board_id_fkey",
        ),
        ("event_log", "event_log_board_id_fkey"),
        ("event_log", "event_log_aggregated_board_id_fkey"),
        ("presence", "presence_board_id_fkey"),
        ("project_ref_counter", "project_ref_counter_project_id_fkey"),
        ("project_members", "project_members_project_id_fkey"),
        ("card_dependencies", "card_dependencies_card_id_fkey"),
        (
            "card_dependencies",
            "card_dependencies_depends_on_card_id_fkey",
        ),
    ];

    for (table, constraint) in constraints {
        sqlx::query(&format!(
            "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"
        ))
        .execute(pool)
        .await
        .with_context(|| format!("failed to drop FK {constraint}"))?;
    }

    Ok(())
}

async fn create_foreign_keys(pool: &PgPool) -> Result<()> {
    let constraints = [
        "ALTER TABLE boards ADD CONSTRAINT boards_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE columns ADD CONSTRAINT columns_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE",
        "ALTER TABLE cards ADD CONSTRAINT cards_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE cards ADD CONSTRAINT cards_column_id_fkey FOREIGN KEY (column_id) REFERENCES columns(id) ON DELETE CASCADE",
        "ALTER TABLE cards ADD CONSTRAINT cards_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE",
        "ALTER TABLE card_attachments ADD CONSTRAINT card_attachments_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE",
        "ALTER TABLE milestones ADD CONSTRAINT milestones_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE labels ADD CONSTRAINT labels_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE card_labels ADD CONSTRAINT card_labels_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE",
        "ALTER TABLE card_labels ADD CONSTRAINT card_labels_label_id_fkey FOREIGN KEY (label_id) REFERENCES labels(id) ON DELETE CASCADE",
        "ALTER TABLE card_assignees ADD CONSTRAINT card_assignees_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE",
        "ALTER TABLE comments ADD CONSTRAINT comments_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE",
        "ALTER TABLE checklist_items ADD CONSTRAINT checklist_items_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE",
        "ALTER TABLE github_links ADD CONSTRAINT github_links_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id)",
        "ALTER TABLE board_templates ADD CONSTRAINT board_templates_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE card_templates ADD CONSTRAINT card_templates_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE aggregated_board_sources ADD CONSTRAINT aggregated_board_sources_aggregated_board_id_fkey FOREIGN KEY (aggregated_board_id) REFERENCES aggregated_boards(id) ON DELETE CASCADE",
        "ALTER TABLE aggregated_board_sources ADD CONSTRAINT aggregated_board_sources_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE",
        "ALTER TABLE aggregated_board_members ADD CONSTRAINT aggregated_board_members_aggregated_board_id_fkey FOREIGN KEY (aggregated_board_id) REFERENCES aggregated_boards(id) ON DELETE CASCADE",
        "ALTER TABLE event_log ADD CONSTRAINT event_log_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE",
        "ALTER TABLE event_log ADD CONSTRAINT event_log_aggregated_board_id_fkey FOREIGN KEY (aggregated_board_id) REFERENCES aggregated_boards(id) ON DELETE CASCADE",
        "ALTER TABLE presence ADD CONSTRAINT presence_board_id_fkey FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE",
        "ALTER TABLE project_ref_counter ADD CONSTRAINT project_ref_counter_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE project_members ADD CONSTRAINT project_members_project_id_fkey FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE",
        "ALTER TABLE card_dependencies ADD CONSTRAINT card_dependencies_card_id_fkey FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE",
        "ALTER TABLE card_dependencies ADD CONSTRAINT card_dependencies_depends_on_card_id_fkey FOREIGN KEY (depends_on_card_id) REFERENCES cards(id) ON DELETE CASCADE",
    ];

    for sql in constraints {
        sqlx::query(sql)
            .execute(pool)
            .await
            .with_context(|| format!("failed to create FK: {sql}"))?;
    }

    Ok(())
}

async fn migrate_entity(
    pool: &PgPool,
    entity: &EntityMigration,
    mapping: &HashMap<String, String>,
) -> Result<()> {
    let mut total = 0u64;

    loop {
        let rows: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT {id} FROM {table} \
             WHERE {id} !~ '^[0-9A-Z]{{26}}$' \
             ORDER BY {id} LIMIT $1",
            id = entity.id_column,
            table = entity.table,
        ))
        .bind(POSTGRES_BATCH_SIZE)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to fetch batch from {}", entity.table))?;

        let batch_len = rows.len();
        if batch_len == 0 {
            break;
        }

        let mut old_ids = Vec::with_capacity(batch_len);
        let mut new_ids = Vec::with_capacity(batch_len);
        for id in rows {
            let ulid = mapping.get(&id).cloned().ok_or_else(|| {
                anyhow::anyhow!("missing ULID mapping for {} id {}", entity.table, id)
            })?;
            old_ids.push(id);
            new_ids.push(ulid);
        }

        rewrite_column(pool, entity.table, entity.id_column, &old_ids, &new_ids)
            .await
            .with_context(|| format!("failed to rewrite {}.{}", entity.table, entity.id_column))?;

        for (ref_table, ref_column) in entity.references {
            rewrite_column(pool, ref_table, ref_column, &old_ids, &new_ids)
                .await
                .with_context(|| format!("failed to rewrite {}.{}", ref_table, ref_column))?;
        }

        total += batch_len as u64;
    }

    if total > 0 {
        tracing::info!(
            table = entity.table,
            rows = total,
            "Postgres entity migrated"
        );
    }

    Ok(())
}

async fn rewrite_column(
    pool: &PgPool,
    table: &str,
    column: &str,
    old_ids: &[String],
    new_ids: &[String],
) -> Result<()> {
    let query = format!(
        "UPDATE {table} SET {column} = m.new_id \
         FROM (SELECT * FROM unnest($1::text[], $2::text[]) AS t(old_id, new_id)) m \
         WHERE {table}.{column} = m.old_id",
        table = table,
        column = column,
    );

    sqlx::query(&query)
        .bind(old_ids)
        .bind(new_ids)
        .execute(pool)
        .await
        .with_context(|| format!("failed to rewrite {table}.{column}"))?;

    Ok(())
}

// ============================================================================
// Keto
// ============================================================================

const KETO_NAMESPACES: &[&str] = &[
    "KanbanProject",
    "KanbanBoard",
    "KanbanCard",
    "KanbanAggregatedBoard",
];

async fn migrate_keto(ctx: &MigrationContext, mapping: &HashMap<String, String>) -> Result<()> {
    if mapping.is_empty() {
        tracing::info!("Keto: no UUIDs remaining to migrate");
        return Ok(());
    }

    for namespace in KETO_NAMESPACES {
        migrate_keto_namespace(ctx, namespace, mapping)
            .await
            .with_context(|| format!("failed to migrate Keto namespace {}", namespace))?;
    }

    Ok(())
}

/// Name of the backup table used to survive a partial migration run.
const MAPPING_BACKUP_TABLE: &str = "system_migration_uuid_to_ulid_mapping";

/// Return the UUID→ULID mapping, building it from Postgres if this is the first
/// run. The mapping is persisted to a backup table before any store is mutated,
/// so if the migration is interrupted after Postgres is rewritten, a restart can
/// still migrate Keto and OpenSearch using the same deterministic translation.
async fn load_or_build_mapping(pool: &PgPool) -> Result<HashMap<String, String>> {
    if mapping_backup_exists(pool).await? {
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {MAPPING_BACKUP_TABLE}"))
                .fetch_one(pool)
                .await
                .context("failed to count mapping backup rows")?;

        if count > 0 {
            tracing::info!(
                count,
                "resuming from interrupted uuid_to_ulids run using backup mapping"
            );
            return load_mapping_backup(pool).await;
        }
    }

    let mapping = build_uuid_to_ulid_mapping(pool).await?;
    if mapping.is_empty() {
        return Ok(mapping);
    }

    create_mapping_backup(pool)
        .await
        .context("failed to create mapping backup table")?;
    persist_mapping_backup(pool, &mapping)
        .await
        .context("failed to persist mapping backup")?;

    Ok(mapping)
}

async fn mapping_backup_exists(pool: &PgPool) -> Result<bool> {
    let exists: Option<String> = sqlx::query_scalar(&format!(
        "SELECT to_regclass('public.{MAPPING_BACKUP_TABLE}')::text"
    ))
    .fetch_one(pool)
    .await
    .context("failed to check mapping backup table existence")?;
    Ok(exists.is_some())
}

async fn create_mapping_backup(pool: &PgPool) -> Result<()> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {MAPPING_BACKUP_TABLE} (
            old_id TEXT PRIMARY KEY,
            new_id TEXT NOT NULL
        )"
    ))
    .execute(pool)
    .await
    .context("failed to create mapping backup table")?;
    Ok(())
}

async fn persist_mapping_backup(pool: &PgPool, mapping: &HashMap<String, String>) -> Result<()> {
    let mut old_ids = Vec::with_capacity(mapping.len());
    let mut new_ids = Vec::with_capacity(mapping.len());
    for (old, new) in mapping {
        old_ids.push(old.clone());
        new_ids.push(new.clone());
    }

    sqlx::query(&format!(
        "INSERT INTO {MAPPING_BACKUP_TABLE} (old_id, new_id)
         SELECT * FROM unnest($1::text[], $2::text[]) AS t(old_id, new_id)
         ON CONFLICT (old_id) DO NOTHING"
    ))
    .bind(&old_ids)
    .bind(&new_ids)
    .execute(pool)
    .await
    .context("failed to insert mapping backup rows")?;

    Ok(())
}

async fn load_mapping_backup(pool: &PgPool) -> Result<HashMap<String, String>> {
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT old_id, new_id FROM {MAPPING_BACKUP_TABLE}"
    ))
    .fetch_all(pool)
    .await
    .context("failed to load mapping backup")?;

    Ok(rows.into_iter().collect())
}

async fn drop_mapping_backup(pool: &PgPool) -> Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {MAPPING_BACKUP_TABLE}"))
        .execute(pool)
        .await
        .context("failed to drop mapping backup table")?;
    Ok(())
}

async fn build_uuid_to_ulid_mapping(pool: &PgPool) -> Result<HashMap<String, String>> {
    let mut mapping = HashMap::new();

    for entity in ENTITY_MIGRATIONS {
        let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(&format!(
            "SELECT {id}, {created} FROM {table} WHERE {id} !~ '^[0-9A-Z]{{26}}$'",
            id = entity.id_column,
            created = entity.created_at_column,
            table = entity.table,
        ))
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to read {} for UUID→ULID mapping", entity.table))?;

        for (id, created_at) in rows {
            let ulid = Id::from_uuid_with_timestamp(&id, created_at.timestamp_millis() as u64)
                .with_context(|| format!("invalid UUID entity id in {}: {id}", entity.table))?
                .to_string();
            mapping.insert(id, ulid);
        }
    }

    Ok(mapping)
}

async fn migrate_keto_namespace(
    ctx: &MigrationContext,
    namespace: &str,
    mapping: &HashMap<String, String>,
) -> Result<()> {
    let mut page_token = String::new();
    let mut total = 0u64;

    loop {
        let (tuples, next_token) = crate::auth::keto_compat::list_relation_tuples(
            &ctx.keto,
            namespace,
            None,
            None,
            KETO_PAGE_SIZE,
            &page_token,
        )
        .await
        .map_err(|e| anyhow::anyhow!("keto list_relation_tuples: {e}"))?;

        for tuple in tuples {
            let converted = convert_keto_tuple(&tuple, mapping);
            if converted == tuple {
                continue;
            }

            let new_subject = converted
                .subject
                .as_ref()
                .map(subject_to_string)
                .ok_or_else(|| anyhow::anyhow!("keto tuple missing subject"))?;
            let old_subject = tuple.subject.as_ref().map(subject_to_string);

            // Write-before-delete: if the grant fails we stop rather than leave
            // the old tuple gone and the new tuple missing.
            crate::auth::keto_retry::retry(|| async {
                ctx.keto
                    .grant(
                        &converted.namespace,
                        &converted.object,
                        &converted.relation,
                        &new_subject,
                    )
                    .await
            })
            .await
            .map_err(|e| anyhow::anyhow!("keto grant for migrated tuple: {e}"))?;

            crate::auth::keto_retry::retry(|| async {
                ctx.keto
                    .delete_relation_tuples(
                        &tuple.namespace,
                        Some(&tuple.object),
                        Some(&tuple.relation),
                        old_subject.as_deref(),
                    )
                    .await
            })
            .await
            .map_err(|e| anyhow::anyhow!("keto delete old tuple: {e}"))?;

            total += 1;
        }

        if next_token.is_empty() {
            break;
        }
        page_token = next_token;
    }

    if total > 0 {
        tracing::info!(namespace, tuples = total, "Keto namespace migrated");
    }

    Ok(())
}

fn convert_keto_tuple(
    tuple: &crate::keto_proto::RelationTuple,
    mapping: &HashMap<String, String>,
) -> crate::keto_proto::RelationTuple {
    let mut converted = tuple.clone();
    converted.object = mapping
        .get(&tuple.object)
        .cloned()
        .unwrap_or_else(|| tuple.object.clone());

    if let Some(subject) = &tuple.subject {
        converted.subject = Some(convert_keto_subject(subject, mapping));
    }

    converted
}

fn convert_keto_subject(
    subject: &crate::keto_proto::Subject,
    mapping: &HashMap<String, String>,
) -> crate::keto_proto::Subject {
    use crate::keto_proto::subject::Ref;
    use crate::keto_proto::{Subject, SubjectSet};

    match subject.r#ref.as_ref() {
        Some(Ref::Id(id)) => Subject {
            r#ref: Some(Ref::Id(id.clone())),
        },
        Some(Ref::Set(set)) => {
            let new_object = mapping
                .get(&set.object)
                .cloned()
                .unwrap_or_else(|| set.object.clone());
            Subject {
                r#ref: Some(Ref::Set(SubjectSet {
                    namespace: set.namespace.clone(),
                    object: new_object,
                    relation: set.relation.clone(),
                })),
            }
        }
        None => subject.clone(),
    }
}

fn subject_to_string(subject: &crate::keto_proto::Subject) -> String {
    use crate::keto_proto::subject::Ref;

    match subject.r#ref.as_ref() {
        Some(Ref::Id(id)) => id.clone(),
        Some(Ref::Set(set)) => {
            if set.relation.is_empty() {
                format!("{}:{}", set.namespace, set.object)
            } else {
                format!("{}:{}#{}", set.namespace, set.object, set.relation)
            }
        }
        None => String::new(),
    }
}

// ============================================================================
// OpenSearch
// ============================================================================

async fn migrate_opensearch(
    ctx: &MigrationContext,
    mapping: &HashMap<String, String>,
) -> Result<()> {
    if mapping.is_empty() {
        tracing::info!("OpenSearch: no UUIDs remaining to migrate");
        return Ok(());
    }

    let index = &ctx.opensearch_index_name;

    if !index_exists(&ctx.opensearch, index).await? {
        tracing::info!(index, "OpenSearch index does not exist; skipping");
        return Ok(());
    }

    // Stream all documents using scroll. The initial search request returns the
    // first page, so we process it before asking for subsequent pages.
    let (scroll_id, first_docs) = start_scroll(&ctx.opensearch, index).await?;
    let mut total = 0u64;
    let mut docs = first_docs;

    loop {
        if docs.is_empty() {
            break;
        }

        for mut doc in docs {
            let old_id = doc.id.clone();
            doc.id = mapping.get(&doc.id).cloned().unwrap_or(doc.id);
            doc.board_id = mapping.get(&doc.board_id).cloned().unwrap_or(doc.board_id);
            doc.project_id = mapping
                .get(&doc.project_id)
                .cloned()
                .unwrap_or(doc.project_id);

            if doc.id == old_id {
                continue;
            }

            ctx.opensearch
                .index_card(index, &doc)
                .await
                .with_context(|| format!("failed to index migrated card {}", old_id))?;

            ctx.opensearch
                .delete_doc(index, &old_id)
                .await
                .with_context(|| format!("failed to delete old OpenSearch doc {}", old_id))?;

            total += 1;
        }

        let (next_docs, has_more) = scroll_page(&ctx.opensearch, index, &scroll_id).await?;
        if next_docs.is_empty() {
            break;
        }
        docs = next_docs;

        if !has_more {
            // This was the last full page; process it then stop.
            continue;
        }
    }

    if total > 0 {
        ctx.opensearch
            .refresh(index)
            .await
            .context("failed to refresh OpenSearch index after migration")?;
        tracing::info!(index, docs = total, "OpenSearch index migrated");
    }

    Ok(())
}

async fn index_exists(
    opensearch: &crate::integrations::opensearch::OpenSearchClient,
    index: &str,
) -> Result<bool> {
    let url = format!("{}/{}", opensearch.base_url(), index);
    let resp = opensearch
        .http()
        .head(&url)
        .send()
        .await
        .with_context(|| format!("opensearch index HEAD failed for {index}"))?;
    Ok(resp.status() == reqwest::StatusCode::OK)
}

async fn start_scroll(
    opensearch: &crate::integrations::opensearch::OpenSearchClient,
    index: &str,
) -> Result<(String, Vec<crate::integrations::opensearch::CardDocument>)> {
    let url = format!("{}/{}/_search?scroll=1m", opensearch.base_url(), index);
    let body = serde_json::json!({
        "size": OPENSEARCH_PAGE_SIZE,
        "query": { "match_all": {} },
        "sort": ["_doc"]
    });

    let resp = opensearch
        .http()
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .with_context(|| format!("opensearch scroll start failed for {index}"))?
        .error_for_status()
        .with_context(|| format!("opensearch scroll start returned error for {index}"))?
        .json::<serde_json::Value>()
        .await
        .context("failed to parse opensearch scroll response")?;

    let scroll_id = resp["_scroll_id"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("opensearch scroll response missing _scroll_id"))?;

    let docs = parse_hits(&resp);
    Ok((scroll_id, docs))
}

fn parse_hits(resp: &serde_json::Value) -> Vec<crate::integrations::opensearch::CardDocument> {
    resp["hits"]["hits"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|hit| serde_json::from_value(hit["_source"].clone()).ok())
        .collect()
}

async fn scroll_page(
    opensearch: &crate::integrations::opensearch::OpenSearchClient,
    index: &str,
    scroll_id: &str,
) -> Result<(Vec<crate::integrations::opensearch::CardDocument>, bool)> {
    let url = format!("{}/_search/scroll", opensearch.base_url());
    let body = serde_json::json!({
        "scroll": "1m",
        "scroll_id": scroll_id,
    });

    let resp = opensearch
        .http()
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .with_context(|| format!("opensearch scroll page failed for {index}"))?
        .error_for_status()
        .with_context(|| format!("opensearch scroll page returned error for {index}"))?
        .json::<serde_json::Value>()
        .await
        .context("failed to parse opensearch scroll page")?;

    let docs = parse_hits(&resp);
    let has_more = docs.len() == OPENSEARCH_PAGE_SIZE as usize;
    Ok((docs, has_more))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_to_string_formats_ids_and_sets() {
        use crate::keto_proto::{Subject, SubjectSet, subject::Ref};

        let id_subject = Subject {
            r#ref: Some(Ref::Id("user:alice".to_string())),
        };
        assert_eq!(subject_to_string(&id_subject), "user:alice");

        let set_subject = Subject {
            r#ref: Some(Ref::Set(SubjectSet {
                namespace: "KanbanProject".to_string(),
                object: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                relation: String::new(),
            })),
        };
        assert_eq!(
            subject_to_string(&set_subject),
            "KanbanProject:550e8400-e29b-41d4-a716-446655440000"
        );

        let relation_set = Subject {
            r#ref: Some(Ref::Set(SubjectSet {
                namespace: "KanbanProject".to_string(),
                object: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                relation: "owner".to_string(),
            })),
        };
        assert_eq!(
            subject_to_string(&relation_set),
            "KanbanProject:550e8400-e29b-41d4-a716-446655440000#owner"
        );
    }

    #[tokio::test]
    async fn migration_rewrites_uuids_to_ulids_across_stores() {
        use std::sync::Arc;
        use std::time::Duration;

        use chrono::Utc;
        use sqlx::postgres::PgPoolOptions;

        use crate::auth::keto_retry::KetoRetryExt;
        use crate::id::Id;
        use crate::integrations::opensearch::CardDocument;
        use crate::system_migrations::MigrationContext;

        let keto = crate::test_support::setup_keto().await;
        let opensearch = crate::test_support::setup_opensearch().await;

        // The system migration drops and recreates foreign keys, so it cannot
        // safely run against the shared test database while other tests are
        // active. Create an isolated database within the same Postgres server.
        let shared_pool = crate::test_support::setup_pool().await;
        let base_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL should be set by the test harness");
        let db_name = format!(
            "kanban_migration_test_{}",
            Id::new().to_string().to_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
            .execute(&shared_pool)
            .await
            .expect("failed to create isolated test database");

        let mut isolated_url = url::Url::parse(&base_url).expect("invalid DATABASE_URL");
        isolated_url.set_path(&format!("/{db_name}"));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect(isolated_url.as_str())
            .await
            .expect("failed to connect to isolated test database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("failed to run schema migrations on isolated database");

        // Use fixed UUIDs so the assertions below can verify the deterministic output.
        let project_uuid = "550e8400-e29b-41d4-a716-446655440000";
        let board_uuid = "660e8400-e29b-41d4-a716-446655440000";
        let column_uuid = "770e8400-e29b-41d4-a716-446655440000";
        let card_uuid = "880e8400-e29b-41d4-a716-446655440000";
        let created_at = Utc::now();
        let created_at_ms = created_at.timestamp_millis() as u64;

        let project_ulid = Id::from_uuid_with_timestamp(project_uuid, created_at_ms)
            .unwrap()
            .to_string();
        let board_ulid = Id::from_uuid_with_timestamp(board_uuid, created_at_ms)
            .unwrap()
            .to_string();
        let card_ulid = Id::from_uuid_with_timestamp(card_uuid, created_at_ms)
            .unwrap()
            .to_string();

        // Seed Postgres rows with UUIDs.
        sqlx::query(
            "INSERT INTO projects (id, name, slug, owner_id, created_at, updated_at)
             VALUES ($1, 'Migrate Test', 'migrate-test', 'user:test', $2, $2)",
        )
        .bind(project_uuid)
        .bind(created_at)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO boards (id, project_id, name, slug, created_at, updated_at)
             VALUES ($1, $2, 'Board', 'board', $3, $3)",
        )
        .bind(board_uuid)
        .bind(project_uuid)
        .bind(created_at)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO columns (id, board_id, title, position, created_at, updated_at)
             VALUES ($1, $2, 'Todo', 0, $3, $3)",
        )
        .bind(column_uuid)
        .bind(board_uuid)
        .bind(created_at)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO cards \
             (id, board_id, column_id, project_id, ref, title, position, revision,
              created_by, created_at, updated_at)
             VALUES ($1, $2, $3, $4, 'TEST-1', 'Card', 0, 0, 'user:test', $5, $5)",
        )
        .bind(card_uuid)
        .bind(board_uuid)
        .bind(column_uuid)
        .bind(project_uuid)
        .bind(created_at)
        .execute(&pool)
        .await
        .unwrap();

        // Seed Keto tuples with UUIDs. Subjects are base64-encoded to match the
        // encoding used by the service (avoids Keto treating "user:test" as a
        // subject set because of the colon).
        let test_subject = crate::auth::keto_retry::keto_subject_id("user:test");
        keto.grant("KanbanProject", project_uuid, "view", &test_subject)
            .await
            .expect("grant project view");
        keto.grant(
            "KanbanBoard",
            board_uuid,
            "parent",
            &format!("KanbanProject:{project_uuid}"),
        )
        .await
        .expect("grant board parent");

        // Seed an OpenSearch document with UUIDs.
        let index = format!(
            "sunbeam-kanban-test-{}",
            Id::new().to_string().to_lowercase()
        );
        opensearch
            .create_cards_index(&index)
            .await
            .expect("create test index");
        opensearch
            .index_card(
                &index,
                &CardDocument {
                    id: card_uuid.to_string(),
                    board_id: board_uuid.to_string(),
                    project_id: project_uuid.to_string(),
                    card_ref: "TEST-1".to_string(),
                    title: "Card".to_string(),
                    description: String::new(),
                    priority: "medium".to_string(),
                    labels: vec![],
                    assignees: vec![],
                    completed_at: None,
                },
            )
            .await
            .expect("index test card");
        opensearch
            .refresh(&index)
            .await
            .expect("refresh test index");

        // Run the migration.
        let ctx = MigrationContext::new(
            pool.clone(),
            Arc::clone(&keto),
            Arc::clone(&opensearch),
            index.clone(),
        );
        UuidToUlidsMigration
            .run(&ctx)
            .await
            .expect("migration should succeed");

        // Postgres assertions.
        let project_id: String =
            sqlx::query_scalar("SELECT id FROM projects WHERE slug = 'migrate-test'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(project_id, project_ulid);

        let board_project_id: String =
            sqlx::query_scalar("SELECT project_id FROM boards WHERE id = $1")
                .bind(&board_ulid)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(board_project_id, project_ulid);

        let card: (String, String, String) =
            sqlx::query_as("SELECT project_id, board_id, column_id FROM cards WHERE id = $1")
                .bind(&card_ulid)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(card.0, project_ulid);
        assert_eq!(card.1, board_ulid);

        // Keto assertions: the old tuples should be gone and the new ones present.
        // Use the retry variant because the in-memory test Keto can lag on
        // read-after-write.
        let allowed = keto
            .check_permission_with_retry("KanbanProject", &project_ulid, "view", "user:test")
            .await
            .expect("check migrated project view");
        assert!(allowed, "migrated project view tuple should exist");

        let old_allowed = keto
            .check_permission_with_retry("KanbanProject", project_uuid, "view", "user:test")
            .await
            .expect("check old project view");
        assert!(!old_allowed, "old project view tuple should be gone");

        // OpenSearch assertions.
        opensearch.refresh(&index).await.unwrap();
        let search_body = serde_json::json!({ "query": { "match_all": {} } });
        let response = opensearch
            .search(&index, &search_body)
            .await
            .expect("search migrated index")
            .expect("index should exist");
        assert_eq!(response.hits.total.value, 1);
        let hit = &response.hits.hits[0];
        assert_eq!(hit.id, card_ulid);
        assert_eq!(hit.source.id, card_ulid);
        assert_eq!(hit.source.board_id, board_ulid);
        assert_eq!(hit.source.project_id, project_ulid);

        // Idempotency: running again should not change anything.
        UuidToUlidsMigration
            .run(&ctx)
            .await
            .expect("migration should be idempotent");

        let project_id2: String =
            sqlx::query_scalar("SELECT id FROM projects WHERE slug = 'migrate-test'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(project_id2, project_ulid);

        // Clean up the test OpenSearch index.
        opensearch.delete_index(&index).await.unwrap();
    }
}
