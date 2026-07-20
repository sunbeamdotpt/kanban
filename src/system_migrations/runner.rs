// SPDX-License-Identifier: AGPL-3.0-or-later
//! Runner that executes registered startup migrations once each.

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{SystemMigration, context::MigrationContext};

/// Executes a static list of system migrations and records progress in Postgres.
pub struct MigrationRunner {
    migrations: Vec<Box<dyn SystemMigration>>,
}

impl MigrationRunner {
    pub fn new(migrations: Vec<Box<dyn SystemMigration>>) -> Self {
        Self { migrations }
    }

    /// Run all pending migrations in registration order.
    pub async fn run_all(&self, ctx: &MigrationContext) -> Result<()> {
        ensure_migrations_table(&ctx.pool)
            .await
            .context("failed to create system_migrations table")?;

        for migration in &self.migrations {
            if is_applied(&ctx.pool, migration.name())
                .await
                .with_context(|| format!("failed to check ledger for {}", migration.name()))?
            {
                tracing::info!(
                    migration = migration.name(),
                    "system migration already applied"
                );
                continue;
            }

            tracing::info!(
                migration = migration.name(),
                description = migration.description(),
                "applying system migration"
            );
            migration
                .run(ctx)
                .await
                .with_context(|| format!("system migration {} failed", migration.name()))?;

            record_applied(&ctx.pool, migration.name())
                .await
                .with_context(|| {
                    format!("failed to record system migration {}", migration.name())
                })?;

            tracing::info!(migration = migration.name(), "system migration applied");
        }

        Ok(())
    }
}

async fn ensure_migrations_table(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS system_migrations (\
            name TEXT PRIMARY KEY,\
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()\
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn is_applied(pool: &PgPool, name: &str) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM system_migrations WHERE name = $1")
        .bind(name)
        .fetch_one(pool)
        .await?;
    Ok(count > 0)
}

async fn record_applied(pool: &PgPool, name: &str) -> Result<()> {
    sqlx::query("INSERT INTO system_migrations (name) VALUES ($1)")
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::id::Id;
    use crate::system_migrations::context::MigrationContext;

    struct CountingMigration {
        name: &'static str,
        counter: Arc<AtomicUsize>,
    }

    impl SystemMigration for CountingMigration {
        fn name(&self) -> &'static str {
            self.name
        }

        fn description(&self) -> &'static str {
            "test migration"
        }

        fn run<'a>(
            &'a self,
            _ctx: &'a MigrationContext,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            let counter = Arc::clone(&self.counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn runner_applies_pending_and_skips_applied() {
        let pool = crate::test_support::setup_pool().await;
        let counter = Arc::new(AtomicUsize::new(0));

        // Use a unique migration name per test run so parallel tests don't share
        // ledger rows.
        let name = format!("test_migration_{}", Id::new());
        let migration = CountingMigration {
            name: Box::leak(name.into_boxed_str()),
            counter: Arc::clone(&counter),
        };

        let ctx = MigrationContext::new(
            pool.clone(),
            crate::test_support::setup_permission().await,
            Arc::new(crate::integrations::opensearch::OpenSearchClient::new(
                crate::integrations::opensearch::OpenSearchConfig {
                    url: "http://localhost:9200".to_string(),
                },
            )),
            "test".to_string(),
        );

        let runner = MigrationRunner::new(vec![Box::new(migration)]);

        runner
            .run_all(&ctx)
            .await
            .expect("first run should succeed");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        runner
            .run_all(&ctx)
            .await
            .expect("second run should succeed");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "applied migration should not run again"
        );
    }
}
