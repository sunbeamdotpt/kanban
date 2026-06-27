// SPDX-License-Identifier: AGPL-3.0-or-later
//! `SystemMigration` trait — one unit of work that can mutate Postgres, Keto,
//! and/or OpenSearch during application startup.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;

use super::context::MigrationContext;

/// A single startup migration.
///
/// Implementations are registered in [`crate::system_migrations::registry`] and
/// executed in order by the [`MigrationRunner`]. Each migration is run once and
/// recorded in the `system_migrations` ledger table.
pub trait SystemMigration: Send + Sync {
    /// Unique name recorded in the ledger.
    fn name(&self) -> &'static str;

    /// Human-readable description for logs.
    fn description(&self) -> &'static str;

    /// Execute the migration. Any error is fatal and stops service startup.
    fn run<'a>(
        &'a self,
        ctx: &'a MigrationContext,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
