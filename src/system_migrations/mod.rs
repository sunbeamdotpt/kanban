// SPDX-License-Identifier: AGPL-3.0-or-later
//! Startup system migrations.
//!
//! This framework runs after SQLx schema migrations and mutates data across
//! Postgres, Keto, and OpenSearch. Each migration is recorded in the
//! `system_migrations` ledger table and executed exactly once.

pub mod context;
pub mod migration;
pub mod registry;
pub mod runner;

pub mod migrations;

pub use context::MigrationContext;
pub use migration::SystemMigration;
pub use registry::all_migrations;
pub use runner::MigrationRunner;
