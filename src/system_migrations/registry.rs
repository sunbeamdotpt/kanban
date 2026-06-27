// SPDX-License-Identifier: AGPL-3.0-or-later
//! Static registry of all system migrations.

use super::SystemMigration;
use super::migrations::uuid_to_ulids::UuidToUlidsMigration;

/// Return every system migration in the order they should run.
pub fn all_migrations() -> Vec<Box<dyn SystemMigration>> {
    vec![Box::new(UuidToUlidsMigration)]
}
