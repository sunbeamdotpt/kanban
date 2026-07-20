// SPDX-License-Identifier: AGPL-3.0-or-later
//! Context passed to every system migration.

use std::sync::Arc;

use sqlx::PgPool;

use crate::auth::permission_client::PermissionClient;
use crate::integrations::opensearch::OpenSearchClient;

/// Shared resources available to startup migrations.
#[derive(Clone, Debug)]
pub struct MigrationContext {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
    pub opensearch: Arc<OpenSearchClient>,
    pub opensearch_index_name: String,
}

impl MigrationContext {
    pub fn new(
        pool: PgPool,
        permission: Arc<PermissionClient>,
        opensearch: Arc<OpenSearchClient>,
        opensearch_index_name: String,
    ) -> Self {
        Self {
            pool,
            permission,
            opensearch,
            opensearch_index_name,
        }
    }
}
