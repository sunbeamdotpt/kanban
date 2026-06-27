// SPDX-License-Identifier: AGPL-3.0-or-later
//! Context passed to every system migration.

use std::sync::Arc;

use sqlx::PgPool;

use crate::integrations::opensearch::OpenSearchClient;
use sunbeam_g2v::middleware::auth::keto::KetoClient;

/// Shared resources available to startup migrations.
#[derive(Clone, Debug)]
pub struct MigrationContext {
    pub pool: PgPool,
    pub keto: Arc<KetoClient>,
    pub opensearch: Arc<OpenSearchClient>,
    pub opensearch_index_name: String,
}

impl MigrationContext {
    pub fn new(
        pool: PgPool,
        keto: Arc<KetoClient>,
        opensearch: Arc<OpenSearchClient>,
        opensearch_index_name: String,
    ) -> Self {
        Self {
            pool,
            keto,
            opensearch,
            opensearch_index_name,
        }
    }
}
