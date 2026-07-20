// SPDX-License-Identifier: AGPL-3.0-or-later
//! Permission expansion helper — enumerate object IDs a subject has a relation on.
//!
//! Used by `ListProjects`, `ListBoards`, and similar handlers to filter results
//! to "rows the user can see" without joining the permission backend in
//! Postgres.

use std::collections::BTreeSet;

use anyhow::{Result, anyhow};

use super::permission_client::PermissionClient;

/// Query parameters for [`expand_objects`].
pub struct ExpandQuery<'a> {
    /// Permission namespace, e.g. `"KanbanProject"`.
    pub namespace: &'a str,
    /// Permission relation, e.g. `"view"`.
    pub relation: &'a str,
    /// Subject string, e.g. `"user:sienna"`. Not logged (PII).
    pub subject: &'a str,
}

/// Return the sorted, deduplicated set of object IDs for which `subject` holds
/// `relation` in `namespace`.
///
/// Uses the sso-gateway `ExpandObjects` RPC, which resolves tenant-scoped
/// expansion server-side.
///
/// # Errors
///
/// - Returns `Err` if the permission backend call fails.
/// - Returns `Err` if the result set would exceed `max_results`.
pub async fn expand_objects(
    client: &PermissionClient,
    q: ExpandQuery<'_>,
    max_results: usize,
) -> Result<BTreeSet<String>> {
    // A zero ceiling is a caller bug; fail before paying for an RPC.
    if max_results == 0 {
        return Err(anyhow!("expand exceeded max_results: 0"));
    }

    let objects = client
        .expand_objects(q.namespace, q.relation, q.subject)
        .await
        .map_err(|e| anyhow!("expand_objects: {e}"))?;

    if objects.len() > max_results {
        return Err(anyhow!("expand exceeded max_results: {}", objects.len()));
    }

    Ok(objects.into_iter().collect())
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::permission_client::PermissionClient;
    use crate::auth::permission_retry::PermissionRetryExt;
    use crate::id::Id;

    /// Permission namespace used for these integration tests.
    const PERMISSION_TYPE: &str = "KanbanProject";
    /// Computed relation used for expand queries (cannot be written directly).
    const RELATION: &str = "view";
    /// Assignable role used when seeding tuples (writes must use roles).
    const GRANT_RELATION: &str = "viewer";

    struct PermissionCtx {
        client: PermissionClient,
    }

    /// Try to reach the shared permission backend via the test harness.
    /// Returns `None` and prints a message if it is unreachable, so tests skip
    /// gracefully.
    async fn probe() -> Option<PermissionCtx> {
        let client = crate::test_support::setup_permission().await;

        // Probe with a check; if the gateway is down this returns an Err.
        if let Err(e) = client
            .check_permission(PERMISSION_TYPE, "probe-object", RELATION, "probe-subject")
            .await
        {
            eprintln!(
                "[permission_expand] permission backend not reachable: {e}; skipping integration tests."
            );
            return None;
        }

        Some(PermissionCtx {
            client: client.as_ref().clone(),
        })
    }

    /// Seed `n` tuples for `subject` in `PERMISSION_TYPE`/`GRANT_RELATION`, each with a
    /// unique object derived from `base_id`. Returns the seeded object IDs.
    async fn seed_tuples(ctx: &PermissionCtx, subject: &str, base_id: &str, n: u32) -> Vec<String> {
        let mut objects = Vec::with_capacity(n as usize);
        for i in 0..n {
            let obj = format!("{base_id}-{i}");
            ctx.client
                .grant_with_retry(PERMISSION_TYPE, &obj, GRANT_RELATION, subject)
                .await
                .expect("seed_tuples: grant failed");
            objects.push(obj);
        }
        objects
    }

    /// Delete all tuples for `subject` in `PERMISSION_TYPE`/`GRANT_RELATION` (test teardown).
    async fn cleanup(ctx: &PermissionCtx, subject: &str) {
        if let Err(e) = ctx
            .client
            .delete_relation_tuples(
                PERMISSION_TYPE,
                None,
                Some(GRANT_RELATION.to_string()),
                Some(subject.to_string()),
            )
            .await
        {
            eprintln!("[permission_expand] cleanup failed for subject={subject}: {e}");
        }
    }

    /// A subject with no tuples gets an empty set and no error.
    #[tokio::test]
    async fn expand_returns_empty_set_for_unknown_subject() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_unknown_{}", Id::new());

        let result = expand_objects(
            &ctx.client,
            ExpandQuery {
                namespace: PERMISSION_TYPE,
                relation: RELATION,
                subject: &subject,
            },
            10_000,
        )
        .await
        .expect("expand_objects should not error for unknown subject");

        assert!(result.is_empty(), "expected empty set, got {result:?}");
    }

    /// Five tuples should return all objects via ExpandObjects.
    #[tokio::test]
    async fn expand_returns_seeded_objects() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_expand_{}", Id::new());
        let base_id = format!("obj-{}", Id::new());
        let seeded = seed_tuples(&ctx, &subject, &base_id, 5).await;

        let result = expand_objects(
            &ctx.client,
            ExpandQuery {
                namespace: PERMISSION_TYPE,
                relation: RELATION,
                subject: &subject,
            },
            10_000,
        )
        .await
        .expect("expand_objects should succeed");

        let expected: BTreeSet<String> = seeded.into_iter().collect();
        assert_eq!(result, expected, "expand result mismatch");

        cleanup(&ctx, &subject).await;
    }

    /// Five tuples with max_results=3 errors before fetching beyond the ceiling.
    #[tokio::test]
    async fn expand_errors_when_exceeds_ceiling() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_ceiling_{}", Id::new());
        let base_id = format!("obj-{}", Id::new());
        seed_tuples(&ctx, &subject, &base_id, 5).await;

        let result = expand_objects(
            &ctx.client,
            ExpandQuery {
                namespace: PERMISSION_TYPE,
                relation: RELATION,
                subject: &subject,
            },
            3,
        )
        .await;

        assert!(result.is_err(), "expected Err when results exceed ceiling");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("expand exceeded max_results"),
            "unexpected error message: {msg}"
        );

        cleanup(&ctx, &subject).await;
    }

    /// max_results=0 errors immediately without fetching anything.
    #[tokio::test]
    async fn expand_errors_when_max_results_is_zero() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_zero_ceiling_{}", Id::new());

        let result = expand_objects(
            &ctx.client,
            ExpandQuery {
                namespace: PERMISSION_TYPE,
                relation: RELATION,
                subject: &subject,
            },
            0,
        )
        .await;

        assert!(result.is_err(), "expected Err when max_results is zero");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expand exceeded max_results"),
            "expected max_results error"
        );
    }
}
