// SPDX-License-Identifier: AGPL-3.0-or-later
//! Keto Expand helper — enumerate object IDs a subject has a relation on.
//!
//! Used by `ListProjects`, `ListBoards`, and `ListCardsByBoard` (as a fallback)
//! to filter results to "rows the user can see" without joining Keto state in
//! Postgres.
//!
//! # Transport note
//!
//! `KetoClient` does not expose a public `ReadService` channel, so this helper
//! calls `list_relation_tuples` from `crate::auth::keto_compat`, which talks to
//! Keto's `ReadService.ListRelationTuples` over gRPC on the read endpoint
//! (port 4466). No side-channel transport is introduced here.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{Result, anyhow};
use sunbeam_g2v::error::ServiceError;
use sunbeam_g2v::middleware::auth::keto::KetoClient;
use tokio::time::sleep;

/// Query parameters for [`expand_objects`].
pub struct ExpandQuery<'a> {
    /// Keto namespace, e.g. `"KanbanProject"`.
    pub namespace: &'a str,
    /// Keto relation, e.g. `"view"`.
    pub relation: &'a str,
    /// Keto subject string, e.g. `"user:sienna"`. Not logged (PII).
    pub subject: &'a str,
    /// Page size hint passed to Keto per request. Tune for throughput vs.
    /// latency; 256 is a reasonable default.
    pub max_page_size: u32,
}

/// Returns `true` if a Keto error looks like a transient SQLite serialization
/// conflict from the in-memory test Keto image.
fn is_keto_retryable(e: &str) -> bool {
    let msg = e.to_lowercase();
    msg.contains("unable to serialize access") || msg.contains("concurrent update")
}

/// Return the sorted, deduplicated set of object IDs for which `subject` holds
/// `relation` in `namespace`.
///
/// Pagination is handled internally. If the running total would exceed
/// `max_results` before the next page is fetched, the function returns
/// `Err` rather than fetching more data.
///
/// Keto read calls are retried a few times when the backend reports a transient
/// serialization conflict, which is common with the in-memory SQLite Keto used
/// in integration tests.
///
/// # Errors
///
/// - Returns `Err` if any Keto gRPC call fails.
/// - Returns `Err` if the result set would exceed `max_results`.
pub async fn expand_objects(
    client: &KetoClient,
    q: ExpandQuery<'_>,
    max_results: usize,
) -> Result<BTreeSet<String>> {
    let mut objects = BTreeSet::new();
    let mut page_token = String::new();
    let mut page_num: u32 = 0;
    let page_size = q.max_page_size.min(1000) as i32;
    const MAX_RETRIES: usize = 4;

    loop {
        // Guard: refuse to fetch the next page if we're already at the ceiling.
        if objects.len() >= max_results {
            return Err(anyhow!("expand exceeded max_results: {}", objects.len()));
        }

        let mut last_err = None;
        let result = loop {
            match crate::auth::keto_compat::list_relation_tuples(
                client,
                q.namespace,
                Some(q.relation),
                Some(q.subject),
                page_size,
                &page_token,
            )
            .await
            {
                Ok(r) => break Ok(r),
                Err(ServiceError::Internal(ref e)) if is_keto_retryable(e.as_str()) => {
                    let attempt = last_err.as_ref().map(|i: &usize| *i).unwrap_or(0);
                    if attempt >= MAX_RETRIES {
                        break Err(ServiceError::Internal(e.clone()));
                    }
                    sleep(Duration::from_millis(25 * (attempt + 1) as u64)).await;
                    last_err = Some(attempt + 1);
                }
                Err(e) => break Err(e),
            }
        };

        let (tuples, next_token) =
            result.map_err(|e| anyhow!("keto list_relation_tuples: {}", e))?;

        let count = tuples.len();
        page_num += 1;
        tracing::debug!(
            namespace = q.namespace,
            relation = q.relation,
            page = page_num,
            items = count,
            "expand_objects: fetched page",
        );

        for tuple in tuples {
            objects.insert(tuple.object);
            if objects.len() > max_results {
                return Err(anyhow!("expand exceeded max_results: {}", objects.len()));
            }
        }

        if next_token.is_empty() {
            break;
        }
        page_token = next_token;
    }

    Ok(objects)
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Id;
    use sunbeam_g2v::middleware::auth::keto::{KetoClient, KetoConfig};

    use crate::auth::keto_retry::KetoRetryExt;

    /// Keto namespace used for these integration tests.
    ///
    /// Must be declared in the Keto namespace config for the dev instance.
    /// `KanbanProject` is present in both the production OPL config and the
    /// legacy in-memory config used by the testcontainers harness.
    const NS: &str = "KanbanProject";
    const RELATION: &str = "view";

    struct KetoCtx {
        client: KetoClient,
    }

    /// Try to reach the shared dev Keto. Returns `None` and prints a message if
    /// it is unreachable, so tests skip gracefully instead of panicking when
    /// the compose stack is down.
    async fn probe() -> Option<KetoCtx> {
        let grpc_endpoint =
            std::env::var("KETO_GRPC_URL").unwrap_or_else(|_| "http://localhost:4466".to_string());
        let write_grpc_endpoint = std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());

        let client = KetoClient::new(KetoConfig {
            grpc_endpoint,
            write_grpc_endpoint,
        });

        // Probe with a check; if Keto is down this returns an Err.
        if let Err(e) = client
            .check_permission(NS, "probe-object", RELATION, "probe-subject")
            .await
        {
            eprintln!(
                "[keto_expand] Keto not reachable: {e}; skipping integration tests. \
                 Run `sunbeam ops compose up keto` to enable."
            );
            return None;
        }

        Some(KetoCtx { client })
    }

    /// Seed `n` tuples for `subject` in `NS`/`RELATION`, each with a unique
    /// object derived from `base_id`. Returns the list of seeded object IDs.
    async fn seed_tuples(ctx: &KetoCtx, subject: &str, base_id: &str, n: u32) -> Vec<String> {
        let mut objects = Vec::with_capacity(n as usize);
        for i in 0..n {
            let obj = format!("{base_id}-{i}");
            ctx.client
                .grant_with_retry(NS, &obj, RELATION, subject)
                .await
                .expect("seed_tuples: grant failed");
            objects.push(obj);
        }
        objects
    }

    /// Delete all tuples for `subject` in `NS`/`RELATION` (test teardown).
    async fn cleanup(ctx: &KetoCtx, subject: &str) {
        if let Err(e) = crate::auth::keto_compat::delete_relation_tuples(
            &ctx.client,
            NS,
            Some(RELATION),
            Some(subject),
        )
        .await
        {
            eprintln!("[keto_expand] cleanup failed for subject={subject}: {e}");
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
                namespace: NS,
                relation: RELATION,
                subject: &subject,
                max_page_size: 256,
            },
            10_000,
        )
        .await
        .expect("expand_objects should not error for unknown subject");

        assert!(result.is_empty(), "expected empty set, got {result:?}");
    }

    /// Five tuples with page_size=2 spans three pages and returns all objects.
    #[tokio::test]
    async fn expand_paginates_when_results_exceed_page() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_expand_{}", Id::new());
        let base_id = format!("obj-{}", Id::new());
        let seeded = seed_tuples(&ctx, &subject, &base_id, 5).await;

        let result = expand_objects(
            &ctx.client,
            ExpandQuery {
                namespace: NS,
                relation: RELATION,
                subject: &subject,
                max_page_size: 2,
            },
            10_000,
        )
        .await
        .expect("expand_objects should succeed");

        // All 5 objects must be present.
        let expected: BTreeSet<String> = seeded.into_iter().collect();
        assert_eq!(result, expected, "paginated result mismatch");

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
                namespace: NS,
                relation: RELATION,
                subject: &subject,
                max_page_size: 256,
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
                namespace: NS,
                relation: RELATION,
                subject: &subject,
                max_page_size: 256,
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

    /// Page sizes above Keto's 1000 cap are truncated internally.
    #[tokio::test]
    async fn expand_caps_page_size_at_1000() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_page_cap_{}", Id::new());
        let base_id = format!("obj-{}", Id::new());
        let seeded = seed_tuples(&ctx, &subject, &base_id, 5).await;

        let result = expand_objects(
            &ctx.client,
            ExpandQuery {
                namespace: NS,
                relation: RELATION,
                subject: &subject,
                max_page_size: 10_000,
            },
            10_000,
        )
        .await
        .expect("expand_objects should succeed");

        let expected: BTreeSet<String> = seeded.into_iter().collect();
        assert_eq!(result, expected);

        cleanup(&ctx, &subject).await;
    }

    #[test]
    fn is_keto_retryable_detects_sqlite_conflicts() {
        assert!(is_keto_retryable(
            "Unable to serialize access due to a concurrent update"
        ));
        assert!(is_keto_retryable(
            "database is locked: concurrent update in another session"
        ));
    }

    #[test]
    fn is_keto_retryable_ignores_unrelated_errors() {
        assert!(!is_keto_retryable("permission denied"));
        assert!(!is_keto_retryable("connection refused"));
        assert!(!is_keto_retryable(""));
    }
}
