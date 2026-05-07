//! Keto Expand helper — enumerate object IDs a subject has a relation on.
//!
//! Used by `ListProjects`, `ListBoards`, `ListCardsByBoard`-as-fallback to
//! filter to "rows the user can see" without a Postgres join on Keto state.
//!
//! MF-7 resolution: kanban-local first; upstream to sunbeam-g2v in v2 if a
//! second consumer appears (per `feedback_beam_ui_first.md`-analog).
//!
//! # Transport note
//!
//! `KetoClient` does not expose a public `ReadService` channel, so this helper
//! calls the `list_relation_tuples` method added to `KetoClient` in
//! `libs/sunbeam-g2v/src/middleware/auth/keto.rs`.  That method was added as
//! part of this stage (Stage 1.5a) and uses `ReadService.ListRelationTuples`
//! over gRPC on the read endpoint (port 4466).  No side-channel transport is
//! introduced here.

use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use sunbeam_g2v::middleware::auth::keto::KetoClient;

/// Query parameters for [`expand_objects`].
pub struct ExpandQuery<'a> {
    /// Keto namespace, e.g. `"KanbanProject"`.
    pub namespace: &'a str,
    /// Keto relation, e.g. `"view"`.
    pub relation: &'a str,
    /// Keto subject string, e.g. `"user:sienna"`.
    /// Not logged (PII).
    pub subject: &'a str,
    /// Page size hint passed to Keto per request. Tune for throughput vs.
    /// latency; 256 is a reasonable default.
    pub max_page_size: u32,
}

/// Return the sorted, deduplicated set of object IDs for which `subject` holds
/// `relation` in `namespace`.
///
/// Pagination is handled internally.  If the running total would exceed
/// `max_results` before the next page is fetched, the function returns
/// `Err` rather than fetching more data.
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

    loop {
        // Guard: refuse to fetch the next page if we're already at the ceiling.
        if objects.len() >= max_results {
            return Err(anyhow!(
                "expand exceeded max_results: {}",
                objects.len()
            ));
        }

        let (tuples, next_token) = client
            .list_relation_tuples(
                q.namespace,
                Some(q.relation),
                Some(q.subject),
                page_size,
                &page_token,
            )
            .await
            .map_err(|e| anyhow!("keto list_relation_tuples: {}", e))?;

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
                return Err(anyhow!(
                    "expand exceeded max_results: {}",
                    objects.len()
                ));
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
    use sunbeam_g2v::middleware::auth::keto::{KetoClient, KetoConfig};
    use uuid::Uuid;

    /// Keto namespace for kanban integration tests.
    /// Must be declared in the keto namespace config for the dev instance.
    const NS: &str = "KanbanTest";
    const RELATION: &str = "view";

    struct KetoCtx {
        client: KetoClient,
    }

    /// Attempt to reach the shared dev Keto. Returns `None` and prints a
    /// message if unreachable so tests skip gracefully (no panic on CI when
    /// the compose stack is down).
    async fn probe() -> Option<KetoCtx> {
        let grpc_endpoint = std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string());
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
    /// object derived from `base_id`.  Returns the set of seeded object IDs.
    async fn seed_tuples(ctx: &KetoCtx, subject: &str, base_id: &str, n: u32) -> Vec<String> {
        let mut objects = Vec::with_capacity(n as usize);
        for i in 0..n {
            let obj = format!("{base_id}-{i}");
            ctx.client
                .grant(NS, &obj, RELATION, subject)
                .await
                .expect("seed_tuples: grant failed");
            objects.push(obj);
        }
        objects
    }

    /// Delete all tuples for `subject` in `NS`/`RELATION` (test teardown).
    async fn cleanup(ctx: &KetoCtx, subject: &str) {
        if let Err(e) = ctx
            .client
            .delete_relation_tuples(NS, Some(RELATION), Some(subject))
            .await
        {
            eprintln!("[keto_expand] cleanup failed for subject={subject}: {e}");
        }
    }

    /// A subject with no tuples → empty set, no error.
    #[tokio::test]
    async fn expand_returns_empty_set_for_unknown_subject() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_unknown_{}", Uuid::new_v4());

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

    /// 5 tuples with page_size=2 → 3 pages, all 5 objects returned.
    #[tokio::test]
    async fn expand_paginates_when_results_exceed_page() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_expand_{}", Uuid::new_v4());
        let base_id = format!("obj-{}", Uuid::new_v4());
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

    /// 5 tuples, max_results=3 → Err before fetching beyond the ceiling.
    #[tokio::test]
    async fn expand_errors_when_exceeds_ceiling() {
        let Some(ctx) = probe().await else { return };

        let subject = format!("user:_test_ceiling_{}", Uuid::new_v4());
        let base_id = format!("obj-{}", Uuid::new_v4());
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
}
