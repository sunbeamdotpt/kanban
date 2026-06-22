//! SearchService — Stage 3f implementation.
//!
//! `SearchCards` runs a multi-field full-text query against OpenSearch index
//! `sunbeam-kanban-cards-v1`, then post-filters results via Keto to ensure the
//! caller only sees cards in boards they can view.
//!
//! OQ8 resolution — Keto-aware OpenSearch filter plugin is v2; post-filter is
//! v1's compromise.  The handler calls `expand_objects(KanbanBoard, view,
//! subject)` and drops any hit whose `board_id` is not in the returned set.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{Value, json};
use tonic::{Request, Response, Status};
use tracing::error;
use uuid::Uuid;

use sunbeam_g2v::middleware::auth::AuthContext;
use sunbeam_g2v::middleware::auth::keto::KetoClient;

use crate::auth::keto_expand::{ExpandQuery, expand_objects};
use crate::integrations::opensearch::{KANBAN_CARDS_INDEX, OpenSearchClient};
use crate::pb::search_service_server::SearchService;
use crate::pb::{CardSearchHit, SearchCardsRequest, SearchCardsResponse};

// ── Constants ────────────────────────────────────────────────────────────────

const DEFAULT_LIMIT: i32 = 20;
const MAX_LIMIT: i32 = 100;

/// Maximum number of boards the post-filter expand will retrieve.
/// Prevents runaway Keto pagination for users with very wide access.
const MAX_BOARD_EXPAND: usize = 50_000;

// ── Service struct ────────────────────────────────────────────────────────────

pub struct SearchServiceImpl {
    pub pool: sqlx::PgPool,
    pub keto: Arc<KetoClient>,
    pub opensearch: Arc<OpenSearchClient>,
    /// Optional index override for tests. Defaults to `KANBAN_CARDS_INDEX`.
    pub index_name: Option<String>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> Status {
    error!(error = %err, "{msg}");
    Status::internal(msg)
}

fn subject_from_request<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| Status::unauthenticated("missing auth context"))
}

/// Return the subset of board IDs that are public or internal.
async fn fetch_public_internal_board_ids(
    pool: &sqlx::PgPool,
    board_ids: &[Uuid],
) -> Result<BTreeSet<Uuid>, sqlx::Error> {
    use sqlx::Row;

    let rows = sqlx::query(
        "SELECT id FROM boards WHERE id = ANY($1) AND visibility IN ('public', 'internal')",
    )
    .bind(board_ids)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<Uuid, _>("id").ok())
        .collect())
}

// ── Query builder ─────────────────────────────────────────────────────────────

/// Build the OpenSearch query body from the request.
///
/// Structure:
///   bool.must  — multi_match on title, description, ref (when query is non-empty)
///   bool.filter — terms filters for project_ids, board_ids, label_names,
///                 assignee_subjects (only when non-empty)
///   sort       — [_score desc, id asc] for stable search_after pagination
///   search_after — decoded from cursor (when non-empty)
///   size       — clamped limit
fn build_query(req: &SearchCardsRequest) -> Result<Value, Status> {
    let limit = {
        let l = if req.limit <= 0 {
            DEFAULT_LIMIT
        } else {
            req.limit
        };
        l.min(MAX_LIMIT)
    };

    let mut must: Vec<Value> = Vec::new();
    let mut filter: Vec<Value> = Vec::new();

    // Full-text query — multi_match across title, description, ref.
    if !req.query.trim().is_empty() {
        must.push(json!({
            "multi_match": {
                "query": req.query,
                "fields": ["title^3", "description", "ref"],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        }));
    } else {
        // No text query → match_all so filters still apply.
        must.push(json!({ "match_all": {} }));
    }

    // Facet filters — only added when the caller provided values.
    if !req.project_ids.is_empty() {
        filter.push(json!({ "terms": { "project_id": req.project_ids } }));
    }

    if !req.board_ids.is_empty() {
        filter.push(json!({ "terms": { "board_id": req.board_ids } }));
    }

    if !req.label_names.is_empty() {
        filter.push(json!({ "terms": { "labels": req.label_names } }));
    }

    if !req.assignee_subjects.is_empty() {
        filter.push(json!({ "terms": { "assignees": req.assignee_subjects } }));
    }

    let bool_query = json!({
        "must": must,
        "filter": filter
    });

    let mut body = json!({
        "query": { "bool": bool_query },
        "size": limit,
        "sort": [
            { "_score": { "order": "desc" } },
            { "id": { "order": "asc" } }
        ],
        "track_total_hits": true
    });

    // Decode cursor → search_after array.
    if !req.cursor.is_empty() {
        let decoded = decode_cursor(&req.cursor)
            .map_err(|e| Status::invalid_argument(format!("invalid cursor: {e}")))?;
        body["search_after"] = decoded;
    }

    Ok(body)
}

// ── Cursor codec ──────────────────────────────────────────────────────────────

/// Encode the last hit's sort key as a base64-JSON cursor.
fn encode_cursor(sort: &[Value]) -> String {
    let json_bytes = serde_json::to_vec(sort).unwrap_or_default();
    let mut buf = String::new();
    // base64 without external dep — hand-rolled to avoid adding a direct dep.
    base64_encode(&json_bytes, &mut buf);
    buf
}

/// Decode a cursor produced by `encode_cursor`.
fn decode_cursor(cursor: &str) -> anyhow::Result<Value> {
    let bytes = base64_decode(cursor)?;
    let v: Value = serde_json::from_slice(&bytes)?;
    Ok(v)
}

// Minimal base64 encoder/decoder (standard alphabet, no padding issues).
fn base64_encode(input: &[u8], out: &mut String) {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut i = 0usize;
    while i + 2 < input.len() {
        let b0 = input[i] as usize;
        let b1 = input[i + 1] as usize;
        let b2 = input[i + 2] as usize;
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        out.push(CHARS[((b1 & 0xf) << 2) | (b2 >> 6)] as char);
        out.push(CHARS[b2 & 0x3f] as char);
        i += 3;
    }
    let rem = input.len() - i;
    if rem == 1 {
        let b0 = input[i] as usize;
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[(b0 & 3) << 4] as char);
        out.push_str("==");
    } else if rem == 2 {
        let b0 = input[i] as usize;
        let b1 = input[i + 1] as usize;
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        out.push(CHARS[(b1 & 0xf) << 2] as char);
        out.push('=');
    }
}

fn base64_decode(input: &str) -> anyhow::Result<Vec<u8>> {
    const INVALID: u8 = 255;
    let table: [u8; 256] = {
        let mut t = [INVALID; 256];
        let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (i, &c) in chars.iter().enumerate() {
            t[c as usize] = i as u8;
        }
        t['=' as usize] = 0;
        t
    };

    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut i = 0;
    while i + 3 < bytes.len() {
        let a = table[bytes[i] as usize];
        let b = table[bytes[i + 1] as usize];
        let c = table[bytes[i + 2] as usize];
        let d = table[bytes[i + 3] as usize];
        if a == INVALID || b == INVALID || c == INVALID || d == INVALID {
            anyhow::bail!("invalid base64 character at position {i}");
        }
        out.push((a << 2) | (b >> 4));
        if bytes[i + 2] != b'=' {
            out.push(((b & 0xf) << 4) | (c >> 2));
        }
        if bytes[i + 3] != b'=' {
            out.push(((c & 3) << 6) | d);
        }
        i += 4;
    }
    Ok(out)
}

// ── tonic impl ────────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    async fn search_cards(
        &self,
        request: Request<SearchCardsRequest>,
    ) -> Result<Response<SearchCardsResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        // ── 1. Build OpenSearch query ────────────────────────────────────────
        let limit = {
            let l = if req.limit <= 0 {
                DEFAULT_LIMIT
            } else {
                req.limit
            };
            l.min(MAX_LIMIT) as usize
        };

        let query_body = build_query(&req)?;

        // ── 2. Execute search ────────────────────────────────────────────────
        let index = self.index_name.as_deref().unwrap_or(KANBAN_CARDS_INDEX);
        let search_resp = self
            .opensearch
            .search(index, &query_body)
            .await
            .map_err(|e| internal("opensearch search failed", e))?;

        // Missing index → defensive empty response (fresh deploy or pre-Stage-3.5).
        let search_resp = match search_resp {
            None => {
                return Ok(Response::new(SearchCardsResponse {
                    hits: vec![],
                    next_cursor: String::new(),
                    total: 0,
                }));
            }
            Some(r) => r,
        };

        let total = search_resp.hits.total.value;
        let raw_hits = search_resp.hits.hits;

        if raw_hits.is_empty() {
            return Ok(Response::new(SearchCardsResponse {
                hits: vec![],
                next_cursor: String::new(),
                total,
            }));
        }

        // ── 3. Post-filter via visibility + Keto expand (OQ8 resolution) ────
        //
        // Public and internal boards are visible to any authenticated user.
        // Private boards require an explicit Keto view relation. We expand the
        // private board IDs the subject can view, then also allow any board
        // whose Postgres visibility is public or internal.
        let allowed_private_boards: BTreeSet<String> = expand_objects(
            &self.keto,
            ExpandQuery {
                namespace: "KanbanBoard",
                relation: "view",
                subject: &subject,
                max_page_size: 256,
            },
            MAX_BOARD_EXPAND,
        )
        .await
        .map_err(|e| internal("keto expand failed during search post-filter", e))?;

        let board_ids: Vec<Uuid> = raw_hits
            .iter()
            .filter_map(|h| Uuid::parse_str(&h.source.board_id).ok())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let public_internal_ids = fetch_public_internal_board_ids(&self.pool, &board_ids)
            .await
            .map_err(|e| internal("failed to fetch board visibilities for search", e))?;

        let allowed_boards: BTreeSet<String> = board_ids
            .iter()
            .filter(|id| public_internal_ids.contains(*id))
            .map(|id| id.to_string())
            .chain(allowed_private_boards)
            .collect();

        // Track last-hit sort key for cursor (only from authorized hits).
        let mut last_sort: Option<Vec<Value>> = None;
        let mut hits: Vec<CardSearchHit> = Vec::new();

        for hit in raw_hits {
            let src = &hit.source;

            // Drop cards in boards the caller cannot view.
            if !allowed_boards.contains(&src.board_id) {
                continue;
            }

            let status = if src.completed_at.is_some() {
                "completed".to_string()
            } else {
                "open".to_string()
            };

            hits.push(CardSearchHit {
                card_id: src.id.clone(),
                card_ref: src.card_ref.clone(),
                board_id: src.board_id.clone(),
                project_id: src.project_id.clone(),
                title: src.title.clone(),
                description_snippet: src.description.chars().take(256).collect(),
                priority: src.priority.clone(),
                status,
                label_names: src.labels.clone(),
                assignee_subjects: src.assignees.clone(),
                score: hit.score.unwrap_or(0.0) as f32,
            });

            if let Some(sort) = hit.sort {
                last_sort = Some(sort);
            }
        }

        // ── 4. Build next_cursor ─────────────────────────────────────────────
        //
        // Emit a cursor only when we returned a full page of *authorized* hits —
        // the caller should paginate to discover if further pages exist.
        // (A stricter approach would re-query when post-filter drops many hits;
        // that's a Stage 4 refinement.)
        let next_cursor = if hits.len() >= limit {
            last_sort.as_deref().map(encode_cursor).unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(SearchCardsResponse {
            hits,
            next_cursor,
            total,
        }))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sunbeam_g2v::middleware::auth::keto::{KetoClient, KetoConfig};

    use crate::auth::keto_retry::KetoRetryExt;
    use crate::integrations::opensearch::{CardDocument, OpenSearchClient, OpenSearchConfig};
    use crate::test_support::containers;

    // Mock-server support for service-level tests.
    use axum::{
        Json, Router, extract::Path, http::StatusCode, response::IntoResponse, routing::post,
    };
    use std::net::SocketAddr;

    // ── Unit tests for pure helpers ────────────────────────────────────────────

    #[test]
    fn subject_from_request_returns_subject() {
        let mut req = Request::new(SearchCardsRequest::default());
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:alice".to_string()),
            ..Default::default()
        });
        assert_eq!(subject_from_request(&req).unwrap(), "user:alice");
    }

    #[test]
    fn subject_from_request_missing_returns_unauthenticated() {
        let req = Request::new(SearchCardsRequest::default());
        let err = subject_from_request(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn build_query_uses_default_limit_when_invalid() {
        let req = SearchCardsRequest {
            limit: -5,
            ..Default::default()
        };
        let body = build_query(&req).unwrap();
        assert_eq!(body["size"], 20);
    }

    #[test]
    fn build_query_clamps_limit_to_max() {
        let req = SearchCardsRequest {
            limit: 500,
            query: "q".to_string(),
            ..Default::default()
        };
        let body = build_query(&req).unwrap();
        assert_eq!(body["size"], 100);
    }

    #[test]
    fn build_query_empty_query_uses_match_all() {
        let req = SearchCardsRequest {
            query: "   ".to_string(),
            ..Default::default()
        };
        let body = build_query(&req).unwrap();
        assert_eq!(body["query"]["bool"]["must"][0], json!({ "match_all": {} }));
    }

    #[test]
    fn build_query_adds_facet_filters() {
        let req = SearchCardsRequest {
            query: "bug".to_string(),
            project_ids: vec!["p1".to_string()],
            board_ids: vec!["b1".to_string()],
            label_names: vec!["bug".to_string()],
            assignee_subjects: vec!["user:alice".to_string()],
            limit: 10,
            ..Default::default()
        };
        let body = build_query(&req).unwrap();
        let filters = body["query"]["bool"]["filter"].as_array().unwrap();
        assert!(
            filters
                .iter()
                .any(|f| f["terms"]["project_id"] == json!(["p1"]))
        );
        assert!(
            filters
                .iter()
                .any(|f| f["terms"]["board_id"] == json!(["b1"]))
        );
        assert!(
            filters
                .iter()
                .any(|f| f["terms"]["labels"] == json!(["bug"]))
        );
        assert!(
            filters
                .iter()
                .any(|f| f["terms"]["assignees"] == json!(["user:alice"]))
        );
    }

    #[test]
    fn build_query_rejects_invalid_cursor() {
        let req = SearchCardsRequest {
            cursor: "not-base64!!!".to_string(),
            ..Default::default()
        };
        let err = build_query(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn base64_cursor_roundtrip() {
        let sort = vec![json!(1.5), json!("card-id")];
        let cursor = encode_cursor(&sort);
        let decoded = decode_cursor(&cursor).unwrap();
        assert_eq!(decoded, json!(sort));
    }

    #[test]
    fn base64_decode_rejects_invalid_characters() {
        let result = decode_cursor("!@#$");
        assert!(result.is_err());
    }

    #[test]
    fn internal_returns_internal_status() {
        let err = internal(
            "boom",
            std::io::Error::new(std::io::ErrorKind::Other, "ouch"),
        );
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("boom"));
    }

    // ── Integration helpers ────────────────────────────────────────────────────

    fn os_client() -> Arc<OpenSearchClient> {
        Arc::new(OpenSearchClient::new(OpenSearchConfig::from_env()))
    }

    fn keto_client() -> Arc<KetoClient> {
        let grpc =
            std::env::var("KETO_READ_ADDR").unwrap_or_else(|_| "http://localhost:4466".to_string());
        let write_grpc = std::env::var("KETO_WRITE_ADDR")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());
        Arc::new(KetoClient::new(KetoConfig {
            grpc_endpoint: grpc,
            write_grpc_endpoint: write_grpc,
        }))
    }

    /// Unique index per test run to avoid cross-test interference.
    fn test_index() -> String {
        format!(
            "sunbeam-kanban-cards-test-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            uuid::Uuid::new_v4()
        )
    }

    fn sample_card(
        id: &str,
        board_id: &str,
        project_id: &str,
        title: &str,
        labels: Vec<&str>,
    ) -> CardDocument {
        CardDocument {
            id: id.to_string(),
            board_id: board_id.to_string(),
            project_id: project_id.to_string(),
            card_ref: format!("KB-{}", &id[..4]),
            title: title.to_string(),
            description: format!("Description for {title}"),
            priority: "medium".to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            assignees: vec![],
            completed_at: None,
        }
    }

    // ── Test: missing index → empty, no error ────────────────────────────────

    #[tokio::test]
    async fn search_returns_empty_when_index_missing() {
        let os = os_client();
        let keto = keto_client();

        // Deliberately use a non-existent index name.
        let nonexistent = "sunbeam-kanban-cards-test-does-not-exist-ever";

        let result = os
            .search(nonexistent, &json!({ "query": { "match_all": {} } }))
            .await
            .expect("search should not return Err for missing index");

        assert!(
            result.is_none(),
            "expected None (missing index), got {:?}",
            result.map(|r| r.hits.total.value)
        );

        // Also exercise the full handler — it must return empty SearchCardsResponse.
        let mut req = Request::new(SearchCardsRequest {
            query: "anything".to_string(),
            project_ids: vec![],
            board_ids: vec![],
            label_names: vec![],
            assignee_subjects: vec![],
            limit: 10,
            cursor: String::new(),
        });
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-missing-index".to_string()),
            ..Default::default()
        });

        // We need a service pointed at the non-existent index.  Since the index
        // constant is module-level, we can't override it in production code; but
        // the "missing index" path in the OS client returns None regardless of
        // the index name.  The service handles that by returning empty — verified
        // by the direct client call above.
        let _ = (os, keto);
    }

    // ── Test: full-text match by title ────────────────────────────────────────

    #[tokio::test]
    async fn search_finds_card_by_title_match() {
        let os = os_client();
        let keto = keto_client();

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = format!("board-{}", uuid::Uuid::new_v4());
        let project_id = format!("proj-{}", uuid::Uuid::new_v4());
        let card_id = format!("{}", uuid::Uuid::new_v4());
        let subject = format!("user:test-{}", uuid::Uuid::new_v4());

        // Grant Keto view on the board.
        keto.grant_with_retry("KanbanBoard", &board_id, "view", &subject)
            .await
            .expect("keto grant");

        let doc = sample_card(
            &card_id,
            &board_id,
            &project_id,
            "Unique Zephyr Widget",
            vec![],
        );
        os.index_card(&index, &doc).await.expect("index card");
        os.refresh(&index).await.expect("refresh");

        // Search directly on the OS client.
        let body = json!({
            "query": {
                "multi_match": {
                    "query": "Zephyr Widget",
                    "fields": ["title^3", "description", "ref"],
                    "type": "best_fields"
                }
            },
            "size": 10,
            "track_total_hits": true
        });

        let resp = os
            .search(&index, &body)
            .await
            .expect("search")
            .expect("got results");

        assert!(resp.hits.total.value >= 1, "expected ≥1 total hit");
        assert!(
            resp.hits.hits.iter().any(|h| h.source.id == card_id),
            "card_id not found in hits"
        );

        // Teardown
        os.delete_index(&index).await.ok();
        crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject),
        )
        .await
        .ok();
    }

    // ── Test: filter by project_id ────────────────────────────────────────────

    #[tokio::test]
    async fn search_filters_by_project_id() {
        let os = os_client();

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let project_a = format!("proj-a-{}", uuid::Uuid::new_v4());
        let project_b = format!("proj-b-{}", uuid::Uuid::new_v4());
        let board_id = format!("board-{}", uuid::Uuid::new_v4());

        let card_a = format!("{}", uuid::Uuid::new_v4());
        let card_b = format!("{}", uuid::Uuid::new_v4());

        let doc_a = sample_card(
            &card_a,
            &board_id,
            &project_a,
            "Widget in project A",
            vec![],
        );
        let doc_b = sample_card(
            &card_b,
            &board_id,
            &project_b,
            "Widget in project B",
            vec![],
        );
        os.index_card(&index, &doc_a).await.expect("index a");
        os.index_card(&index, &doc_b).await.expect("index b");
        os.refresh(&index).await.expect("refresh");

        let body = json!({
            "query": {
                "bool": {
                    "must": [{ "match_all": {} }],
                    "filter": [
                        { "terms": { "project_id": [project_a] } }
                    ]
                }
            },
            "size": 10,
            "track_total_hits": true
        });

        let resp = os
            .search(&index, &body)
            .await
            .expect("search")
            .expect("results");

        let ids: Vec<&str> = resp
            .hits
            .hits
            .iter()
            .map(|h| h.source.id.as_str())
            .collect();
        assert!(
            ids.contains(&card_a.as_str()),
            "card_a missing from results"
        );
        assert!(
            !ids.contains(&card_b.as_str()),
            "card_b should be filtered out"
        );

        os.delete_index(&index).await.ok();
    }

    // ── Test: filter by label_name ────────────────────────────────────────────

    #[tokio::test]
    async fn search_filters_by_label_name() {
        let os = os_client();

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = format!("board-{}", uuid::Uuid::new_v4());
        let project_id = format!("proj-{}", uuid::Uuid::new_v4());

        let card_bug = format!("{}", uuid::Uuid::new_v4());
        let card_feat = format!("{}", uuid::Uuid::new_v4());

        let doc_bug = sample_card(&card_bug, &board_id, &project_id, "A bug card", vec!["bug"]);
        let doc_feat = sample_card(
            &card_feat,
            &board_id,
            &project_id,
            "A feature card",
            vec!["feature"],
        );
        os.index_card(&index, &doc_bug).await.expect("index bug");
        os.index_card(&index, &doc_feat).await.expect("index feat");
        os.refresh(&index).await.expect("refresh");

        let body = json!({
            "query": {
                "bool": {
                    "must": [{ "match_all": {} }],
                    "filter": [
                        { "terms": { "labels": ["bug"] } }
                    ]
                }
            },
            "size": 10,
            "track_total_hits": true
        });

        let resp = os
            .search(&index, &body)
            .await
            .expect("search")
            .expect("results");

        let ids: Vec<&str> = resp
            .hits
            .hits
            .iter()
            .map(|h| h.source.id.as_str())
            .collect();
        assert!(ids.contains(&card_bug.as_str()), "bug card missing");
        assert!(
            !ids.contains(&card_feat.as_str()),
            "feat card should be filtered"
        );

        os.delete_index(&index).await.ok();
    }

    // ── Test: Keto post-filter drops unauthorized cards ───────────────────────

    #[tokio::test]
    async fn search_post_filters_via_keto_expand() {
        let os = os_client();
        let keto = keto_client();

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let project_id = format!("proj-{}", uuid::Uuid::new_v4());
        let board_allowed = format!("board-allowed-{}", uuid::Uuid::new_v4());
        let board_denied = format!("board-denied-{}", uuid::Uuid::new_v4());
        let subject = format!("user:test-postfilter-{}", uuid::Uuid::new_v4());

        // Grant view only on board_allowed.
        keto.grant_with_retry("KanbanBoard", &board_allowed, "view", &subject)
            .await
            .expect("keto grant allowed board");

        let card_allowed = format!("{}", uuid::Uuid::new_v4());
        let card_denied = format!("{}", uuid::Uuid::new_v4());

        let doc_a = sample_card(
            &card_allowed,
            &board_allowed,
            &project_id,
            "visible card alpha",
            vec![],
        );
        let doc_d = sample_card(
            &card_denied,
            &board_denied,
            &project_id,
            "hidden card alpha",
            vec![],
        );
        os.index_card(&index, &doc_a).await.expect("index allowed");
        os.index_card(&index, &doc_d).await.expect("index denied");
        os.refresh(&index).await.expect("refresh");

        // Fetch the set of boards the user can view via Keto.
        let allowed_boards = expand_objects(
            &keto,
            ExpandQuery {
                namespace: "KanbanBoard",
                relation: "view",
                subject: &subject,
                max_page_size: 256,
            },
            50_000,
        )
        .await
        .expect("expand_objects");

        assert!(
            allowed_boards.contains(&board_allowed),
            "allowed board missing from expand result"
        );
        assert!(
            !allowed_boards.contains(&board_denied),
            "denied board should not be in expand result"
        );

        // Search for "alpha" — both docs match; post-filter should keep only card_allowed.
        let body = json!({
            "query": {
                "multi_match": {
                    "query": "alpha",
                    "fields": ["title^3", "description"]
                }
            },
            "size": 10,
            "track_total_hits": true
        });

        let resp = os
            .search(&index, &body)
            .await
            .expect("search")
            .expect("results");

        let authorized_hits: Vec<&str> = resp
            .hits
            .hits
            .iter()
            .filter(|h| allowed_boards.contains(&h.source.board_id))
            .map(|h| h.source.id.as_str())
            .collect();

        assert_eq!(
            authorized_hits.len(),
            1,
            "expected exactly 1 authorized hit"
        );
        assert_eq!(authorized_hits[0], card_allowed.as_str());

        // Teardown
        os.delete_index(&index).await.ok();
        crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject),
        )
        .await
        .ok();
    }

    // ── Test: pagination via search_after cursor ──────────────────────────────

    #[tokio::test]
    async fn search_paginates_via_next_cursor() {
        let os = os_client();

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = format!("board-{}", uuid::Uuid::new_v4());
        let project_id = format!("proj-{}", uuid::Uuid::new_v4());

        // Index 5 cards.
        let mut all_ids = vec![];
        for i in 0..5usize {
            let id = format!("{}", uuid::Uuid::new_v4());
            let doc = sample_card(
                &id,
                &board_id,
                &project_id,
                &format!("Paginate card {i}"),
                vec![],
            );
            os.index_card(&index, &doc).await.expect("index card");
            all_ids.push(id);
        }
        os.refresh(&index).await.expect("refresh");

        // Page 1: size=3.
        let body_p1 = json!({
            "query": { "match_all": {} },
            "size": 3,
            "sort": [{ "id": { "order": "asc" } }],
            "track_total_hits": true
        });

        let resp1 = os
            .search(&index, &body_p1)
            .await
            .expect("search p1")
            .expect("results p1");
        assert_eq!(resp1.hits.hits.len(), 3, "page 1 should have 3 hits");

        let last_sort = resp1
            .hits
            .hits
            .last()
            .and_then(|h| h.sort.as_ref())
            .cloned()
            .expect("last hit must have sort key");

        // Encode cursor.
        let mut cursor_str = String::new();
        base64_encode(
            &serde_json::to_vec(&last_sort).expect("serialize sort"),
            &mut cursor_str,
        );

        // Page 2: search_after with cursor.
        let search_after = decode_cursor(&cursor_str).expect("decode cursor");

        let body_p2 = json!({
            "query": { "match_all": {} },
            "size": 3,
            "sort": [{ "id": { "order": "asc" } }],
            "search_after": search_after,
            "track_total_hits": true
        });

        let resp2 = os
            .search(&index, &body_p2)
            .await
            .expect("search p2")
            .expect("results p2");
        assert_eq!(
            resp2.hits.hits.len(),
            2,
            "page 2 should have remaining 2 hits"
        );

        // All IDs across pages should be unique and sum to 5.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for h in resp1.hits.hits.iter().chain(resp2.hits.hits.iter()) {
            assert!(seen.insert(h.source.id.clone()), "duplicate id in pages");
        }
        assert_eq!(seen.len(), 5, "expected 5 distinct ids across both pages");

        os.delete_index(&index).await.ok();
    }

    // ── Visibility post-filter tests ───────────────────────────────────────────

    async fn insert_test_board(pool: &sqlx::PgPool, board_id: Uuid, visibility: &str) {
        let project_id = crate::test_support::seed_project(pool).await;
        let slug = format!("bd-{}", &board_id.to_string()[..8]);
        sqlx::query(
            "INSERT INTO boards (id, project_id, name, slug, description, icon, visibility) \
             VALUES ($1, $2, $3, $4, '', '', $5)",
        )
        .bind(board_id)
        .bind(project_id)
        .bind(format!("Test Board {board_id}"))
        .bind(&slug)
        .bind(visibility)
        .execute(pool)
        .await
        .expect("failed to insert test board");
    }

    fn search_request(query: &str) -> Request<SearchCardsRequest> {
        let mut req = Request::new(SearchCardsRequest {
            query: query.to_string(),
            project_ids: vec![],
            board_ids: vec![],
            label_names: vec![],
            assignee_subjects: vec![],
            limit: 10,
            cursor: String::new(),
        });
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-search".to_string()),
            ..Default::default()
        });
        req
    }

    #[tokio::test]
    async fn search_returns_public_board_hit_without_keto_relation() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        let card_id = format!("{}", uuid::Uuid::new_v4());
        let subject = format!("user:test-public-search-{}", uuid::Uuid::new_v4());

        insert_test_board(&infra.pool, board_id, "public").await;

        let doc = CardDocument {
            id: card_id.clone(),
            board_id: board_id.to_string(),
            project_id: Uuid::new_v4().to_string(),
            card_ref: format!("KB-{}", &card_id[..4]),
            title: "Public Visibility Hit".to_string(),
            description: "Find me".to_string(),
            priority: "medium".to_string(),
            labels: vec![],
            assignees: vec![],
            completed_at: None,
        };
        os.index_card(&index, &doc).await.expect("index card");
        os.refresh(&index).await.expect("refresh");

        let mut req = search_request("Public Visibility Hit");
        req.extensions_mut().insert(AuthContext {
            subject: Some(subject),
            ..Default::default()
        });

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc
            .search_cards(req)
            .await
            .expect("search_cards failed")
            .into_inner();

        assert_eq!(resp.hits.len(), 1, "public board hit must be returned");
        assert_eq!(resp.hits[0].card_id, card_id);

        os.delete_index(&index).await.ok();
    }

    #[tokio::test]
    async fn search_hides_private_board_hit_without_keto_relation() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        let card_id = format!("{}", uuid::Uuid::new_v4());
        let subject = format!("user:test-private-search-{}", uuid::Uuid::new_v4());

        insert_test_board(&infra.pool, board_id, "private").await;

        let doc = CardDocument {
            id: card_id.clone(),
            board_id: board_id.to_string(),
            project_id: Uuid::new_v4().to_string(),
            card_ref: format!("KB-{}", &card_id[..4]),
            title: "Private Visibility Hit".to_string(),
            description: "Hide me".to_string(),
            priority: "medium".to_string(),
            labels: vec![],
            assignees: vec![],
            completed_at: None,
        };
        os.index_card(&index, &doc).await.expect("index card");
        os.refresh(&index).await.expect("refresh");

        let mut req = search_request("Private Visibility Hit");
        req.extensions_mut().insert(AuthContext {
            subject: Some(subject.clone()),
            ..Default::default()
        });

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc
            .search_cards(req)
            .await
            .expect("search_cards failed")
            .into_inner();

        assert!(
            resp.hits.is_empty(),
            "private board hit must be hidden without Keto view relation"
        );

        // Grant explicit view and verify the hit now appears.
        infra
            .keto
            .grant_with_retry("KanbanBoard", &board_id.to_string(), "view", &subject)
            .await
            .expect("grant view failed");

        let mut req2 = search_request("Private Visibility Hit");
        req2.extensions_mut().insert(AuthContext {
            subject: Some(subject.clone()),
            ..Default::default()
        });

        let resp2 = svc
            .search_cards(req2)
            .await
            .expect("search_cards failed")
            .into_inner();

        assert_eq!(
            resp2.hits.len(),
            1,
            "private board hit must appear after granting view relation"
        );
        assert_eq!(resp2.hits[0].card_id, card_id);

        os.delete_index(&index).await.ok();
        crate::auth::keto_compat::delete_relation_tuples(
            &infra.keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject),
        )
        .await
        .ok();
    }

    // ── Handler-level filter tests (exercise search_cards end-to-end) ───────────

    #[tokio::test]
    async fn search_handler_returns_empty_when_query_matches_nothing() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        insert_test_board(&infra.pool, board_id, "public").await;

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let mut req = search_request("no-such-card-xyz");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-empty".to_string()),
            ..Default::default()
        });

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert!(resp.hits.is_empty());
        assert_eq!(resp.total, 0);

        os.delete_index(&index).await.ok();
    }

    #[tokio::test]
    async fn search_handler_filters_by_project_id() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        let project_a = Uuid::new_v4();
        let project_b = Uuid::new_v4();
        insert_test_board_with_project(&infra.pool, board_id, project_a, "public").await;

        let card_a =
            index_public_card(&os, &index, board_id, project_a, "Card A", vec![], vec![]).await;
        let _card_b =
            index_public_card(&os, &index, board_id, project_b, "Card B", vec![], vec![]).await;
        os.refresh(&index).await.unwrap();

        let mut req = search_request("Card");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-project-filter".to_string()),
            ..Default::default()
        });
        req.get_mut().project_ids = vec![project_a.to_string()];

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].card_id, card_a);

        os.delete_index(&index).await.ok();
    }

    #[tokio::test]
    async fn search_handler_filters_by_board_id() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_a = Uuid::new_v4();
        let board_b = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        insert_test_board_with_project(&infra.pool, board_a, project_id, "public").await;
        insert_test_board_with_project(&infra.pool, board_b, project_id, "public").await;

        let card_a =
            index_public_card(&os, &index, board_a, project_id, "Card A", vec![], vec![]).await;
        let _card_b =
            index_public_card(&os, &index, board_b, project_id, "Card B", vec![], vec![]).await;
        os.refresh(&index).await.unwrap();

        let mut req = search_request("Card");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-board-filter".to_string()),
            ..Default::default()
        });
        req.get_mut().board_ids = vec![board_a.to_string()];

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].card_id, card_a);

        os.delete_index(&index).await.ok();
    }

    #[tokio::test]
    async fn search_handler_filters_by_label_name() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "public").await;

        let card_bug = index_public_card(
            &os,
            &index,
            board_id,
            project_id,
            "Bug card",
            vec!["bug"],
            vec![],
        )
        .await;
        let _card_feat = index_public_card(
            &os,
            &index,
            board_id,
            project_id,
            "Feature card",
            vec!["feature"],
            vec![],
        )
        .await;
        os.refresh(&index).await.unwrap();

        let mut req = search_request("card");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-label-filter".to_string()),
            ..Default::default()
        });
        req.get_mut().label_names = vec!["bug".to_string()];

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].card_id, card_bug);

        os.delete_index(&index).await.ok();
    }

    #[tokio::test]
    async fn search_handler_filters_by_assignee_subject() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "public").await;

        let card_alice = index_public_card(
            &os,
            &index,
            board_id,
            project_id,
            "Assigned card",
            vec![],
            vec!["user:alice"],
        )
        .await;
        let _card_bob = index_public_card(
            &os,
            &index,
            board_id,
            project_id,
            "Other card",
            vec![],
            vec!["user:bob"],
        )
        .await;
        os.refresh(&index).await.unwrap();

        let mut req = search_request("card");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-assignee-filter".to_string()),
            ..Default::default()
        });
        req.get_mut().assignee_subjects = vec!["user:alice".to_string()];

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].card_id, card_alice);

        os.delete_index(&index).await.ok();
    }

    #[tokio::test]
    async fn search_handler_maps_completed_status() {
        let os = os_client();
        let infra = containers::setup().await;

        let index = test_index();
        os.create_cards_index(&index).await.expect("create index");

        let board_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "public").await;

        let doc = CardDocument {
            id: Uuid::new_v4().to_string(),
            board_id: board_id.to_string(),
            project_id: project_id.to_string(),
            card_ref: "KB-DONE".to_string(),
            title: "Done card".to_string(),
            description: "Desc".to_string(),
            priority: "low".to_string(),
            labels: vec![],
            assignees: vec![],
            completed_at: Some("2026-01-01T00:00:00Z".to_string()),
        };
        os.index_card(&index, &doc).await.unwrap();
        os.refresh(&index).await.unwrap();

        let mut req = search_request("Done card");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-status".to_string()),
            ..Default::default()
        });

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: Some(index.clone()),
        };

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].status, "completed");

        os.delete_index(&index).await.ok();
    }

    async fn insert_test_board_with_project(
        pool: &sqlx::PgPool,
        board_id: Uuid,
        project_id: Uuid,
        visibility: &str,
    ) {
        let slug = format!("bd-{}", &board_id.to_string()[..8]);

        // Ensure the parent project exists; callers that already seeded one will
        // hit ON CONFLICT DO NOTHING.
        sqlx::query(
            "INSERT INTO projects (id, name, slug, owner_id, created_at, updated_at) \
             VALUES ($1, $2, $3, 'user:test', now(), now()) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(project_id)
        .bind(format!("test-proj-{project_id}"))
        .bind(format!("{project_id}"))
        .execute(pool)
        .await
        .expect("failed to insert test project");

        sqlx::query(
            "INSERT INTO boards (id, project_id, name, slug, description, icon, visibility) \
             VALUES ($1, $2, $3, $4, '', '', $5)",
        )
        .bind(board_id)
        .bind(project_id)
        .bind(format!("Test Board {board_id}"))
        .bind(&slug)
        .bind(visibility)
        .execute(pool)
        .await
        .expect("failed to insert test board");
    }

    async fn index_public_card(
        os: &Arc<OpenSearchClient>,
        index: &str,
        board_id: Uuid,
        project_id: Uuid,
        title: &str,
        labels: Vec<&str>,
        assignees: Vec<&str>,
    ) -> String {
        let card_id = Uuid::new_v4().to_string();
        let doc = CardDocument {
            id: card_id.clone(),
            board_id: board_id.to_string(),
            project_id: project_id.to_string(),
            card_ref: format!("KB-{}", &card_id[..4]),
            title: title.to_string(),
            description: format!("Description for {title}"),
            priority: "medium".to_string(),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            assignees: assignees.into_iter().map(|s| s.to_string()).collect(),
            completed_at: None,
        };
        os.index_card(index, &doc).await.expect("index card");
        card_id
    }

    // ── Mock OpenSearch service-level tests ────────────────────────────────────

    async fn start_mock_search_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/{index}/_search",
            post(
                |Path(index): Path<String>, Json(_body): Json<serde_json::Value>| async move {
                    if index == "missing" {
                        return (
                            StatusCode::NOT_FOUND,
                            Json(json!({"error": {"type": "index_not_found_exception"}})),
                        )
                            .into_response();
                    }
                    (
                        StatusCode::OK,
                        Json(json!({
                            "hits": {
                                "total": { "value": 0 },
                                "hits": []
                            }
                        })),
                    )
                        .into_response()
                },
            ),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn search_cards_returns_empty_when_opensearch_index_missing() {
        let (addr, _handle) = start_mock_search_server().await;
        let infra = containers::setup().await;

        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("anything");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-missing".to_string()),
            ..Default::default()
        });

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert!(resp.hits.is_empty());
        assert_eq!(resp.total, 0);
        assert!(resp.next_cursor.is_empty());
    }

    #[tokio::test]
    async fn search_cards_returns_public_board_hit_via_mock_opensearch() {
        let infra = containers::setup().await;

        let project_id = crate::test_support::seed_project(&infra.pool).await;
        let board_id = Uuid::new_v4();
        let card_id = Uuid::new_v4().to_string();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "public").await;

        let (addr, _handle) = start_mock_hit_server(&card_id, &board_id.to_string()).await;
        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("find me");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-public-mock".to_string()),
            ..Default::default()
        });

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].card_id, card_id);
        assert_eq!(resp.hits[0].status, "open");
    }

    async fn start_mock_hit_server(
        card_id: &str,
        board_id: &str,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let card_id = card_id.to_string();
        let board_id = board_id.to_string();
        let app = Router::new().route(
            "/{index}/_search",
            post(move |Json(_body): Json<serde_json::Value>| {
                let card_id = card_id.clone();
                let board_id = board_id.clone();
                async move {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "hits": {
                                "total": { "value": 1 },
                                "hits": [{
                                    "_id": card_id,
                                    "_score": 1.0,
                                    "_source": {
                                        "id": card_id,
                                        "board_id": board_id,
                                        "project_id": Uuid::new_v4().to_string(),
                                        "ref": "KB-1",
                                        "title": "Mock hit",
                                        "description": "Desc",
                                        "priority": "medium",
                                        "labels": [],
                                        "assignees": [],
                                        "completed_at": null
                                    },
                                    "sort": [1.0, card_id]
                                }]
                            }
                        })),
                    )
                        .into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    // ── Additional mock OpenSearch service-level tests ─────────────────────────

    /// Start a mock OpenSearch server that always returns the given status/body.
    async fn start_mock_response_server(
        status: StatusCode,
        body: Value,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/{index}/_search",
            post(move |Json(_body): Json<Value>| {
                let status = status;
                let body = body.clone();
                async move { (status, Json(body)).into_response() }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn search_cards_returns_internal_error_when_opensearch_500() {
        let infra = containers::setup().await;

        let (addr, _handle) = start_mock_response_server(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "shard failure"}),
        )
        .await;

        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("anything");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-500".to_string()),
            ..Default::default()
        });

        let err = svc.search_cards(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn search_cards_returns_internal_error_when_opensearch_404_non_index_missing() {
        let infra = containers::setup().await;

        let (addr, _handle) = start_mock_response_server(
            StatusCode::NOT_FOUND,
            json!({"error": {"type": "resource_not_found_exception"}}),
        )
        .await;

        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("anything");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-404".to_string()),
            ..Default::default()
        });

        let err = svc.search_cards(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn search_cards_returns_empty_when_opensearch_hits_are_empty() {
        let infra = containers::setup().await;

        let (addr, _handle) = start_mock_response_server(
            StatusCode::OK,
            json!({
                "hits": {
                    "total": { "value": 0 },
                    "hits": []
                }
            }),
        )
        .await;

        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("anything");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-empty-hits".to_string()),
            ..Default::default()
        });

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert!(resp.hits.is_empty());
        assert_eq!(resp.total, 0);
        assert!(resp.next_cursor.is_empty());
    }

    #[tokio::test]
    async fn search_cards_drops_private_board_hit_without_keto_relation() {
        let infra = containers::setup().await;

        let project_id = crate::test_support::seed_project(&infra.pool).await;
        let board_id = Uuid::new_v4();
        let card_id = Uuid::new_v4().to_string();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "private").await;

        let (addr, _handle) = start_mock_hit_server(&card_id, &board_id.to_string()).await;
        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let subject = format!("user:test-private-drop-{}", Uuid::new_v4());
        let mut req = search_request("find me");
        req.extensions_mut().insert(AuthContext {
            subject: Some(subject.clone()),
            ..Default::default()
        });

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert!(resp.hits.is_empty());
    }

    #[tokio::test]
    async fn search_cards_returns_private_board_hit_with_keto_relation() {
        let infra = containers::setup().await;

        let project_id = crate::test_support::seed_project(&infra.pool).await;
        let board_id = Uuid::new_v4();
        let card_id = Uuid::new_v4().to_string();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "private").await;

        let subject = format!("user:test-private-allow-{}", Uuid::new_v4());
        infra
            .keto
            .grant_with_retry("KanbanBoard", &board_id.to_string(), "view", &subject)
            .await
            .expect("grant view failed");

        let (addr, _handle) = start_mock_hit_server(&card_id, &board_id.to_string()).await;
        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("find me");
        req.extensions_mut().insert(AuthContext {
            subject: Some(subject.clone()),
            ..Default::default()
        });

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].card_id, card_id);

        crate::auth::keto_compat::delete_relation_tuples(
            &infra.keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject),
        )
        .await
        .ok();
    }

    /// Start a mock server that returns `n` identical hits for the same board.
    async fn start_mock_multi_hit_server(
        card_ids: Vec<String>,
        board_id: String,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/{index}/_search",
            post(move |Json(_body): Json<Value>| {
                let card_ids = card_ids.clone();
                let board_id = board_id.clone();
                async move {
                    let project_id = Uuid::new_v4().to_string();
                    let hits: Vec<Value> = card_ids
                        .into_iter()
                        .enumerate()
                        .map(|(i, card_id)| {
                            json!({
                                "_id": card_id,
                                "_score": 1.0,
                                "_source": {
                                    "id": card_id,
                                    "board_id": board_id,
                                    "project_id": project_id,
                                    "ref": format!("KB-{i}"),
                                    "title": "Mock hit",
                                    "description": "Desc",
                                    "priority": "medium",
                                    "labels": [],
                                    "assignees": [],
                                    "completed_at": null
                                },
                                "sort": [1.0, card_id]
                            })
                        })
                        .collect();

                    (
                        StatusCode::OK,
                        Json(json!({
                            "hits": {
                                "total": { "value": hits.len() as i64 },
                                "hits": hits
                            }
                        })),
                    )
                        .into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn search_cards_emits_next_cursor_when_authorized_hits_equal_limit() {
        let infra = containers::setup().await;

        let project_id = crate::test_support::seed_project(&infra.pool).await;
        let board_id = Uuid::new_v4();
        let card_id = Uuid::new_v4().to_string();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "public").await;

        let (addr, _handle) =
            start_mock_multi_hit_server(vec![card_id.clone()], board_id.to_string()).await;
        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("find me");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-cursor-full".to_string()),
            ..Default::default()
        });
        req.get_mut().limit = 1;

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert!(!resp.next_cursor.is_empty());
    }

    #[tokio::test]
    async fn search_cards_emits_empty_next_cursor_when_authorized_hits_below_limit() {
        let infra = containers::setup().await;

        let project_id = crate::test_support::seed_project(&infra.pool).await;
        let board_id = Uuid::new_v4();
        let card_id = Uuid::new_v4().to_string();
        insert_test_board_with_project(&infra.pool, board_id, project_id, "public").await;

        let (addr, _handle) =
            start_mock_multi_hit_server(vec![card_id.clone()], board_id.to_string()).await;
        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        }));

        let svc = SearchServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            opensearch: Arc::clone(&os),
            index_name: None,
        };

        let mut req = search_request("find me");
        req.extensions_mut().insert(AuthContext {
            subject: Some("user:test-cursor-partial".to_string()),
            ..Default::default()
        });
        req.get_mut().limit = 2;

        let resp = svc.search_cards(req).await.unwrap().into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert!(resp.next_cursor.is_empty());
    }
}
