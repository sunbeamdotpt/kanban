//! OpenSearch HTTP client — thin wrapper over `reqwest`.
//!
//! Only the `search` method is exposed here.  Document indexing (PUT /_doc/:id)
//! is intentionally absent from this module:
//!
//!   Stage 3.5 — TODO: index cards on CreateCard/UpdateCard from
//!   `services/cards.rs`.  Call `OpenSearchClient::index_card` after the
//!   Postgres INSERT/UPDATE commits.  Tracked as Stage 3.5.
//!
//! # Index
//!
//! The canonical index name is `sunbeam-kanban-cards-v1`.
//! Field schema (matches `apps/kanban-old/server/opensearch.ts`):
//!
//!   id             keyword   — card UUID
//!   board_id       keyword
//!   project_id     keyword
//!   ref            keyword   — e.g. "KB-42"
//!   title          text (standard analyser)
//!   description    text (standard analyser)
//!   labels         keyword   (multi-value)
//!   assignees      keyword   (multi-value)
//!   priority       keyword
//!   completed_at   date | null
//!   created_at     date
//!   updated_at     date

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Constants ────────────────────────────────────────────────────────────────

pub const KANBAN_CARDS_INDEX: &str = "sunbeam-kanban-cards-v1";

// ── Config ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct OpenSearchConfig {
    /// e.g. "http://localhost:9200"
    pub url: String,
}

impl OpenSearchConfig {
    /// Load from `OPENSEARCH_URL`; defaults to `http://localhost:9200`.
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("OPENSEARCH_URL")
                .unwrap_or_else(|_| "http://localhost:9200".to_string()),
        }
    }
}

// ── Response types ────────────────────────────────────────────────────────────

/// Subset of the OpenSearch `_search` response we actually use.
#[derive(Debug, Deserialize)]
pub struct SearchResponse {
    pub hits: HitsWrapper,
}

#[derive(Debug, Deserialize)]
pub struct HitsWrapper {
    pub total: TotalValue,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Deserialize)]
pub struct TotalValue {
    pub value: i64,
}

#[derive(Debug, Deserialize)]
pub struct SearchHit {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_score")]
    pub score: Option<f64>,
    #[serde(rename = "_source")]
    pub source: CardDocument,
    /// Present when a `sort` clause is in the query; used for `search_after`.
    pub sort: Option<Vec<Value>>,
}

/// The indexed document shape (mirrors `apps/kanban-old/server/opensearch.ts`).
#[derive(Debug, Deserialize, Serialize)]
pub struct CardDocument {
    pub id: String,
    pub board_id: String,
    pub project_id: String,
    #[serde(rename = "ref", default)]
    pub card_ref: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub priority: String,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub assignees: Vec<String>,
    pub completed_at: Option<String>,
}

// ── Error sentinel ────────────────────────────────────────────────────────────

/// Returned when we can structurally detect "index_not_found_exception".
#[derive(Debug, Deserialize)]
struct OpenSearchError {
    error: Option<ErrorBody>,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    kind: Option<String>,
}

// ── Client ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct OpenSearchClient {
    http: Client,
    base_url: String,
}

impl OpenSearchClient {
    pub fn new(cfg: OpenSearchConfig) -> Self {
        Self {
            http: Client::new(),
            base_url: cfg.url.trim_end_matches('/').to_string(),
        }
    }

    /// POST `/<index>/_search` with `body`.
    ///
    /// Returns `Ok(None)` when OpenSearch returns 404 with
    /// `index_not_found_exception` — callers should treat this as an empty
    /// result set rather than an error.
    pub async fn search(&self, index: &str, body: &Value) -> Result<Option<SearchResponse>> {
        let url = format!("{}/{}/_search", self.base_url, index);

        let resp = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("opensearch POST {url} failed"))?;

        let status = resp.status();

        // 404 can mean index_not_found — parse and decide.
        if status == reqwest::StatusCode::NOT_FOUND {
            let text = resp.text().await.unwrap_or_default();
            let parsed: OpenSearchError =
                serde_json::from_str(&text).unwrap_or(OpenSearchError { error: None });
            let is_index_missing = parsed
                .error
                .and_then(|e| e.kind)
                .map(|k| k.contains("index_not_found"))
                .unwrap_or(false);

            if is_index_missing {
                tracing::warn!(
                    index,
                    "opensearch: index not found — returning empty result"
                );
                return Ok(None);
            }

            anyhow::bail!("opensearch search 404 (non-index-missing): {text}");
        }

        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("opensearch search {status}: {text}");
        }

        let sr: SearchResponse = resp
            .json()
            .await
            .context("opensearch: failed to deserialise SearchResponse")?;

        Ok(Some(sr))
    }

    // ── Index management (used by integration tests) ─────────────────────────

    /// Create `index` with the kanban card field mapping.
    ///
    /// Silently succeeds if the index already exists.
    pub async fn create_cards_index(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}", self.base_url, index);

        let mapping = serde_json::json!({
            "mappings": {
                "properties": {
                    "id":           { "type": "keyword" },
                    "board_id":     { "type": "keyword" },
                    "project_id":   { "type": "keyword" },
                    "ref":          { "type": "keyword" },
                    "title":        { "type": "text", "analyzer": "standard" },
                    "description":  { "type": "text", "analyzer": "standard" },
                    "labels":       { "type": "keyword" },
                    "assignees":    { "type": "keyword" },
                    "priority":     { "type": "keyword" },
                    "completed_at": { "type": "date" },
                    "created_at":   { "type": "date" },
                    "updated_at":   { "type": "date" }
                }
            }
        });

        let resp = self
            .http
            .put(&url)
            .header("Content-Type", "application/json")
            .json(&mapping)
            .send()
            .await
            .with_context(|| format!("opensearch PUT {url} failed"))?;

        let status = resp.status();

        // 400 with resource_already_exists_exception is fine.
        if status == reqwest::StatusCode::BAD_REQUEST {
            let text = resp.text().await.unwrap_or_default();
            if text.contains("resource_already_exists_exception") {
                return Ok(());
            }
            anyhow::bail!("opensearch create index 400: {text}");
        }

        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("opensearch create index {status}: {text}");
        }

        Ok(())
    }

    /// Index a single card document (PUT `/<index>/_doc/<id>`).
    pub async fn index_card(&self, index: &str, doc: &CardDocument) -> Result<()> {
        let url = format!("{}/{}/_doc/{}", self.base_url, index, doc.id);

        let resp = self
            .http
            .put(&url)
            .header("Content-Type", "application/json")
            .json(doc)
            .send()
            .await
            .with_context(|| format!("opensearch PUT doc {url} failed"))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("opensearch index doc {status}: {text}");
        }

        Ok(())
    }

    /// Refresh an index so indexed docs are immediately searchable.
    /// Used in tests after bulk indexing.
    pub async fn refresh(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}/_refresh", self.base_url, index);
        let resp = self
            .http
            .post(&url)
            .send()
            .await
            .with_context(|| format!("opensearch POST refresh {url} failed"))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("opensearch refresh {status}: {text}");
        }

        Ok(())
    }

    /// Delete an index (used in test teardown).
    pub async fn delete_index(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}", self.base_url, index);
        let resp = self
            .http
            .delete(&url)
            .send()
            .await
            .with_context(|| format!("opensearch DELETE {url} failed"))?;

        let status = resp.status();
        // 404 means it was already gone — fine for teardown.
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("opensearch delete index {status}: {text}");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_from_env_uses_env_or_default() {
        let expected = std::env::var("OPENSEARCH_URL")
            .unwrap_or_else(|_| "http://localhost:9200".to_string());
        let cfg = OpenSearchConfig::from_env();
        assert_eq!(cfg.url, expected);
    }

    #[test]
    fn client_trims_trailing_slash_from_url() {
        let client = OpenSearchClient::new(OpenSearchConfig {
            url: "http://localhost:9200/".to_string(),
        });
        assert_eq!(client.base_url, "http://localhost:9200");
    }

    #[test]
    fn card_document_serializes_and_deserializes() {
        let doc = CardDocument {
            id: "card-1".to_string(),
            board_id: "board-1".to_string(),
            project_id: "proj-1".to_string(),
            card_ref: "KB-1".to_string(),
            title: "Test".to_string(),
            description: "Desc".to_string(),
            priority: "high".to_string(),
            labels: vec!["bug".to_string()],
            assignees: vec!["user:alice".to_string()],
            completed_at: None,
        };
        let json_str = serde_json::to_string(&doc).unwrap();
        assert!(json_str.contains("\"ref\":\"KB-1\""));

        let round: CardDocument = serde_json::from_str(&json_str).unwrap();
        assert_eq!(round.id, doc.id);
        assert_eq!(round.labels, doc.labels);
        assert_eq!(round.completed_at, None);
    }

    // ── Mock-server tests for HTTP methods ─────────────────────────────────────

    use axum::{
        extract::Path,
        http::StatusCode,
        response::IntoResponse,
        routing::{delete, post, put},
        Json, Router,
    };
    use serde_json::json;
    use std::net::SocketAddr;

    fn mock_app() -> Router {
        Router::new()
            .route("/{index}/_search", post(search_handler))
            .route("/{index}", put(create_index_handler))
            .route("/{index}/_doc/{id}", put(index_doc_handler))
            .route("/{index}/_refresh", post(refresh_handler))
            .route("/{index}", delete(delete_index_handler))
    }

    async fn search_handler(Path(index): Path<String>, Json(body): Json<Value>) -> impl IntoResponse {
        if index == "missing" {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": {"type": "index_not_found_exception"}})),
            )
                .into_response();
        }
        if index == "bad" {
            return (StatusCode::NOT_FOUND, "plain 404").into_response();
        }
        if index == "error" {
            return (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response();
        }

        let hits = if body["query"].get("match_all").is_some() {
            vec![
                json!({
                    "_id": "card-1",
                    "_score": 1.0,
                    "_source": {
                        "id": "card-1",
                        "board_id": "board-1",
                        "project_id": "proj-1",
                        "ref": "KB-1",
                        "title": "Hit",
                        "description": "Desc",
                        "priority": "medium",
                        "labels": [],
                        "assignees": [],
                        "completed_at": null
                    }
                }),
            ]
        } else {
            vec![]
        };

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

    async fn create_index_handler(Path(index): Path<String>) -> impl IntoResponse {
        if index == "existing" {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": {"type": "resource_already_exists_exception"}})),
            )
                .into_response();
        }
        if index == "error" {
            return (StatusCode::BAD_REQUEST, "bad mapping").into_response();
        }
        (StatusCode::OK, Json(json!({"acknowledged": true}))).into_response()
    }

    async fn index_doc_handler(Path((index, _id)): Path<(String, String)>) -> impl IntoResponse {
        if index == "error" {
            return (StatusCode::INTERNAL_SERVER_ERROR, "index doc failed").into_response();
        }
        (StatusCode::CREATED, Json(json!({"result": "created"}))).into_response()
    }

    async fn refresh_handler() -> impl IntoResponse {
        (StatusCode::OK, Json(json!({"_shards": {"successful": 1}})))
    }

    async fn delete_index_handler(Path(index): Path<String>) -> impl IntoResponse {
        if index == "gone" {
            return (StatusCode::NOT_FOUND, "not found").into_response();
        }
        (StatusCode::OK, Json(json!({"acknowledged": true}))).into_response()
    }

    async fn start_mock_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, mock_app()).await.unwrap();
        });
        (addr, handle)
    }

    fn client_for(addr: SocketAddr) -> OpenSearchClient {
        OpenSearchClient::new(OpenSearchConfig {
            url: format!("http://{addr}"),
        })
    }

    #[tokio::test]
    async fn search_returns_parsed_response() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let result = client
            .search("hits", &json!({ "query": { "match_all": {} } }))
            .await
            .expect("search should succeed");

        let resp = result.expect("expected Some response");
        assert_eq!(resp.hits.total.value, 1);
        assert_eq!(resp.hits.hits.len(), 1);
        assert_eq!(resp.hits.hits[0].id, "card-1");
    }

    #[tokio::test]
    async fn search_returns_none_for_missing_index() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let result = client
            .search("missing", &json!({ "query": { "match_all": {} } }))
            .await
            .expect("search should not error for missing index");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn search_errors_for_non_index_404() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let result = client
            .search("bad", &json!({ "query": { "match_all": {} } }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn search_errors_for_5xx() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let result = client
            .search("error", &json!({ "query": { "match_all": {} } }))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_cards_index_succeeds_and_ignores_existing() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        client.create_cards_index("new-index").await.expect("create index");
        client.create_cards_index("existing").await.expect("existing index should be ignored");
    }

    #[tokio::test]
    async fn create_cards_index_errors_on_unexpected_400() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let result = client.create_cards_index("error").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn index_card_succeeds() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let doc = CardDocument {
            id: "card-1".to_string(),
            board_id: "board-1".to_string(),
            project_id: "proj-1".to_string(),
            card_ref: "KB-1".to_string(),
            title: "T".to_string(),
            description: "D".to_string(),
            priority: "low".to_string(),
            labels: vec![],
            assignees: vec![],
            completed_at: None,
        };
        client.index_card("hits", &doc).await.expect("index card");
    }

    #[tokio::test]
    async fn index_card_errors_on_5xx() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        let doc = CardDocument {
            id: "card-1".to_string(),
            board_id: "board-1".to_string(),
            project_id: "proj-1".to_string(),
            card_ref: "KB-1".to_string(),
            title: "T".to_string(),
            description: "D".to_string(),
            priority: "low".to_string(),
            labels: vec![],
            assignees: vec![],
            completed_at: None,
        };
        let result = client.index_card("error", &doc).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn refresh_succeeds() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);
        client.refresh("hits").await.expect("refresh");
    }

    #[tokio::test]
    async fn delete_index_succeeds_and_ignores_404() {
        let (addr, _handle) = start_mock_server().await;
        let client = client_for(addr);

        client.delete_index("hits").await.expect("delete index");
        client.delete_index("gone").await.expect("404 delete is ignored");
    }
}
