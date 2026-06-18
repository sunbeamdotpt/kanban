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
