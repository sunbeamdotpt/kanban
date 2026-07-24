// SPDX-License-Identifier: AGPL-3.0-or-later
//! GitHub issue/PR link service.
//!
//! Implements `GithubLinkService` from `proto/sunbeam/kanban/v1/github.proto`.
//! Cards are soft-linked to GitHub issues and pull requests; the server
//! proxies all GitHub API calls (the FE never talks to GitHub directly).
//! State sync is display-only: `LinkIssue` performs an initial title/state
//! fetch (degrading gracefully when GitHub is unreachable) and `ResyncLink`
//! re-fetches on demand, failing when the fetch fails.
//!
//! Mutations write the `github_links` row and an `event_log` outbox row in
//! the same transaction (`GitHubLinkAdded` / `GitHubLinkRefreshed`), which the
//! outbox dispatcher routes to the board's NATS subject. Per the permission
//! matrix, `CheckedObjectId` is always the card_id — GitHub link ids are not
//! permission objects — so the link id from the request body is additionally
//! scoped by the checked card id and tenant.

use chrono::{DateTime, Utc};
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use sunbeam_g2v::middleware::auth::AuthContext;
use tracing::{error, warn};

use crate::auth::permission_dispatch::CheckedObjectId;
use crate::cpb::sunbeam::kanban::v1::{
    GitHubIssueResult, GitHubLinkDetail, GithubLinkService, LinkIssueRequest, LinkIssueResponse,
    ListLinksByCardRequest, ListLinksByCardResponse, ResyncLinkRequest, ResyncLinkResponse,
    SearchGithubIssuesRequest, SearchGithubIssuesResponse, UnlinkIssueRequest, UnlinkIssueResponse,
};
use crate::event_log::insert_board_event;
use crate::id::Id;
use crate::services::cards::to_proto_ts;

// ── Constants ────────────────────────────────────────────────────────────────

const KIND_ISSUE: &str = "issue";
const KIND_PULL_REQUEST: &str = "pull_request";
const DEFAULT_SEARCH_LIMIT: i32 = 20;
const MAX_SEARCH_LIMIT: i32 = 50;
/// GitHub rejects API calls without a User-Agent header.
const USER_AGENT: &str = "sunbeam-kanban";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct GitHubServiceImpl {
    pub pool: PgPool,
    pub api_base_url: String,
    pub api_token: String,
    pub http: reqwest::Client,
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    error!(error = %err, "{msg}");
    ConnectError::internal(msg)
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

/// The checked object id is always the card_id for this service (per the
/// permission dispatch matrix).
fn checked_card_id(ctx: &RequestContext) -> Result<Id, ConnectError> {
    ctx.extensions()
        .get::<CheckedObjectId>()
        .ok_or_else(|| ConnectError::internal("missing CheckedObjectId extension"))?
        .0
        .parse::<Id>()
        .map_err(|_| ConnectError::invalid_argument("invalid card_id"))
}

fn tenant_id_from_request(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))
}

// ── Pure mapping helpers ──────────────────────────────────────────────────────

/// Split a stored "owner/repo" string into its parts.
fn split_repo(repo: &str) -> (String, String) {
    match repo.split_once('/') {
        Some((owner, name)) => (owner.to_string(), name.to_string()),
        None => (repo.to_string(), String::new()),
    }
}

/// Build the GitHub search query: the user's free text plus a `repo:` scope
/// when a repo is named, otherwise a `user:` scope spanning the owner's repos.
fn build_search_query(repo_owner: &str, repo_name: &str, query: &str) -> String {
    let scope = if !repo_name.is_empty() {
        format!("repo:{repo_owner}/{repo_name}")
    } else if !repo_owner.is_empty() {
        format!("user:{repo_owner}")
    } else {
        String::new()
    };
    format!("{} {}", query.trim(), scope).trim().to_string()
}

/// Display fields extracted from a GitHub issue/PR API response.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IssueInfo {
    kind: &'static str,
    title: Option<String>,
    state: String,
    url: Option<String>,
}

/// Map a GitHub issue API JSON payload to display fields. The GitHub issues
/// endpoint also serves PRs; those carry a `pull_request` key.
fn parse_issue_json(v: &Value) -> IssueInfo {
    IssueInfo {
        kind: if v.get("pull_request").is_some() {
            KIND_PULL_REQUEST
        } else {
            KIND_ISSUE
        },
        title: v.get("title").and_then(Value::as_str).map(str::to_string),
        state: v
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_string(),
        url: v
            .get("html_url")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

/// Map one item of a GitHub `/search/issues` response to a `GitHubIssueResult`.
/// The repo coordinates come from `repository_url`
/// (`https://api.github.com/repos/{owner}/{repo}`).
fn parse_search_item(item: &Value) -> GitHubIssueResult {
    let (repo_owner, repo_name) = item
        .get("repository_url")
        .and_then(Value::as_str)
        .and_then(|u| u.rsplit("/repos/").next())
        .map(split_repo)
        .unwrap_or_default();
    let info = parse_issue_json(item);
    GitHubIssueResult {
        repo_owner,
        repo_name,
        number: item.get("number").and_then(Value::as_i64).unwrap_or(0) as i32,
        kind: info.kind.to_string(),
        title: info.title.unwrap_or_default(),
        state: info.state,
        url: info.url.unwrap_or_default(),
        ..Default::default()
    }
}

/// Fallback web URL used when GitHub did not provide `html_url` (or the fetch
/// failed entirely). GitHub redirects `/issues/{n}` to the PR when applicable.
fn fallback_url(repo_owner: &str, repo_name: &str, number: i32) -> String {
    format!("https://github.com/{repo_owner}/{repo_name}/issues/{number}")
}

// ── Row mapping ───────────────────────────────────────────────────────────────

/// One `github_links` row.
struct LinkRow {
    id: Id,
    card_id: Id,
    repo: String,
    issue_id: i32,
    state: String,
    title: Option<String>,
    url: String,
    kind: String,
    synced_at: DateTime<Utc>,
}

impl LinkRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, ConnectError> {
        Ok(Self {
            id: row
                .try_get("id")
                .map_err(|e| internal("github_links row: id", e))?,
            card_id: row
                .try_get("card_id")
                .map_err(|e| internal("github_links row: card_id", e))?,
            repo: row
                .try_get("repo")
                .map_err(|e| internal("github_links row: repo", e))?,
            issue_id: row
                .try_get("issue_id")
                .map_err(|e| internal("github_links row: issue_id", e))?,
            state: row
                .try_get("state")
                .map_err(|e| internal("github_links row: state", e))?,
            title: row
                .try_get("title")
                .map_err(|e| internal("github_links row: title", e))?,
            url: row
                .try_get("url")
                .map_err(|e| internal("github_links row: url", e))?,
            kind: row
                .try_get("kind")
                .map_err(|e| internal("github_links row: kind", e))?,
            synced_at: row
                .try_get("synced_at")
                .map_err(|e| internal("github_links row: synced_at", e))?,
        })
    }

    fn to_detail(&self) -> GitHubLinkDetail {
        let (repo_owner, repo_name) = split_repo(&self.repo);
        GitHubLinkDetail {
            id: self.id.to_string(),
            card_id: self.card_id.to_string(),
            repo_owner,
            repo_name,
            issue_or_pr_number: self.issue_id,
            kind: self.kind.clone(),
            title: self.title.clone().unwrap_or_default(),
            state: self.state.clone(),
            url: self.url.clone(),
            last_synced_at: Some(to_proto_ts(self.synced_at)).into(),
            ..Default::default()
        }
    }
}

const LINK_COLUMNS: &str = "id, card_id, repo, issue_id, state, title, url, kind, synced_at";

// ── event_log helper ──────────────────────────────────────────────────────────
//
// Link events use the shared `crate::event_log::insert_board_event` helper,
// which bumps `boards.revision` and merges it into the payload in the same tx.

// ── GitHub API client ─────────────────────────────────────────────────────────

impl GitHubServiceImpl {
    fn github_get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self
            .http
            .get(url)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json");
        if !self.api_token.is_empty() {
            req = req.bearer_auth(&self.api_token);
        }
        req
    }

    /// Fetch one issue/PR from the GitHub API.
    async fn fetch_issue(
        &self,
        repo_owner: &str,
        repo_name: &str,
        number: i32,
    ) -> Result<IssueInfo, ConnectError> {
        let url = format!(
            "{}/repos/{}/{}/issues/{}",
            self.api_base_url.trim_end_matches('/'),
            repo_owner,
            repo_name,
            number
        );
        let resp = self
            .github_get(&url)
            .send()
            .await
            .map_err(|e| internal("github api request failed", e))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(internal(
                "github api returned a non-success status",
                format_args!("GET {url} -> {status}"),
            ));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| internal("failed to decode github api response", e))?;
        Ok(parse_issue_json(&body))
    }

    /// Look up the board a card belongs to; `not_found` when the card does
    /// not exist in this tenant.
    async fn card_board_id(&self, card_id: Id, tenant_id: &str) -> Result<Id, ConnectError> {
        let row = sqlx::query("SELECT board_id FROM cards WHERE id = $1 AND tenant_id = $2")
            .bind(card_id)
            .bind(tenant_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("failed to fetch card", e))?
            .ok_or_else(|| ConnectError::not_found("card not found"))?;
        row.try_get("board_id")
            .map_err(|e| internal("card row missing board_id", e))
    }
}

// ── impl GithubLinkService ────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl GithubLinkService for GitHubServiceImpl {
    // ── LinkIssue ─────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // Initial title/state sync from GitHub, degraded to safe defaults when the
    // fetch fails — link creation must not depend on GitHub availability.
    // event_log: GitHubLinkAdded.

    async fn link_issue(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, LinkIssueRequest>,
    ) -> ServiceResult<LinkIssueResponse> {
        let card_id = checked_card_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.repo_owner.is_empty() || req.repo_name.is_empty() {
            return Err(ConnectError::invalid_argument(
                "repo_owner and repo_name are required",
            ));
        }
        if req.repo_owner.contains('/') || req.repo_name.contains('/') {
            return Err(ConnectError::invalid_argument(
                "repo_owner and repo_name must not contain '/'",
            ));
        }
        if req.number <= 0 {
            return Err(ConnectError::invalid_argument(
                "number must be a positive issue/PR number",
            ));
        }

        // Card must exist in this tenant; the event is routed by its board.
        let board_id = self.card_board_id(card_id, &tenant_id).await?;

        // Initial sync from GitHub. Degraded path: keep the link with safe
        // defaults when GitHub is unreachable, 404s, or rate-limits us.
        let info = match self
            .fetch_issue(&req.repo_owner, &req.repo_name, req.number)
            .await
        {
            Ok(info) => Some(info),
            Err(e) => {
                warn!(
                    error = %e,
                    card_id = %card_id,
                    repo = %format!("{}/{}", req.repo_owner, req.repo_name),
                    number = req.number,
                    "initial GitHub sync failed; creating link with default state"
                );
                None
            }
        };

        let kind = info.as_ref().map_or(KIND_ISSUE, |i| i.kind);
        let state = info
            .as_ref()
            .map_or_else(|| "open".to_string(), |i| i.state.clone());
        let title = info.as_ref().and_then(|i| i.title.clone());
        let url = info
            .and_then(|i| i.url)
            .unwrap_or_else(|| fallback_url(&req.repo_owner, &req.repo_name, req.number));
        let repo = format!("{}/{}", req.repo_owner, req.repo_name);

        let link_id = Id::new();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = sqlx::query(&format!(
            "INSERT INTO github_links (id, tenant_id, card_id, repo, issue_id, state, title, url, kind, synced_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
             RETURNING {LINK_COLUMNS}"
        ))
        .bind(link_id)
        .bind(&tenant_id)
        .bind(card_id)
        .bind(&repo)
        .bind(req.number)
        .bind(&state)
        .bind(&title)
        .bind(&url)
        .bind(kind)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to insert github link", e))?;
        let link = LinkRow::from_row(&row)?;

        let payload = json!({
            "card_id": card_id.to_string(),
            "link_id": link_id.to_string(),
            "board_id": board_id.to_string(),
        });
        insert_board_event(&mut tx, &tenant_id, board_id, "GitHubLinkAdded", payload).await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(LinkIssueResponse {
            link: Some(link.to_detail()).into(),
            ..Default::default()
        }))
    }

    // ── UnlinkIssue ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix); the link id
    // comes from the request body. No event is emitted (the proto defines
    // none for unlink).

    async fn unlink_issue(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UnlinkIssueRequest>,
    ) -> ServiceResult<UnlinkIssueResponse> {
        let card_id = checked_card_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        let link_id = req
            .link_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid link_id"))?;

        let result = sqlx::query(
            "DELETE FROM github_links WHERE id = $1 AND card_id = $2 AND tenant_id = $3",
        )
        .bind(link_id)
        .bind(card_id)
        .bind(&tenant_id)
        .execute(&self.pool)
        .await
        .map_err(|e| internal("failed to delete github link", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("github link not found"));
        }

        Ok(Response::new(UnlinkIssueResponse::default()))
    }

    // ── ListLinksByCard ───────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + view, per matrix).

    async fn list_links_by_card(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, ListLinksByCardRequest>,
    ) -> ServiceResult<ListLinksByCardResponse> {
        let card_id = checked_card_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;

        let rows = sqlx::query(&format!(
            "SELECT {LINK_COLUMNS} FROM github_links
             WHERE card_id = $1 AND tenant_id = $2
             ORDER BY created_at ASC"
        ))
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list github links", e))?;

        let links = rows
            .iter()
            .map(LinkRow::from_row)
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(LinkRow::to_detail)
            .collect();

        Ok(Response::new(ListLinksByCardResponse {
            links,
            ..Default::default()
        }))
    }

    // ── SearchGithubIssues ────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + view, per matrix). Live proxy to
    // the GitHub search API; nothing is persisted.

    async fn search_github_issues(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, SearchGithubIssuesRequest>,
    ) -> ServiceResult<SearchGithubIssuesResponse> {
        // The permission middleware already gated this call on the checked
        // card; the handler itself does not touch the database.
        let _card_id = checked_card_id(&ctx)?;
        let req = request.to_owned_message();

        let limit = if req.limit <= 0 {
            DEFAULT_SEARCH_LIMIT
        } else {
            req.limit.clamp(1, MAX_SEARCH_LIMIT)
        };
        let q = build_search_query(&req.repo_owner, &req.repo_name, &req.query);

        let url = format!("{}/search/issues", self.api_base_url.trim_end_matches('/'));
        let resp = self
            .github_get(&url)
            .query(&[("q", q.as_str()), ("per_page", &limit.to_string())])
            .send()
            .await
            .map_err(|e| internal("github search request failed", e))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(internal(
                "github search returned a non-success status",
                format_args!("GET {url} -> {status}"),
            ));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| internal("failed to decode github search response", e))?;

        let results = body
            .get("items")
            .and_then(Value::as_array)
            .map(|items| items.iter().map(parse_search_item).collect())
            .unwrap_or_default();

        Ok(Response::new(SearchGithubIssuesResponse {
            results,
            ..Default::default()
        }))
    }

    // ── ResyncLink ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix); the link id
    // comes from the request body. Re-fetching from GitHub is the whole point
    // of this RPC, so a fetch failure is an error and nothing is updated.
    // event_log: GitHubLinkRefreshed.

    async fn resync_link(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ResyncLinkRequest>,
    ) -> ServiceResult<ResyncLinkResponse> {
        let card_id = checked_card_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        let link_id = req
            .link_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid link_id"))?;

        let row = sqlx::query(&format!(
            "SELECT {LINK_COLUMNS} FROM github_links WHERE id = $1 AND card_id = $2 AND tenant_id = $3"
        ))
        .bind(link_id)
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch github link", e))?
        .ok_or_else(|| ConnectError::not_found("github link not found"))?;
        let link = LinkRow::from_row(&row)?;

        let (repo_owner, repo_name) = split_repo(&link.repo);
        let info = self
            .fetch_issue(&repo_owner, &repo_name, link.issue_id)
            .await?;

        // The event is routed by board, which the card lookup provides.
        let board_id = self.card_board_id(card_id, &tenant_id).await?;

        let url = info
            .url
            .unwrap_or_else(|| fallback_url(&repo_owner, &repo_name, link.issue_id));

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = sqlx::query(&format!(
            "UPDATE github_links
             SET state = $4, title = $5, kind = $6, url = $7, synced_at = now()
             WHERE id = $1 AND card_id = $2 AND tenant_id = $3
             RETURNING {LINK_COLUMNS}"
        ))
        .bind(link_id)
        .bind(card_id)
        .bind(&tenant_id)
        .bind(&info.state)
        .bind(&info.title)
        .bind(info.kind)
        .bind(&url)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to update github link", e))?;
        let updated = LinkRow::from_row(&row)?;

        let payload = json!({
            "card_id": card_id.to_string(),
            "link_id": link_id.to_string(),
            "board_id": board_id.to_string(),
            "new_state": info.state,
        });
        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "GitHubLinkRefreshed",
            payload,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(ResyncLinkResponse {
            link: Some(updated.to_detail()).into(),
            ..Default::default()
        }))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{connect_ctx, connect_request};
    use sunbeam_g2v::middleware::auth::AuthContext;

    // ── Pure mapping unit tests ──────────────────────────────────────────────

    #[test]
    fn split_repo_splits_owner_and_name() {
        assert_eq!(
            split_repo("sunbeam/kanban"),
            ("sunbeam".to_string(), "kanban".to_string())
        );
        assert_eq!(split_repo("bare"), ("bare".to_string(), String::new()));
    }

    #[test]
    fn build_search_query_scopes_to_repo_when_named() {
        assert_eq!(
            build_search_query("sunbeam", "kanban", "login bug"),
            "login bug repo:sunbeam/kanban"
        );
    }

    #[test]
    fn build_search_query_scopes_to_user_without_repo() {
        assert_eq!(
            build_search_query("sunbeam", "", "login bug"),
            "login bug user:sunbeam"
        );
    }

    #[test]
    fn build_search_query_handles_empty_query_and_owner() {
        assert_eq!(
            build_search_query("sunbeam", "kanban", ""),
            "repo:sunbeam/kanban"
        );
        assert_eq!(build_search_query("", "", "anything"), "anything");
    }

    #[test]
    fn parse_issue_json_detects_pull_requests() {
        let pr = serde_json::json!({
            "title": "Add feature",
            "state": "closed",
            "html_url": "https://github.com/o/r/pull/7",
            "pull_request": {"url": "https://api.github.com/repos/o/r/pulls/7"}
        });
        let info = parse_issue_json(&pr);
        assert_eq!(info.kind, KIND_PULL_REQUEST);
        assert_eq!(info.title.as_deref(), Some("Add feature"));
        assert_eq!(info.state, "closed");
        assert_eq!(info.url.as_deref(), Some("https://github.com/o/r/pull/7"));
    }

    #[test]
    fn parse_issue_json_defaults_missing_fields() {
        let issue = serde_json::json!({});
        let info = parse_issue_json(&issue);
        assert_eq!(info.kind, KIND_ISSUE);
        assert_eq!(info.title, None);
        assert_eq!(info.state, "open");
        assert_eq!(info.url, None);
    }

    #[test]
    fn parse_search_item_maps_repo_from_repository_url() {
        let item = serde_json::json!({
            "repository_url": "https://api.github.com/repos/sunbeam/kanban",
            "number": 42,
            "title": "Bug",
            "state": "open",
            "html_url": "https://github.com/sunbeam/kanban/issues/42"
        });
        let result = parse_search_item(&item);
        assert_eq!(result.repo_owner, "sunbeam");
        assert_eq!(result.repo_name, "kanban");
        assert_eq!(result.number, 42);
        assert_eq!(result.kind, "issue");
        assert_eq!(result.title, "Bug");
        assert_eq!(result.state, "open");
        assert_eq!(result.url, "https://github.com/sunbeam/kanban/issues/42");
    }

    #[test]
    fn parse_search_item_tolerates_missing_repository_url() {
        let item = serde_json::json!({"number": 1});
        let result = parse_search_item(&item);
        assert_eq!(result.repo_owner, "");
        assert_eq!(result.repo_name, "");
        assert_eq!(result.number, 1);
    }

    // ── DB-backed integration tests ──────────────────────────────────────────

    fn service_with(pool: PgPool, api_base_url: String) -> GitHubServiceImpl {
        GitHubServiceImpl {
            pool,
            api_base_url,
            api_token: String::new(),
            http: reqwest::Client::new(),
        }
    }

    /// Service pointed at an unreachable GitHub API to exercise degraded
    /// paths without real network access.
    async fn setup_service() -> GitHubServiceImpl {
        service_with(
            crate::test_support::setup_pool().await,
            "http://127.0.0.1:1".to_string(),
        )
    }

    // ── Mock GitHub API server ───────────────────────────────────────────────

    /// Canned responses served by the local mock GitHub API.
    #[derive(Clone)]
    struct MockGitHub {
        issue_body: Value,
        search_status: axum::http::StatusCode,
        search_body: Value,
        /// `per_page` values observed on `/search/issues`, for limit clamp
        /// assertions.
        seen_per_page: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    async fn mock_issue_handler(
        axum::extract::State(state): axum::extract::State<MockGitHub>,
    ) -> axum::Json<Value> {
        axum::Json(state.issue_body.clone())
    }

    async fn mock_search_handler(
        axum::extract::State(state): axum::extract::State<MockGitHub>,
        axum::extract::Query(params): axum::extract::Query<
            std::collections::HashMap<String, String>,
        >,
    ) -> impl axum::response::IntoResponse {
        if let Some(per_page) = params.get("per_page") {
            state
                .seen_per_page
                .lock()
                .expect("seen_per_page mutex poisoned")
                .push(per_page.clone());
        }
        (state.search_status, axum::Json(state.search_body.clone()))
    }

    /// Spawn a local axum mock of the GitHub API. Returns the base URL to
    /// point `GitHubServiceImpl.api_base_url` at plus the shared `per_page`
    /// observation log.
    async fn start_mock_github(
        issue_body: Value,
        search_status: axum::http::StatusCode,
        search_body: Value,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let seen_per_page = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = MockGitHub {
            issue_body,
            search_status,
            search_body,
            seen_per_page: std::sync::Arc::clone(&seen_per_page),
        };
        let app = axum::Router::new()
            .route(
                "/repos/{owner}/{repo}/issues/{number}",
                axum::routing::get(mock_issue_handler),
            )
            .route("/search/issues", axum::routing::get(mock_search_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock listener should bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock server failed");
        });
        (format!("http://{addr}"), seen_per_page)
    }

    fn authed_ctx_with_object(object_id: &Id) -> RequestContext {
        let mut ctx = connect_ctx(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            format!("user:test-{}", Id::new()),
        ));
        ctx.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        ctx
    }

    /// Insert a github_links row directly and return its id.
    async fn seed_link(pool: &PgPool, card_id: Id) -> Id {
        let link_id = Id::new();
        sqlx::query(
            "INSERT INTO github_links (id, tenant_id, card_id, repo, issue_id, state, title, url, kind)
             VALUES ($1, $2, $3, 'sunbeam/kanban', 42, 'open', 'seeded', 'https://github.com/sunbeam/kanban/issues/42', 'issue')",
        )
        .bind(link_id)
        .bind(crate::test_support::test_tenant_id())
        .bind(card_id)
        .execute(pool)
        .await
        .expect("seed_link: INSERT failed");
        link_id
    }

    #[tokio::test]
    async fn link_issue_creates_link_when_github_unreachable() {
        let svc = setup_service().await;
        let tenant_id = crate::test_support::test_tenant_id();
        let (_project, board_id, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;

        let resp = svc
            .link_issue(
                authed_ctx_with_object(&card_id),
                connect_request(&LinkIssueRequest {
                    card_id: card_id.to_string(),
                    repo_owner: "sunbeam".into(),
                    repo_name: "kanban".into(),
                    number: 42,
                    ..Default::default()
                }),
            )
            .await
            .expect("link_issue should succeed on the degraded path");

        let link = resp.body.link.into_option().expect("link");
        assert_eq!(link.card_id, card_id.to_string());
        assert_eq!(link.repo_owner, "sunbeam");
        assert_eq!(link.repo_name, "kanban");
        assert_eq!(link.issue_or_pr_number, 42);
        // Degraded defaults: kind issue, state open, no title.
        assert_eq!(link.kind, "issue");
        assert_eq!(link.state, "open");
        assert_eq!(link.title, "");
        assert_eq!(link.url, "https://github.com/sunbeam/kanban/issues/42");

        // The event_log row was written in the same transaction.
        let row = sqlx::query(
            "SELECT event_type, payload FROM event_log WHERE board_id = $1 AND tenant_id = $2 AND event_type = 'GitHubLinkAdded'",
        )
        .bind(board_id)
        .bind(&tenant_id)
        .fetch_one(&svc.pool)
        .await
        .expect("GitHubLinkAdded event should exist");
        let payload: Value = row.get("payload");
        assert_eq!(payload["card_id"], card_id.to_string());
        assert_eq!(payload["link_id"], link.id);
        assert_eq!(payload["board_id"], board_id.to_string());
    }

    #[tokio::test]
    async fn link_issue_rejects_unknown_card() {
        let svc = setup_service().await;
        let missing_card = Id::new();

        let err = svc
            .link_issue(
                authed_ctx_with_object(&missing_card),
                connect_request(&LinkIssueRequest {
                    card_id: missing_card.to_string(),
                    repo_owner: "sunbeam".into(),
                    repo_name: "kanban".into(),
                    number: 42,
                    ..Default::default()
                }),
            )
            .await
            .expect_err("link_issue should fail for an unknown card");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn list_links_by_card_returns_links_in_creation_order() {
        let svc = setup_service().await;
        let (_project, _board, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;
        let first = seed_link(&svc.pool, card_id).await;
        let second = seed_link(&svc.pool, card_id).await;

        let resp = svc
            .list_links_by_card(
                authed_ctx_with_object(&card_id),
                connect_request(&ListLinksByCardRequest {
                    card_id: card_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_links_by_card should succeed");

        let links = resp.body.links;
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].id, first.to_string());
        assert_eq!(links[1].id, second.to_string());
        assert_eq!(links[0].repo_owner, "sunbeam");
        assert_eq!(links[0].repo_name, "kanban");
        assert_eq!(links[0].issue_or_pr_number, 42);
        assert_eq!(links[0].kind, "issue");
        assert_eq!(links[0].title, "seeded");
    }

    #[tokio::test]
    async fn unlink_issue_deletes_row_and_not_found_on_replay() {
        let svc = setup_service().await;
        let (_project, _board, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;
        let link_id = seed_link(&svc.pool, card_id).await;

        svc.unlink_issue(
            authed_ctx_with_object(&card_id),
            connect_request(&UnlinkIssueRequest {
                link_id: link_id.to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("unlink_issue should succeed");

        let remaining: i64 = sqlx::query("SELECT COUNT(*) AS n FROM github_links WHERE id = $1")
            .bind(link_id)
            .fetch_one(&svc.pool)
            .await
            .map(|r| r.get("n"))
            .expect("count query failed");
        assert_eq!(remaining, 0);

        let err = svc
            .unlink_issue(
                authed_ctx_with_object(&card_id),
                connect_request(&UnlinkIssueRequest {
                    link_id: link_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("unlinking twice should be not_found");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn resync_link_fails_and_keeps_row_when_github_unreachable() {
        let svc = setup_service().await;
        let (_project, _board, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;
        let link_id = seed_link(&svc.pool, card_id).await;

        let err = svc
            .resync_link(
                authed_ctx_with_object(&card_id),
                connect_request(&ResyncLinkRequest {
                    link_id: link_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("resync should fail when GitHub is unreachable");
        assert_eq!(err.code, connectrpc::ErrorCode::Internal);

        // The row must be untouched.
        let row = sqlx::query("SELECT state, title FROM github_links WHERE id = $1")
            .bind(link_id)
            .fetch_one(&svc.pool)
            .await
            .expect("row should still exist");
        let state: String = row.get("state");
        let title: Option<String> = row.get("title");
        assert_eq!(state, "open");
        assert_eq!(title.as_deref(), Some("seeded"));
    }

    #[tokio::test]
    async fn resync_link_rejects_unknown_link() {
        let svc = setup_service().await;
        let (_project, _board, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;

        let err = svc
            .resync_link(
                authed_ctx_with_object(&card_id),
                connect_request(&ResyncLinkRequest {
                    link_id: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("resync of an unknown link should be not_found");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    // ── Mock-backed GitHub API happy paths ───────────────────────────────────

    #[tokio::test]
    async fn link_issue_syncs_issue_fields_from_github() {
        let pool = crate::test_support::setup_pool().await;
        let (api_base_url, _seen) = start_mock_github(
            serde_json::json!({
                "title": "Crash on login",
                "state": "closed",
                "html_url": "https://github.com/sunbeam/kanban/issues/42",
                "number": 42
            }),
            axum::http::StatusCode::OK,
            serde_json::json!({"items": []}),
        )
        .await;
        let svc = service_with(pool, api_base_url);
        let tenant_id = crate::test_support::test_tenant_id();
        let (_project, board_id, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;

        let resp = svc
            .link_issue(
                authed_ctx_with_object(&card_id),
                connect_request(&LinkIssueRequest {
                    card_id: card_id.to_string(),
                    repo_owner: "sunbeam".into(),
                    repo_name: "kanban".into(),
                    number: 42,
                    ..Default::default()
                }),
            )
            .await
            .expect("link_issue should succeed with a reachable GitHub");

        let link = resp.body.link.into_option().expect("link");
        assert_eq!(link.kind, "issue");
        assert_eq!(link.title, "Crash on login");
        assert_eq!(link.state, "closed");
        assert_eq!(link.url, "https://github.com/sunbeam/kanban/issues/42");

        // The fetched fields were persisted, not just returned.
        let row = sqlx::query("SELECT state, title, kind, url FROM github_links WHERE id = $1")
            .bind(link.id.parse::<Id>().expect("link id"))
            .fetch_one(&svc.pool)
            .await
            .expect("link row should exist");
        let state: String = row.get("state");
        let title: Option<String> = row.get("title");
        let kind: String = row.get("kind");
        let url: String = row.get("url");
        assert_eq!(state, "closed");
        assert_eq!(title.as_deref(), Some("Crash on login"));
        assert_eq!(kind, "issue");
        assert_eq!(url, "https://github.com/sunbeam/kanban/issues/42");

        let event = sqlx::query(
            "SELECT payload FROM event_log WHERE board_id = $1 AND tenant_id = $2 AND event_type = 'GitHubLinkAdded'",
        )
        .bind(board_id)
        .bind(&tenant_id)
        .fetch_one(&svc.pool)
        .await
        .expect("GitHubLinkAdded event should exist");
        let payload: Value = event.get("payload");
        assert_eq!(payload["link_id"], link.id);
    }

    #[tokio::test]
    async fn link_issue_detects_pull_requests_from_github() {
        let pool = crate::test_support::setup_pool().await;
        let (api_base_url, _seen) = start_mock_github(
            serde_json::json!({
                "title": "Add OAuth login",
                "state": "open",
                "html_url": "https://github.com/sunbeam/kanban/pull/7",
                "number": 7,
                "pull_request": {"url": "https://api.github.com/repos/sunbeam/kanban/pulls/7"}
            }),
            axum::http::StatusCode::OK,
            serde_json::json!({"items": []}),
        )
        .await;
        let svc = service_with(pool, api_base_url);
        let (_project, _board, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;

        let resp = svc
            .link_issue(
                authed_ctx_with_object(&card_id),
                connect_request(&LinkIssueRequest {
                    card_id: card_id.to_string(),
                    repo_owner: "sunbeam".into(),
                    repo_name: "kanban".into(),
                    number: 7,
                    ..Default::default()
                }),
            )
            .await
            .expect("link_issue should succeed for a PR");

        let link = resp.body.link.into_option().expect("link");
        assert_eq!(link.kind, "pull_request");
        assert_eq!(link.title, "Add OAuth login");
        assert_eq!(link.state, "open");
        assert_eq!(link.url, "https://github.com/sunbeam/kanban/pull/7");
    }

    #[tokio::test]
    async fn resync_link_updates_row_and_emits_refreshed_event() {
        let pool = crate::test_support::setup_pool().await;
        let (api_base_url, _seen) = start_mock_github(
            serde_json::json!({
                "title": "Renamed title",
                "state": "closed",
                "html_url": "https://github.com/sunbeam/kanban/pull/42",
                "number": 42,
                "pull_request": {"url": "https://api.github.com/repos/sunbeam/kanban/pulls/42"}
            }),
            axum::http::StatusCode::OK,
            serde_json::json!({"items": []}),
        )
        .await;
        let svc = service_with(pool, api_base_url);
        let tenant_id = crate::test_support::test_tenant_id();
        let (_project, board_id, _column, card_id) =
            crate::test_support::seed_card_chain(&svc.pool).await;
        // Seeded as an open issue; the mock now reports a closed PR.
        let link_id = seed_link(&svc.pool, card_id).await;

        let resp = svc
            .resync_link(
                authed_ctx_with_object(&card_id),
                connect_request(&ResyncLinkRequest {
                    link_id: link_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("resync should succeed with a reachable GitHub");

        let link = resp.body.link.into_option().expect("link");
        assert_eq!(link.state, "closed");
        assert_eq!(link.title, "Renamed title");
        assert_eq!(link.kind, "pull_request");
        assert_eq!(link.url, "https://github.com/sunbeam/kanban/pull/42");

        let row = sqlx::query("SELECT state, title, kind, url FROM github_links WHERE id = $1")
            .bind(link_id)
            .fetch_one(&svc.pool)
            .await
            .expect("link row should exist");
        let state: String = row.get("state");
        let title: Option<String> = row.get("title");
        let kind: String = row.get("kind");
        assert_eq!(state, "closed");
        assert_eq!(title.as_deref(), Some("Renamed title"));
        assert_eq!(kind, "pull_request");

        let event = sqlx::query(
            "SELECT payload FROM event_log WHERE board_id = $1 AND tenant_id = $2 AND event_type = 'GitHubLinkRefreshed'",
        )
        .bind(board_id)
        .bind(&tenant_id)
        .fetch_one(&svc.pool)
        .await
        .expect("GitHubLinkRefreshed event should exist");
        let payload: Value = event.get("payload");
        assert_eq!(payload["card_id"], card_id.to_string());
        assert_eq!(payload["link_id"], link_id.to_string());
        assert_eq!(payload["board_id"], board_id.to_string());
        assert_eq!(payload["new_state"], "closed");
    }

    #[tokio::test]
    async fn search_github_issues_maps_results_and_clamps_limit() {
        let pool = crate::test_support::setup_pool().await;
        let (api_base_url, seen_per_page) = start_mock_github(
            serde_json::json!({}),
            axum::http::StatusCode::OK,
            serde_json::json!({
                "total_count": 2,
                "items": [
                    {
                        "repository_url": "https://api.github.com/repos/sunbeam/kanban",
                        "number": 42,
                        "title": "Crash on login",
                        "state": "open",
                        "html_url": "https://github.com/sunbeam/kanban/issues/42"
                    },
                    {
                        "repository_url": "https://api.github.com/repos/sunbeam/kanban",
                        "number": 43,
                        "title": "Fix the crash",
                        "state": "closed",
                        "html_url": "https://github.com/sunbeam/kanban/pull/43",
                        "pull_request": {"url": "https://api.github.com/repos/sunbeam/kanban/pulls/43"}
                    }
                ]
            }),
        )
        .await;
        let svc = service_with(pool, api_base_url);

        let resp = svc
            .search_github_issues(
                authed_ctx_with_object(&Id::new()),
                connect_request(&SearchGithubIssuesRequest {
                    repo_owner: "sunbeam".into(),
                    repo_name: "kanban".into(),
                    query: "crash".into(),
                    limit: 100, // clamps to MAX_SEARCH_LIMIT
                    ..Default::default()
                }),
            )
            .await
            .expect("search should succeed");

        let results = resp.body.results;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].repo_owner, "sunbeam");
        assert_eq!(results[0].repo_name, "kanban");
        assert_eq!(results[0].number, 42);
        assert_eq!(results[0].kind, "issue");
        assert_eq!(results[0].title, "Crash on login");
        assert_eq!(results[0].state, "open");
        assert_eq!(
            results[0].url,
            "https://github.com/sunbeam/kanban/issues/42"
        );
        assert_eq!(results[1].number, 43);
        assert_eq!(results[1].kind, "pull_request");
        assert_eq!(results[1].url, "https://github.com/sunbeam/kanban/pull/43");

        // limit=100 was clamped to 50 before hitting GitHub.
        let seen = seen_per_page.lock().expect("seen_per_page mutex poisoned");
        assert_eq!(seen.as_slice(), ["50".to_string()]);
    }

    #[tokio::test]
    async fn search_github_issues_returns_internal_on_github_error() {
        let pool = crate::test_support::setup_pool().await;
        let (api_base_url, _seen) = start_mock_github(
            serde_json::json!({}),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"message": "boom"}),
        )
        .await;
        let svc = service_with(pool, api_base_url);

        let err = svc
            .search_github_issues(
                authed_ctx_with_object(&Id::new()),
                connect_request(&SearchGithubIssuesRequest {
                    repo_owner: "sunbeam".into(),
                    repo_name: "kanban".into(),
                    query: "crash".into(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("search should fail when GitHub 500s");
        assert_eq!(err.code, connectrpc::ErrorCode::Internal);
    }
}
