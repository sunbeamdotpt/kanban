// SPDX-License-Identifier: AGPL-3.0-or-later
//! Per-RPC authorization dispatcher.
//!
//! The sso-gateway `PermissionService` is configured with tenant-scoped
//! namespaces, but a single fixed (namespace, relation) pair is not enough for
//! the many different permission checks the Kanban API needs. This middleware
//! instead looks up each RPC in a static dispatch matrix and runs the right
//! permission check for that method.
//!
//! # Object-id header threading
//!
//! The object ID comes from the `x-sunbeam-object-id` request header. The
//! frontend sets it explicitly on every call, and this middleware never
//! inspects or deserializes request bodies — that would break server-streaming.
//! After the permission check grants access, the middleware inserts
//! `Extension<CheckedObjectId>` into the request extensions so handlers use
//! the already-authorized ID instead of anything from the body. This prevents
//! a header-vs-body bypass: a handler reading from the body would only get an
//! ID that has already passed authorization.
//!
//! # Authentication
//!
//! This middleware runs after `auth_middleware`, which validates the
//! `Authorization: Bearer <token>` header with the sso-gateway introspection
//! endpoint and inserts `Extension<AuthContext>` and `Extension<TenantId>`.
//! Token revocation is handled by the gateway, so no additional local
//! revocation check is performed here.
//!
//! # Service-credential permission checks
//!
//! Checks are made with the service's own singleton [`PermissionClient`]
//! (client-credentials, `permission:admin`), scoped to the caller's tenant via
//! the `x-tenant-id` header. End-user tokens never need permission scopes of
//! their own; the service authorizes on their behalf after introspection has
//! established who they are.
//!
use std::{fmt, sync::Arc};

use axum::{
    Extension,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use sunbeam_g2v::middleware::auth::{AuthContext, TenantId};

use super::permission_client::{PermissionClient, PermissionError};

// ============================================================================
// Public types
// ============================================================================

/// Object id extracted from the `x-sunbeam-object-id` header and verified by
/// a successful permission check.
///
/// Handlers should read this extension instead of parsing the request body,
/// so they always act on the ID that the permission backend already authorized.
#[derive(Clone, Debug)]
pub struct CheckedObjectId(pub String);

/// Tells the dispatcher where to get the object id for a given RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectIdSource {
    /// Read from the `x-sunbeam-object-id` request header.
    ///
    /// The dispatcher:
    /// 1. Returns `400 Bad Request` if the header is absent.
    /// 2. Calls `PermissionClient::check_permission(namespace, object_id, relation, subject)`.
    /// 3. Returns `403 Forbidden` if the gateway denies.
    /// 4. Inserts `Extension<CheckedObjectId>` on success.
    Header,
    /// No specific object check is required.
    ///
    /// Used for RPCs that are open to any authenticated user (e.g.
    /// `ListProjects`, `SearchCards`), where the handler filters the result set
    /// afterwards via `permission_expand`.
    None,
}

/// One row in the static dispatch matrix.
#[derive(Clone, Debug)]
pub struct DispatchEntry {
    /// Fully-qualified gRPC method path, e.g.
    /// `"/sunbeam.kanban.v1.BoardService/GetBoard"`.
    pub method: &'static str,
    /// Permission namespace for the check, e.g. `"KanbanBoard"`.
    /// Empty string when `object_id_source` is `None`.
    pub namespace: &'static str,
    /// Permission relation for the check, e.g. `"view"`.
    /// Empty string when `object_id_source` is `None`.
    pub relation: &'static str,
    /// Where to obtain the object id.
    pub object_id_source: ObjectIdSource,
}

// ============================================================================
// State
// ============================================================================

/// Shared state carried by `Extension<Arc<DispatchState>>`.
pub struct DispatchState {
    /// Root service-credential permission client. Per-request checks use a
    /// cached per-tenant derivative (see [`PermissionClient::tenant_client`])
    /// so the service — not the caller's token — authorizes every check.
    pub permission: Arc<PermissionClient>,
}

// ============================================================================
// Static matrix — 68 entries, one per RPC
// ============================================================================

/// Return the static dispatch matrix.
///
/// The method paths in this matrix must exactly match the RPC method paths
/// emitted by the proto compiler for every service in
/// `proto/sunbeam/kanban/v1/*.proto`. The `permission-coverage` binary enforces
/// this invariant at CI time.
pub fn matrix() -> &'static [DispatchEntry] {
    &MATRIX
}

static MATRIX: [DispatchEntry; 77] = [
    // ── ProjectService (9) ──────────────────────────────────────────────────
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/ListProjects",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // post-filter via permission_expand
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/GetProject",
        namespace: "KanbanProject",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/CreateProject",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // no existing object to check
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/UpdateProject",
        namespace: "KanbanProject",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/DeleteProject",
        namespace: "KanbanProject",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/ListMembers",
        namespace: "KanbanProject",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/AddMember",
        namespace: "KanbanProject",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/RemoveMember",
        namespace: "KanbanProject",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.ProjectService/SubscribeProject",
        namespace: "KanbanProject",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    // ── BoardService (10) ───────────────────────────────────────────────────
    // ListBoards, GetBoard and SubscribeBoard use application-level visibility
    // filtering (private/internal/public) in the handler.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/ListBoards",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/GetBoard",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/CreateBoard",
        namespace: "KanbanProject",
        relation: "edit",
        object_id_source: ObjectIdSource::Header, // parent project id
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/UpdateBoard",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/DeleteBoard",
        namespace: "KanbanBoard",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/AddColumn",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/UpdateColumn",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/RemoveColumn",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/MoveColumn",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.BoardService/SubscribeBoard",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None,
    },
    // ── CardService (19) ────────────────────────────────────────────────────
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/GetCard",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/BatchGetCards",
        namespace: "KanbanBoard",
        relation: "view",
        object_id_source: ObjectIdSource::Header, // board id; handler filters per card
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/ListCardsByBoard",
        namespace: "KanbanBoard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/CreateCard",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/UpdateCard",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/MoveCard",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/DeleteCard",
        namespace: "KanbanCard",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/AddCardDependency",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header, // board id; handler verifies source card, target may be any board in the tenant
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/RemoveCardDependency",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header, // board id; handler verifies source card, target may be any board in the tenant
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/BulkUpdateCardLabels",
        namespace: "KanbanBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header, // board id; handler iterates cards
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/AssignCard",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/UnassignCard",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/AddChecklistItem",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/UpdateChecklistItem",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/RemoveChecklistItem",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/AddComment",
        namespace: "KanbanCard",
        relation: "view", // any viewer can comment
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/EditComment",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/DeleteComment",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.CardService/ListComments",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    // ── AttachmentService (5) ───────────────────────────────────────────────
    // object_id is the card_id (not attachment_id): the backend authorizes the card.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AttachmentService/RequestPresignedUpload",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AttachmentService/ConfirmUpload",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AttachmentService/RequestPresignedDownload",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AttachmentService/DeleteAttachment",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AttachmentService/ListAttachmentsByCard",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    // ── GithubLinkService (5) ───────────────────────────────────────────────
    // object_id is the card_id throughout; GitHub link IDs are not permission objects.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.GithubLinkService/LinkIssue",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.GithubLinkService/UnlinkIssue",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.GithubLinkService/ListLinksByCard",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.GithubLinkService/SearchGithubIssues",
        namespace: "KanbanCard",
        relation: "view",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.GithubLinkService/ResyncLink",
        namespace: "KanbanCard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    // ── AggregatedBoardService (9) ──────────────────────────────────────────
    // object_id is the aggregated_board_id.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/CreateAggregatedBoard",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None,
    },
    // GetAggregatedBoard and SubscribeAggregatedBoard use application-level
    // visibility filtering (private/internal/public) in the handler.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/GetAggregatedBoard",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/UpdateAggregatedBoard",
        namespace: "KanbanAggregatedBoard",
        relation: "edit",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/DeleteAggregatedBoard",
        namespace: "KanbanAggregatedBoard",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/ListAggregatedBoards",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // post-filter via permission_expand
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/AddSourceBoard",
        namespace: "KanbanAggregatedBoard",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/RemoveSourceBoard",
        namespace: "KanbanAggregatedBoard",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/MoveSourceBoard",
        namespace: "KanbanAggregatedBoard",
        relation: "manage",
        object_id_source: ObjectIdSource::Header,
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.AggregatedBoardService/SubscribeAggregatedBoard",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None,
    },
    // ── SearchService (1) ───────────────────────────────────────────────────
    DispatchEntry {
        method: "/sunbeam.kanban.v1.SearchService/SearchCards",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // post-filter via permission_expand in handler
    },
    // ── TemplatesService (10) ───────────────────────────────────────────────
    // Templates are project-owned resources.  Global templates are read-only
    // and visible to every authenticated user; project-scoped templates require
    // the corresponding KanbanProject permission in the handler.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/ListTemplates",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // post-filter by project visibility
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/GetTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler enforces visibility
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/CreateTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/UpdateTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/DeleteTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/ListCardTemplates",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // post-filter by project visibility
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/GetCardTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler enforces visibility
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/CreateCardTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/UpdateCardTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.TemplatesService/DeleteCardTemplate",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    // ── MilestoneService (5) ────────────────────────────────────────────────
    // Milestones are project-owned resources with no OpenFGA object of their
    // own; the handler checks KanbanProject view/manage (templates pattern).
    DispatchEntry {
        method: "/sunbeam.kanban.v1.MilestoneService/CreateMilestone",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.MilestoneService/ListMilestones",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/view
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.MilestoneService/GetMilestone",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/view
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.MilestoneService/UpdateMilestone",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.MilestoneService/DeleteMilestone",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    // ── LabelService (4) ────────────────────────────────────────────────────
    // Labels are project-owned or global (tenant-wide) catalog entries with no
    // OpenFGA object of their own; the handler checks KanbanProject
    // view/manage (templates pattern). Global writes require manage on at
    // least one project of the tenant.
    DispatchEntry {
        method: "/sunbeam.kanban.v1.LabelService/CreateLabel",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.LabelService/ListLabels",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/view
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.LabelService/UpdateLabel",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
    DispatchEntry {
        method: "/sunbeam.kanban.v1.LabelService/DeleteLabel",
        namespace: "",
        relation: "",
        object_id_source: ObjectIdSource::None, // handler checks KanbanProject/manage
    },
];

/// RPC methods that bypass the `permission_dispatch` middleware entirely.
///
/// These are served by a separate Axum router that does not run
/// `auth_middleware` or `permission_dispatch`, so they must not be counted by the
/// matrix coverage checks.
pub const BYPASSED_METHODS: &[&str] = &[
    "/sunbeam.kanban.v1.PublicBoardService/GetPublicBoard",
    "/sunbeam.kanban.v1.PublicBoardService/ListPublicBoards",
];

// ============================================================================
// Subject hashing for correlation logging (no PII in logs)
// ============================================================================

/// Return a short hex digest of the first 8 bytes of `subject` for log
/// correlation. This is deliberately low-fidelity: it lets an operator match
/// log lines across a single request without exposing the full subject string.
fn hash_subject_prefix(subject: &str) -> impl fmt::Display {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    subject.hash(&mut h);
    // Format as 016x so the field width is stable in log output.
    format!(
        "{:016x}",
        <std::collections::hash_map::DefaultHasher as Hasher>::finish(&h)
    )
}

// ============================================================================
// Middleware
// ============================================================================

/// Per-RPC authorization dispatcher.
///
/// Must run *after* `auth_middleware` (which inserts `Extension<AuthContext>`)
/// and *before* the Connect-RPC service handlers.
///
/// Register with:
/// ```rust,ignore
/// use axum::middleware;
/// router.layer(middleware::from_fn_with_state(state, dispatch))
/// ```
pub async fn dispatch(
    State(state): State<Arc<DispatchState>>,
    Extension(auth): Extension<AuthContext>,
    Extension(tenant): Extension<TenantId>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let req = dispatch_check(state, auth, tenant.0, req).await?;
    Ok(next.run(req).await)
}

/// Inner authorization check, separated from the Tower middleware shell so it
/// can be unit-tested without spinning up an Axum router. Returns the request
/// (with `CheckedObjectId` inserted for Header-source RPCs) when authorized,
/// or the same `(StatusCode, String)` the middleware would return on
/// rejection.
pub(crate) async fn dispatch_check(
    state: Arc<DispatchState>,
    auth: AuthContext,
    tenant_id: String,
    mut req: Request,
) -> Result<Request, (StatusCode, String)> {
    // 1. Require authentication.
    if !auth.is_authenticated() {
        return Err((
            StatusCode::UNAUTHORIZED,
            "authentication required".to_string(),
        ));
    }

    let subject = auth.subject.as_deref().unwrap_or("");

    // 2. Locate the RPC in the matrix.
    let method = req.uri().path();
    let entry = MATRIX.iter().find(|e| e.method == method);
    let entry = match entry {
        Some(e) => e,
        None => {
            // An RPC path not in the matrix is a programming error: the
            // coverage binary catches this in CI.  Fail closed at runtime.
            tracing::error!(
                method,
                "permission_dispatch: unknown method — failing closed"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "unknown RPC method".to_string(),
            ));
        }
    };

    // 3–5. Object-id dispatch.
    match entry.object_id_source {
        ObjectIdSource::None => {
            // No specific object to authorize; the handler filters results.
        }
        ObjectIdSource::Header => {
            // 4. Read the object id from the header.
            let object_id = req
                .headers()
                .get("x-sunbeam-object-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_owned());

            let object_id = match object_id {
                Some(id) if !id.is_empty() => id,
                _ => {
                    tracing::warn!(
                        method,
                        "permission_dispatch: missing x-sunbeam-object-id header"
                    );
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "x-sunbeam-object-id header is required for this RPC".to_string(),
                    ));
                }
            };

            // 5. Build the tenant-scoped service client (cached per tenant)
            //    and run the check with the service's own credentials.
            let client = state
                .permission
                .tenant_client(&tenant_id)
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "permission_dispatch: failed to derive tenant client");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "authorization client error".to_string(),
                    )
                })?;

            // 6. Permission check.
            let allowed = client
                .check_permission(entry.namespace, &object_id, entry.relation, subject)
                .await
                .map_err(|e| {
                    tracing::error!(
                        method,
                        namespace = entry.namespace,
                        relation = entry.relation,
                        error = %e,
                        "permission_dispatch: check_permission error"
                    );
                    match e {
                        PermissionError::Denied => {
                            (StatusCode::FORBIDDEN, "permission denied".to_string())
                        }
                        PermissionError::Backend(_) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "authorization check failed".to_string(),
                        ),
                    }
                })?;

            if !allowed {
                tracing::warn!(
                    method,
                    namespace = entry.namespace,
                    relation = entry.relation,
                    subject_hash = %hash_subject_prefix(subject),
                    "permission_dispatch: permission denial"
                );
                return Err((StatusCode::FORBIDDEN, "permission denied".to_string()));
            }

            // 7. Insert the checked object id so handlers consume the
            //    authorized id, not an unchecked body field.
            req.extensions_mut().insert(CheckedObjectId(object_id));
        }
    }

    Ok(req)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ── Matrix shape tests (always run; no external deps) ───────────────────

    /// Every proto RPC must appear in the matrix exactly once.
    #[test]
    fn matrix_covers_all_rpcs() {
        let expected: HashSet<String> = expected_methods_from_protos()
            .into_iter()
            .filter(|m| !BYPASSED_METHODS.contains(&m.as_str()))
            .collect();
        let actual: HashSet<&str> = MATRIX.iter().map(|e| e.method).collect();

        let missing: Vec<_> = expected
            .iter()
            .filter(|m| !actual.contains(m.as_str()))
            .collect();
        let extra: Vec<_> = actual.iter().filter(|m| !expected.contains(**m)).collect();

        assert!(
            missing.is_empty() && extra.is_empty(),
            "matrix coverage mismatch:\n  missing from matrix: {missing:?}\n  extra in matrix:   {extra:?}"
        );
    }

    #[test]
    fn matrix_has_no_duplicate_methods() {
        let methods: Vec<&str> = MATRIX.iter().map(|e| e.method).collect();
        let unique: HashSet<&str> = methods.iter().copied().collect();
        assert_eq!(
            methods.len(),
            unique.len(),
            "duplicate method entries in MATRIX"
        );
    }

    /// Namespaces must be one of the known values (or empty for None-source entries).
    #[test]
    fn matrix_uses_only_known_namespaces() {
        let known = [
            "KanbanProject",
            "KanbanBoard",
            "KanbanCard",
            "KanbanAggregatedBoard",
            "",
        ];
        for entry in &MATRIX {
            assert!(
                known.contains(&entry.namespace),
                "unknown namespace {:?} in entry for {}",
                entry.namespace,
                entry.method
            );
        }
    }

    /// None-source entries must have empty namespace and relation.
    #[test]
    fn matrix_none_source_has_empty_namespace_and_relation() {
        for entry in &MATRIX {
            if entry.object_id_source == ObjectIdSource::None {
                assert!(
                    entry.namespace.is_empty() && entry.relation.is_empty(),
                    "None-source entry {:?} has non-empty namespace/relation",
                    entry.method
                );
            }
        }
    }

    /// Header-source entries must have non-empty namespace and relation.
    #[test]
    fn matrix_header_source_has_namespace_and_relation() {
        for entry in &MATRIX {
            if entry.object_id_source == ObjectIdSource::Header {
                assert!(
                    !entry.namespace.is_empty() && !entry.relation.is_empty(),
                    "Header-source entry {:?} has empty namespace or relation",
                    entry.method
                );
            }
        }
    }

    // ── Middleware behaviour tests (no external deps) ────────────────────────

    use axum::body::Body;
    use axum::http::{Request as HttpRequest, Uri};

    fn make_auth(authenticated: bool) -> AuthContext {
        if authenticated {
            AuthContext::authenticated("tenant-1", "user:test")
        } else {
            AuthContext::unauthenticated()
        }
    }

    /// Build a minimal Request that carries the required extensions.
    fn make_request(method_path: &str, auth: AuthContext) -> HttpRequest<Body> {
        let uri: Uri = method_path.parse().unwrap();
        let mut req = HttpRequest::builder().uri(uri).body(Body::empty()).unwrap();
        req.extensions_mut().insert(auth);
        req
    }

    /// Dummy state whose client is never called (the tested paths bail before
    /// the permission check).
    fn dummy_state() -> Arc<DispatchState> {
        let permission =
            PermissionClient::new(&crate::auth::permission_client::PermissionClientConfig {
                base_url: "http://localhost:1".to_string(),
                token_url: "http://localhost:1/oauth2/token".to_string(),
                client_id: "unused".to_string(),
                client_secret: "unused".to_string(),
            })
            .expect("dummy permission client should build");
        Arc::new(DispatchState {
            permission: Arc::new(permission),
        })
    }

    #[tokio::test]
    async fn dispatch_rejects_unauthenticated() {
        // Use a None-source entry so we never reach the permission check.
        let auth = make_auth(false);
        let mut req = make_request("/sunbeam.kanban.v1.ProjectService/ListProjects", auth);

        // We need Extension<Arc<DispatchState>> too — but dispatch checks
        // is_authenticated first, so we can use a dummy state.
        let state = dummy_state();
        req.extensions_mut().insert(state);

        let result = dispatch_raw(req).await;
        assert_eq!(
            result,
            StatusCode::UNAUTHORIZED,
            "unauthenticated request must be rejected"
        );
    }

    #[tokio::test]
    async fn dispatch_returns_400_when_header_missing_for_header_source() {
        // DeleteBoard requires ObjectIdSource::Header.
        let auth = make_auth(true);
        let mut req = make_request("/sunbeam.kanban.v1.BoardService/DeleteBoard", auth.clone());

        let state = dummy_state();
        req.extensions_mut().insert(auth);
        req.extensions_mut().insert(state);

        // No x-sunbeam-object-id header — should get 400 before the backend is called.
        let result = dispatch_raw(req).await;
        assert_eq!(
            result,
            StatusCode::BAD_REQUEST,
            "missing x-sunbeam-object-id header must yield 400"
        );
    }

    /// Verify that `GetBoard` uses `ObjectIdSource::None`.
    #[test]
    fn object_id_source_none_entry_exists_for_get_board() {
        let entry = MATRIX
            .iter()
            .find(|e| e.method == "/sunbeam.kanban.v1.BoardService/GetBoard")
            .expect("GetBoard must be in MATRIX");
        assert_eq!(entry.object_id_source, ObjectIdSource::None);
        assert!(entry.namespace.is_empty());
        assert!(entry.relation.is_empty());
    }

    #[test]
    fn none_source_entries_do_not_require_header() {
        for entry in &MATRIX {
            if entry.object_id_source == ObjectIdSource::None {
                // Verify the None-source entries are the ones we expect.
                let none_methods = [
                    "/sunbeam.kanban.v1.ProjectService/ListProjects",
                    "/sunbeam.kanban.v1.ProjectService/CreateProject",
                    "/sunbeam.kanban.v1.BoardService/ListBoards",
                    "/sunbeam.kanban.v1.BoardService/GetBoard",
                    "/sunbeam.kanban.v1.BoardService/SubscribeBoard",
                    "/sunbeam.kanban.v1.AggregatedBoardService/CreateAggregatedBoard",
                    "/sunbeam.kanban.v1.AggregatedBoardService/GetAggregatedBoard",
                    "/sunbeam.kanban.v1.AggregatedBoardService/ListAggregatedBoards",
                    "/sunbeam.kanban.v1.AggregatedBoardService/SubscribeAggregatedBoard",
                    "/sunbeam.kanban.v1.SearchService/SearchCards",
                    "/sunbeam.kanban.v1.TemplatesService/ListTemplates",
                    "/sunbeam.kanban.v1.TemplatesService/GetTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/CreateTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/UpdateTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/DeleteTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/ListCardTemplates",
                    "/sunbeam.kanban.v1.TemplatesService/GetCardTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/CreateCardTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/UpdateCardTemplate",
                    "/sunbeam.kanban.v1.TemplatesService/DeleteCardTemplate",
                    "/sunbeam.kanban.v1.MilestoneService/CreateMilestone",
                    "/sunbeam.kanban.v1.MilestoneService/ListMilestones",
                    "/sunbeam.kanban.v1.MilestoneService/GetMilestone",
                    "/sunbeam.kanban.v1.MilestoneService/UpdateMilestone",
                    "/sunbeam.kanban.v1.MilestoneService/DeleteMilestone",
                    "/sunbeam.kanban.v1.LabelService/CreateLabel",
                    "/sunbeam.kanban.v1.LabelService/ListLabels",
                    "/sunbeam.kanban.v1.LabelService/UpdateLabel",
                    "/sunbeam.kanban.v1.LabelService/DeleteLabel",
                ];
                assert!(
                    none_methods.contains(&entry.method),
                    "unexpected None-source entry: {}",
                    entry.method
                );
            }
        }
    }

    #[test]
    fn matrix_size_is_77() {
        assert_eq!(MATRIX.len(), 77, "matrix must contain exactly 77 entries");
    }

    #[test]
    fn hash_subject_prefix_is_stable() {
        let h1 = hash_subject_prefix("user:sienna").to_string();
        let h2 = hash_subject_prefix("user:sienna").to_string();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16, "hash output must be 16 hex chars");
    }

    #[test]
    fn hash_subject_prefix_differs_for_different_subjects() {
        let h1 = hash_subject_prefix("user:alice").to_string();
        let h2 = hash_subject_prefix("user:bob").to_string();
        assert_ne!(h1, h2);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    async fn dispatch_raw(req: Request) -> StatusCode {
        let res = dispatch_check(
            req.extensions()
                .get::<Arc<DispatchState>>()
                .cloned()
                .unwrap(),
            req.extensions().get::<AuthContext>().cloned().unwrap(),
            "tenant-1".to_string(),
            req,
        )
        .await;
        match res {
            Ok(_) => StatusCode::OK,
            Err((code, _)) => code,
        }
    }

    /// Scan the Kanban proto files and return every gRPC method path.
    fn expected_methods_from_protos() -> Vec<String> {
        use std::io::BufRead;

        let proto_dir = std::path::Path::new("proto/sunbeam/kanban/v1");
        let mut methods = Vec::new();

        for entry in std::fs::read_dir(proto_dir).expect("proto dir should exist") {
            let entry = entry.expect("proto dir entry");
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("proto") {
                continue;
            }

            let file = std::fs::File::open(&path).expect("proto file should open");
            let reader = std::io::BufReader::new(file);

            let mut package: Option<String> = None;
            let mut current_service: Option<String> = None;

            for line in reader.lines().map_while(Result::ok) {
                let trimmed = line.trim();
                if trimmed.starts_with("package ") {
                    package = trimmed
                        .strip_prefix("package ")
                        .and_then(|s| s.strip_suffix(';'))
                        .map(|s| s.to_string());
                } else if trimmed.starts_with("service ") {
                    current_service = trimmed
                        .strip_prefix("service ")
                        .and_then(|s| s.split_whitespace().next())
                        .map(|s| s.to_string());
                } else if trimmed.starts_with("rpc ") {
                    let Some(pkg) = package.as_ref() else {
                        continue;
                    };
                    let Some(svc) = current_service.as_ref() else {
                        continue;
                    };
                    let rpc_name = trimmed
                        .strip_prefix("rpc ")
                        // Method name ends at the first `(` (canonical buf
                        // style: `rpc Name(Request)`) or whitespace.
                        .and_then(|s| s.split(['(', ' ', '\t']).next())
                        .map(|s| s.to_string());
                    if let Some(rpc) = rpc_name {
                        methods.push(format!("/{pkg}.{svc}/{rpc}"));
                    }
                }
            }
        }

        methods
    }
}
