// SPDX-License-Identifier: AGPL-3.0-or-later
//! Board and card templates.
//!
//! Templates are project-owned resources within a tenant. Global templates have
//! no `project_id`, are read-only, and are visible to every authenticated user
//! in the same tenant.
//! Project-scoped templates reuse the parent project's permissions:
//!   * `view`  → list and get
//!   * `manage` → create, update, and delete
//!
//! The dynamic `sqlx` API is used throughout, so `cargo check` works without a
//! live database connection.

use std::sync::Arc;

use crate::id::Id;
use buffa_types::google::protobuf::{FieldMask, Timestamp};
use chrono::{DateTime, Utc};
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use sqlx::{PgPool, Row};
use tracing::error;

use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_retry::PermissionRetryExt;
use crate::cpb::sunbeam::kanban::v1::{
    BoardTemplate, CardTemplate, CreateCardTemplateRequest, CreateCardTemplateResponse,
    CreateTemplateRequest, CreateTemplateResponse, DeleteCardTemplateRequest,
    DeleteCardTemplateResponse, DeleteTemplateRequest, DeleteTemplateResponse,
    GetCardTemplateRequest, GetCardTemplateResponse, GetTemplateRequest, GetTemplateResponse,
    ListCardTemplatesRequest, ListCardTemplatesResponse, ListTemplatesRequest,
    ListTemplatesResponse, TemplateChecklistItem, TemplateColumn, TemplatesService,
    UpdateCardTemplateRequest, UpdateCardTemplateResponse, UpdateTemplateRequest,
    UpdateTemplateResponse,
};

// ── Constants ────────────────────────────────────────────────────────────────

const PERMISSION_TYPE_PROJECT: &str = "KanbanProject";
const SYSTEM_TENANT: &str = "system";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct TemplatesServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
}

// ── Timestamp helpers (chrono ↔ buffa_types) ────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
        ..Default::default()
    }
}

// ── Error helpers ────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    error!(error = %err, "{msg}");
    ConnectError::internal(msg)
}

fn subject_from_request(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing auth context"))
}

fn tenant_id_from_request(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))
}

/// Extract the caller's tenant from the auth context and derive a permission
/// client scoped to that tenant's store (see `TENANT_HEADER`).
async fn tenant_client_for(
    permission: &PermissionClient,
    ctx: &RequestContext,
) -> Result<PermissionClient, ConnectError> {
    let tenant = ctx
        .extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))?;
    permission
        .tenant_client(&tenant)
        .await
        .map_err(|e| internal("failed to build tenant permission client", e))
}

fn parse_id(s: &str, field: &str) -> Result<Id, ConnectError> {
    match s.parse::<Id>() {
        Ok(id) => Ok(id),
        Err(_) => Err(ConnectError::invalid_argument(format!("invalid {field}"))),
    }
}

// ── Project-level permission checks ────────────────────────────────────────────────

async fn check_project_permission(
    permission: &PermissionClient,
    project_id: Id,
    relation: &str,
    subject: &str,
) -> Result<(), ConnectError> {
    let allowed = permission
        .check_permission_with_retry(
            PERMISSION_TYPE_PROJECT,
            &project_id.to_string(),
            relation,
            subject,
        )
        .await
        .map_err(|e| internal("failed to check project permission", e))?;

    if allowed {
        Ok(())
    } else {
        Err(ConnectError::permission_denied("permission denied"))
    }
}

// ── JSON conversion helpers ───────────────────────────────────────────────────

fn columns_to_json(columns: &[TemplateColumn]) -> serde_json::Value {
    let values: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| {
            serde_json::json!({
                "title": c.title,
                "position": c.position,
                "accent": c.accent,
            })
        })
        .collect();
    serde_json::Value::Array(values)
}

fn columns_from_json(value: &serde_json::Value) -> Vec<TemplateColumn> {
    let Some(arr) = value.as_array() else {
        return Vec::new();
    };

    arr.iter()
        .map(|v| TemplateColumn {
            title: v
                .get("title")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            position: v.get("position").and_then(|x| x.as_i64()).unwrap_or(0) as i32,
            accent: v
                .get("accent")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            ..Default::default()
        })
        .collect()
}

fn checklist_to_json(items: &[TemplateChecklistItem]) -> serde_json::Value {
    let values: Vec<serde_json::Value> = items
        .iter()
        .map(|i| {
            serde_json::json!({
                "title": i.title,
            })
        })
        .collect();
    serde_json::Value::Array(values)
}

fn checklist_from_json(value: &serde_json::Value) -> Vec<TemplateChecklistItem> {
    let Some(arr) = value.as_array() else {
        return Vec::new();
    };

    arr.iter()
        .map(|v| TemplateChecklistItem {
            title: v
                .get("title")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            ..Default::default()
        })
        .collect()
}

// ── Row → proto helpers ───────────────────────────────────────────────────────

fn board_template_from_row(row: &sqlx::postgres::PgRow) -> BoardTemplate {
    let id: Id = row.get("id");
    let project_id: Option<Id> = row.get("project_id");
    let name: String = row.get("name");
    let description: Option<String> = row.get("description");
    let columns: serde_json::Value = row.get("columns");
    let is_global: bool = row.get("is_global");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    BoardTemplate {
        id: id.to_string(),
        project_id: project_id.map(|u| u.to_string()).unwrap_or_default(),
        name,
        description: description.unwrap_or_default(),
        columns: columns_from_json(&columns),
        is_global,
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        ..Default::default()
    }
}

fn card_template_from_row(row: &sqlx::postgres::PgRow) -> CardTemplate {
    let id: Id = row.get("id");
    let project_id: Option<Id> = row.get("project_id");
    let name: String = row.get("name");
    let description: Option<String> = row.get("description");
    let title: Option<String> = row.get("title");
    let default_description: Option<String> = row.get("default_description");
    let label_names: Vec<String> = row.get("label_names");
    let checklist_items: serde_json::Value = row.get("checklist_items");
    let is_global: bool = row.get("is_global");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    CardTemplate {
        id: id.to_string(),
        project_id: project_id.map(|u| u.to_string()).unwrap_or_default(),
        name,
        description: description.unwrap_or_default(),
        title: title.unwrap_or_default(),
        default_description: default_description.unwrap_or_default(),
        label_names,
        checklist_items: checklist_from_json(&checklist_items),
        is_global,
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        ..Default::default()
    }
}

// ── Field-mask helper ────────────────────────────────────────────────────────

fn mask_contains(mask: &buffa::MessageField<FieldMask>, path: &str) -> bool {
    mask.as_option()
        .map(|m| m.paths.iter().any(|p| p == path))
        .unwrap_or(false)
}

// ── impl TemplatesService ────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl TemplatesService for TemplatesServiceImpl {
    // ── Board templates ──────────────────────────────────────────────────────

    async fn list_templates(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListTemplatesRequest>,
    ) -> ServiceResult<ListTemplatesResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        // Global templates are visible to any authenticated user within the
        // caller's tenant. The legacy `system` tenant seeds remain available
        // until the migration is updated.
        let global_rows = sqlx::query(
            "SELECT id, project_id, name, description, columns, is_global, created_at, updated_at \
             FROM board_templates \
             WHERE tenant_id IN ($1, $2) AND is_global = true \
             ORDER BY name",
        )
        .bind(&tenant_id)
        .bind(SYSTEM_TENANT)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list global board templates", e))?;

        let mut templates: Vec<BoardTemplate> =
            global_rows.iter().map(board_template_from_row).collect();

        // If a project is requested, include its scoped templates when the user
        // has view permission on the project.
        if !req.project_id.is_empty() {
            let project_id = parse_id(&req.project_id, "project_id")?;
            let visible = check_project_permission(&permission, project_id, "view", &subject)
                .await
                .is_ok();

            if visible {
                // Globals are already in the list; only add project-scoped rows.
                let project_rows = sqlx::query(
                    "SELECT id, project_id, name, description, columns, is_global, created_at, updated_at \
                     FROM board_templates \
                     WHERE tenant_id IN ($1, $2) AND project_id = $3 AND is_global = false \
                     ORDER BY name",
                )
                .bind(&tenant_id)
                .bind(SYSTEM_TENANT)
                .bind(project_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| internal("failed to list project board templates", e))?;

                templates.extend(project_rows.iter().map(board_template_from_row));
            }
        }

        Ok(Response::new(ListTemplatesResponse {
            templates,
            ..Default::default()
        }))
    }

    async fn get_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetTemplateRequest>,
    ) -> ServiceResult<GetTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let row = sqlx::query(
            "SELECT id, project_id, name, description, columns, is_global, created_at, updated_at \
             FROM board_templates \
             WHERE id = $1 AND tenant_id IN ($2, $3)",
        )
        .bind(template_id)
        .bind(&tenant_id)
        .bind(SYSTEM_TENANT)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board template", e))?
        .ok_or_else(|| ConnectError::not_found("template not found"))?;

        let is_global: bool = row.get("is_global");
        if !is_global {
            let project_id: Option<Id> = row.get("project_id");
            if let Some(pid) = project_id {
                check_project_permission(&permission, pid, "view", &subject).await?;
            }
        }

        Ok(Response::new(GetTemplateResponse {
            template: Some(board_template_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn create_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateTemplateRequest>,
    ) -> ServiceResult<CreateTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        if req.name.is_empty() {
            return Err(ConnectError::invalid_argument("name is required"));
        }

        // Global templates are read-only in v1.
        if req.project_id.is_empty() {
            return Err(ConnectError::permission_denied(
                "global templates cannot be created via API",
            ));
        }

        let project_id = parse_id(&req.project_id, "project_id")?;
        check_project_permission(&permission, project_id, "manage", &subject).await?;

        let columns = columns_to_json(&req.columns);
        let template_id = Id::new();

        let row = sqlx::query(
            r#"
            INSERT INTO board_templates (id, tenant_id, project_id, name, description, columns, is_global, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, false, $7)
            RETURNING id, project_id, name, description, columns, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(columns)
        .bind(&subject)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to create board template", e))?;

        Ok(Response::new(CreateTemplateResponse {
            template: Some(board_template_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn update_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateTemplateRequest>,
    ) -> ServiceResult<UpdateTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing = sqlx::query(
            "SELECT project_id, is_global FROM board_templates WHERE id = $1 AND tenant_id = $2",
        )
        .bind(template_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board template for update", e))?
        .ok_or_else(|| ConnectError::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(ConnectError::permission_denied(
                "global templates cannot be modified",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&permission, pid, "manage", &subject).await?;
        }

        let name = if mask_contains(&req.update_mask, "name") {
            Some(req.name)
        } else {
            None
        };
        let description = if mask_contains(&req.update_mask, "description") {
            Some(req.description)
        } else {
            None
        };
        let columns = if mask_contains(&req.update_mask, "columns") {
            Some(columns_to_json(&req.columns))
        } else {
            None
        };

        let row = sqlx::query(
            r#"
            UPDATE board_templates SET
                name        = COALESCE($2, name),
                description = COALESCE($3, description),
                columns     = COALESCE($4, columns),
                updated_at  = now()
            WHERE id = $1 AND tenant_id = $5
            RETURNING id, project_id, name, description, columns, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
        .bind(name)
        .bind(description)
        .bind(columns)
        .bind(&tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to update board template", e))?;

        Ok(Response::new(UpdateTemplateResponse {
            template: Some(board_template_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn delete_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteTemplateRequest>,
    ) -> ServiceResult<DeleteTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing = sqlx::query(
            "SELECT project_id, is_global FROM board_templates WHERE id = $1 AND tenant_id = $2",
        )
        .bind(template_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board template for delete", e))?
        .ok_or_else(|| ConnectError::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(ConnectError::permission_denied(
                "global templates cannot be deleted",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&permission, pid, "manage", &subject).await?;
        }

        let result = sqlx::query("DELETE FROM board_templates WHERE id = $1 AND tenant_id = $2")
            .bind(template_id)
            .bind(&tenant_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete board template", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("template not found"));
        }

        Ok(Response::new(DeleteTemplateResponse::default()))
    }

    // ── Card templates ───────────────────────────────────────────────────────

    async fn list_card_templates(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListCardTemplatesRequest>,
    ) -> ServiceResult<ListCardTemplatesResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        let global_rows = sqlx::query(
            "SELECT id, project_id, name, description, title, default_description, \
             label_names, checklist_items, is_global, created_at, updated_at \
             FROM card_templates \
             WHERE tenant_id IN ($1, $2) AND is_global = true \
             ORDER BY name",
        )
        .bind(&tenant_id)
        .bind(SYSTEM_TENANT)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list global card templates", e))?;

        let mut templates: Vec<CardTemplate> =
            global_rows.iter().map(card_template_from_row).collect();

        if !req.project_id.is_empty() {
            let project_id = parse_id(&req.project_id, "project_id")?;
            let visible = check_project_permission(&permission, project_id, "view", &subject)
                .await
                .is_ok();

            if visible {
                // Globals are already in the list; only add project-scoped rows.
                let project_rows = sqlx::query(
                    "SELECT id, project_id, name, description, title, default_description, \
                     label_names, checklist_items, is_global, created_at, updated_at \
                     FROM card_templates \
                     WHERE tenant_id IN ($1, $2) AND project_id = $3 AND is_global = false \
                     ORDER BY name",
                )
                .bind(&tenant_id)
                .bind(SYSTEM_TENANT)
                .bind(project_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| internal("failed to list project card templates", e))?;

                templates.extend(project_rows.iter().map(card_template_from_row));
            }
        }

        Ok(Response::new(ListCardTemplatesResponse {
            templates,
            ..Default::default()
        }))
    }

    async fn get_card_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetCardTemplateRequest>,
    ) -> ServiceResult<GetCardTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let row = sqlx::query(
            "SELECT id, project_id, name, description, title, default_description, \
             label_names, checklist_items, is_global, created_at, updated_at \
             FROM card_templates \
             WHERE id = $1 AND tenant_id IN ($2, $3)",
        )
        .bind(template_id)
        .bind(&tenant_id)
        .bind(SYSTEM_TENANT)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch card template", e))?
        .ok_or_else(|| ConnectError::not_found("template not found"))?;

        let is_global: bool = row.get("is_global");
        if !is_global {
            let project_id: Option<Id> = row.get("project_id");
            if let Some(pid) = project_id {
                check_project_permission(&permission, pid, "view", &subject).await?;
            }
        }

        Ok(Response::new(GetCardTemplateResponse {
            template: Some(card_template_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn create_card_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateCardTemplateRequest>,
    ) -> ServiceResult<CreateCardTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        if req.name.is_empty() {
            return Err(ConnectError::invalid_argument("name is required"));
        }

        if req.project_id.is_empty() {
            return Err(ConnectError::permission_denied(
                "global templates cannot be created via API",
            ));
        }

        let project_id = parse_id(&req.project_id, "project_id")?;
        check_project_permission(&permission, project_id, "manage", &subject).await?;

        let checklist = checklist_to_json(&req.checklist_items);
        let template_id = Id::new();

        let row = sqlx::query(
            r#"
            INSERT INTO card_templates (
                id, tenant_id, project_id, name, description, title, default_description,
                label_names, checklist_items, is_global, created_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false, $10)
            RETURNING id, project_id, name, description, title, default_description,
                      label_names, checklist_items, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&req.title)
        .bind(&req.default_description)
        .bind(&req.label_names)
        .bind(checklist)
        .bind(&subject)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to create card template", e))?;

        Ok(Response::new(CreateCardTemplateResponse {
            template: Some(card_template_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn update_card_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateCardTemplateRequest>,
    ) -> ServiceResult<UpdateCardTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing = sqlx::query(
            "SELECT project_id, is_global FROM card_templates WHERE id = $1 AND tenant_id = $2",
        )
        .bind(template_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch card template for update", e))?
        .ok_or_else(|| ConnectError::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(ConnectError::permission_denied(
                "global templates cannot be modified",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&permission, pid, "manage", &subject).await?;
        }

        let name = if mask_contains(&req.update_mask, "name") {
            Some(req.name)
        } else {
            None
        };
        let description = if mask_contains(&req.update_mask, "description") {
            Some(req.description)
        } else {
            None
        };
        let title = if mask_contains(&req.update_mask, "title") {
            Some(req.title)
        } else {
            None
        };
        let default_description = if mask_contains(&req.update_mask, "default_description") {
            Some(req.default_description)
        } else {
            None
        };
        let label_names = if mask_contains(&req.update_mask, "label_names") {
            Some(req.label_names)
        } else {
            None
        };
        let checklist_items = if mask_contains(&req.update_mask, "checklist_items") {
            Some(checklist_to_json(&req.checklist_items))
        } else {
            None
        };

        let row = sqlx::query(
            r#"
            UPDATE card_templates SET
                name                = COALESCE($2, name),
                description         = COALESCE($3, description),
                title               = COALESCE($4, title),
                default_description = COALESCE($5, default_description),
                label_names         = COALESCE($6, label_names),
                checklist_items     = COALESCE($7, checklist_items),
                updated_at          = now()
            WHERE id = $1 AND tenant_id = $8
            RETURNING id, project_id, name, description, title, default_description,
                      label_names, checklist_items, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
        .bind(name)
        .bind(description)
        .bind(title)
        .bind(default_description)
        .bind(label_names)
        .bind(checklist_items)
        .bind(&tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to update card template", e))?;

        Ok(Response::new(UpdateCardTemplateResponse {
            template: Some(card_template_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn delete_card_template(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteCardTemplateRequest>,
    ) -> ServiceResult<DeleteCardTemplateResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing = sqlx::query(
            "SELECT project_id, is_global FROM card_templates WHERE id = $1 AND tenant_id = $2",
        )
        .bind(template_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch card template for delete", e))?
        .ok_or_else(|| ConnectError::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(ConnectError::permission_denied(
                "global templates cannot be deleted",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&permission, pid, "manage", &subject).await?;
        }

        let result = sqlx::query("DELETE FROM card_templates WHERE id = $1 AND tenant_id = $2")
            .bind(template_id)
            .bind(&tenant_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete card template", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("template not found"));
        }

        Ok(Response::new(DeleteCardTemplateResponse::default()))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::{connect_ctx, connect_request, containers};

    async fn make_service() -> TemplatesServiceImpl {
        let infra = containers::setup().await;
        TemplatesServiceImpl {
            pool: infra.pool,
            permission: Arc::clone(&infra.permission),
        }
    }

    /// Build a request context with an authenticated subject in its extensions.
    fn authed_ctx(subject: &str) -> RequestContext {
        connect_ctx(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            subject,
        ))
    }

    /// Create a minimal project and grant the test subject manage and view on it.
    async fn create_test_project(
        pool: &PgPool,
        permission: &PermissionClient,
        subject: &str,
    ) -> Id {
        let project_id = Id::new();
        let slug = format!("tp-{}", &project_id.to_string()[18..26]);
        let tenant_id = crate::test_support::test_tenant_id();

        sqlx::query(
            "INSERT INTO projects (id, tenant_id, name, slug, description, owner_id) VALUES ($1, $2, $3, $4, '', $5)",
        )
        .bind(project_id)
        .bind(tenant_id)
        .bind(format!("Test Project {project_id}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");

        permission
            .grant_with_retry(
                PERMISSION_TYPE_PROJECT,
                &project_id.to_string(),
                "admin",
                subject,
            )
            .await
            .expect("grant admin failed");
        permission
            .grant_with_retry(
                PERMISSION_TYPE_PROJECT,
                &project_id.to_string(),
                "viewer",
                subject,
            )
            .await
            .expect("grant viewer failed");

        project_id
    }

    async fn cleanup_project(pool: &PgPool, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    async fn cleanup_template(pool: &PgPool, table: &str, template_id: Id) {
        let tenant_id = crate::test_support::test_tenant_id();
        let _ = sqlx::query(&format!(
            "DELETE FROM {table} WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant_id)
        .bind(template_id)
        .execute(pool)
        .await;
    }

    // ── Board template tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn list_templates_includes_global_seeds() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());

        let list = svc
            .list_templates(
                authed_ctx(&subject),
                connect_request(&ListTemplatesRequest {
                    project_id: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_templates failed")
            .body;

        assert!(
            list.templates
                .iter()
                .any(|t| t.name == "Kanban" && t.is_global),
            "global Kanban seed template should be present"
        );
    }

    #[tokio::test]
    async fn create_then_get_board_template() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_template(
                authed_ctx(&subject),
                connect_request(&CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Sprint Retro".to_string(),
                    description: "Retro columns".to_string(),
                    columns: vec![
                        TemplateColumn {
                            title: "What went well".to_string(),
                            position: 0,
                            accent: "green".to_string(),
                            ..Default::default()
                        },
                        TemplateColumn {
                            title: "What to improve".to_string(),
                            position: 1,
                            accent: "amber".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Sprint Retro");
        assert_eq!(created.project_id, project_id.to_string());
        assert!(!created.is_global);
        assert_eq!(created.columns.len(), 2);
        assert_eq!(created.columns[0].title, "What went well");

        let fetched = svc
            .get_template(
                authed_ctx(&subject),
                connect_request(&GetTemplateRequest {
                    template_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.columns.len(), 2);

        cleanup_template(
            &svc.pool,
            "board_templates",
            created.id.parse::<Id>().unwrap(),
        )
        .await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn list_templates_includes_project_scoped_when_authorized() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_template(
                authed_ctx(&subject),
                connect_request(&CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Project Template".to_string(),
                    description: String::new(),
                    columns: vec![TemplateColumn {
                        title: "Only".to_string(),
                        position: 0,
                        accent: "blue".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        let list_all = svc
            .list_templates(
                authed_ctx(&subject),
                connect_request(&ListTemplatesRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_templates failed")
            .body;

        assert!(
            list_all
                .templates
                .iter()
                .any(|t| t.name == "Kanban" && t.is_global),
            "global templates should still appear"
        );
        assert!(
            list_all
                .templates
                .iter()
                .any(|t| t.id == created.id && !t.is_global),
            "project template should appear"
        );
        {
            // Global templates must not be duplicated by the project-scoped arm.
            let mut ids: Vec<&str> = list_all.templates.iter().map(|t| t.id.as_str()).collect();
            ids.sort_unstable();
            let before = ids.len();
            ids.dedup();
            assert_eq!(before, ids.len(), "list_templates returned duplicate rows");
        }

        let list_global = svc
            .list_templates(
                authed_ctx(&subject),
                connect_request(&ListTemplatesRequest {
                    project_id: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_templates failed")
            .body;

        assert!(
            !list_global.templates.iter().any(|t| t.id == created.id),
            "project template should not appear without project_id"
        );

        cleanup_template(
            &svc.pool,
            "board_templates",
            created.id.parse::<Id>().unwrap(),
        )
        .await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn update_template_applies_mask() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_template(
                authed_ctx(&subject),
                connect_request(&CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Original".to_string(),
                    description: "Original desc".to_string(),
                    columns: vec![TemplateColumn {
                        title: "Old".to_string(),
                        position: 0,
                        accent: "red".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        let updated = svc
            .update_template(
                authed_ctx(&subject),
                connect_request(&UpdateTemplateRequest {
                    template_id: created.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["name".to_string(), "columns".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    name: "Updated".to_string(),
                    description: String::new(),
                    columns: vec![TemplateColumn {
                        title: "New".to_string(),
                        position: 0,
                        accent: "green".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            )
            .await
            .expect("update_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        assert_eq!(updated.name, "Updated");
        assert_eq!(updated.description, "Original desc");
        assert_eq!(updated.columns.len(), 1);
        assert_eq!(updated.columns[0].title, "New");

        cleanup_template(
            &svc.pool,
            "board_templates",
            created.id.parse::<Id>().unwrap(),
        )
        .await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn delete_template_removes_row() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_template(
                authed_ctx(&subject),
                connect_request(&CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "To Delete".to_string(),
                    description: String::new(),
                    columns: vec![],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        svc.delete_template(
            authed_ctx(&subject),
            connect_request(&DeleteTemplateRequest {
                template_id: created.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete_template failed");

        let result = svc
            .get_template(
                authed_ctx(&subject),
                connect_request(&GetTemplateRequest {
                    template_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await;
        assert!(result.is_err(), "deleted template should not be found");

        cleanup_project(&svc.pool, project_id).await;
    }

    // ── Card template tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_card_template() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_card_template(
                authed_ctx(&subject),
                connect_request(&CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Bug Card".to_string(),
                    description: "Bug template".to_string(),
                    title: "[BUG] ".to_string(),
                    default_description: "Steps to reproduce:\n".to_string(),
                    label_names: vec!["bug".to_string(), "triage".to_string()],
                    checklist_items: vec![
                        TemplateChecklistItem {
                            title: "Reproduce".to_string(),
                            ..Default::default()
                        },
                        TemplateChecklistItem {
                            title: "Fix".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_card_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Bug Card");
        assert_eq!(created.title, "[BUG] ");
        assert_eq!(created.label_names, vec!["bug", "triage"]);
        assert_eq!(created.checklist_items.len(), 2);

        let fetched = svc
            .get_card_template(
                authed_ctx(&subject),
                connect_request(&GetCardTemplateRequest {
                    template_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get_card_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        assert_eq!(fetched.id, created.id);

        cleanup_template(
            &svc.pool,
            "card_templates",
            created.id.parse::<Id>().unwrap(),
        )
        .await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn list_card_templates_includes_project_scoped_when_authorized() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_card_template(
                authed_ctx(&subject),
                connect_request(&CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Task Card".to_string(),
                    description: String::new(),
                    title: String::new(),
                    default_description: String::new(),
                    label_names: vec![],
                    checklist_items: vec![],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_card_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        let list = svc
            .list_card_templates(
                authed_ctx(&subject),
                connect_request(&ListCardTemplatesRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_card_templates failed")
            .body;

        assert!(
            list.templates
                .iter()
                .any(|t| t.id == created.id && !t.is_global),
            "project card template should appear"
        );

        cleanup_template(
            &svc.pool,
            "card_templates",
            created.id.parse::<Id>().unwrap(),
        )
        .await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn update_card_template_applies_mask() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_card_template(
                authed_ctx(&subject),
                connect_request(&CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Original".to_string(),
                    description: "Original desc".to_string(),
                    title: "Old title".to_string(),
                    default_description: "Old body".to_string(),
                    label_names: vec!["old".to_string()],
                    checklist_items: vec![],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_card_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        let updated = svc
            .update_card_template(
                authed_ctx(&subject),
                connect_request(&UpdateCardTemplateRequest {
                    template_id: created.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["title".to_string(), "label_names".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    name: String::new(),
                    description: String::new(),
                    title: "New title".to_string(),
                    default_description: String::new(),
                    label_names: vec!["new".to_string()],
                    checklist_items: vec![],
                    ..Default::default()
                }),
            )
            .await
            .expect("update_card_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        assert_eq!(updated.title, "New title");
        assert_eq!(updated.label_names, vec!["new"]);
        assert_eq!(updated.name, "Original");
        assert_eq!(updated.description, "Original desc");

        cleanup_template(
            &svc.pool,
            "card_templates",
            created.id.parse::<Id>().unwrap(),
        )
        .await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn delete_card_template_removes_row() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_card_template(
                authed_ctx(&subject),
                connect_request(&CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "To Delete".to_string(),
                    description: String::new(),
                    title: String::new(),
                    default_description: String::new(),
                    label_names: vec![],
                    checklist_items: vec![],
                    ..Default::default()
                }),
            )
            .await
            .expect("create_card_template failed")
            .body
            .template
            .into_option()
            .expect("template missing");

        svc.delete_card_template(
            authed_ctx(&subject),
            connect_request(&DeleteCardTemplateRequest {
                template_id: created.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete_card_template failed");

        let result = svc
            .get_card_template(
                authed_ctx(&subject),
                connect_request(&GetCardTemplateRequest {
                    template_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await;
        assert!(result.is_err(), "deleted card template should not be found");

        cleanup_project(&svc.pool, project_id).await;
    }
}
