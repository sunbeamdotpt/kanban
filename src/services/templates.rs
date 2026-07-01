// SPDX-License-Identifier: AGPL-3.0-or-later
//! Board and card templates.
//!
//! Templates are project-owned resources. Global templates have no
//! `project_id`, are read-only, and are visible to every authenticated user.
//! Project-scoped templates reuse the parent project's permissions:
//!   * `view`  → list and get
//!   * `manage` → create, update, and delete
//!
//! The dynamic `sqlx` API is used throughout, so `cargo check` works without a
//! live database connection.

use std::sync::Arc;

use crate::id::Id;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use tracing::error;

use sunbeam_g2v::middleware::auth::AuthContext;
use sunbeam_g2v::middleware::auth::keto::KetoClient;

use crate::auth::keto_retry::KetoRetryExt;
use crate::pb::templates_service_server::TemplatesService;
use crate::pb::{
    BoardTemplate, CardTemplate, CreateCardTemplateRequest, CreateCardTemplateResponse,
    CreateTemplateRequest, CreateTemplateResponse, DeleteCardTemplateRequest,
    DeleteCardTemplateResponse, DeleteTemplateRequest, DeleteTemplateResponse,
    GetCardTemplateRequest, GetCardTemplateResponse, GetTemplateRequest, GetTemplateResponse,
    ListCardTemplatesRequest, ListCardTemplatesResponse, ListTemplatesRequest,
    ListTemplatesResponse, TemplateChecklistItem, TemplateColumn, UpdateCardTemplateRequest,
    UpdateCardTemplateResponse, UpdateTemplateRequest, UpdateTemplateResponse,
};

// ── Constants ────────────────────────────────────────────────────────────────

const KETO_NS_PROJECT: &str = "KanbanProject";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct TemplatesServiceImpl {
    pub pool: PgPool,
    pub keto: Arc<KetoClient>,
}

// ── Timestamp helpers (chrono ↔ prost_types) ─────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

// ── Error helpers ────────────────────────────────────────────────────────────

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

fn parse_id(s: &str, field: &str) -> Result<Id, Status> {
    match s.parse::<Id>() {
        Ok(id) => Ok(id),
        Err(_) => Err(Status::invalid_argument(format!("invalid {field}"))),
    }
}

// ── Project-level Keto checks ────────────────────────────────────────────────

async fn check_project_permission(
    keto: &KetoClient,
    project_id: Id,
    relation: &str,
    subject: &str,
) -> Result<(), Status> {
    let allowed = keto
        .check_permission_with_retry(KETO_NS_PROJECT, &project_id.to_string(), relation, subject)
        .await
        .map_err(|e| internal("failed to check project permission", e))?;

    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied("permission denied"))
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
        created_at: Some(to_proto_ts(created_at)),
        updated_at: Some(to_proto_ts(updated_at)),
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
        created_at: Some(to_proto_ts(created_at)),
        updated_at: Some(to_proto_ts(updated_at)),
    }
}

// ── Field-mask helper ────────────────────────────────────────────────────────

fn mask_contains(mask: &Option<prost_types::FieldMask>, path: &str) -> bool {
    match mask {
        Some(m) => m.paths.iter().any(|p| p == path),
        None => false,
    }
}

// ── impl TemplatesService ────────────────────────────────────────────────────

#[tonic::async_trait]
impl TemplatesService for TemplatesServiceImpl {
    // ── Board templates ──────────────────────────────────────────────────────

    async fn list_templates(
        &self,
        request: Request<ListTemplatesRequest>,
    ) -> Result<Response<ListTemplatesResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        // Global templates are always visible to authenticated users.
        let global_rows = sqlx::query(
            "SELECT id, project_id, name, description, columns, is_global, created_at, updated_at \
             FROM board_templates WHERE is_global = true ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list global board templates", e))?;

        let mut templates: Vec<BoardTemplate> =
            global_rows.iter().map(board_template_from_row).collect();

        // If a project is requested, include its scoped templates when the user
        // has view permission on the project.
        if !req.project_id.is_empty() {
            let project_id = parse_id(&req.project_id, "project_id")?;
            let visible = check_project_permission(&self.keto, project_id, "view", &subject)
                .await
                .is_ok();

            if visible {
                let project_rows = sqlx::query(
                    "SELECT id, project_id, name, description, columns, is_global, created_at, updated_at \
                     FROM board_templates WHERE is_global = false AND project_id = $1 ORDER BY name",
                )
                .bind(project_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| internal("failed to list project board templates", e))?;

                templates.extend(project_rows.iter().map(board_template_from_row));
            }
        }

        Ok(Response::new(ListTemplatesResponse { templates }))
    }

    async fn get_template(
        &self,
        request: Request<GetTemplateRequest>,
    ) -> Result<Response<GetTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let row = sqlx::query(
            "SELECT id, project_id, name, description, columns, is_global, created_at, updated_at \
             FROM board_templates WHERE id = $1",
        )
        .bind(template_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board template", e))?
        .ok_or_else(|| Status::not_found("template not found"))?;

        let is_global: bool = row.get("is_global");
        if !is_global {
            let project_id: Option<Id> = row.get("project_id");
            if let Some(pid) = project_id {
                check_project_permission(&self.keto, pid, "view", &subject).await?;
            }
        }

        Ok(Response::new(GetTemplateResponse {
            template: Some(board_template_from_row(&row)),
        }))
    }

    async fn create_template(
        &self,
        request: Request<CreateTemplateRequest>,
    ) -> Result<Response<CreateTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        // Global templates are read-only in v1.
        if req.project_id.is_empty() {
            return Err(Status::permission_denied(
                "global templates cannot be created via API",
            ));
        }

        let project_id = parse_id(&req.project_id, "project_id")?;
        check_project_permission(&self.keto, project_id, "manage", &subject).await?;

        let columns = columns_to_json(&req.columns);
        let template_id = Id::new();

        let row = sqlx::query(
            r#"
            INSERT INTO board_templates (id, project_id, name, description, columns, is_global, created_by)
            VALUES ($1, $2, $3, $4, $5, false, $6)
            RETURNING id, project_id, name, description, columns, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(columns)
        .bind(&subject)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to create board template", e))?;

        Ok(Response::new(CreateTemplateResponse {
            template: Some(board_template_from_row(&row)),
        }))
    }

    async fn update_template(
        &self,
        request: Request<UpdateTemplateRequest>,
    ) -> Result<Response<UpdateTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing =
            sqlx::query("SELECT project_id, is_global FROM board_templates WHERE id = $1")
                .bind(template_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch board template for update", e))?
                .ok_or_else(|| Status::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(Status::permission_denied(
                "global templates cannot be modified",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&self.keto, pid, "manage", &subject).await?;
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
            WHERE id = $1
            RETURNING id, project_id, name, description, columns, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
        .bind(name)
        .bind(description)
        .bind(columns)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to update board template", e))?;

        Ok(Response::new(UpdateTemplateResponse {
            template: Some(board_template_from_row(&row)),
        }))
    }

    async fn delete_template(
        &self,
        request: Request<DeleteTemplateRequest>,
    ) -> Result<Response<DeleteTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing =
            sqlx::query("SELECT project_id, is_global FROM board_templates WHERE id = $1")
                .bind(template_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch board template for delete", e))?
                .ok_or_else(|| Status::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(Status::permission_denied(
                "global templates cannot be deleted",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&self.keto, pid, "manage", &subject).await?;
        }

        let result = sqlx::query("DELETE FROM board_templates WHERE id = $1")
            .bind(template_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete board template", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("template not found"));
        }

        Ok(Response::new(DeleteTemplateResponse {}))
    }

    // ── Card templates ───────────────────────────────────────────────────────

    async fn list_card_templates(
        &self,
        request: Request<ListCardTemplatesRequest>,
    ) -> Result<Response<ListCardTemplatesResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        let global_rows = sqlx::query(
            "SELECT id, project_id, name, description, title, default_description, \
             label_names, checklist_items, is_global, created_at, updated_at \
             FROM card_templates WHERE is_global = true ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list global card templates", e))?;

        let mut templates: Vec<CardTemplate> =
            global_rows.iter().map(card_template_from_row).collect();

        if !req.project_id.is_empty() {
            let project_id = parse_id(&req.project_id, "project_id")?;
            let visible = check_project_permission(&self.keto, project_id, "view", &subject)
                .await
                .is_ok();

            if visible {
                let project_rows = sqlx::query(
                    "SELECT id, project_id, name, description, title, default_description, \
                     label_names, checklist_items, is_global, created_at, updated_at \
                     FROM card_templates WHERE is_global = false AND project_id = $1 ORDER BY name",
                )
                .bind(project_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| internal("failed to list project card templates", e))?;

                templates.extend(project_rows.iter().map(card_template_from_row));
            }
        }

        Ok(Response::new(ListCardTemplatesResponse { templates }))
    }

    async fn get_card_template(
        &self,
        request: Request<GetCardTemplateRequest>,
    ) -> Result<Response<GetCardTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let row = sqlx::query(
            "SELECT id, project_id, name, description, title, default_description, \
             label_names, checklist_items, is_global, created_at, updated_at \
             FROM card_templates WHERE id = $1",
        )
        .bind(template_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch card template", e))?
        .ok_or_else(|| Status::not_found("template not found"))?;

        let is_global: bool = row.get("is_global");
        if !is_global {
            let project_id: Option<Id> = row.get("project_id");
            if let Some(pid) = project_id {
                check_project_permission(&self.keto, pid, "view", &subject).await?;
            }
        }

        Ok(Response::new(GetCardTemplateResponse {
            template: Some(card_template_from_row(&row)),
        }))
    }

    async fn create_card_template(
        &self,
        request: Request<CreateCardTemplateRequest>,
    ) -> Result<Response<CreateCardTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        if req.project_id.is_empty() {
            return Err(Status::permission_denied(
                "global templates cannot be created via API",
            ));
        }

        let project_id = parse_id(&req.project_id, "project_id")?;
        check_project_permission(&self.keto, project_id, "manage", &subject).await?;

        let checklist = checklist_to_json(&req.checklist_items);
        let template_id = Id::new();

        let row = sqlx::query(
            r#"
            INSERT INTO card_templates (
                id, project_id, name, description, title, default_description,
                label_names, checklist_items, is_global, created_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false, $9)
            RETURNING id, project_id, name, description, title, default_description,
                      label_names, checklist_items, is_global, created_at, updated_at
            "#,
        )
        .bind(template_id)
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
            template: Some(card_template_from_row(&row)),
        }))
    }

    async fn update_card_template(
        &self,
        request: Request<UpdateCardTemplateRequest>,
    ) -> Result<Response<UpdateCardTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing =
            sqlx::query("SELECT project_id, is_global FROM card_templates WHERE id = $1")
                .bind(template_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card template for update", e))?
                .ok_or_else(|| Status::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(Status::permission_denied(
                "global templates cannot be modified",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&self.keto, pid, "manage", &subject).await?;
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
            WHERE id = $1
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
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to update card template", e))?;

        Ok(Response::new(UpdateCardTemplateResponse {
            template: Some(card_template_from_row(&row)),
        }))
    }

    async fn delete_card_template(
        &self,
        request: Request<DeleteCardTemplateRequest>,
    ) -> Result<Response<DeleteCardTemplateResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let template_id = parse_id(&req.template_id, "template_id")?;

        let existing =
            sqlx::query("SELECT project_id, is_global FROM card_templates WHERE id = $1")
                .bind(template_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card template for delete", e))?
                .ok_or_else(|| Status::not_found("template not found"))?;

        let is_global: bool = existing.get("is_global");
        if is_global {
            return Err(Status::permission_denied(
                "global templates cannot be deleted",
            ));
        }

        let project_id: Option<Id> = existing.get("project_id");
        if let Some(pid) = project_id {
            check_project_permission(&self.keto, pid, "manage", &subject).await?;
        }

        let result = sqlx::query("DELETE FROM card_templates WHERE id = $1")
            .bind(template_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete card template", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("template not found"));
        }

        Ok(Response::new(DeleteCardTemplateResponse {}))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::containers;

    async fn make_service() -> TemplatesServiceImpl {
        let infra = containers::setup().await;
        TemplatesServiceImpl {
            pool: infra.pool,
            keto: Arc::clone(&infra.keto),
        }
    }

    /// Build a request with an authenticated subject in its extensions.
    fn authed_request<T>(body: T, subject: &str) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut()
            .insert(AuthContext::authenticated(subject, None));
        req
    }

    /// Create a minimal project and grant the test subject manage and view on it.
    async fn create_test_project(pool: &PgPool, keto: &KetoClient, subject: &str) -> Id {
        let project_id = Id::new();
        let slug = format!("tp-{}", &project_id.to_string()[18..26]);

        sqlx::query(
            "INSERT INTO projects (id, name, slug, description, owner_id) VALUES ($1, $2, $3, '', $4)",
        )
        .bind(project_id)
        .bind(format!("Test Project {project_id}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");

        keto.grant_with_retry(KETO_NS_PROJECT, &project_id.to_string(), "manage", subject)
            .await
            .expect("grant manage failed");
        keto.grant_with_retry(KETO_NS_PROJECT, &project_id.to_string(), "view", subject)
            .await
            .expect("grant view failed");

        project_id
    }

    async fn cleanup_project(pool: &PgPool, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    async fn cleanup_template(pool: &PgPool, table: &str, template_id: Id) {
        let _ = sqlx::query(&format!("DELETE FROM {table} WHERE id = $1"))
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
            .list_templates(authed_request(
                ListTemplatesRequest {
                    project_id: String::new(),
                },
                &subject,
            ))
            .await
            .expect("list_templates failed")
            .into_inner();

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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_template(authed_request(
                CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Sprint Retro".to_string(),
                    description: "Retro columns".to_string(),
                    columns: vec![
                        TemplateColumn {
                            title: "What went well".to_string(),
                            position: 0,
                            accent: "green".to_string(),
                        },
                        TemplateColumn {
                            title: "What to improve".to_string(),
                            position: 1,
                            accent: "amber".to_string(),
                        },
                    ],
                },
                &subject,
            ))
            .await
            .expect("create_template failed")
            .into_inner()
            .template
            .expect("template missing");

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Sprint Retro");
        assert_eq!(created.project_id, project_id.to_string());
        assert!(!created.is_global);
        assert_eq!(created.columns.len(), 2);
        assert_eq!(created.columns[0].title, "What went well");

        let fetched = svc
            .get_template(authed_request(
                GetTemplateRequest {
                    template_id: created.id.clone(),
                },
                &subject,
            ))
            .await
            .expect("get_template failed")
            .into_inner()
            .template
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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_template(authed_request(
                CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Project Template".to_string(),
                    description: String::new(),
                    columns: vec![TemplateColumn {
                        title: "Only".to_string(),
                        position: 0,
                        accent: "blue".to_string(),
                    }],
                },
                &subject,
            ))
            .await
            .expect("create_template failed")
            .into_inner()
            .template
            .expect("template missing");

        let list_all = svc
            .list_templates(authed_request(
                ListTemplatesRequest {
                    project_id: project_id.to_string(),
                },
                &subject,
            ))
            .await
            .expect("list_templates failed")
            .into_inner();

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

        let list_global = svc
            .list_templates(authed_request(
                ListTemplatesRequest {
                    project_id: String::new(),
                },
                &subject,
            ))
            .await
            .expect("list_templates failed")
            .into_inner();

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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_template(authed_request(
                CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Original".to_string(),
                    description: "Original desc".to_string(),
                    columns: vec![TemplateColumn {
                        title: "Old".to_string(),
                        position: 0,
                        accent: "red".to_string(),
                    }],
                },
                &subject,
            ))
            .await
            .expect("create_template failed")
            .into_inner()
            .template
            .expect("template missing");

        let updated = svc
            .update_template(authed_request(
                UpdateTemplateRequest {
                    template_id: created.id.clone(),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["name".to_string(), "columns".to_string()],
                    }),
                    name: "Updated".to_string(),
                    description: String::new(),
                    columns: vec![TemplateColumn {
                        title: "New".to_string(),
                        position: 0,
                        accent: "green".to_string(),
                    }],
                },
                &subject,
            ))
            .await
            .expect("update_template failed")
            .into_inner()
            .template
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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_template(authed_request(
                CreateTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "To Delete".to_string(),
                    description: String::new(),
                    columns: vec![],
                },
                &subject,
            ))
            .await
            .expect("create_template failed")
            .into_inner()
            .template
            .expect("template missing");

        svc.delete_template(authed_request(
            DeleteTemplateRequest {
                template_id: created.id.clone(),
            },
            &subject,
        ))
        .await
        .expect("delete_template failed");

        let result = svc
            .get_template(authed_request(
                GetTemplateRequest {
                    template_id: created.id.clone(),
                },
                &subject,
            ))
            .await;
        assert!(result.is_err(), "deleted template should not be found");

        cleanup_project(&svc.pool, project_id).await;
    }

    // ── Card template tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_card_template() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_card_template(authed_request(
                CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Bug Card".to_string(),
                    description: "Bug template".to_string(),
                    title: "[BUG] ".to_string(),
                    default_description: "Steps to reproduce:\n".to_string(),
                    label_names: vec!["bug".to_string(), "triage".to_string()],
                    checklist_items: vec![
                        TemplateChecklistItem {
                            title: "Reproduce".to_string(),
                        },
                        TemplateChecklistItem {
                            title: "Fix".to_string(),
                        },
                    ],
                },
                &subject,
            ))
            .await
            .expect("create_card_template failed")
            .into_inner()
            .template
            .expect("template missing");

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Bug Card");
        assert_eq!(created.title, "[BUG] ");
        assert_eq!(created.label_names, vec!["bug", "triage"]);
        assert_eq!(created.checklist_items.len(), 2);

        let fetched = svc
            .get_card_template(authed_request(
                GetCardTemplateRequest {
                    template_id: created.id.clone(),
                },
                &subject,
            ))
            .await
            .expect("get_card_template failed")
            .into_inner()
            .template
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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_card_template(authed_request(
                CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Task Card".to_string(),
                    description: String::new(),
                    title: String::new(),
                    default_description: String::new(),
                    label_names: vec![],
                    checklist_items: vec![],
                },
                &subject,
            ))
            .await
            .expect("create_card_template failed")
            .into_inner()
            .template
            .expect("template missing");

        let list = svc
            .list_card_templates(authed_request(
                ListCardTemplatesRequest {
                    project_id: project_id.to_string(),
                },
                &subject,
            ))
            .await
            .expect("list_card_templates failed")
            .into_inner();

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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_card_template(authed_request(
                CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "Original".to_string(),
                    description: "Original desc".to_string(),
                    title: "Old title".to_string(),
                    default_description: "Old body".to_string(),
                    label_names: vec!["old".to_string()],
                    checklist_items: vec![],
                },
                &subject,
            ))
            .await
            .expect("create_card_template failed")
            .into_inner()
            .template
            .expect("template missing");

        let updated = svc
            .update_card_template(authed_request(
                UpdateCardTemplateRequest {
                    template_id: created.id.clone(),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["title".to_string(), "label_names".to_string()],
                    }),
                    name: String::new(),
                    description: String::new(),
                    title: "New title".to_string(),
                    default_description: String::new(),
                    label_names: vec!["new".to_string()],
                    checklist_items: vec![],
                },
                &subject,
            ))
            .await
            .expect("update_card_template failed")
            .into_inner()
            .template
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
        let project_id = create_test_project(&svc.pool, &svc.keto, &subject).await;

        let created = svc
            .create_card_template(authed_request(
                CreateCardTemplateRequest {
                    project_id: project_id.to_string(),
                    name: "To Delete".to_string(),
                    description: String::new(),
                    title: String::new(),
                    default_description: String::new(),
                    label_names: vec![],
                    checklist_items: vec![],
                },
                &subject,
            ))
            .await
            .expect("create_card_template failed")
            .into_inner()
            .template
            .expect("template missing");

        svc.delete_card_template(authed_request(
            DeleteCardTemplateRequest {
                template_id: created.id.clone(),
            },
            &subject,
        ))
        .await
        .expect("delete_card_template failed");

        let result = svc
            .get_card_template(authed_request(
                GetCardTemplateRequest {
                    template_id: created.id.clone(),
                },
                &subject,
            ))
            .await;
        assert!(result.is_err(), "deleted card template should not be found");

        cleanup_project(&svc.pool, project_id).await;
    }
}
