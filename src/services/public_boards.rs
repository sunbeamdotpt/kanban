// SPDX-License-Identifier: AGPL-3.0-or-later
//! Public, unauthenticated read access to boards.
//!
//! Mounted on its own Axum router without `JwtLayer` or `permission_dispatch`, so
//! every handler must confirm the requested board's visibility is exactly
//! `public` before returning anything.
//!
//! # Multitenancy note
//!
//! Public boards are intentionally scoped by their unique board/project IDs.
//! The RPC requests do not carry a `tenant_id`, and the service does not apply
//! a tenant filter. This preserves the existing public-by-URL semantics: anyone
//! who knows a public board's UUID can read it. Cross-tenant isolation relies
//! on the fact that board and project IDs are generated as random ULIDs and do
//! not collide across tenants.

use crate::id::Id;
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use sqlx::Row;

use crate::cpb::sunbeam::kanban::v1::{
    GetPublicBoardRequest, GetPublicBoardResponse, ListPublicBoardsRequest,
    ListPublicBoardsResponse, PublicBoardService,
};
use crate::services::boards::{board_from_row, fetch_cards_count, fetch_columns_count};
use crate::services::visibility::is_public;

pub struct PublicBoardServiceImpl {
    pub pool: sqlx::PgPool,
}

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    tracing::error!(error = %err, "{msg}");
    ConnectError::internal(msg)
}

#[allow(refining_impl_trait)]
impl PublicBoardService for PublicBoardServiceImpl {
    async fn get_public_board(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GetPublicBoardRequest>,
    ) -> ServiceResult<GetPublicBoardResponse> {
        let req = request.to_owned_message();
        let board_id = req
            .board_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let row = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, visibility, created_at, updated_at \
             FROM boards WHERE id = $1",
        )
        .bind(board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board", e))?
        .ok_or_else(|| ConnectError::not_found("board not found"))?;

        let visibility: String = row.get("visibility");
        if !is_public(&visibility) {
            // Treat non-public boards as not found from the public endpoint.
            return Err(ConnectError::not_found("board not found"));
        }

        let columns_count = fetch_columns_count(&self.pool, board_id, None).await;
        let cards_count = fetch_cards_count(&self.pool, board_id, None).await;
        let board = board_from_row(&row, columns_count, cards_count);

        Ok(Response::new(GetPublicBoardResponse {
            board: Some(board).into(),
            ..Default::default()
        }))
    }

    async fn list_public_boards(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListPublicBoardsRequest>,
    ) -> ServiceResult<ListPublicBoardsResponse> {
        let req = request.to_owned_message();
        let project_id = req
            .project_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let rows = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, visibility, created_at, updated_at \
             FROM boards WHERE project_id = $1 AND visibility = 'public' ORDER BY created_at ASC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list public boards", e))?;

        let mut boards = Vec::with_capacity(rows.len());
        for row in &rows {
            let bid: Id = row.get("id");
            let columns_count = fetch_columns_count(&self.pool, bid, None).await;
            let cards_count = fetch_cards_count(&self.pool, bid, None).await;
            boards.push(board_from_row(row, columns_count, cards_count));
        }

        Ok(Response::new(ListPublicBoardsResponse {
            boards,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Id;

    use crate::cpb::sunbeam::kanban::v1::BoardVisibility;
    use crate::services::visibility::DEFAULT_VISIBILITY;
    use crate::test_support::{connect_request, containers};

    fn make_service(infra: &containers::TestInfra) -> PublicBoardServiceImpl {
        PublicBoardServiceImpl {
            pool: infra.pool.clone(),
        }
    }

    async fn create_test_project(pool: &sqlx::PgPool, subject: &str) -> Id {
        let pid = Id::new();
        let slug = format!("tp-{}", &pid.to_string()[18..26]);
        let tenant_id = crate::test_support::test_tenant_id();
        sqlx::query(
            "INSERT INTO projects (id, tenant_id, name, slug, description, owner_id) VALUES ($1, $2, $3, $4, '', $5)",
        )
        .bind(pid)
        .bind(tenant_id)
        .bind(format!("Test Project {pid}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");
        pid
    }

    async fn insert_board(pool: &sqlx::PgPool, project_id: Id, name: &str, visibility: &str) -> Id {
        let board_id = Id::new();
        let slug = format!("bd-{}", &board_id.to_string()[18..26]);
        let tenant_id = crate::test_support::test_tenant_id();
        sqlx::query(
            "INSERT INTO boards (id, tenant_id, project_id, name, slug, description, icon, visibility) \
             VALUES ($1, $2, $3, $4, $5, '', '', $6)",
        )
        .bind(board_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(name)
        .bind(&slug)
        .bind(visibility)
        .execute(pool)
        .await
        .expect("failed to insert test board");
        board_id
    }

    #[tokio::test]
    async fn get_public_board_returns_public_board_without_auth() {
        let infra = containers::setup().await;
        let svc = make_service(&infra);
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let board_id = insert_board(&infra.pool, project_id, "Public Board", "public").await;

        let resp = svc
            .get_public_board(
                // No auth context — these RPCs skip the auth middleware entirely.
                RequestContext::default(),
                connect_request(&GetPublicBoardRequest {
                    board_id: board_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get_public_board failed")
            .body;

        let board = resp.board.into_option().expect("board missing");
        assert_eq!(board.id, board_id.to_string());
        assert_eq!(board.visibility, BoardVisibility::Public);
    }

    #[tokio::test]
    async fn get_public_board_hides_internal_and_private_boards() {
        let infra = containers::setup().await;
        let svc = make_service(&infra);
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let internal_id = insert_board(&infra.pool, project_id, "Internal Board", "internal").await;
        let private_id =
            insert_board(&infra.pool, project_id, "Private Board", DEFAULT_VISIBILITY).await;

        for id in &[internal_id, private_id] {
            let result = svc
                .get_public_board(
                    RequestContext::default(),
                    connect_request(&GetPublicBoardRequest {
                        board_id: id.to_string(),
                        ..Default::default()
                    }),
                )
                .await;
            assert!(
                result.is_err(),
                "public endpoint must hide non-public board {id}"
            );
            assert_eq!(
                result.unwrap_err().code,
                connectrpc::ErrorCode::NotFound,
                "non-public board must return NotFound"
            );
        }
    }

    #[tokio::test]
    async fn list_public_boards_returns_only_public_boards() {
        let infra = containers::setup().await;
        let svc = make_service(&infra);
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&infra.pool, &subject).await;

        let public_id = insert_board(&infra.pool, project_id, "Public Board", "public").await;
        let _internal_id =
            insert_board(&infra.pool, project_id, "Internal Board", "internal").await;
        let _private_id =
            insert_board(&infra.pool, project_id, "Private Board", DEFAULT_VISIBILITY).await;

        let resp = svc
            .list_public_boards(
                RequestContext::default(),
                connect_request(&ListPublicBoardsRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_public_boards failed")
            .body;

        assert_eq!(resp.boards.len(), 1);
        assert_eq!(resp.boards[0].id, public_id.to_string());
        assert_eq!(resp.boards[0].visibility, BoardVisibility::Public);
    }
}
