//! PublicBoardService — unauthenticated read-only access to public boards.
//!
//! This service is mounted on a separate Axum router that does not run the
//! `JwtLayer` or `keto_dispatch` middleware. Every handler must independently
//! verify that the requested board's visibility is exactly `public` before
//! returning it.

use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::pb::public_board_service_server::PublicBoardService;
use crate::pb::{GetPublicBoardRequest, ListBoardsResponse, ListPublicBoardsRequest};
use crate::services::boards::{board_from_row, fetch_cards_count, fetch_columns_count};
use crate::services::visibility::is_public;

pub struct PublicBoardServiceImpl {
    pub pool: sqlx::PgPool,
}

fn internal(msg: &str, err: impl std::fmt::Display) -> Status {
    tracing::error!(error = %err, "{msg}");
    Status::internal(msg)
}

#[tonic::async_trait]
impl PublicBoardService for PublicBoardServiceImpl {
    async fn get_public_board(
        &self,
        request: Request<GetPublicBoardRequest>,
    ) -> Result<Response<crate::pb::Board>, Status> {
        let req = request.into_inner();
        let board_id = Uuid::parse_str(&req.board_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let row = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, visibility, created_at, updated_at \
             FROM boards WHERE id = $1",
        )
        .bind(board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board", e))?
        .ok_or_else(|| Status::not_found("board not found"))?;

        let visibility: String = row.get("visibility");
        if !is_public(&visibility) {
            // Treat non-public boards as not found from the public endpoint.
            return Err(Status::not_found("board not found"));
        }

        let columns_count = fetch_columns_count(&self.pool, board_id).await;
        let cards_count = fetch_cards_count(&self.pool, board_id).await;
        let board = board_from_row(&row, columns_count, cards_count);

        Ok(Response::new(board))
    }

    async fn list_public_boards(
        &self,
        request: Request<ListPublicBoardsRequest>,
    ) -> Result<Response<ListBoardsResponse>, Status> {
        let req = request.into_inner();
        let project_id = Uuid::parse_str(&req.project_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

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
            let bid: Uuid = row.get("id");
            let columns_count = fetch_columns_count(&self.pool, bid).await;
            let cards_count = fetch_cards_count(&self.pool, bid).await;
            boards.push(board_from_row(row, columns_count, cards_count));
        }

        Ok(Response::new(ListBoardsResponse { boards }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Request;
    use uuid::Uuid;

    use crate::pb::public_board_service_server::PublicBoardService;
    use crate::services::visibility::DEFAULT_VISIBILITY;
    use crate::test_support::containers;

    fn make_service(infra: &containers::TestInfra) -> PublicBoardServiceImpl {
        PublicBoardServiceImpl {
            pool: infra.pool.clone(),
        }
    }

    fn public_request<T>(body: T) -> Request<T> {
        // No auth context — these RPCs skip the auth middleware entirely.
        Request::new(body)
    }

    async fn create_test_project(pool: &sqlx::PgPool, subject: &str) -> Uuid {
        let pid = Uuid::new_v4();
        let slug = format!("tp-{}", &pid.to_string()[..8]);
        sqlx::query(
            "INSERT INTO projects (id, name, slug, description, owner_id) VALUES ($1, $2, $3, '', $4)",
        )
        .bind(pid)
        .bind(format!("Test Project {pid}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");
        pid
    }

    async fn insert_board(
        pool: &sqlx::PgPool,
        project_id: Uuid,
        name: &str,
        visibility: &str,
    ) -> Uuid {
        let board_id = Uuid::new_v4();
        let slug = format!("bd-{}", &board_id.to_string()[..8]);
        sqlx::query(
            "INSERT INTO boards (id, project_id, name, slug, description, icon, visibility) \
             VALUES ($1, $2, $3, $4, '', '', $5)",
        )
        .bind(board_id)
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
        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let board_id = insert_board(&infra.pool, project_id, "Public Board", "public").await;

        let resp = svc
            .get_public_board(public_request(GetPublicBoardRequest {
                board_id: board_id.to_string(),
            }))
            .await
            .expect("get_public_board failed")
            .into_inner();

        assert_eq!(resp.id, board_id.to_string());
        assert_eq!(resp.visibility, 3); // PUBLIC
    }

    #[tokio::test]
    async fn get_public_board_hides_internal_and_private_boards() {
        let infra = containers::setup().await;
        let svc = make_service(&infra);
        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let internal_id = insert_board(&infra.pool, project_id, "Internal Board", "internal").await;
        let private_id =
            insert_board(&infra.pool, project_id, "Private Board", DEFAULT_VISIBILITY).await;

        for id in &[internal_id, private_id] {
            let result = svc
                .get_public_board(public_request(GetPublicBoardRequest {
                    board_id: id.to_string(),
                }))
                .await;
            assert!(
                result.is_err(),
                "public endpoint must hide non-public board {id}"
            );
            assert_eq!(
                result.unwrap_err().code(),
                tonic::Code::NotFound,
                "non-public board must return NotFound"
            );
        }
    }

    #[tokio::test]
    async fn list_public_boards_returns_only_public_boards() {
        let infra = containers::setup().await;
        let svc = make_service(&infra);
        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;

        let public_id = insert_board(&infra.pool, project_id, "Public Board", "public").await;
        let _internal_id =
            insert_board(&infra.pool, project_id, "Internal Board", "internal").await;
        let _private_id =
            insert_board(&infra.pool, project_id, "Private Board", DEFAULT_VISIBILITY).await;

        let resp = svc
            .list_public_boards(public_request(ListPublicBoardsRequest {
                project_id: project_id.to_string(),
            }))
            .await
            .expect("list_public_boards failed")
            .into_inner();

        assert_eq!(resp.boards.len(), 1);
        assert_eq!(resp.boards[0].id, public_id.to_string());
        assert_eq!(resp.boards[0].visibility, 3); // PUBLIC
    }
}
