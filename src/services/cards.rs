//! CardService stub — Stage 3 fills in real logic.

use tonic::{Request, Response, Status};

use crate::pb::card_service_server::CardService;
use crate::pb::{
    AddChecklistItemRequest, AddCommentRequest, AssignCardRequest,
    BatchGetCardsRequest, BatchGetCardsResponse, BulkUpdateCardLabelsRequest,
    BulkUpdateCardLabelsResponse, Card, Comment, CreateCardRequest,
    DeleteCardRequest, DeleteCommentRequest, EditCommentRequest,
    GetCardRequest, ListCardsByBoardRequest, ListCardsByBoardResponse,
    ListCommentsRequest, ListCommentsResponse, MoveCardRequest,
    RemoveChecklistItemRequest, UnassignCardRequest, UpdateCardRequest,
    UpdateChecklistItemRequest,
};

pub struct CardServiceImpl;

#[tonic::async_trait]
impl CardService for CardServiceImpl {
    async fn get_card(
        &self,
        _request: Request<GetCardRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn batch_get_cards(
        &self,
        _request: Request<BatchGetCardsRequest>,
    ) -> Result<Response<BatchGetCardsResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn list_cards_by_board(
        &self,
        _request: Request<ListCardsByBoardRequest>,
    ) -> Result<Response<ListCardsByBoardResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn create_card(
        &self,
        _request: Request<CreateCardRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn update_card(
        &self,
        _request: Request<UpdateCardRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn move_card(
        &self,
        _request: Request<MoveCardRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn delete_card(
        &self,
        _request: Request<DeleteCardRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn bulk_update_card_labels(
        &self,
        _request: Request<BulkUpdateCardLabelsRequest>,
    ) -> Result<Response<BulkUpdateCardLabelsResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn assign_card(
        &self,
        _request: Request<AssignCardRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn unassign_card(
        &self,
        _request: Request<UnassignCardRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn add_checklist_item(
        &self,
        _request: Request<AddChecklistItemRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn update_checklist_item(
        &self,
        _request: Request<UpdateChecklistItemRequest>,
    ) -> Result<Response<Card>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn remove_checklist_item(
        &self,
        _request: Request<RemoveChecklistItemRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn add_comment(
        &self,
        _request: Request<AddCommentRequest>,
    ) -> Result<Response<Comment>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn edit_comment(
        &self,
        _request: Request<EditCommentRequest>,
    ) -> Result<Response<Comment>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn delete_comment(
        &self,
        _request: Request<DeleteCommentRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn list_comments(
        &self,
        _request: Request<ListCommentsRequest>,
    ) -> Result<Response<ListCommentsResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
