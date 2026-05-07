//! BoardService stub — Stage 3 fills in real logic.

use std::pin::Pin;

use tonic::{Request, Response, Status};
use tokio_stream::Stream;

use crate::pb::board_service_server::BoardService;
use crate::pb::{
    AddColumnRequest, Board, BoardDetail, BoardEvent, Column,
    CreateBoardRequest, DeleteBoardRequest, GetBoardRequest,
    ListBoardsRequest, ListBoardsResponse, MoveColumnRequest,
    MoveColumnResponse, RemoveColumnRequest, SubscribeBoardRequest,
    UpdateBoardRequest, UpdateColumnRequest,
};

pub struct BoardServiceImpl;

type BoardEventStream = Pin<Box<dyn Stream<Item = Result<BoardEvent, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl BoardService for BoardServiceImpl {
    async fn list_boards(
        &self,
        _request: Request<ListBoardsRequest>,
    ) -> Result<Response<ListBoardsResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn get_board(
        &self,
        _request: Request<GetBoardRequest>,
    ) -> Result<Response<BoardDetail>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn create_board(
        &self,
        _request: Request<CreateBoardRequest>,
    ) -> Result<Response<Board>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn update_board(
        &self,
        _request: Request<UpdateBoardRequest>,
    ) -> Result<Response<Board>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn delete_board(
        &self,
        _request: Request<DeleteBoardRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn add_column(
        &self,
        _request: Request<AddColumnRequest>,
    ) -> Result<Response<Column>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn update_column(
        &self,
        _request: Request<UpdateColumnRequest>,
    ) -> Result<Response<Column>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn remove_column(
        &self,
        _request: Request<RemoveColumnRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn move_column(
        &self,
        _request: Request<MoveColumnRequest>,
    ) -> Result<Response<MoveColumnResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    type SubscribeBoardStream = BoardEventStream;

    async fn subscribe_board(
        &self,
        _request: Request<SubscribeBoardRequest>,
    ) -> Result<Response<Self::SubscribeBoardStream>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
