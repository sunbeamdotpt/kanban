//! SearchService stub — Stage 3 fills in real logic.

use tonic::{Request, Response, Status};

use crate::pb::search_service_server::SearchService;
use crate::pb::{SearchCardsRequest, SearchCardsResponse};

pub struct SearchServiceImpl;

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    async fn search_cards(
        &self,
        _request: Request<SearchCardsRequest>,
    ) -> Result<Response<SearchCardsResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
