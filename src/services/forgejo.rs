//! ForgejoLinkService stub — Stage 3 fills in real logic.

use tonic::{Request, Response, Status};

use crate::pb::forgejo_link_service_server::ForgejoLinkService;
use crate::pb::{
    ForgejoLinkDetail, LinkIssueRequest, ListLinksByCardRequest,
    ListLinksByCardResponse, ResyncLinkRequest, SearchForgejoIssuesRequest,
    SearchForgejoIssuesResponse, UnlinkIssueRequest,
};

pub struct ForgejoServiceImpl;

#[tonic::async_trait]
impl ForgejoLinkService for ForgejoServiceImpl {
    async fn link_issue(
        &self,
        _request: Request<LinkIssueRequest>,
    ) -> Result<Response<ForgejoLinkDetail>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn unlink_issue(
        &self,
        _request: Request<UnlinkIssueRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn list_links_by_card(
        &self,
        _request: Request<ListLinksByCardRequest>,
    ) -> Result<Response<ListLinksByCardResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn search_forgejo_issues(
        &self,
        _request: Request<SearchForgejoIssuesRequest>,
    ) -> Result<Response<SearchForgejoIssuesResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn resync_link(
        &self,
        _request: Request<ResyncLinkRequest>,
    ) -> Result<Response<ForgejoLinkDetail>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
