//! GithubLinkService stub — Stage 3 fills in real logic.

use tonic::{Request, Response, Status};

use crate::pb::github_link_service_server::GithubLinkService;
use crate::pb::{
    GitHubLinkDetail, LinkGitHubIssueRequest, ListGitHubLinksByCardRequest,
    ListGitHubLinksByCardResponse, ResyncGitHubLinkRequest, SearchGithubIssuesRequest,
    SearchGithubIssuesResponse, UnlinkGitHubIssueRequest,
};

pub struct GitHubServiceImpl;

#[tonic::async_trait]
impl GithubLinkService for GitHubServiceImpl {
    async fn link_issue(
        &self,
        _request: Request<LinkGitHubIssueRequest>,
    ) -> Result<Response<GitHubLinkDetail>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn unlink_issue(
        &self,
        _request: Request<UnlinkGitHubIssueRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn list_links_by_card(
        &self,
        _request: Request<ListGitHubLinksByCardRequest>,
    ) -> Result<Response<ListGitHubLinksByCardResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn search_github_issues(
        &self,
        _request: Request<SearchGithubIssuesRequest>,
    ) -> Result<Response<SearchGithubIssuesResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn resync_link(
        &self,
        _request: Request<ResyncGitHubLinkRequest>,
    ) -> Result<Response<GitHubLinkDetail>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
