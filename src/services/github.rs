// SPDX-License-Identifier: AGPL-3.0-or-later
//! Stub implementation of the GitHub issue link service.

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

#[cfg(test)]
mod tests {
    use super::*;

    fn unimplemented_status<T>(result: Result<Response<T>, Status>) -> String {
        match result {
            Err(e) => e.message().to_string(),
            Ok(_) => panic!("expected unimplemented status"),
        }
    }

    #[tokio::test]
    async fn link_issue_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let status = unimplemented_status(
            svc.link_issue(Request::new(LinkGitHubIssueRequest::default()))
                .await,
        );
        assert!(status.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn unlink_issue_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let status = unimplemented_status(
            svc.unlink_issue(Request::new(UnlinkGitHubIssueRequest::default()))
                .await,
        );
        assert!(status.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn list_links_by_card_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let status = unimplemented_status(
            svc.list_links_by_card(Request::new(ListGitHubLinksByCardRequest::default()))
                .await,
        );
        assert!(status.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn search_github_issues_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let status = unimplemented_status(
            svc.search_github_issues(Request::new(SearchGithubIssuesRequest::default()))
                .await,
        );
        assert!(status.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn resync_link_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let status = unimplemented_status(
            svc.resync_link(Request::new(ResyncGitHubLinkRequest::default()))
                .await,
        );
        assert!(status.contains("Stage 3 stub"));
    }
}
