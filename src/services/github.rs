// SPDX-License-Identifier: AGPL-3.0-or-later
//! Stub implementation of the GitHub issue link service.

use connectrpc::{ConnectError, RequestContext, ServiceRequest, ServiceResult};

use crate::cpb::sunbeam::kanban::v1::{
    GithubLinkService, LinkIssueRequest, LinkIssueResponse, ListLinksByCardRequest,
    ListLinksByCardResponse, ResyncLinkRequest, ResyncLinkResponse, SearchGithubIssuesRequest,
    SearchGithubIssuesResponse, UnlinkIssueRequest, UnlinkIssueResponse,
};

pub struct GitHubServiceImpl;

#[allow(refining_impl_trait)]
impl GithubLinkService for GitHubServiceImpl {
    async fn link_issue(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, LinkIssueRequest>,
    ) -> ServiceResult<LinkIssueResponse> {
        Err(ConnectError::unimplemented("Stage 3 stub"))
    }

    async fn unlink_issue(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, UnlinkIssueRequest>,
    ) -> ServiceResult<UnlinkIssueResponse> {
        Err(ConnectError::unimplemented("Stage 3 stub"))
    }

    async fn list_links_by_card(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, ListLinksByCardRequest>,
    ) -> ServiceResult<ListLinksByCardResponse> {
        Err(ConnectError::unimplemented("Stage 3 stub"))
    }

    async fn search_github_issues(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, SearchGithubIssuesRequest>,
    ) -> ServiceResult<SearchGithubIssuesResponse> {
        Err(ConnectError::unimplemented("Stage 3 stub"))
    }

    async fn resync_link(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, ResyncLinkRequest>,
    ) -> ServiceResult<ResyncLinkResponse> {
        Err(ConnectError::unimplemented("Stage 3 stub"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::connect_request;

    fn unimplemented_message<T>(result: ServiceResult<T>) -> String {
        match result {
            Err(e) => e.message.unwrap_or_default(),
            Ok(_) => panic!("expected unimplemented status"),
        }
    }

    #[tokio::test]
    async fn link_issue_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let msg = unimplemented_message(
            svc.link_issue(
                RequestContext::default(),
                connect_request(&LinkIssueRequest::default()),
            )
            .await,
        );
        assert!(msg.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn unlink_issue_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let msg = unimplemented_message(
            svc.unlink_issue(
                RequestContext::default(),
                connect_request(&UnlinkIssueRequest::default()),
            )
            .await,
        );
        assert!(msg.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn list_links_by_card_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let msg = unimplemented_message(
            svc.list_links_by_card(
                RequestContext::default(),
                connect_request(&ListLinksByCardRequest::default()),
            )
            .await,
        );
        assert!(msg.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn search_github_issues_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let msg = unimplemented_message(
            svc.search_github_issues(
                RequestContext::default(),
                connect_request(&SearchGithubIssuesRequest::default()),
            )
            .await,
        );
        assert!(msg.contains("Stage 3 stub"));
    }

    #[tokio::test]
    async fn resync_link_returns_unimplemented() {
        let svc = GitHubServiceImpl;
        let msg = unimplemented_message(
            svc.resync_link(
                RequestContext::default(),
                connect_request(&ResyncLinkRequest::default()),
            )
            .await,
        );
        assert!(msg.contains("Stage 3 stub"));
    }
}
