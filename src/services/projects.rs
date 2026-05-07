//! ProjectService stub — Stage 3 fills in real logic.

use std::pin::Pin;

use tonic::{Request, Response, Status};
use tokio_stream::Stream;

use crate::pb::project_service_server::ProjectService;
use crate::pb::{
    AddMemberRequest, CreateProjectRequest, DeleteProjectRequest,
    GetProjectRequest, ListMembersRequest, ListMembersResponse,
    ListProjectsRequest, ListProjectsResponse, Project, ProjectEvent,
    RemoveMemberRequest, SubscribeProjectRequest, UpdateProjectRequest,
};

pub struct ProjectServiceImpl;

type ProjectEventStream =
    Pin<Box<dyn Stream<Item = Result<ProjectEvent, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    async fn list_projects(
        &self,
        _request: Request<ListProjectsRequest>,
    ) -> Result<Response<ListProjectsResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn get_project(
        &self,
        _request: Request<GetProjectRequest>,
    ) -> Result<Response<Project>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn create_project(
        &self,
        _request: Request<CreateProjectRequest>,
    ) -> Result<Response<Project>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn update_project(
        &self,
        _request: Request<UpdateProjectRequest>,
    ) -> Result<Response<Project>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn delete_project(
        &self,
        _request: Request<DeleteProjectRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn list_members(
        &self,
        _request: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn add_member(
        &self,
        _request: Request<AddMemberRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn remove_member(
        &self,
        _request: Request<RemoveMemberRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    type SubscribeProjectStream = ProjectEventStream;

    async fn subscribe_project(
        &self,
        _request: Request<SubscribeProjectRequest>,
    ) -> Result<Response<Self::SubscribeProjectStream>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
