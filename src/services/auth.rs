//! AuthService stub — Stage 3 fills in real logic.

use tonic::{Request, Response, Status};

use crate::pb::auth_service_server::AuthService;
use crate::pb::{SignalLogoutResponse, WhoAmIResponse};

pub struct AuthServiceImpl;

#[tonic::async_trait]
impl AuthService for AuthServiceImpl {
    async fn who_am_i(
        &self,
        _request: Request<()>,
    ) -> Result<Response<WhoAmIResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn signal_logout(
        &self,
        _request: Request<()>,
    ) -> Result<Response<SignalLogoutResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
