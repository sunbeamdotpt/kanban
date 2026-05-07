//! AttachmentService stub — Stage 3 fills in real logic.

use tonic::{Request, Response, Status};

use crate::pb::attachment_service_server::AttachmentService;
use crate::pb::{
    Attachment, ConfirmUploadRequest, DeleteAttachmentRequest,
    ListAttachmentsByCardRequest, ListAttachmentsByCardResponse,
    RequestPresignedDownloadRequest, RequestPresignedDownloadResponse,
    RequestPresignedUploadRequest, RequestPresignedUploadResponse,
};

pub struct AttachmentServiceImpl;

#[tonic::async_trait]
impl AttachmentService for AttachmentServiceImpl {
    async fn request_presigned_upload(
        &self,
        _request: Request<RequestPresignedUploadRequest>,
    ) -> Result<Response<RequestPresignedUploadResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn confirm_upload(
        &self,
        _request: Request<ConfirmUploadRequest>,
    ) -> Result<Response<Attachment>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn request_presigned_download(
        &self,
        _request: Request<RequestPresignedDownloadRequest>,
    ) -> Result<Response<RequestPresignedDownloadResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn delete_attachment(
        &self,
        _request: Request<DeleteAttachmentRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }

    async fn list_attachments_by_card(
        &self,
        _request: Request<ListAttachmentsByCardRequest>,
    ) -> Result<Response<ListAttachmentsByCardResponse>, Status> {
        Err(Status::unimplemented("Stage 3 stub"))
    }
}
