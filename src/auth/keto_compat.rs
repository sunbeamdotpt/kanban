//! Compatibility helpers for KetoClient methods missing from sunbeam-g2v 0.1.1.
//!
//! The crates.io release of sunbeam-g2v does not include `list_relation_tuples`
//! or `delete_relation_tuples`, so we implement them here using the generated
//! gRPC clients from our local proto build.

use crate::keto_proto::{
    ListRelationTuplesRequest, ListRelationTuplesResponse, RelationQuery, Subject,
    read_service_client::ReadServiceClient,
};
use sunbeam_g2v::error::{ServiceError, ServiceResult};
use sunbeam_g2v::middleware::auth::keto::KetoClient;
use tonic::transport::Channel;

use crate::auth::keto_retry::retry;

/// List relation tuples from Keto ReadService.
pub async fn list_relation_tuples(
    client: &KetoClient,
    namespace: &str,
    relation: Option<&str>,
    subject: Option<&str>,
    page_size: i32,
    page_token: &str,
) -> ServiceResult<(Vec<crate::keto_proto::RelationTuple>, String)> {
    let config = client.config();
    let channel = Channel::from_shared(config.grpc_endpoint.clone())
        .map_err(|e| ServiceError::Internal(format!("invalid keto endpoint: {e}")))?
        .connect_lazy();

    let mut grpc = ReadServiceClient::new(channel);

    let req = ListRelationTuplesRequest {
        relation_query: Some(RelationQuery {
            namespace: Some(namespace.to_string()),
            object: None,
            relation: relation.map(|s| s.to_string()),
            subject: subject.map(|s| Subject {
                r#ref: Some(crate::keto_proto::subject::Ref::Id(s.to_string())),
            }),
        }),
        page_size,
        page_token: page_token.to_string(),
        ..Default::default()
    };

    let resp: ListRelationTuplesResponse = grpc
        .list_relation_tuples(tonic::Request::new(req))
        .await
        .map_err(|status| {
            ServiceError::Internal(format!("keto list_relation_tuples failed: {status}"))
        })?
        .into_inner();

    Ok((resp.relation_tuples, resp.next_page_token))
}

/// Delete relation tuples from Keto WriteService.
pub async fn delete_relation_tuples(
    client: &KetoClient,
    namespace: &str,
    relation: Option<&str>,
    subject: Option<&str>,
) -> ServiceResult<()> {
    use sunbeam_g2v::middleware::auth::keto_proto::{
        DeleteRelationTuplesRequest, RelationQuery as SgvRelationQuery, Subject as SgvSubject,
        write_service_client::WriteServiceClient,
    };

    let config = client.config();
    let req = DeleteRelationTuplesRequest {
        relation_query: Some(SgvRelationQuery {
            namespace: Some(namespace.to_string()),
            object: None,
            relation: relation.map(|s| s.to_string()),
            subject: subject.map(|s| SgvSubject {
                r#ref: Some(sunbeam_g2v::middleware::auth::keto_proto::subject::Ref::Id(
                    s.to_string(),
                )),
            }),
        }),
        ..Default::default()
    };

    retry(|| async {
        let channel = Channel::from_shared(config.write_grpc_endpoint.clone())
            .map_err(|e| ServiceError::Internal(format!("invalid keto write endpoint: {e}")))?
            .connect_lazy();
        let mut grpc = WriteServiceClient::new(channel);
        grpc.delete_relation_tuples(tonic::Request::new(req.clone()))
            .await
            .map_err(|status| {
                ServiceError::Internal(format!("keto delete_relation_tuples failed: {status}"))
            })
    })
    .await?;

    Ok(())
}
