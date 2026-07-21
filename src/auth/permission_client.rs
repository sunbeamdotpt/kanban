// SPDX-License-Identifier: AGPL-3.0-or-later
//! sso-gateway `iam.v1.PermissionService` Connect-RPC client wrapper.
//!
//! All authorization checks and relation tuple mutations go through the
//! sso-gateway public API.
//!
//! The client is built on `sunbeam_g2v::client::{ClientBuilder, ConnectTransport}`
//! so it shares the unified resilience/auth/TLS stack. Per-request user auth is
//! supported by swapping the token provider; service-to-service calls use OAuth2
//! client credentials.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use sunbeam_g2v::client::{ClientBuilder, ConnectTransport, OAuth2ClientCredentials};

use crate::iam_proto::iam::v1::{
    CheckPermissionRequest, CreateRelationTupleRequest, DeleteRelationTupleRequest,
    EnsurePermissionNamespaceRequest, ExpandObjectsRequest, ExpandPermissionsRequest,
    ListRelationTuplesRequest, PageRequest, PermissionServiceClient,
};

/// Header that carries the target tenant on trusted service-to-service calls.
///
/// Matches the gateway's `TENANT_ID_HEADER`. The gateway only honors this from
/// authenticated service clients holding `permission:admin`; end-user tokens
/// always resolve their tenant from introspection and must never send it.
pub const TENANT_HEADER: &str = "x-tenant-id";

/// Errors returned by the permission backend.
#[derive(Debug, thiserror::Error)]
pub enum PermissionError {
    /// The permission backend returned an error or was unreachable.
    #[error("permission backend error: {0}")]
    Backend(String),
    /// The caller is not authorized.
    #[error("permission denied")]
    Denied,
}

/// Configuration for building a [`PermissionClient`].
#[derive(Clone, Debug)]
pub struct PermissionClientConfig {
    /// Base URL of the sso-gateway (e.g. `http://localhost:8080`).
    pub base_url: String,
    /// OAuth2 token endpoint (e.g. `{base_url}/oauth2/token`).
    pub token_url: String,
    /// OAuth2 client id for service-to-service calls.
    pub client_id: String,
    /// OAuth2 client secret for service-to-service calls.
    pub client_secret: String,
}

/// Logical namespace that owns all Kanban object types in the sso-gateway
/// permission registry. Every Kanban type (`KanbanProject`, `KanbanBoard`,
/// ...) resolves to this namespace's OpenFGA store.
pub const KANBAN_NAMESPACE: &str = "kanban";

/// The Kanban OpenFGA authorization model, embedded at compile time from the
/// single source of truth in `.integration/`.
const KANBAN_MODEL_JSON: &str = include_str!("../../.integration/openfga-model.json");

/// Parse the embedded Kanban authorization model into a protobuf `Struct`.
pub fn kanban_model() -> Result<buffa_types::google::protobuf::Struct> {
    serde_json::from_str(KANBAN_MODEL_JSON).context("failed to parse embedded OpenFGA model")
}

/// Wrapper around the generated `PermissionServiceClient`.
#[derive(Clone)]
pub struct PermissionClient {
    inner: PermissionServiceClient<ConnectTransport>,
    /// Service credentials, present only when built via [`PermissionClient::new`].
    /// Required to derive per-tenant clients.
    config: Option<PermissionClientConfig>,
    /// Per-tenant derived clients, keyed by tenant id. Each tenant uses a
    /// `OnceCell` so concurrent first-time lookups serialize on a single build
    /// and a single OAuth2 token fetch.
    tenant_clients:
        Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::OnceCell<PermissionClient>>>>>,
}

impl PermissionClient {
    /// Create a client from explicit OAuth2 service credentials.
    pub fn new(config: &PermissionClientConfig) -> Result<Self> {
        Self::build(config, None)
    }

    /// Shared constructor. When `tenant` is set, every request carries
    /// [`TENANT_HEADER`] so the gateway routes to that tenant's store.
    fn build(config: &PermissionClientConfig, tenant: Option<&str>) -> Result<Self> {
        let base_uri = config
            .base_url
            .parse::<http::Uri>()
            .context("invalid permission service base URL")?;

        let client = ClientBuilder::new(&config.base_url)
            .auth(
                OAuth2ClientCredentials::new(
                    &config.token_url,
                    &config.client_id,
                    &config.client_secret,
                )
                .with_scope("permission:admin"),
            )
            .build()
            .context("failed to build permission service client")?;

        let mut client_config = connectrpc::client::ClientConfig::new(base_uri.clone());
        if let Some(tenant) = tenant {
            client_config = client_config.with_default_header(TENANT_HEADER, tenant);
        }

        let transport = ConnectTransport::new(client, base_uri);
        let inner = PermissionServiceClient::new(transport, client_config);

        Ok(Self {
            inner,
            config: Some(config.clone()),
            tenant_clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Return a client whose calls are routed to `tenant_id`'s permission
    /// store via [`TENANT_HEADER`], ensuring the Kanban namespace is
    /// provisioned for that tenant on first use.
    ///
    /// Only available on clients built from service credentials
    /// ([`PermissionClient::new`]); unauthenticated clients resolve their
    /// tenant implicitly and cannot be re-scoped.
    pub async fn tenant_client(
        &self,
        tenant_id: &str,
    ) -> Result<PermissionClient, PermissionError> {
        let config = self.config.as_ref().ok_or_else(|| {
            PermissionError::Backend(
                "cannot derive a tenant client without service credentials".to_string(),
            )
        })?;

        // Get or create the OnceCell for this tenant. The cell guarantees that
        // only one async initialization runs, so concurrent first-time callers
        // share a single OAuth2 token fetch and namespace ensure.
        let cell = {
            let mut clients = self.tenant_clients.lock().await;
            clients
                .entry(tenant_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        let client = cell
            .get_or_try_init(|| async {
                let client = Self::build(config, Some(tenant_id))
                    .map_err(|e| PermissionError::Backend(format!("{e:#}")))?;
                client.ensure_kanban_namespace().await?;
                Ok::<_, PermissionError>(client)
            })
            .await?;

        Ok(client.clone())
    }

    /// Create a client without authentication. Useful for tests and local dev
    /// stacks that do not enforce client auth on the gateway.
    pub fn unauthenticated(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into();
        let base_uri = base_url
            .parse::<http::Uri>()
            .context("invalid permission service base URL")?;

        let client = ClientBuilder::new(&base_url)
            .build()
            .context("failed to build permission service client")?;

        let transport = ConnectTransport::new(client, base_uri.clone());
        let inner = PermissionServiceClient::new(
            transport,
            connectrpc::client::ClientConfig::new(base_uri),
        );

        Ok(Self {
            inner,
            config: None,
            tenant_clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Register or update a permission namespace with a full OpenFGA
    /// authorization model via `EnsurePermissionNamespace`.
    ///
    /// Idempotent: re-ensuring an identical model is a no-op on the gateway;
    /// a changed model publishes a new model version into the existing store
    /// without touching relation tuples.
    pub async fn ensure_namespace(
        &self,
        namespace: impl Into<String>,
        model: buffa_types::google::protobuf::Struct,
    ) -> Result<(), PermissionError> {
        let req = EnsurePermissionNamespaceRequest {
            namespace: namespace.into(),
            model: buffa::MessageField::some(model),
            __buffa_unknown_fields: Default::default(),
        };

        self.inner
            .ensure_permission_namespace(req)
            .await
            .map_err(|e| PermissionError::Backend(e.to_string()))?;

        Ok(())
    }

    /// Register the Kanban namespace ([`KANBAN_NAMESPACE`]) with the embedded
    /// authorization model. Called once at server boot and by the test
    /// harness after stack bootstrap.
    pub async fn ensure_kanban_namespace(&self) -> Result<(), PermissionError> {
        let model = kanban_model().map_err(|e| PermissionError::Backend(format!("{e:#}")))?;
        self.ensure_namespace(KANBAN_NAMESPACE, model).await
    }

    /// Check whether `subject_id` has `relation` on `object` in `namespace`.
    pub async fn check_permission(
        &self,
        namespace: impl Into<String>,
        object: impl Into<String>,
        relation: impl Into<String>,
        subject_id: impl Into<String>,
    ) -> Result<bool, PermissionError> {
        let req = CheckPermissionRequest {
            namespace: namespace.into(),
            object: object.into(),
            relation: relation.into(),
            subject_id: subject_id.into(),
            context: Default::default(),
            contextual_tuples: Vec::new(),
            consistency: String::new(),
            __buffa_unknown_fields: Default::default(),
        };

        let resp = self
            .inner
            .check_permission(req)
            .await
            .map_err(|e| PermissionError::Backend(e.to_string()))?;

        Ok(resp.into_owned().allowed)
    }

    /// Create a relation tuple granting `subject_id` the `relation` on `object`.
    pub async fn grant(
        &self,
        namespace: impl Into<String>,
        object: impl Into<String>,
        relation: impl Into<String>,
        subject_id: impl Into<String>,
    ) -> Result<(), PermissionError> {
        let req = CreateRelationTupleRequest {
            namespace: namespace.into(),
            object: object.into(),
            relation: relation.into(),
            subject_id: subject_id.into(),
            __buffa_unknown_fields: Default::default(),
        };

        let result = self.inner.create_relation_tuple(req).await;

        // OpenFGA rejects duplicate tuple writes; Kanban treats grants as
        // idempotent (Keto semantics) so retry and double-grant paths stay
        // safe. Anything else is a real backend error.
        if let Err(e) = result {
            let msg = e.to_string();
            if !msg.contains("cannot write a tuple which already exists") {
                return Err(PermissionError::Backend(msg));
            }
        }

        Ok(())
    }

    /// Expand the set of object IDs for which `subject_id` holds `relation` in
    /// `namespace`. Uses the sso-gateway `ExpandObjects` RPC.
    ///
    /// The gateway returns fully-qualified object references (`Type:id`,
    /// OpenFGA style); Kanban stores bare ids, so the type prefix is stripped
    /// and any entry of a different type is dropped.
    pub async fn expand_objects(
        &self,
        namespace: impl Into<String>,
        relation: impl Into<String>,
        subject_id: impl Into<String>,
    ) -> Result<Vec<String>, PermissionError> {
        let namespace = namespace.into();
        let req = ExpandObjectsRequest {
            namespace: namespace.clone(),
            relation: relation.into(),
            subject_id: subject_id.into(),
            subject_set_namespace: String::new(),
            subject_set_object: String::new(),
            subject_set_relation: String::new(),
            max_depth: 0,
            context: Default::default(),
            contextual_tuples: Vec::new(),
            consistency: String::new(),
            __buffa_unknown_fields: Default::default(),
        };

        let resp = self
            .inner
            .expand_objects(req)
            .await
            .map_err(|e| PermissionError::Backend(e.to_string()))?;

        let prefix = format!("{namespace}:");
        Ok(resp
            .into_owned()
            .objects
            .into_iter()
            .filter_map(|o| o.strip_prefix(&prefix).map(str::to_owned))
            .collect())
    }

    /// Expand the permission tree for an object/relation.
    pub async fn expand_permissions(
        &self,
        namespace: impl Into<String>,
        object: impl Into<String>,
        relation: impl Into<String>,
    ) -> Result<Vec<crate::iam_proto::iam::v1::RelationTuple>, PermissionError> {
        let req = ExpandPermissionsRequest {
            namespace: namespace.into(),
            object: object.into(),
            relation: relation.into(),
            context: Default::default(),
            contextual_tuples: Vec::new(),
            consistency: String::new(),
            __buffa_unknown_fields: Default::default(),
        };

        let resp = self
            .inner
            .expand_permissions(req)
            .await
            .map_err(|e| PermissionError::Backend(e.to_string()))?;

        Ok(resp.into_owned().tuples)
    }

    /// Fetch a provisioned namespace (authorization model + claimed object
    /// types) from the gateway. Useful for verifying provisioning state.
    pub async fn get_namespace(
        &self,
        namespace: impl Into<String>,
    ) -> Result<crate::iam_proto::iam::v1::PermissionNamespace, PermissionError> {
        let req = crate::iam_proto::iam::v1::GetPermissionNamespaceRequest {
            namespace: namespace.into(),
            __buffa_unknown_fields: Default::default(),
        };

        let resp = self
            .inner
            .get_permission_namespace(req)
            .await
            .map_err(|e| PermissionError::Backend(e.to_string()))?;

        Ok(resp.into_owned())
    }

    /// List relation tuples matching the given filters, with pagination.
    pub async fn list_relation_tuples(
        &self,
        namespace: impl Into<String>,
        object: Option<String>,
        relation: Option<String>,
        _subject_id: Option<String>,
        page_size: u32,
        page_token: impl Into<String>,
    ) -> Result<(Vec<crate::iam_proto::iam::v1::RelationTuple>, String), PermissionError> {
        let req = ListRelationTuplesRequest {
            namespace: namespace.into(),
            object: object.unwrap_or_default(),
            relation: relation.unwrap_or_default(),
            page: buffa::MessageField::some(PageRequest {
                page_size,
                page_token: page_token.into(),
                __buffa_unknown_fields: Default::default(),
            }),
            __buffa_unknown_fields: Default::default(),
        };

        let resp = self
            .inner
            .list_relation_tuples(req)
            .await
            .map_err(|e| PermissionError::Backend(e.to_string()))?;

        let owned = resp.into_owned();
        let next_token = owned.page.next_page_token.clone();
        Ok((owned.tuples, next_token))
    }

    /// Delete all relation tuples matching the filters.
    ///
    /// sso-gateway only supports `DeleteRelationTuple` by gateway tuple id, so
    /// this lists matching tuples and deletes each one.
    pub async fn delete_relation_tuples(
        &self,
        namespace: impl Into<String>,
        object: Option<String>,
        relation: Option<String>,
        subject_id: Option<String>,
    ) -> Result<(), PermissionError> {
        let namespace = namespace.into();
        let mut page_token = String::new();

        // Page at the gateway's maximum page size (keyset pagination, max 200).
        loop {
            let (tuples, next_token) = self
                .list_relation_tuples(
                    namespace.clone(),
                    object.clone(),
                    relation.clone(),
                    None,
                    200,
                    page_token,
                )
                .await?;

            for tuple in tuples {
                if let Some(ref subject) = subject_id
                    && tuple.subject_id != *subject
                {
                    continue;
                }
                let delete_req = DeleteRelationTupleRequest {
                    id: tuple.id,
                    __buffa_unknown_fields: Default::default(),
                };
                self.inner
                    .delete_relation_tuple(delete_req)
                    .await
                    .map_err(|e| PermissionError::Backend(e.to_string()))?;
            }

            if next_token.is_empty() {
                break;
            }
            page_token = next_token;
        }

        Ok(())
    }
}

impl std::fmt::Debug for PermissionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PermissionClient").finish_non_exhaustive()
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Id;

    /// Object type claimed by the Kanban namespace.
    const TYPE_PROJECT: &str = "KanbanProject";
    /// Assignable role used when seeding tuples (writes must use roles).
    const GRANT_RELATION: &str = "viewer";
    /// Computed relation used for checks (cannot be written directly).
    const CHECK_RELATION: &str = "view";

    /// Create a brand-new tenant and return its id. The test uses the shared
    /// cross-tenant service app plus `x-tenant-id` to target it.
    async fn fresh_tenant_id(sso_gateway_url: &str, slug_prefix: &str) -> String {
        let slug = format!("{slug_prefix}-{}", Id::new()).to_lowercase();
        crate::test_support::containers::create_tenant(
            sso_gateway_url,
            &slug,
            "Kanban Isolation Test Tenant",
        )
        .await
        .expect("failed to create isolation tenant")
    }

    /// Tuples written in one tenant must be invisible in another, even for
    /// the same object id and subject string.
    #[tokio::test]
    async fn tenant_isolation_denies_cross_tenant_access() {
        let infra = crate::test_support::containers::setup().await;
        let root = infra.permission.as_ref().clone();
        let tenant_b = fresh_tenant_id(&infra.sso_gateway_url, "kanban-iso").await;
        let tenant_b_client = root
            .tenant_client(&tenant_b)
            .await
            .expect("failed to derive tenant-B client");

        let user_a = format!("user:{}", Id::new());
        let user_b = format!("user:{}", Id::new());
        let object = format!("obj-{}", Id::new());

        // Grant in the root/home tenant (no header).
        root.grant(TYPE_PROJECT, &object, GRANT_RELATION, &user_a)
            .await
            .expect("grant in root tenant");
        assert!(
            root.check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user_a)
                .await
                .expect("check in root tenant"),
            "subject must see its own grant in the root tenant"
        );

        // The same subject/object must not be visible when targeting tenant B.
        assert!(
            !tenant_b_client
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user_a)
                .await
                .expect("cross-tenant check"),
            "root tenant grant must not leak into tenant B checks"
        );
        let expanded_b = tenant_b_client
            .expand_objects(TYPE_PROJECT, CHECK_RELATION, &user_a)
            .await
            .expect("cross-tenant expand");
        assert!(
            !expanded_b.contains(&object),
            "root tenant object must not appear in tenant B expand: {expanded_b:?}"
        );

        // Same object id in tenant B with a different subject stays independent.
        tenant_b_client
            .grant(TYPE_PROJECT, &object, GRANT_RELATION, &user_b)
            .await
            .expect("grant in tenant B");
        assert!(
            tenant_b_client
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user_b)
                .await
                .expect("check in tenant B"),
            "subject must see its own grant in tenant B"
        );
        assert!(
            !root
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user_b)
                .await
                .expect("cross-tenant check back"),
            "tenant B grant must not leak into root tenant checks"
        );

        root.delete_relation_tuples(
            TYPE_PROJECT,
            Some(object.clone()),
            Some(GRANT_RELATION.to_string()),
            Some(user_a),
        )
        .await
        .expect("cleanup in root tenant");
        tenant_b_client
            .delete_relation_tuples(
                TYPE_PROJECT,
                Some(object),
                Some(GRANT_RELATION.to_string()),
                Some(user_b),
            )
            .await
            .expect("cleanup in tenant B");
    }

    /// `tenant_client` scopes service-credential calls to another tenant via
    /// the `x-tenant-id` header; production handlers rely on this routing.
    #[tokio::test]
    async fn tenant_client_header_routes_to_target_tenant() {
        let infra = crate::test_support::containers::setup().await;
        let root = infra.permission.as_ref().clone();
        let tenant_b = fresh_tenant_id(&infra.sso_gateway_url, "kanban-hdr").await;
        let derived_b = root
            .tenant_client(&tenant_b)
            .await
            .expect("failed to derive tenant-B client");

        let user = format!("user:{}", Id::new());
        let object = format!("obj-{}", Id::new());

        // A grant written through the header-scoped client lands in tenant B.
        derived_b
            .grant(TYPE_PROJECT, &object, GRANT_RELATION, &user)
            .await
            .expect("grant via derived client");
        assert!(
            derived_b
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user)
                .await
                .expect("check via derived client"),
            "grant via derived client must be visible in tenant B"
        );
        assert!(
            !root
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user)
                .await
                .expect("check via root credentials"),
            "grant via derived client must not land in the root tenant"
        );

        derived_b
            .delete_relation_tuples(
                TYPE_PROJECT,
                Some(object),
                Some(GRANT_RELATION.to_string()),
                Some(user),
            )
            .await
            .expect("cleanup via derived client");
    }

    /// `agent:` subjects are first-class: they hold roles independently of
    /// the `user:` subject with the same id.
    #[tokio::test]
    async fn agent_subjects_are_distinct_from_users() {
        let infra = crate::test_support::containers::setup().await;
        let client = infra.permission.as_ref().clone();

        let raw_id = Id::new();
        let agent = format!("agent:{raw_id}");
        let user = format!("user:{raw_id}");
        let object = format!("obj-{}", Id::new());

        client
            .grant(TYPE_PROJECT, &object, GRANT_RELATION, &agent)
            .await
            .expect("grant to agent subject");
        assert!(
            client
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &agent)
                .await
                .expect("agent check"),
            "agent subject must resolve its own grant"
        );
        assert!(
            !client
                .check_permission(TYPE_PROJECT, &object, CHECK_RELATION, &user)
                .await
                .expect("user check"),
            "user:<id> and agent:<id> must stay distinct subjects"
        );

        let expanded = client
            .expand_objects(TYPE_PROJECT, CHECK_RELATION, &agent)
            .await
            .expect("agent expand");
        assert!(
            expanded.contains(&object),
            "agent expand must include the granted object: {expanded:?}"
        );

        client
            .delete_relation_tuples(
                TYPE_PROJECT,
                Some(object),
                Some(GRANT_RELATION.to_string()),
                Some(agent),
            )
            .await
            .expect("cleanup agent tuple");
    }

    /// Ensuring the Kanban namespace is idempotent and the provisioned
    /// namespace claims every object type the model declares.
    #[tokio::test]
    async fn ensure_kanban_namespace_is_idempotent_and_complete() {
        let infra = crate::test_support::containers::setup().await;
        let client = infra.permission.as_ref().clone();

        client
            .ensure_kanban_namespace()
            .await
            .expect("first ensure");
        client
            .ensure_kanban_namespace()
            .await
            .expect("second ensure must be idempotent");

        let ns = client
            .get_namespace(KANBAN_NAMESPACE)
            .await
            .expect("read back kanban namespace");
        for ty in [
            "user",
            "agent",
            "KanbanProject",
            "KanbanBoard",
            "KanbanCard",
            "KanbanAggregatedBoard",
        ] {
            assert!(
                ns.types.iter().any(|t| t == ty),
                "namespace must claim type {ty}; got {:?}",
                ns.types
            );
        }
    }
}
