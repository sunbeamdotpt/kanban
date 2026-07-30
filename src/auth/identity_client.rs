// SPDX-License-Identifier: AGPL-3.0-or-later
//! sso-gateway `iam.v1.IdentityService` client wrapper (user directory lookups).
//!
//! Built on the SDK's [`AuthClient`], which is generated from
//! `buf.build/sunbeamdotpt/sso-gateway`, so the identity surface always tracks
//! the published gateway API. Service-to-service calls authenticate with OAuth2
//! client credentials scoped `identity:read`; per-tenant routing sends the
//! `x-tenant-id` header exactly like the permission client.
//!
//! The gateway has no lookup-by-email RPC (`ListIdentities` ignores filters),
//! so email resolution pages the tenant directory and matches `traits.email`
//! client-side — the same approach the CLI uses.

use anyhow::{Context, Result};
use sdk::auth::AuthClient;
use sdk::auth::v1::{GetIdentityRequest, Identity, ListIdentitiesRequest, PageRequest};
use sunbeam_g2v::client::OAuth2ClientCredentials;

use crate::id::Id;

/// OAuth2 scope required to read the user directory.
const IDENTITY_SCOPE: &str = "identity:read";

/// Directory page size for email scans (gateway maximum).
const DIRECTORY_PAGE_SIZE: u32 = 200;

/// Configuration for building an [`IdentityClient`].
#[derive(Clone, Debug)]
pub struct IdentityClientConfig {
    /// Base URL of the sso-gateway (e.g. `http://localhost:8080`).
    pub base_url: String,
    /// OAuth2 token endpoint (e.g. `{base_url}/oauth2/token`).
    pub token_url: String,
    /// OAuth2 client id for service-to-service calls.
    pub client_id: String,
    /// OAuth2 client secret for service-to-service calls.
    pub client_secret: String,
}

/// Errors from user-directory resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// The input is neither an identity ULID nor an email address.
    #[error("invalid user reference: {0}")]
    Invalid(String),
    /// The input is well-formed but no such identity exists in the tenant.
    #[error("user not found: {0}")]
    NotFound(String),
    /// The identity backend returned an error or was unreachable.
    #[error("identity backend error: {0}")]
    Backend(String),
}

/// Wrapper around the SDK [`AuthClient`] scoped to the identity surface.
#[derive(Clone)]
pub struct IdentityClient {
    auth: AuthClient,
}

/// Format an sso-gateway identity ULID as the canonical Kanban assignee
/// subject (`user:<ulid>`, the OIDC `sub` form stored in `card_assignees`).
pub fn canonical_subject(identity_id: &str) -> String {
    format!("user:{identity_id}")
}

/// Extract the email address from an identity's traits document.
fn identity_email(identity: &Identity) -> String {
    identity
        .traits
        .as_option()
        .and_then(|t| serde_json::to_value(t).ok())
        .and_then(|v| v.get("email").and_then(|e| e.as_str()).map(str::to_owned))
        .unwrap_or_default()
}

impl IdentityClient {
    /// Create a client from explicit OAuth2 service credentials.
    pub fn new(config: &IdentityClientConfig) -> Result<Self> {
        let base_uri = config
            .base_url
            .parse::<http::Uri>()
            .context("invalid identity service base URL")?;

        let client = AuthClient::builder(&config.base_url)
            .auth(
                OAuth2ClientCredentials::new(
                    &config.token_url,
                    &config.client_id,
                    &config.client_secret,
                )
                .with_scope(IDENTITY_SCOPE),
            )
            .build()
            .context("failed to build identity service client")?;

        Ok(Self {
            auth: AuthClient::new(client, base_uri).context("invalid identity service base URL")?,
        })
    }

    /// Fetch an identity by its gateway ULID within `tenant_id`.
    pub async fn get_identity(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<Identity>, ResolveError> {
        let result = self
            .auth
            .clone()
            .with_tenant(tenant_id)
            .identity()
            .get_identity(GetIdentityRequest {
                id: id.to_string(),
                ..Default::default()
            })
            .await;

        match result {
            Ok(resp) => Ok(Some(resp.into_owned())),
            Err(e) if e.code == connectrpc::ErrorCode::NotFound => Ok(None),
            Err(e) => Err(ResolveError::Backend(e.to_string())),
        }
    }

    /// Find an identity by email address within `tenant_id`.
    ///
    /// `ListIdentities` has no server-side filter, so this pages the whole
    /// directory and matches `traits.email` case-insensitively.
    pub async fn find_by_email(
        &self,
        tenant_id: &str,
        email: &str,
    ) -> Result<Option<Identity>, ResolveError> {
        let client = self.auth.clone().with_tenant(tenant_id);
        let identity = client.identity();
        let mut page_token = String::new();

        loop {
            let resp = identity
                .list_identities(ListIdentitiesRequest {
                    page: buffa::MessageField::some(PageRequest {
                        page_size: DIRECTORY_PAGE_SIZE,
                        page_token: page_token.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await
                .map_err(|e| ResolveError::Backend(e.to_string()))?
                .into_owned();

            if let Some(found) = resp
                .identities
                .iter()
                .find(|i| identity_email(i).eq_ignore_ascii_case(email))
            {
                return Ok(Some(found.clone()));
            }

            let next = resp.page.next_page_token.clone();
            if next.is_empty() {
                return Ok(None);
            }
            page_token = next;
        }
    }

    /// Resolve a user reference to the canonical assignee subject
    /// (`user:<ulid>`).
    ///
    /// Accepted inputs: an identity ULID, the canonical `user:<ulid>` subject,
    /// or an email address. Anything else is rejected. The resolved identity
    /// must exist in `tenant_id`'s directory.
    pub async fn resolve_subject(
        &self,
        tenant_id: &str,
        input: &str,
    ) -> Result<String, ResolveError> {
        let input = input.trim();
        if input.is_empty() {
            return Err(ResolveError::Invalid("empty user reference".to_string()));
        }

        if let Some(rest) = input.strip_prefix("user:") {
            return self.resolve_by_id(tenant_id, input, rest).await;
        }
        if Id::is_ulid(input) {
            return self.resolve_by_id(tenant_id, input, input).await;
        }
        if input.contains('@') {
            return match self.find_by_email(tenant_id, input).await? {
                Some(identity) => Ok(canonical_subject(&identity.id)),
                None => Err(ResolveError::NotFound(input.to_string())),
            };
        }

        Err(ResolveError::Invalid(format!(
            "{input} is neither an identity ULID nor an email address"
        )))
    }

    /// Resolve a ULID-shaped input to the canonical subject, verifying the
    /// identity exists.
    async fn resolve_by_id(
        &self,
        tenant_id: &str,
        original: &str,
        id: &str,
    ) -> Result<String, ResolveError> {
        if !Id::is_ulid(id) {
            return Err(ResolveError::Invalid(format!(
                "{original} does not carry a valid identity ULID"
            )));
        }
        match self.get_identity(tenant_id, id).await? {
            Some(identity) => Ok(canonical_subject(&identity.id)),
            None => Err(ResolveError::NotFound(original.to_string())),
        }
    }
}

impl std::fmt::Debug for IdentityClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityClient").finish_non_exhaustive()
    }
}
