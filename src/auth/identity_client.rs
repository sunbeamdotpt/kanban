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
//!
//! # Identity traits contract
//!
//! Hydration reads these traits from the identity's traits document (see
//! `.maintainer/interfaces.md` for the authoritative contract):
//!
//! | Trait         | Used for       | Schema                              |
//! |---------------|----------------|-------------------------------------|
//! | `email`       | `Assignee.email`, email resolution | required everywhere |
//! | `given_name`  | `Assignee.display_name` (first part) | deployed `employee` schema |
//! | `family_name` | `Assignee.display_name` (last part)  | deployed `employee` schema |
//!
//! Display name is `"given_name family_name"`, falling back to whichever
//! part is present. **Do not trust the schema file vendored in the
//! sso-gateway repo** (`deploy/kratos-identity.schema.json`) — it lags the
//! deployed schemas (it documents an email-only base schema while
//! production uses `employee` with name traits). Check the live directory
//! (`sunbeam user get <email>`) when in doubt. There is no avatar trait
//! anywhere yet (SSO-028).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sdk::auth::AuthClient;
use sdk::auth::v1::{GetIdentityRequest, Identity, ListIdentitiesRequest, PageRequest};
use sunbeam_g2v::client::OAuth2ClientCredentials;

use crate::id::Id;

/// OAuth2 scope required to read the user directory.
const IDENTITY_SCOPE: &str = "identity:read";

/// Directory page size for email scans (gateway maximum).
const DIRECTORY_PAGE_SIZE: u32 = 200;

/// How long a resolved subject→profile mapping is trusted. Profiles change
/// rarely and every card read hydrates assignees, so without a cache list
/// endpoints would hammer the gateway directory (KANBAN-035).
const PROFILE_CACHE_TTL: Duration = Duration::from_secs(300);

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

/// Best-effort directory profile for an assignee: display name and email.
/// Either field may be empty when the identity lacks the trait.
#[derive(Clone, Debug, Default)]
pub struct AssigneeProfile {
    /// `"given_name family_name"` from the identity traits; empty when unset.
    pub display_name: String,
    /// `email` trait; empty when unset.
    pub email: String,
}

/// (tenant_id, subject) → (profile, resolved_at) cache for `resolve_profile`.
type ProfileCache = Arc<Mutex<HashMap<(String, String), (AssigneeProfile, Instant)>>>;

/// Wrapper around the SDK [`AuthClient`] scoped to the identity surface.
#[derive(Clone)]
pub struct IdentityClient {
    auth: AuthClient,
    profile_cache: ProfileCache,
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

/// Extract a display name from an identity's traits document:
/// `"given_name family_name"` (deployed `employee` schema), falling back to
/// whichever part is present. Empty when neither trait is set.
fn identity_display_name(identity: &Identity) -> String {
    let traits = identity
        .traits
        .as_option()
        .and_then(|t| serde_json::to_value(t).ok());
    let part = |key: &str| {
        traits
            .as_ref()
            .and_then(|v| v.get(key).and_then(|s| s.as_str()))
            .unwrap_or("")
    };
    let (given, family) = (part("given_name"), part("family_name"));
    match (given.is_empty(), family.is_empty()) {
        (false, false) => format!("{given} {family}"),
        (false, true) => given.to_string(),
        (true, false) => family.to_string(),
        (true, true) => String::new(),
    }
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
            profile_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Resolve the directory profile (display name + email) for a canonical
    /// assignee subject (`user:<ulid>`), best-effort.
    ///
    /// Returns `None` for non-canonical subjects (legacy rows), unknown
    /// identities, and backend errors — assignee hydration must never fail a
    /// card read. Fresh results are cached for [`PROFILE_CACHE_TTL`].
    pub async fn resolve_profile(&self, tenant_id: &str, subject: &str) -> Option<AssigneeProfile> {
        let key = (tenant_id.to_string(), subject.to_string());
        if let Ok(cache) = self.profile_cache.lock()
            && let Some((profile, at)) = cache.get(&key)
            && at.elapsed() < PROFILE_CACHE_TTL
        {
            return Some(profile.clone());
        }

        let id = subject.strip_prefix("user:")?;
        if !Id::is_ulid(id) {
            return None;
        }
        let profile = match self.get_identity(tenant_id, id).await {
            Ok(Some(identity)) => {
                let profile = AssigneeProfile {
                    display_name: identity_display_name(&identity),
                    email: identity_email(&identity),
                };
                if profile.display_name.is_empty() && profile.email.is_empty() {
                    return None;
                }
                profile
            }
            Ok(None) => return None,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    subject,
                    "assignee profile hydration failed; returning empty hints"
                );
                return None;
            }
        };

        if let Ok(mut cache) = self.profile_cache.lock() {
            cache.insert(key, (profile.clone(), Instant::now()));
        }
        Some(profile)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn identity_with_traits(traits: serde_json::Value) -> Identity {
        Identity {
            id: "01KWF0KYZ0FRNR23ZXAWJZV43T".to_string(),
            traits: buffa::MessageField::some(
                serde_json::from_value(traits).expect("traits must build"),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn display_name_joins_given_and_family() {
        let identity = identity_with_traits(serde_json::json!({
            "email": "ada@example.com",
            "given_name": "Ada",
            "family_name": "Lovelace",
        }));
        assert_eq!(identity_display_name(&identity), "Ada Lovelace");
        assert_eq!(identity_email(&identity), "ada@example.com");
    }

    #[test]
    fn display_name_falls_back_to_whichever_part_exists() {
        let given_only = identity_with_traits(serde_json::json!({"given_name": "Ada"}));
        assert_eq!(identity_display_name(&given_only), "Ada");
        let family_only = identity_with_traits(serde_json::json!({"family_name": "Lovelace"}));
        assert_eq!(identity_display_name(&family_only), "Lovelace");
    }

    #[test]
    fn display_name_is_empty_without_name_traits() {
        let email_only = identity_with_traits(serde_json::json!({"email": "ada@example.com"}));
        assert_eq!(identity_display_name(&email_only), "");
        let no_traits = Identity::default();
        assert_eq!(identity_display_name(&no_traits), "");
        assert_eq!(identity_email(&no_traits), "");
    }
}
