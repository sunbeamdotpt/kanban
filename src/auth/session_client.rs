// SPDX-License-Identifier: AGPL-3.0-or-later
//! sso-gateway introspection session client.
//!
//! `sunbeam-g2v` 0.5's `IntrospectionSessionClient` normalizes the OAuth2
//! introspection response into a generic session shape that drops the
//! `tenant_id` field returned by sso-gateway. This client preserves that field
//! so the auth middleware can inject `TenantId` into request extensions.
//!
//! The sso-gateway's introspection endpoint requires a service access token
//! with the `tenant:admin` scope (no HTTP Basic client authentication), so
//! calls authenticate with a bearer token from an OAuth2 client-credentials
//! [`TokenProvider`], which caches by `expires_in` and refreshes on demand.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value, json};
use sunbeam_g2v::client::TokenProvider;
use sunbeam_g2v::middleware::auth::{AuthError, session::SessionClient};
use tracing::warn;

/// Default introspection HTTP timeout.
const DEFAULT_INTROSPECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Session client that introspects bearer tokens at the sso-gateway
/// `/oauth2/introspect` endpoint and preserves the `tenant_id` claim.
///
/// Every rejection is WARN-logged with a failure class and the call latency:
/// the g2v auth middleware collapses all session errors into a bare 401, so
/// these logs are the only place the failure reason survives (KANBAN-031).
#[derive(Clone)]
pub struct SsoGatewaySessionClient {
    client: reqwest::Client,
    introspection_url: String,
    provider: Arc<dyn TokenProvider>,
}

/// Failure classes for authn rejection logs.
mod failure_class {
    pub const MISSING_TOKEN: &str = "missing-token";
    pub const SERVICE_TOKEN: &str = "service-token";
    pub const TIMEOUT: &str = "timeout";
    pub const TRANSPORT: &str = "transport";
    pub const GATEWAY_ERROR: &str = "gateway-error";
    pub const BAD_RESPONSE: &str = "bad-response";
    pub const INACTIVE: &str = "inactive";
}

/// Log one authn rejection with its failure class and introspection latency.
fn log_rejection(class: &str, detail: &str, started: Instant) {
    warn!(
        failure_class = class,
        latency_ms = started.elapsed().as_millis() as u64,
        detail,
        "authn rejection: token introspection failed"
    );
}

impl SsoGatewaySessionClient {
    /// Create a client that introspects at `introspection_url`, authenticating
    /// with service tokens from `provider`.
    pub fn new(
        introspection_url: impl Into<String>,
        provider: impl TokenProvider,
    ) -> Result<Self, AuthError> {
        Self::with_timeout(introspection_url, provider, DEFAULT_INTROSPECTION_TIMEOUT)
    }

    /// Create a client with an explicit introspection HTTP timeout.
    pub fn with_timeout(
        introspection_url: impl Into<String>,
        provider: impl TokenProvider,
        timeout: Duration,
    ) -> Result<Self, AuthError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| {
                AuthError::InvalidSession(format!("failed to build introspection HTTP client: {e}"))
            })?;
        Ok(Self {
            client,
            introspection_url: introspection_url.into(),
            provider: Arc::new(provider),
        })
    }

    /// POST one introspection request for `token`, authenticated with a
    /// service token from the provider.
    ///
    /// The returned error string is prefixed with the failure class so
    /// `to_session` can log rejections without re-classifying.
    async fn introspect(&self, token: &str) -> Result<reqwest::Response, (String, AuthError)> {
        let service_token = self.provider.token().await.map_err(|e| {
            (
                failure_class::SERVICE_TOKEN.to_string(),
                AuthError::InvalidSession(format!("failed to fetch service token: {e}")),
            )
        })?;
        self.client
            .post(&self.introspection_url)
            .bearer_auth(service_token)
            .form(&[("token", token)])
            .send()
            .await
            .map_err(|e| {
                let class = if e.is_timeout() {
                    failure_class::TIMEOUT
                } else {
                    failure_class::TRANSPORT
                };
                (
                    class.to_string(),
                    AuthError::InvalidSession(format!("introspection request failed: {e}")),
                )
            })
    }
}

#[async_trait]
impl SessionClient for SsoGatewaySessionClient {
    async fn to_session(
        &self,
        cookie: Option<&str>,
        token: Option<&str>,
    ) -> Result<Value, AuthError> {
        let _ = cookie;
        let started = Instant::now();
        let Some(token) = token else {
            log_rejection(failure_class::MISSING_TOKEN, "missing token", started);
            return Err(AuthError::InvalidSession("missing token".to_string()));
        };

        let response = match self.introspect(token).await {
            Ok(response) => response,
            Err((class, err)) => {
                log_rejection(&class, &err.to_string(), started);
                return Err(err);
            }
        };
        // A 401 means the cached service token was rejected gateway-side
        // (revoked or expired early); invalidate it and retry once with a
        // fresh token before failing the request.
        let response = if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.provider.invalidate();
            match self.introspect(token).await {
                Ok(response) => response,
                Err((class, err)) => {
                    log_rejection(&class, &err.to_string(), started);
                    return Err(err);
                }
            }
        } else {
            response
        };

        if !response.status().is_success() {
            let status = response.status();
            log_rejection(
                failure_class::GATEWAY_ERROR,
                &format!("introspection returned {status}"),
                started,
            );
            return Err(AuthError::InvalidSession(format!(
                "introspection returned {status}"
            )));
        }

        let body: Value = match response.json().await {
            Ok(body) => body,
            Err(e) => {
                log_rejection(
                    failure_class::BAD_RESPONSE,
                    &format!("invalid introspection response: {e}"),
                    started,
                );
                return Err(AuthError::InvalidSession(format!(
                    "invalid introspection response: {e}"
                )));
            }
        };

        let active = body
            .get("active")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !active {
            log_rejection(failure_class::INACTIVE, "token inactive", started);
            return Err(AuthError::InvalidSession("token inactive".to_string()));
        }

        let subject = body
            .get("sub")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if subject.is_empty() {
            log_rejection(
                failure_class::BAD_RESPONSE,
                "introspection response missing sub",
                started,
            );
            return Err(AuthError::InvalidSession(
                "introspection response missing sub".to_string(),
            ));
        }

        // Preserve the tenant_id claim so the auth middleware can resolve it.
        // Fall back to the subject if sso-gateway did not include tenant_id
        // (legacy/dev tokens).
        let tenant_id = body
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.clone());

        let scope = body.get("scope").cloned().unwrap_or_else(|| json!(""));

        Ok(json!({
            "tenant_id": tenant_id,
            "sub": subject,
            "scope": scope,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::Form,
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::post,
    };
    use serde::Deserialize;
    use serde_json::json;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use sunbeam_g2v::client::{BearerToken, OAuth2ClientCredentials};

    #[derive(Deserialize)]
    struct IntrospectForm {
        token: String,
    }

    async fn start_mock_introspection_server(
        handler: impl Fn(String) -> (StatusCode, serde_json::Value) + Send + Sync + Clone + 'static,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/oauth2/introspect",
            post(move |Form(form): Form<IntrospectForm>| {
                let handler = handler.clone();
                async move {
                    let (status, body) = handler(form.token);
                    (status, Json(body)).into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (addr, handle)
    }

    fn client_for(addr: SocketAddr) -> SsoGatewaySessionClient {
        SsoGatewaySessionClient::new(
            format!("http://{addr}/oauth2/introspect"),
            BearerToken::new("service-token"),
        )
        .expect("client should build")
    }

    #[tokio::test]
    async fn to_session_returns_tenant_id_and_subject() {
        let (addr, _handle) = start_mock_introspection_server(|_| {
            (
                StatusCode::OK,
                json!({
                    "active": true,
                    "sub": "user:test",
                    "tenant_id": "tenant-42",
                    "scope": "read write",
                }),
            )
        })
        .await;

        let client = client_for(addr);
        let session = client
            .to_session(None, Some("valid-token"))
            .await
            .expect("session should succeed");

        assert_eq!(session["tenant_id"], "tenant-42");
        assert_eq!(session["sub"], "user:test");
        assert_eq!(session["scope"], "read write");
    }

    #[tokio::test]
    async fn to_session_falls_back_to_subject_when_tenant_id_missing() {
        let (addr, _handle) = start_mock_introspection_server(|_| {
            (
                StatusCode::OK,
                json!({
                    "active": true,
                    "sub": "user:legacy",
                }),
            )
        })
        .await;

        let client = client_for(addr);
        let session = client
            .to_session(None, Some("legacy-token"))
            .await
            .expect("session should succeed");

        assert_eq!(session["tenant_id"], "user:legacy");
        assert_eq!(session["sub"], "user:legacy");
    }

    #[tokio::test]
    async fn to_session_rejects_inactive_token() {
        let (addr, _handle) = start_mock_introspection_server(|_| {
            (
                StatusCode::OK,
                json!({
                    "active": false,
                    "sub": "user:test",
                }),
            )
        })
        .await;

        let client = client_for(addr);
        let err = client
            .to_session(None, Some("inactive-token"))
            .await
            .expect_err("inactive token should fail");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }

    #[tokio::test]
    async fn to_session_rejects_missing_subject() {
        let (addr, _handle) = start_mock_introspection_server(|_| {
            (
                StatusCode::OK,
                json!({
                    "active": true,
                }),
            )
        })
        .await;

        let client = client_for(addr);
        let err = client
            .to_session(None, Some("no-sub-token"))
            .await
            .expect_err("missing sub should fail");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }

    #[tokio::test]
    async fn to_session_retries_once_on_401() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::clone(&attempts);
        let (addr, _handle) = start_mock_introspection_server(move |_| {
            let n = attempts_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                (StatusCode::UNAUTHORIZED, json!({"error": "invalid_token"}))
            } else {
                (
                    StatusCode::OK,
                    json!({
                        "active": true,
                        "sub": "user:test",
                        "tenant_id": "tenant-42",
                    }),
                )
            }
        })
        .await;

        let client = client_for(addr);
        let session = client
            .to_session(None, Some("valid-token"))
            .await
            .expect("session should succeed after one retry");

        assert_eq!(session["tenant_id"], "tenant-42");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn to_session_rejects_persistent_401() {
        let (addr, _handle) = start_mock_introspection_server(|_| {
            (StatusCode::UNAUTHORIZED, json!({"error": "invalid_token"}))
        })
        .await;

        let client = client_for(addr);
        let err = client
            .to_session(None, Some("any-token"))
            .await
            .expect_err("persistent 401 should fail");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }

    #[tokio::test]
    async fn to_session_rejects_missing_token() {
        let client = SsoGatewaySessionClient::new(
            "http://localhost:1/oauth2/introspect",
            BearerToken::new("x"),
        )
        .expect("client should build");

        let err = client
            .to_session(None, None)
            .await
            .expect_err("missing token should fail");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }

    /// A stalled gateway must fail closed within the configured timeout
    /// instead of hanging the request (KANBAN-031).
    #[tokio::test]
    async fn to_session_fails_closed_on_introspection_timeout() {
        let app = Router::new().route(
            "/oauth2/introspect",
            post(move |Form(_form): Form<IntrospectForm>| async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Json(json!({"active": true, "sub": "user:test"}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = SsoGatewaySessionClient::with_timeout(
            format!("http://{addr}/oauth2/introspect"),
            BearerToken::new("service-token"),
            Duration::from_millis(100),
        )
        .expect("client should build");

        let err = client
            .to_session(None, Some("any-token"))
            .await
            .expect_err("stalled introspection should time out");
        let AuthError::InvalidSession(detail) = err else {
            panic!("expected InvalidSession");
        };
        assert!(
            detail.contains("introspection request failed"),
            "timeout should surface as a transport-class failure: {detail}"
        );
    }

    #[derive(Deserialize)]
    struct TokenForm {
        grant_type: String,
        client_id: String,
        client_secret: String,
        scope: Option<String>,
    }

    /// End-to-end over a mock gateway: the client must fetch a service token
    /// via client_credentials (scoped `tenant:admin`) and introspect with
    /// `Authorization: Bearer`, never HTTP Basic.
    #[tokio::test]
    async fn to_session_authenticates_with_client_credentials_bearer() {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let token_calls_clone = Arc::clone(&token_calls);

        let app = Router::new()
            .route(
                "/oauth2/token",
                post(move |Form(form): Form<TokenForm>| {
                    let token_calls = Arc::clone(&token_calls_clone);
                    async move {
                        token_calls.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(form.grant_type, "client_credentials");
                        assert_eq!(form.client_id, "kanban-service");
                        assert_eq!(form.client_secret, "secret");
                        assert_eq!(form.scope.as_deref(), Some("tenant:admin"));
                        Json(json!({
                            "access_token": "service-access-token",
                            "expires_in": 3600,
                            "token_type": "bearer",
                        }))
                    }
                }),
            )
            .route(
                "/oauth2/introspect",
                post(
                    |headers: HeaderMap, Form(form): Form<IntrospectForm>| async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        assert_eq!(auth, "Bearer service-access-token");
                        assert_eq!(form.token, "end-user-token");
                        Json(json!({
                            "active": true,
                            "sub": "user:test",
                            "tenant_id": "tenant-42",
                        }))
                    },
                ),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = SsoGatewaySessionClient::new(
            format!("http://{addr}/oauth2/introspect"),
            OAuth2ClientCredentials::new(
                format!("http://{addr}/oauth2/token"),
                "kanban-service",
                "secret",
            )
            .with_scope("tenant:admin"),
        )
        .expect("client should build");

        let session = client
            .to_session(None, Some("end-user-token"))
            .await
            .expect("session should succeed");
        assert_eq!(session["tenant_id"], "tenant-42");

        // A second call must reuse the cached service token.
        let session = client
            .to_session(None, Some("end-user-token"))
            .await
            .expect("session should succeed");
        assert_eq!(session["sub"], "user:test");
        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
    }

    // ── Integration tests (sso-gateway via testcontainers) ─────────────────

    /// Read the harness-provisioned service credentials. Read under ENV_LOCK:
    /// the CLI config unit tests clear SSO_GATEWAY_* under the same lock.
    fn harness_client_credentials() -> (String, String) {
        use crate::config::ENV_LOCK;
        let _guard = ENV_LOCK.lock().unwrap();
        let client_id = std::env::var("SSO_GATEWAY_CLIENT_ID").expect("SSO_GATEWAY_CLIENT_ID");
        let client_secret =
            std::env::var("SSO_GATEWAY_CLIENT_SECRET").expect("SSO_GATEWAY_CLIENT_SECRET");
        (client_id, client_secret)
    }

    /// Mint an access token for the harness's service application with the
    /// given scope, using the client_credentials grant.
    async fn fetch_service_token(sso_gateway_url: &str, scope: &str) -> String {
        let (client_id, client_secret) = harness_client_credentials();
        let resp: serde_json::Value = reqwest::Client::new()
            .post(format!("{sso_gateway_url}/oauth2/token"))
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("scope", scope),
            ])
            .send()
            .await
            .expect("token request failed")
            .error_for_status()
            .expect("token request rejected")
            .json()
            .await
            .expect("invalid token response");
        resp["access_token"]
            .as_str()
            .expect("missing access_token")
            .to_string()
    }

    /// Build a session client wired exactly like `server.rs`: introspection at
    /// the gateway with an OAuth2 client-credentials provider scoped
    /// `tenant:admin`.
    fn gateway_session_client(sso_gateway_url: &str) -> SsoGatewaySessionClient {
        let (client_id, client_secret) = harness_client_credentials();
        SsoGatewaySessionClient::new(
            format!("{sso_gateway_url}/oauth2/introspect"),
            OAuth2ClientCredentials::new(
                format!("{sso_gateway_url}/oauth2/token"),
                client_id,
                client_secret,
            )
            .with_scope("tenant:admin"),
        )
        .expect("client should build")
    }

    #[tokio::test]
    async fn introspects_real_token_against_sso_gateway() {
        let infra = crate::test_support::containers::setup().await;
        let token = fetch_service_token(&infra.sso_gateway_url, "permission:admin").await;

        let client = gateway_session_client(&infra.sso_gateway_url);
        let session = client
            .to_session(None, Some(&token))
            .await
            .expect("introspection against the real gateway should succeed");

        assert_eq!(session["tenant_id"], crate::test_support::test_tenant_id());
        assert!(
            session["sub"].as_str().is_some_and(|sub| !sub.is_empty()),
            "introspected session must carry a subject"
        );
    }

    /// The gateway rejects introspection calls whose bearer token lacks the
    /// `tenant:admin` scope, so a service token scoped only `permission:admin`
    /// must fail.
    #[tokio::test]
    async fn introspection_without_tenant_admin_scope_is_rejected() {
        let infra = crate::test_support::containers::setup().await;
        let limited_token = fetch_service_token(&infra.sso_gateway_url, "permission:admin").await;

        let client = SsoGatewaySessionClient::new(
            format!("{}/oauth2/introspect", infra.sso_gateway_url),
            BearerToken::new(limited_token.clone()),
        )
        .expect("client should build");

        let err = client
            .to_session(None, Some(&limited_token))
            .await
            .expect_err("introspection without tenant:admin must be rejected");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }
}
