// SPDX-License-Identifier: AGPL-3.0-or-later
//! sso-gateway introspection session client.
//!
//! `sunbeam-g2v` 0.5's `IntrospectionSessionClient` normalizes the OAuth2
//! introspection response into a generic session shape that drops the
//! `tenant_id` field returned by sso-gateway. This client preserves that field
//! so the auth middleware can inject `TenantId` into request extensions.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sunbeam_g2v::middleware::auth::{
    AuthError, introspection::IntrospectionConfig, session::SessionClient,
};

/// Session client that introspects bearer tokens at the sso-gateway
/// `/oauth2/introspect` endpoint and preserves the `tenant_id` claim.
#[derive(Clone, Debug)]
pub struct SsoGatewaySessionClient {
    client: reqwest::Client,
    config: IntrospectionConfig,
}

impl SsoGatewaySessionClient {
    /// Create a client from configuration.
    pub fn new(config: IntrospectionConfig) -> Result<Self, AuthError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| {
                AuthError::InvalidSession(format!("failed to build introspection HTTP client: {e}"))
            })?;
        Ok(Self { client, config })
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
        let token = token.ok_or_else(|| AuthError::InvalidSession("missing token".to_string()))?;

        let response = self
            .client
            .post(&self.config.url)
            .basic_auth(&self.config.client_id, Some(&self.config.client_secret))
            .form(&[("token", token)])
            .send()
            .await
            .map_err(|e| AuthError::InvalidSession(format!("introspection request failed: {e}")))?;

        if !response.status().is_success() {
            return Err(AuthError::InvalidSession(format!(
                "introspection returned {}",
                response.status()
            )));
        }

        let body: Value = response.json().await.map_err(|e| {
            AuthError::InvalidSession(format!("invalid introspection response: {e}"))
        })?;

        let active = body
            .get("active")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !active {
            return Err(AuthError::InvalidSession("token inactive".to_string()));
        }

        let subject = body
            .get("sub")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if subject.is_empty() {
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
        http::StatusCode,
        response::IntoResponse,
        routing::post,
    };
    use serde::Deserialize;
    use serde_json::json;
    use std::net::SocketAddr;

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
        SsoGatewaySessionClient::new(IntrospectionConfig {
            url: format!("http://{addr}/oauth2/introspect"),
            client_id: "test-client".to_string(),
            client_secret: "test-secret".to_string(),
        })
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
    async fn to_session_rejects_non_success_status() {
        let (addr, _handle) = start_mock_introspection_server(|_| {
            (
                StatusCode::UNAUTHORIZED,
                json!({"error": "invalid_client"}),
            )
        })
        .await;

        let client = client_for(addr);
        let err = client
            .to_session(None, Some("any-token"))
            .await
            .expect_err("401 should fail");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }

    #[tokio::test]
    async fn to_session_rejects_missing_token() {
        let client = SsoGatewaySessionClient::new(IntrospectionConfig {
            url: "http://localhost:1/oauth2/introspect".to_string(),
            client_id: "x".to_string(),
            client_secret: "y".to_string(),
        })
        .expect("client should build");

        let err = client
            .to_session(None, None)
            .await
            .expect_err("missing token should fail");

        assert!(matches!(err, AuthError::InvalidSession(_)));
    }
}
