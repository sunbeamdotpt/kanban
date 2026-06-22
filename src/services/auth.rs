// SPDX-License-Identifier: AGPL-3.0-or-later
//! AuthService implementation.
//!
//! Exposes `WhoAmI` to return the caller's identity from the JWT, and
//! `SignalLogout` to record a logout watermark in Valkey so that later
//! requests with an older `iat` are rejected.
//!
//! Future improvement: call Kratos `/sessions/whoami` to enrich the response
//! with profile data (avatar URL, traits) that may not be present in the JWT.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::logout_watermark::LogoutWatermark;
use crate::pb::auth_service_server::AuthService;
use crate::pb::{SignalLogoutResponse, WhoAmIResponse};

pub struct AuthServiceImpl {
    pub watermark: Arc<LogoutWatermark>,
}

#[tonic::async_trait]
impl AuthService for AuthServiceImpl {
    async fn who_am_i(&self, request: Request<()>) -> Result<Response<WhoAmIResponse>, Status> {
        let auth = request
            .extensions()
            .get::<AuthContext>()
            .cloned()
            .ok_or_else(|| Status::internal("AuthContext extension missing"))?;

        if !auth.is_authenticated {
            return Err(Status::unauthenticated("not authenticated"));
        }

        let subject = auth
            .subject
            .clone()
            .ok_or_else(|| Status::unauthenticated("not authenticated"))?;

        let (display_name, email, roles, expires_at_ms) = if let Some(claims) = &auth.claims {
            let display_name = claims
                .extra
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned())
                .or_else(|| {
                    // Fallback 1: email local part.
                    claims
                        .extra
                        .get("email")
                        .and_then(|v| v.as_str())
                        .and_then(|e| e.split('@').next())
                        .map(|s| s.to_owned())
                })
                .unwrap_or_else(|| subject.clone());

            let email = claims
                .extra
                .get("email")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();

            let roles: Vec<String> = claims
                .extra
                .get("roles")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_owned()))
                        .collect()
                })
                .unwrap_or_default();

            let expires_at_ms = claims.exp * 1000;

            (display_name, email, roles, expires_at_ms)
        } else {
            // No claims: fallback display_name to subject, empty email/roles, 0 expiry.
            (subject.clone(), String::new(), vec![], 0i64)
        };

        Ok(Response::new(WhoAmIResponse {
            subject,
            display_name,
            email,
            roles,
            expires_at_ms,
        }))
    }

    async fn signal_logout(
        &self,
        request: Request<()>,
    ) -> Result<Response<SignalLogoutResponse>, Status> {
        let auth = request
            .extensions()
            .get::<AuthContext>()
            .cloned()
            .ok_or_else(|| Status::internal("AuthContext extension missing"))?;

        if !auth.is_authenticated {
            return Err(Status::unauthenticated("not authenticated"));
        }

        let subject = auth
            .subject
            .clone()
            .ok_or_else(|| Status::unauthenticated("not authenticated"))?;

        let watermark_ms = self
            .watermark
            .signal_logout(&subject)
            .await
            .map_err(|e| {
                tracing::error!(subject = %subject, error = %e, "signal_logout: watermark write failed");
                Status::internal("logout watermark write failed")
            })?;

        Ok(Response::new(SignalLogoutResponse {
            subject,
            watermark_ms: watermark_ms as i64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use serde_json::Value as JsonValue;
    use sunbeam_g2v::middleware::auth::jwt::JwtClaims;

    fn valkey_url() -> Option<String> {
        std::env::var("VALKEY_URL").ok()
    }

    fn make_watermark(url: &str) -> Arc<LogoutWatermark> {
        Arc::new(LogoutWatermark::new(url).expect("LogoutWatermark::new failed"))
    }

    fn make_service(watermark: Arc<LogoutWatermark>) -> AuthServiceImpl {
        AuthServiceImpl { watermark }
    }

    fn minimal_claims(sub: &str) -> JwtClaims {
        JwtClaims {
            sub: sub.to_string(),
            iat: 0,
            exp: i64::MAX / 1000,
            iss: None,
            aud: None,
            extra: HashMap::new(),
        }
    }

    fn claims_with_extras(sub: &str, extras: HashMap<String, JsonValue>) -> JwtClaims {
        JwtClaims {
            sub: sub.to_string(),
            iat: 0,
            exp: i64::MAX / 1000,
            iss: None,
            aud: None,
            extra: extras,
        }
    }

    fn request_with_auth<T>(body: T, auth: AuthContext) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut().insert(auth);
        req
    }

    // ── WhoAmI ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn whoami_returns_subject_from_auth_context() {
        // Unit test — no Valkey needed; watermark is never called by WhoAmI.
        // We still need an Arc<LogoutWatermark> for construction; use a dummy URL
        // that will never be dialled in this test.
        let wm = Arc::new(
            LogoutWatermark::new("redis://127.0.0.1:6399").expect("client construction is lazy"),
        );
        let svc = make_service(wm);

        let auth = AuthContext::authenticated(
            "user:test-whoami",
            Some(minimal_claims("user:test-whoami")),
        );
        let req = request_with_auth((), auth);

        let resp = svc.who_am_i(req).await.expect("WhoAmI should succeed");
        assert_eq!(resp.get_ref().subject, "user:test-whoami");
    }

    #[tokio::test]
    async fn whoami_pulls_display_name_from_claims_extra_name() {
        let wm = Arc::new(LogoutWatermark::new("redis://127.0.0.1:6399").unwrap());
        let svc = make_service(wm);

        let mut extras = HashMap::new();
        extras.insert("name".to_string(), JsonValue::String("Sienna".to_string()));

        let auth = AuthContext::authenticated(
            "user:sienna",
            Some(claims_with_extras("user:sienna", extras)),
        );
        let req = request_with_auth((), auth);

        let resp = svc.who_am_i(req).await.expect("WhoAmI should succeed");
        assert_eq!(resp.get_ref().display_name, "Sienna");
    }

    #[tokio::test]
    async fn whoami_pulls_email_from_claims_extra_email() {
        let wm = Arc::new(LogoutWatermark::new("redis://127.0.0.1:6399").unwrap());
        let svc = make_service(wm);

        let mut extras = HashMap::new();
        extras.insert(
            "email".to_string(),
            JsonValue::String("sienna@r3t.io".to_string()),
        );

        let auth = AuthContext::authenticated(
            "user:sienna",
            Some(claims_with_extras("user:sienna", extras)),
        );
        let req = request_with_auth((), auth);

        let resp = svc.who_am_i(req).await.expect("WhoAmI should succeed");
        assert_eq!(resp.get_ref().email, "sienna@r3t.io");
    }

    #[tokio::test]
    async fn whoami_returns_unauthenticated_when_no_auth_context() {
        let wm = Arc::new(LogoutWatermark::new("redis://127.0.0.1:6399").unwrap());
        let svc = make_service(wm);

        let auth = AuthContext::unauthenticated();
        let req = request_with_auth((), auth);

        let err = svc
            .who_am_i(req)
            .await
            .expect_err("should be unauthenticated");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    // ── SignalLogout ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn signal_logout_writes_watermark_to_valkey() {
        let url = valkey_url().expect("VALKEY_URL not set");
        let wm = make_watermark(&url);
        let svc = make_service(Arc::clone(&wm));

        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let subject = format!("user:test-sl-write-{ts}");

        let auth = AuthContext::authenticated(&subject, Some(minimal_claims(&subject)));
        let req = request_with_auth((), auth);

        let resp = svc
            .signal_logout(req)
            .await
            .expect("SignalLogout should succeed");
        let returned_wm = resp.get_ref().watermark_ms;
        assert_eq!(resp.get_ref().subject, subject);
        assert!(returned_wm > 0, "watermark_ms must be positive");

        // Confirm persisted in Valkey by reading it back directly.
        let stored = wm
            .watermark_for(&subject)
            .await
            .expect("watermark_for failed");
        assert_eq!(
            stored as i64, returned_wm,
            "stored watermark must match returned value"
        );
    }

    #[tokio::test]
    async fn signal_logout_subsequent_call_increases_watermark() {
        let url = valkey_url().expect("VALKEY_URL not set");
        let wm = make_watermark(&url);
        let svc = make_service(Arc::clone(&wm));

        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let subject = format!("user:test-sl-seq-{ts}");

        let auth1 = AuthContext::authenticated(&subject, Some(minimal_claims(&subject)));
        let resp1 = svc
            .signal_logout(request_with_auth((), auth1))
            .await
            .expect("first SignalLogout");
        let wm1 = resp1.get_ref().watermark_ms;

        // Small delay to guarantee clock advances.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        let auth2 = AuthContext::authenticated(&subject, Some(minimal_claims(&subject)));
        let resp2 = svc
            .signal_logout(request_with_auth((), auth2))
            .await
            .expect("second SignalLogout");
        let wm2 = resp2.get_ref().watermark_ms;

        assert!(
            wm2 > wm1,
            "second watermark ({wm2}) must exceed first ({wm1})"
        );
    }
}
