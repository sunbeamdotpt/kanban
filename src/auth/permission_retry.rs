// SPDX-License-Identifier: AGPL-3.0-or-later
//! Retry helpers for the sso-gateway permission backend.
//!
//! The sso-gateway is backed by Postgres, but transient network/connect errors
//! can still happen, so permission mutations and checks are retried a small
//! number of times on likely-transient failures.

use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;

use super::permission_client::{PermissionClient, PermissionError};

const MAX_RETRIES: usize = 8;

/// Returns `true` if a permission backend error looks transient.
fn is_retryable(e: &str) -> bool {
    let msg = e.to_lowercase();
    msg.contains("unavailable")
        || msg.contains("connection refused")
        || msg.contains("timeout")
        || msg.contains("unexpected end")
        || msg.contains("connection reset")
}

pub(crate) async fn retry<T, F, Fut>(mut f: F) -> Result<T, PermissionError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PermissionError>>,
{
    let mut last_err = None;
    for attempt in 0..MAX_RETRIES {
        match f().await {
            Ok(v) => return Ok(v),
            Err(PermissionError::Backend(ref msg)) if is_retryable(msg) => {
                last_err = Some(PermissionError::Backend(msg.clone()));
                sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err
        .unwrap_or_else(|| PermissionError::Backend("permission retry exhausted".to_string())))
}

/// Retry extension for permission operations.
pub trait PermissionRetryExt {
    /// Create a relation tuple, retrying on transient errors.
    fn grant_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), PermissionError>> + Send + 'a>>;

    /// Check a permission, retrying on transient errors.
    fn check_permission_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<bool, PermissionError>> + Send + 'a>>;
}

impl PermissionRetryExt for PermissionClient {
    fn grant_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), PermissionError>> + Send + 'a>> {
        Box::pin(retry(move || async move {
            self.grant(namespace, object, relation, subject).await
        }))
    }

    fn check_permission_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<bool, PermissionError>> + Send + 'a>> {
        Box::pin(async move {
            let allowed = retry(move || async move {
                self.check_permission(namespace, object, relation, subject)
                    .await
            })
            .await?;

            Ok(allowed)
        })
    }
}
