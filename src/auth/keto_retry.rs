// SPDX-License-Identifier: AGPL-3.0-or-later
//! Retry wrappers for Keto read/write operations.
//!
//! The in-memory SQLite Keto image used in integration tests serializes writes
//! poorly and occasionally returns "Unable to serialize access due to a
//! concurrent update in another session". These helpers retry on that specific
//! error so that service code does not need to sprinkle retries everywhere.
//!
//! They are thin wrappers around `sunbeam_g2v::middleware::auth::keto::KetoClient`;
//! non-retryable errors are returned immediately.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use sunbeam_g2v::error::ServiceError;
use sunbeam_g2v::middleware::auth::keto::KetoClient;
use tokio::time::sleep;

const MAX_RETRIES: usize = 8;

/// Returns `true` if a Keto error message looks like a transient SQLite
/// serialization conflict.
fn is_retryable(e: &str) -> bool {
    let msg = e.to_lowercase();
    msg.contains("unable to serialize access") || msg.contains("concurrent update")
}

pub(crate) async fn retry<T, F, Fut>(mut f: F) -> Result<T, ServiceError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ServiceError>>,
{
    let mut last_err = None;
    for attempt in 0..MAX_RETRIES {
        match f().await {
            Ok(v) => return Ok(v),
            Err(ServiceError::Internal(ref msg)) if is_retryable(msg) => {
                last_err = Some(ServiceError::Internal(msg.clone()));
                sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| ServiceError::Internal("keto retry exhausted".to_string())))
}

/// Keto operations with retry on transient serialization conflicts.
pub trait KetoRetryExt {
    /// Writes a relation tuple, retrying on transient serialization conflicts.
    fn grant_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + 'a>>;

    /// Checks a permission, retrying on transient serialization conflicts.
    fn check_permission_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, ServiceError>> + Send + 'a>>;
}

impl KetoRetryExt for KetoClient {
    fn grant_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + 'a>> {
        Box::pin(retry(|| self.grant(namespace, object, relation, subject)))
    }

    fn check_permission_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, ServiceError>> + Send + 'a>> {
        Box::pin(retry(|| {
            self.check_permission(namespace, object, relation, subject)
        }))
    }
}
