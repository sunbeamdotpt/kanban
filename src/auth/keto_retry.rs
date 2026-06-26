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

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sunbeam_g2v::error::ServiceError;
use sunbeam_g2v::middleware::auth::keto::KetoClient;
use tokio::time::sleep;

const MAX_RETRIES: usize = 8;

/// Encode a subject identifier so it is safe for Keto relation tuples.
///
/// `sunbeam-g2v` 0.3.2 parses subjects containing `:` as subject sets, so
/// identifiers like `user:alice` must be encoded before being passed to Keto.
/// Base64 URL-safe encoding keeps the string deterministic, collision-free, and
/// free of Keto's subject-set delimiters.
pub(crate) fn keto_subject_id(subject: &str) -> String {
    URL_SAFE_NO_PAD.encode(subject.as_bytes())
}

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
        let subject_id = keto_subject_id(subject);
        Box::pin(retry(move || {
            let subject_id = subject_id.clone();
            async move { self.grant(namespace, object, relation, &subject_id).await }
        }))
    }

    fn check_permission_with_retry<'a>(
        &'a self,
        namespace: &'a str,
        object: &'a str,
        relation: &'a str,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, ServiceError>> + Send + 'a>> {
        let subject_id = keto_subject_id(subject);
        Box::pin(async move {
            // Keep a separate encoded subject for the post-check polling path;
            // the retry closure below takes ownership of the original.
            #[cfg(test)]
            let subject_id_for_wait = subject_id.clone();

            let allowed = retry(move || {
                let subject_id = subject_id.clone();
                async move {
                    self.check_permission(namespace, object, relation, &subject_id)
                        .await
                }
            })
            .await?;

            // In integration tests the in-memory SQLite Keto image can return
            // `allowed=false` immediately after a successful grant because the
            // read and write API ports do not always share the same connection
            // cache. Poll briefly so tests don't flake on read-after-write races.
            #[cfg(test)]
            let allowed = wait_for_allowed(
                self,
                namespace,
                object,
                relation,
                &subject_id_for_wait,
                allowed,
            )
            .await?;

            Ok(allowed)
        })
    }
}

/// Poll the Keto read endpoint briefly when the first check returns false.
///
/// This is only compiled for tests; it exists so production code keeps the
/// strict single-check behavior while tests can tolerate Keto's in-memory
/// read-after-write lag.
#[cfg(test)]
async fn wait_for_allowed(
    keto: &KetoClient,
    namespace: &str,
    object: &str,
    relation: &str,
    subject: &str,
    initial: bool,
) -> Result<bool, ServiceError> {
    if initial {
        return Ok(true);
    }

    let mut allowed = false;
    for _ in 0..20 {
        sleep(Duration::from_millis(25)).await;
        allowed = retry(|| keto.check_permission(namespace, object, relation, subject)).await?;
        if allowed {
            break;
        }
    }
    Ok(allowed)
}
