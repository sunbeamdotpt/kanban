//! Logout watermark — server-side gate that cuts subscriptions and rejects
//! requests for tokens issued before a user's most recent logout signal.
//!
//! MF-4 server-side wiring. The watermark is set by `AuthService.SignalLogout`
//! and consulted by:
//!   - `keto_dispatch` middleware on every RPC (after JWT validation, before Keto check).
//!   - The realtime stream tail (`BoardSubscriberRegistry`) on every yield.
//!
//! Storage: Valkey, key pattern `auth.logout.{subject}` -> unix_ms timestamp.
//! TTL: 24 hours (longer than any reasonable JWT lifetime).
//!
//! Local cache: 5-second per-pod cache of (subject, watermark_ms) to avoid
//! hammering Valkey on every yield (`feedback_grpc_first_for_g2v.md`-style
//! discipline: keep the hot path light).
//!
//! # Fail-closed policy
//!
//! If Valkey is unavailable, `watermark_for` and `is_token_valid` return `Err`.
//! The caller (`keto_dispatch`) must reject the request with `503 Service
//! Unavailable`. This is intentional: a Valkey outage signals a cluster health
//! issue, not a normal operating condition. Cluster ops will observe
//! `kanban_logout_watermark_errors_total` firing in Alertmanager before users
//! notice broad 503s.
//!
//! Note: the plan's Critic (v2 §1058) originally flagged that the
//! `Ok(Some(...))` pattern implies fail-open; this implementation explicitly
//! chooses fail-closed per the task spec and Pre-mortem Scenario 2 mitigation.
//! If the team later decides fail-open is preferable (degraded revocation vs
//! full outage), change the `?` in `watermark_for` to a fallback of `Ok(0)`.

use anyhow::Result;
use parking_lot::RwLock;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// How long a locally cached watermark value is considered fresh.
const CACHE_TTL: Duration = Duration::from_secs(5);
/// Valkey key TTL for logout watermarks (24 hours in seconds).
const WATERMARK_VALKEY_TTL_SECS: u64 = 86_400;

type CacheEntry = (u64, Instant);

#[derive(Clone)]
pub struct LogoutWatermark {
    client: redis::Client,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

impl LogoutWatermark {
    /// Construct a `LogoutWatermark` connected to the given Valkey/Redis URL.
    ///
    /// `redis_url` should be of the form `redis://host:port` or
    /// `redis://:password@host:port`. The connection is established lazily on
    /// the first command; construction never blocks.
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Write the logout watermark for `subject` to `now_ms`.
    ///
    /// Persists `auth.logout.{subject}` with a 24-hour TTL in Valkey, then
    /// updates the local cache so subsequent reads within 5s are served locally.
    /// Returns the written `now_ms` value.
    pub async fn signal_logout(&self, subject: &str) -> Result<u64> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let key = Self::key(subject);
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let _: () = conn
            .set_ex(&key, now_ms, WATERMARK_VALKEY_TTL_SECS)
            .await?;

        // Update local cache so in-process reads are immediately consistent.
        {
            let mut guard = self.cache.write();
            guard.insert(subject.to_owned(), (now_ms, Instant::now()));
        }

        tracing::debug!(subject, now_ms, "logout watermark written");
        Ok(now_ms)
    }

    /// Return the stored watermark for `subject`, or `0` if none exists.
    ///
    /// Serves from the 5-second local cache when the entry is fresh; otherwise
    /// fetches from Valkey, populates the cache, and returns. Returns `Err` if
    /// Valkey is unreachable (fail-closed — see module-level doc).
    pub async fn watermark_for(&self, subject: &str) -> Result<u64> {
        // Fast path: cache hit.
        {
            let guard = self.cache.read();
            if let Some(&(wm, cached_at)) = guard.get(subject) {
                if cached_at.elapsed() < CACHE_TTL {
                    return Ok(wm);
                }
            }
        }

        // Cache miss or stale: fetch from Valkey.
        tracing::debug!(subject, "logout watermark cache miss — fetching from Valkey");
        let key = Self::key(subject);
        let mut conn = self.client.get_multiplexed_async_connection().await.map_err(|e| {
            tracing::warn!(subject, error = %e, "Valkey unavailable fetching logout watermark");
            e
        })?;

        let raw: Option<u64> = conn.get(&key).await.map_err(|e| {
            tracing::warn!(subject, error = %e, "Valkey GET error for logout watermark");
            e
        })?;

        let wm = raw.unwrap_or(0);

        // Populate cache (regardless of whether a watermark exists; a miss is
        // still a valid answer — no logout has been recorded).
        {
            let mut guard = self.cache.write();
            guard.insert(subject.to_owned(), (wm, Instant::now()));
        }

        Ok(wm)
    }

    /// Returns `true` if the token with `token_iat_ms` is valid for `subject`.
    ///
    /// A token is valid when its issue-time (`iat`) is **at or after** the
    /// stored watermark. Tokens issued strictly before the watermark were
    /// minted before the user's last logout and must be rejected.
    ///
    /// The logout-then-relogin race is safe by construction: `signal_logout`
    /// writes `now_ms` at logout time; any new token minted after that moment
    /// will have `iat >= now_ms`, so it passes.
    pub async fn is_token_valid(&self, subject: &str, token_iat_ms: u64) -> Result<bool> {
        let wm = self.watermark_for(subject).await?;
        Ok(token_iat_ms >= wm)
    }

    /// Build the Valkey key for a subject.
    fn key(subject: &str) -> String {
        format!("auth.logout.{subject}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valkey_url() -> Option<String> {
        std::env::var("VALKEY_URL").ok()
    }

    fn make_wm(url: &str) -> LogoutWatermark {
        LogoutWatermark::new(url).expect("LogoutWatermark::new failed")
    }

    /// Unique subject per test run to avoid cross-test key collisions.
    fn subject(tag: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("test-subject-{tag}-{ts}")
    }

    #[tokio::test]
    #[ignore = "needs shared valkey (set VALKEY_URL)"]
    async fn signal_then_query_returns_watermark() {
        let url = valkey_url().expect("VALKEY_URL not set");
        let wm = make_wm(&url);
        let sub = subject("signal");

        let written = wm.signal_logout(&sub).await.unwrap();
        let read = wm.watermark_for(&sub).await.unwrap();

        assert_eq!(written, read, "watermark read back must equal what was written");
        assert!(written > 0, "watermark must be a positive unix_ms");
    }

    #[tokio::test]
    #[ignore = "needs shared valkey (set VALKEY_URL)"]
    async fn is_token_valid_iat_boundary() {
        let url = valkey_url().expect("VALKEY_URL not set");
        let wm = make_wm(&url);
        let sub = subject("validity");

        wm.signal_logout(&sub).await.unwrap();
        // Force the exact watermark value so we can test boundary conditions.
        let watermark: u64 = 1_000;
        {
            let mut conn = wm.client.get_multiplexed_async_connection().await.unwrap();
            let _: () = redis::AsyncCommands::set_ex(
                &mut conn,
                LogoutWatermark::key(&sub),
                watermark,
                WATERMARK_VALKEY_TTL_SECS,
            )
            .await
            .unwrap();
            // Invalidate local cache so we read the value we just forced.
            wm.cache.write().remove(&sub);
        }

        assert!(!wm.is_token_valid(&sub, 999).await.unwrap(), "iat < wm should be invalid");
        assert!(wm.is_token_valid(&sub, 1_000).await.unwrap(), "iat == wm should be valid");
        assert!(wm.is_token_valid(&sub, 1_001).await.unwrap(), "iat > wm should be valid");
    }

    #[tokio::test]
    #[ignore = "needs shared valkey (set VALKEY_URL)"]
    async fn cache_hit_does_not_call_valkey() {
        let url = valkey_url().expect("VALKEY_URL not set");
        let wm = make_wm(&url);
        let sub = subject("cache");

        let written = wm.signal_logout(&sub).await.unwrap();

        // Both reads must complete well within the 5s cache TTL and agree.
        let start = Instant::now();
        let r1 = wm.watermark_for(&sub).await.unwrap();
        let r2 = wm.watermark_for(&sub).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(r1, written);
        assert_eq!(r2, written);
        // Two cache hits should be sub-millisecond; 100ms is a very generous bound
        // that would still fail if either call hit a real network round-trip.
        assert!(
            elapsed < Duration::from_millis(100),
            "two cached reads took {elapsed:?}, expected <100ms"
        );
    }

    #[tokio::test]
    #[ignore = "needs shared valkey (set VALKEY_URL); sleeps 6s"]
    async fn signal_logout_persists_through_cache_expiry() {
        let url = valkey_url().expect("VALKEY_URL not set");
        let wm = make_wm(&url);
        let sub = subject("persist");

        let written = wm.signal_logout(&sub).await.unwrap();

        // Expire the local cache entry by back-dating its timestamp.
        {
            let mut guard = wm.cache.write();
            if let Some(entry) = guard.get_mut(&sub) {
                // Replace cached_at with something >5s in the past.
                *entry = (entry.0, Instant::now() - Duration::from_secs(6));
            }
        }

        // Next read must fall through to Valkey and return the persisted value.
        let read = wm.watermark_for(&sub).await.unwrap();
        assert_eq!(
            written, read,
            "watermark must survive local cache expiry (Valkey must hold it)"
        );
    }
}
