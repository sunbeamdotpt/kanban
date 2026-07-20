// SPDX-License-Identifier: AGPL-3.0-or-later
//! S3-compatible client for attachment storage (SeaweedFS, MinIO, AWS S3, etc.).
//!
//! This module implements AWS Signature V4 from scratch using only `hmac`,
//! `sha2` and `hex`, so it can presign and sign requests without spawning a
//! subprocess or pulling in the full AWS SDK.
//!
//! The presign flow follows the standard SigV4 query-string auth steps:
//!
//!   1. Collect and sort the `X-Amz-*` query parameters.
//!   2. Build the canonical request:
//!      `METHOD\npath\nquery\ncanonical_headers\nsigned_headers\nUNSIGNED-PAYLOAD`
//!   3. Build the string to sign:
//!      `algorithm\ndatestamp\nscope\nsha256(canonical_request)`
//!   4. Derive the signing key:
//!      `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), "s3"), "aws4_request")`
//!   5. Sign the string and hex-encode the result.
//!   6. Append `X-Amz-Signature` to the URL.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

// ── Config ───────────────────────────────────────────────────────────────────

/// S3 connection settings, normally read from environment variables.
#[derive(Clone, Debug)]
pub struct S3Config {
    /// S3-compatible endpoint, e.g. `http://localhost:9000`.
    pub endpoint: String,
    /// AWS region, e.g. `us-east-1`.
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
}

impl S3Config {
    /// Load configuration from the standard `S3_*` environment variables.
    pub fn from_env() -> Self {
        Self {
            endpoint: std::env::var("S3_ENDPOINT").unwrap_or_else(|_| {
                "http://seaweedfs-filer.storage.svc.cluster.local:8333".to_string()
            }),
            region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key: std::env::var("S3_ACCESS_KEY").unwrap_or_default(),
            secret_key: std::env::var("S3_SECRET_KEY").unwrap_or_default(),
            bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "sunbeam-kanban".to_string()),
        }
    }
}

// ── Result types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct HeadResult {
    pub size: i64,
    pub content_type: String,
}

// ── S3Client ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct S3Client {
    config: S3Config,
    http: reqwest::Client,
}

impl S3Client {
    pub fn new(config: S3Config) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    /// Build the full URL for an object key: `/{bucket}/{key}`.
    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.config.endpoint.trim_end_matches('/'),
            self.config.bucket,
            key
        )
    }

    // ── Presign helpers ───────────────────────────────────────────────────────

    /// Generate a presigned PUT URL that is valid for `expires_in` seconds.
    pub fn presign_put(&self, key: &str, mime_type: &str, expires_in: u64) -> String {
        self.presign("PUT", key, expires_in, Some(("content-type", mime_type)))
    }

    /// Generate a presigned GET URL that is valid for `expires_in` seconds.
    pub fn presign_get(&self, key: &str, expires_in: u64) -> String {
        self.presign("GET", key, expires_in, None)
    }

    /// Build a SigV4 signed URL using query-string authentication.
    fn presign(
        &self,
        method: &str,
        key: &str,
        expires_in: u64,
        extra_header: Option<(&str, &str)>,
    ) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let now_secs = now.as_secs();

        // Format timestamps: YYYYMMDDTHHmmssZ and YYYYMMDD
        let date_stamp = format_iso8601(now_secs); // e.g. "20240101T120000Z"
        let short_date = &date_stamp[..8]; // e.g. "20240101"
        let scope = format!("{}/{}/s3/aws4_request", short_date, self.config.region);

        // Construct the object URL path
        let path = format!("/{}/{}", self.config.bucket, key);
        let host = endpoint_host(&self.config.endpoint);

        // Canonical headers: host + optional content-type (for PUT)
        // Note: for presigned URLs, headers must be minimal (browser will send them).
        let mut signed_header_names: Vec<String> = vec!["host".to_string()];
        let mut canonical_headers_map: Vec<(String, String)> =
            vec![("host".to_string(), host.clone())];

        if let Some((hdr_name, hdr_val)) = extra_header {
            let name_lc = hdr_name.to_lowercase();
            canonical_headers_map.push((name_lc.clone(), hdr_val.to_string()));
            signed_header_names.push(name_lc);
        }

        // Sort by header name
        canonical_headers_map.sort_by(|a, b| a.0.cmp(&b.0));
        signed_header_names.sort();

        let signed_headers_str = signed_header_names.join(";");

        // Build canonical query string (params sorted by key)
        let credential = format!("{}/{}", self.config.access_key, scope);
        let mut params: Vec<(String, String)> = vec![
            (
                "X-Amz-Algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            ("X-Amz-Credential".to_string(), credential),
            ("X-Amz-Date".to_string(), date_stamp.clone()),
            ("X-Amz-Expires".to_string(), expires_in.to_string()),
            (
                "X-Amz-SignedHeaders".to_string(),
                signed_headers_str.clone(),
            ),
        ];
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_qs = params
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        // Canonical headers string
        let canonical_headers = canonical_headers_map
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        // Canonical request
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            method, path, canonical_qs, canonical_headers, signed_headers_str
        );

        // String to sign
        let cr_hash = sha256_hex(canonical_request.as_bytes());
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date_stamp, scope, cr_hash);

        // Signing key
        let signing_key =
            derive_signing_key(&self.config.secret_key, short_date, &self.config.region);

        // Signature
        let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

        // Build final URL
        let mut final_params = params;
        final_params.push(("X-Amz-Signature".to_string(), signature));
        final_params.sort_by(|a, b| a.0.cmp(&b.0));
        let final_qs = final_params
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        format!(
            "{}{}?{}",
            self.config.endpoint.trim_end_matches('/'),
            path,
            final_qs
        )
    }

    // ── Real HTTP operations ──────────────────────────────────────────────────

    /// Send a HEAD request for an object and return its size and content type.
    pub async fn head_object(&self, key: &str) -> Result<HeadResult, S3Error> {
        let url = self.object_url(key);
        let signed_headers = self.sign_request_headers("HEAD", key, "");

        let mut req = self.http.head(&url);
        for (k, v) in &signed_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let resp = req.send().await.map_err(|e| S3Error::Http(e.to_string()))?;

        match resp.status().as_u16() {
            200 => {
                let size = resp
                    .headers()
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let content_type = resp
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/octet-stream")
                    .to_string();
                Ok(HeadResult { size, content_type })
            }
            404 => Err(S3Error::NotFound),
            403 => Err(S3Error::Forbidden),
            status => Err(S3Error::Unexpected(status, "HEAD failed".to_string())),
        }
    }

    /// Delete an object from S3.
    pub async fn delete_object(&self, key: &str) -> Result<(), S3Error> {
        let url = self.object_url(key);
        let path = format!("/{}/{}", self.config.bucket, key);
        let signed_headers = self.sign_request("DELETE", &path, "");

        let mut req = self.http.delete(&url);
        for (k, v) in &signed_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let resp = req.send().await.map_err(|e| S3Error::Http(e.to_string()))?;
        let status = resp.status().as_u16();

        match status {
            200 | 204 | 404 => Ok(()), // 404 is OK — already gone
            403 => Err(S3Error::Forbidden),
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(S3Error::Unexpected(status, body))
            }
        }
    }

    /// Create the configured bucket. Treats "already exists" (409) as success.
    pub async fn create_bucket(&self) -> Result<(), S3Error> {
        let url = format!(
            "{}/{}",
            self.config.endpoint.trim_end_matches('/'),
            self.config.bucket
        );
        let path = format!("/{}", self.config.bucket);
        let signed_headers = self.sign_request("PUT", &path, "");

        let mut req = self.http.put(&url);
        for (k, v) in &signed_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let resp = req.send().await.map_err(|e| S3Error::Http(e.to_string()))?;
        match resp.status().as_u16() {
            200 | 204 => Ok(()),
            409 => Ok(()), // bucket already exists
            403 => Err(S3Error::Forbidden),
            status => {
                let body = resp.text().await.unwrap_or_default();
                Err(S3Error::Unexpected(status, body))
            }
        }
    }

    /// Build the `Authorization`, `x-amz-date` and `x-amz-content-sha256`
    /// headers for a normal (non-presigned) request.
    ///
    /// Returns header key/value pairs that should be added to the outgoing
    /// request.
    fn sign_request_headers(
        &self,
        method: &str,
        key: &str,
        body_hash: &str,
    ) -> Vec<(String, String)> {
        let path = format!("/{}/{}", self.config.bucket, key);
        self.sign_request(method, &path, body_hash)
    }

    /// Sign an arbitrary request path with SigV4.
    fn sign_request(&self, method: &str, path: &str, body_hash: &str) -> Vec<(String, String)> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let date_stamp = format_iso8601(now.as_secs());
        let short_date = date_stamp[..8].to_string();
        let scope = format!("{}/{}/s3/aws4_request", short_date, self.config.region);
        let host = endpoint_host(&self.config.endpoint);

        let body_hash = if body_hash.is_empty() {
            // SHA256 of empty body
            sha256_hex(b"")
        } else {
            body_hash.to_string()
        };

        // Headers to sign (sorted)
        let mut headers_to_sign: Vec<(String, String)> = vec![
            ("host".to_string(), host.clone()),
            ("x-amz-content-sha256".to_string(), body_hash.clone()),
            ("x-amz-date".to_string(), date_stamp.clone()),
        ];
        headers_to_sign.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers_str = headers_to_sign
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical_headers = headers_to_sign
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method, path, canonical_headers, signed_headers_str, body_hash
        );

        let cr_hash = sha256_hex(canonical_request.as_bytes());
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date_stamp, scope, cr_hash);

        let signing_key =
            derive_signing_key(&self.config.secret_key, &short_date, &self.config.region);
        let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.config.access_key, scope, signed_headers_str, signature
        );

        let mut result: Vec<(String, String)> = headers_to_sign
            .into_iter()
            .filter(|(k, _)| k != "host") // host is set by reqwest
            .collect();
        result.push(("Authorization".to_string(), auth_header));
        result
    }
}

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum S3Error {
    NotFound,
    Forbidden,
    Http(String),
    Unexpected(u16, String),
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::NotFound => write!(f, "S3 object not found"),
            S3Error::Forbidden => write!(f, "S3 access forbidden"),
            S3Error::Http(e) => write!(f, "S3 HTTP error: {e}"),
            S3Error::Unexpected(status, body) => write!(f, "S3 unexpected {status}: {body}"),
        }
    }
}

impl std::error::Error for S3Error {}

// ── Crypto helpers ───────────────────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    hex::encode(hash)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(m) => m,
        Err(_) => panic!("HMAC accepts any key size"),
    };
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

/// Derive the SigV4 signing key from the secret key, date and region.
fn derive_signing_key(secret: &str, short_date: &str, region: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), short_date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

/// Format a Unix timestamp in SigV4's ISO 8601 basic form: `YYYYMMDDTHHmmssZ`.
fn format_iso8601(secs: u64) -> String {
    // Manual implementation — no chrono needed here, and avoids a format
    // dependency in the crypto path.
    let s = secs;
    // Days from epoch to compute date fields.
    // Using chrono from workspace is fine here; it's already a dep.
    use chrono::{TimeZone, Utc};
    let dt = match Utc.timestamp_opt(s as i64, 0).single() {
        Some(dt) => dt,
        None => panic!("valid timestamp"),
    };
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Return the host (and optional port) portion of an endpoint URL.
fn endpoint_host(endpoint: &str) -> String {
    // Parse just enough to get host:port without pulling in the url crate.
    // Strip scheme prefix then take up to the first '/'.
    let without_scheme = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .to_string()
}

/// Percent-encode a string according to RFC 3986.
///
/// Keeps unreserved characters (`A-Z`, `a-z`, `0-9`, `-`, `_`, `.`, `~`)
/// untouched and escapes everything else.
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{byte:02X}"));
            }
        }
    }
    out
}

// ── Filename sanitization ────────────────────────────────────────────────────

/// Remove path separators and control characters from a filename.
///
/// The result is safe to use as a single path component.
pub fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_control() && *c != '/' && *c != '\\' && *c != '\0')
        .collect::<String>()
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_encode_encodes_special_chars() {
        assert_eq!(uri_encode("hello world"), "hello%20world");
        assert_eq!(uri_encode("foo/bar"), "foo%2Fbar");
        assert_eq!(uri_encode("key=val"), "key%3Dval");
    }

    #[test]
    fn uri_encode_leaves_unreserved_intact() {
        assert_eq!(uri_encode("abcABC123-_.~"), "abcABC123-_.~");
    }

    #[test]
    fn endpoint_host_strips_scheme_and_path() {
        assert_eq!(endpoint_host("http://localhost:9000"), "localhost:9000");
        assert_eq!(
            endpoint_host("https://s3.example.com/bucket"),
            "s3.example.com"
        );
        assert_eq!(endpoint_host("http://seaweedfs:8333"), "seaweedfs:8333");
    }

    #[test]
    fn sanitize_filename_strips_path_separators() {
        assert_eq!(sanitize_filename("../../etc/passwd"), "....etcpasswd");
        assert_eq!(sanitize_filename("normal file.pdf"), "normal file.pdf");
        assert_eq!(sanitize_filename("path/to/file.txt"), "pathtofile.txt");
    }

    #[test]
    fn derive_signing_key_is_deterministic() {
        let k1 = derive_signing_key("mysecret", "20240101", "us-east-1");
        let k2 = derive_signing_key("mysecret", "20240101", "us-east-1");
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 32);
    }

    #[test]
    fn presign_put_contains_amz_signature() {
        let cfg = S3Config {
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            access_key: "testkey".to_string(),
            secret_key: "testsecret".to_string(),
            bucket: "test-bucket".to_string(),
        };
        let client = S3Client::new(cfg);
        let url = client.presign_put("kanban/cards/test/file.pdf", "application/pdf", 900);
        assert!(
            url.contains("X-Amz-Signature="),
            "presigned URL must contain X-Amz-Signature"
        );
        assert!(
            url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"),
            "must include algorithm"
        );
        assert!(url.contains("test-bucket"), "must reference the bucket");
    }

    #[test]
    fn presign_get_contains_amz_signature() {
        let cfg = S3Config {
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            access_key: "testkey".to_string(),
            secret_key: "testsecret".to_string(),
            bucket: "test-bucket".to_string(),
        };
        let client = S3Client::new(cfg);
        let url = client.presign_get("kanban/cards/test/file.pdf", 300);
        assert!(
            url.contains("X-Amz-Signature="),
            "presigned GET URL must contain X-Amz-Signature"
        );
    }

    #[test]
    fn s3_config_from_env_uses_defaults_when_unset() {
        use crate::config::ENV_LOCK;
        let _guard = ENV_LOCK.lock().unwrap();

        for key in [
            "S3_ENDPOINT",
            "S3_REGION",
            "S3_ACCESS_KEY",
            "S3_SECRET_KEY",
            "S3_BUCKET",
        ] {
            // SAFETY: test-only env manipulation serialized through ENV_LOCK.
            unsafe { std::env::remove_var(key) };
        }

        let cfg = S3Config::from_env();
        assert_eq!(
            cfg.endpoint,
            "http://seaweedfs-filer.storage.svc.cluster.local:8333"
        );
        assert_eq!(cfg.region, "us-east-1");
        assert!(cfg.access_key.is_empty());
        assert!(cfg.secret_key.is_empty());
        assert_eq!(cfg.bucket, "sunbeam-kanban");
    }

    #[test]
    fn s3_config_from_env_reads_overrides() {
        use crate::config::ENV_LOCK;
        let _guard = ENV_LOCK.lock().unwrap();

        // SAFETY: test-only env manipulation serialized through ENV_LOCK.
        unsafe { std::env::set_var("S3_ENDPOINT", "http://minio:9000") };
        // SAFETY: test-only env manipulation serialized through ENV_LOCK.
        unsafe { std::env::set_var("S3_REGION", "eu-west-1") };
        // SAFETY: test-only env manipulation serialized through ENV_LOCK.
        unsafe { std::env::set_var("S3_ACCESS_KEY", "access") };
        // SAFETY: test-only env manipulation serialized through ENV_LOCK.
        unsafe { std::env::set_var("S3_SECRET_KEY", "secret") };
        // SAFETY: test-only env manipulation serialized through ENV_LOCK.
        unsafe { std::env::set_var("S3_BUCKET", "bucket") };

        let cfg = S3Config::from_env();
        assert_eq!(cfg.endpoint, "http://minio:9000");
        assert_eq!(cfg.region, "eu-west-1");
        assert_eq!(cfg.access_key, "access");
        assert_eq!(cfg.secret_key, "secret");
        assert_eq!(cfg.bucket, "bucket");

        for key in [
            "S3_ENDPOINT",
            "S3_REGION",
            "S3_ACCESS_KEY",
            "S3_SECRET_KEY",
            "S3_BUCKET",
        ] {
            // SAFETY: test-only env manipulation serialized through ENV_LOCK.
            unsafe { std::env::remove_var(key) };
        }
    }
}
