// SPDX-License-Identifier: AGPL-3.0-or-later
//! Shared helpers for integration tests.
//!
//! These helpers are only compiled during `cargo test`. They live in a
//! `#[cfg(test)]` module so every service can reuse seeders without copying the
//! foreign-key SQL needed to set up a test row.
//!
//! # Seeder contract
//!
//! Every `seed_*` function generates fresh ULIDs, so tests running in parallel
//! (for example under nextest) never collide on unique constraints.
//!
//! # Teardown
//!
//! Tests clean up after themselves. The seeders do not wrap inserts in a
//! transaction that gets rolled back; instead we rely on random ULIDs to keep
//! tests isolated on a shared Postgres instance.

use crate::id::Id;
use sqlx::{PgPool, Row};

/// Build a Connect-RPC [`ServiceRequest`] for calling service handlers
/// directly in tests.
///
/// Uses `Box::leak` to satisfy the `'static` bound; acceptable in tests where
/// each request lives for the process duration anyway.
#[cfg(test)]
pub(crate) fn connect_request<Req>(msg: &Req) -> connectrpc::ServiceRequest<'static, Req>
where
    Req: buffa::Message + buffa::HasMessageView,
{
    use buffa::MessageView;

    let bytes = bytes::Bytes::from(msg.encode_to_vec());
    let bytes: &'static bytes::Bytes = Box::leak(Box::new(bytes));
    let view = Req::View::decode_view(bytes).expect("test request should decode");
    let view: &'static Req::View<'static> = Box::leak(Box::new(view));
    connectrpc::ServiceRequest::from_parts(view, bytes)
}

/// Build a [`RequestContext`] carrying the given auth extensions, mirroring
/// what the auth middleware inserts in front of handlers.
#[cfg(test)]
#[allow(dead_code)] // used as services migrate to Connect-RPC handlers
pub(crate) fn connect_ctx(
    auth: sunbeam_g2v::middleware::auth::AuthContext,
) -> connectrpc::RequestContext {
    let mut ctx = connectrpc::RequestContext::default();
    ctx.extensions_mut().insert(auth);
    ctx
}

/// Create a minimal project and return its id.
pub(crate) async fn seed_project(pool: &PgPool) -> Id {
    let project_id = Id::new();
    let suffix = project_id.to_string();
    let tenant_id = test_tenant_id();

    sqlx::query(
        "INSERT INTO projects (id, tenant_id, name, slug, owner_id, created_at, updated_at)
         VALUES ($1, $2, $3, $4, 'user:test', now(), now())",
    )
    .bind(project_id)
    .bind(tenant_id)
    .bind(format!("test-proj-{}", &suffix[18..26]))
    .bind(suffix[14..26].to_lowercase())
    .execute(pool)
    .await
    .expect("seed_project: INSERT failed");

    project_id
}

/// Create a minimal board under `project_id` and return its id.
pub(crate) async fn seed_board(pool: &PgPool, tenant_id: &str, project_id: Id) -> Id {
    let board_id = Id::new();
    let suffix = board_id.to_string();

    sqlx::query(
        "INSERT INTO boards (id, tenant_id, project_id, name, slug, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, now(), now())",
    )
    .bind(board_id)
    .bind(tenant_id)
    .bind(project_id)
    .bind(format!("test-board-{}", &suffix[18..26]))
    .bind(suffix[14..26].to_lowercase())
    .execute(pool)
    .await
    .expect("seed_board: INSERT failed");

    board_id
}

/// Create a minimal column under `board_id` and return its id.
pub(crate) async fn seed_column(pool: &PgPool, tenant_id: &str, board_id: Id) -> Id {
    let column_id = Id::new();

    sqlx::query(
        "INSERT INTO columns (id, tenant_id, board_id, title, position, created_at, updated_at)
         VALUES ($1, $2, $3, 'todo', 0, now(), now())",
    )
    .bind(column_id)
    .bind(tenant_id)
    .bind(board_id)
    .execute(pool)
    .await
    .expect("seed_column: INSERT failed");

    column_id
}

/// Create a minimal card under `board_id`, `column_id`, and `project_id` and
/// return its id.
pub(crate) async fn seed_card(
    pool: &PgPool,
    tenant_id: &str,
    board_id: Id,
    column_id: Id,
    project_id: Id,
) -> Id {
    let card_id = Id::new();
    let card_suffix = card_id.to_string();

    sqlx::query(
        "INSERT INTO cards \
         (id, tenant_id, board_id, column_id, project_id, ref, title, position, revision, \
          created_by, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, 'test-card', 0, 0, 'user:test', now(), now())",
    )
    .bind(card_id)
    .bind(tenant_id)
    .bind(board_id)
    .bind(column_id)
    .bind(project_id)
    .bind(format!("T{}", card_suffix[20..26].to_uppercase()))
    .execute(pool)
    .await
    .expect("seed_card: INSERT failed");

    card_id
}

/// Create a complete project → board → column → card chain.
///
/// Returns `(project_id, board_id, column_id, card_id)` with fresh ULIDs so
/// parallel tests stay independent.
pub(crate) async fn seed_card_chain(pool: &PgPool) -> (Id, Id, Id, Id) {
    let tenant_id = test_tenant_id();
    let project_id = seed_project(pool).await;
    let board_id = seed_board(pool, &tenant_id, project_id).await;
    let column_id = seed_column(pool, &tenant_id, board_id).await;
    let card_id = seed_card(pool, &tenant_id, board_id, column_id, project_id).await;
    (project_id, board_id, column_id, card_id)
}

/// Insert a raw `event_log` row for testing the outbox dispatcher.
///
/// Returns the inserted row's id.
pub(crate) async fn seed_event_log(pool: &PgPool, board_id: Id, event_type: &str) -> Id {
    let row_id = Id::new();
    let tenant_id = test_tenant_id();
    let row = sqlx::query(
        "INSERT INTO event_log (id, tenant_id, board_id, event_type, payload, created_at)
         VALUES ($1, $2, $3, $4, '{}'::jsonb, now())
         RETURNING id",
    )
    .bind(row_id)
    .bind(tenant_id)
    .bind(board_id)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("seed_event_log: INSERT failed");

    row.get("id")
}

/// Insert a raw `event_log` row that is already marked as dispatched.
///
/// Returns the inserted row's id.
pub(crate) async fn seed_event_log_dispatched(
    pool: &PgPool,
    board_id: Id,
    event_type: &str,
    nats_seq: i64,
) -> Id {
    let row_id = Id::new();
    let tenant_id = test_tenant_id();
    let row = sqlx::query(
        "INSERT INTO event_log \
         (id, tenant_id, board_id, event_type, payload, nats_seq, dispatched_at, created_at)
         VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, now(), now())
         RETURNING id",
    )
    .bind(row_id)
    .bind(tenant_id)
    .bind(board_id)
    .bind(event_type)
    .bind(nats_seq)
    .fetch_one(pool)
    .await
    .expect("seed_event_log_dispatched: INSERT failed");

    row.get("id")
}

/// Start the shared testcontainers stack (if not already started) and return a
/// Postgres pool. Tests can call this directly instead of relying on the
/// removed ctor/dtor harness.
#[cfg(test)]
pub async fn setup_pool() -> PgPool {
    containers::setup().await.pool.clone()
}

/// Start the shared testcontainers stack (if not already started) and return a
/// permission client backed by the sso-gateway.
#[cfg(test)]
pub async fn setup_permission() -> std::sync::Arc<crate::auth::permission_client::PermissionClient>
{
    containers::setup().await.permission.clone()
}

/// Start the shared testcontainers stack (if not already started) and return an
/// identity client backed by the sso-gateway.
#[cfg(test)]
pub async fn setup_identity() -> std::sync::Arc<crate::auth::identity_client::IdentityClient> {
    containers::setup().await.identity.clone()
}

/// Start the shared testcontainers stack (if not already started) and return the
/// sso-gateway base URL.
#[cfg(test)]
pub async fn setup_sso_gateway_url() -> String {
    containers::setup().await.sso_gateway_url.clone()
}

/// Tenant ID of the Kanban service application provisioned by the test harness.
///
/// Read from `KANBAN_TEST_TENANT_ID`, which the container harness exports
/// during bootstrap. Tests that run before the harness starts (pure unit
/// tests that never reach the gateway) fall back to a placeholder value.
#[cfg(test)]
pub fn test_tenant_id() -> String {
    std::env::var("KANBAN_TEST_TENANT_ID").unwrap_or_else(|_| "test-tenant".to_string())
}

// ── Testcontainers-backed dependency harness ───────────────────────────────
//
// This module starts Postgres, NATS (JetStream), sso-gateway, MinIO, and
// OpenSearch in throwaway containers when `containers::setup()` is first called.
// Containers are started through the Docker-compatible API pointed at by
// `DOCKER_HOST`; testcontainers' host-port mapping is used, so the harness
// works natively with lima-docker and other remote Docker contexts.

#[cfg(test)]
pub(crate) mod containers {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use futures::FutureExt;
    use sqlx::postgres::PgPoolOptions;
    use sunbeam_g2v::client::{ClientBuilder, ConnectTransport};
    use sunbeam_g2v::config::NatsConfig;
    use sunbeam_g2v::mq::NatsClient;

    use crate::auth::identity_client::{IdentityClient, IdentityClientConfig};
    use crate::auth::permission_client::{
        KANBAN_NAMESPACE, PermissionClient, PermissionClientConfig,
    };
    use std::collections::HashMap;

    type PermissionClientCache = HashMap<(String, String, String), Arc<PermissionClient>>;
    type IdentityClientCache = HashMap<(String, String, String), Arc<IdentityClient>>;

    use crate::iam_proto::iam::v1::{
        ApplicationServiceClient, CreateApplicationRequest, CreateTenantRequest,
        RotateSecretRequest, TenantServiceClient,
    };
    use testcontainers::core::{ContainerPort, IntoContainerPort};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};
    use tokio::sync::OnceCell;
    use tokio::time::sleep;

    use sdk::testing::SsoGateway;
    use sdk::testing::sso_gateway::SsoGatewayHandle;

    use crate::integrations::s3::{S3Client, S3Config};

    /// Default container images for the harness. Override them with environment variables if needed.
    fn postgres_image() -> String {
        std::env::var("KANBAN_TEST_POSTGRES_IMAGE")
            .unwrap_or_else(|_| "mirror.gcr.io/library/postgres:16-alpine".to_string())
    }
    fn nats_image() -> String {
        std::env::var("KANBAN_TEST_NATS_IMAGE").unwrap_or_else(|_| "nats:2.10-alpine".to_string())
    }
    fn sso_gateway_image() -> String {
        std::env::var("KANBAN_TEST_SSO_GATEWAY_IMAGE")
            .unwrap_or_else(|_| "ghcr.io/sunbeamdotpt/sso-gateway:v2026.07.21".to_string())
    }
    fn minio_image() -> String {
        std::env::var("KANBAN_TEST_MINIO_IMAGE")
            .unwrap_or_else(|_| "minio/minio:RELEASE.2025-02-28T09-55-16Z".to_string())
    }
    fn opensearch_image() -> String {
        std::env::var("KANBAN_TEST_OPENSEARCH_IMAGE")
            .unwrap_or_else(|_| "opensearchproject/opensearch:2.19.1".to_string())
    }

    /// Fixed bootstrap secret shared with the sso-gateway container. Tests use
    /// the `system-bootstrap-client` credentials to obtain permission tokens.
    const SYSTEM_BOOTSTRAP_CLIENT_SECRET: &str =
        "sunbeam-test-bootstrap-secret-key-at-least-32-bytes";

    const MINIO_BUCKET: &str = "sunbeam-kanban";

    /// Detect a usable Docker-compatible socket and set DOCKER_HOST so that
    /// testcontainers works out of the box on macOS with lima-docker, Docker
    /// Desktop, socktainer, or a native Linux daemon.
    ///
    /// A socket file may exist even when its daemon is not running, so this
    /// function actually tries to connect rather than only checking existence.
    fn ensure_docker_host() {
        if std::env::var("DOCKER_HOST").is_ok() {
            return;
        }

        let home = std::env::var("HOME").unwrap_or_default();
        let candidates = [
            format!("{home}/.lima/docker/sock/docker.sock"),
            format!("{home}/.lima/sunbeam-docker/sock/docker.sock"),
            format!("{home}/.socktainer/container.sock"),
            format!("{home}/.docker/run/docker.sock"),
            "/var/run/docker.sock".to_string(),
        ];

        for path in &candidates {
            if std::path::Path::new(path).exists()
                && std::os::unix::net::UnixStream::connect(path).is_ok()
            {
                // SAFETY: test-harness bootstrap. Concurrent tests may race this
                // write, but every racer sets an identical value and DOCKER_HOST
                // is only read by testcontainers afterwards.
                unsafe {
                    std::env::set_var("DOCKER_HOST", format!("unix://{path}"));
                }
                return;
            }
        }
    }

    /// Clients for a single test's dependencies.
    ///
    /// A fresh `TestInfra` is returned on every call so that each test owns
    /// its own Postgres pool and clients. The heavy container handles are kept
    /// alive in the static `SHARED` singleton — and statics are never dropped,
    /// so the containers outlive the test process. `reap_stale_resources`
    /// removes the leftovers at the start of the next run (KANBAN-036).
    pub struct TestInfra {
        pub pool: sqlx::PgPool,
        pub nats: Arc<NatsClient>,
        pub permission: Arc<PermissionClient>,
        pub identity: Arc<IdentityClient>,
        pub sso_gateway_url: String,
    }

    /// Connection URLs, OAuth2 credentials, and container handles shared by the
    /// whole test process.
    struct SharedInfra {
        database_url: String,
        nats_url: String,
        sso_gateway_url: String,
        tenant_id: String,
        client_id: String,
        client_secret: String,
        _pg: Option<ContainerAsync<GenericImage>>,
        _nats: Option<ContainerAsync<GenericImage>>,
        _sso_gateway: Option<SsoGatewayHandle>,
        _minio: Option<ContainerAsync<GenericImage>>,
        _opensearch: Option<ContainerAsync<GenericImage>>,
    }

    impl SharedInfra {
        fn urls(&self) -> (String, String, String, String, String, String) {
            (
                self.database_url.clone(),
                self.nats_url.clone(),
                self.sso_gateway_url.clone(),
                self.tenant_id.clone(),
                self.client_id.clone(),
                self.client_secret.clone(),
            )
        }
    }

    static SHARED: OnceCell<SharedInfra> = OnceCell::const_new();

    /// Shared permission clients keyed by service credentials so that parallel
    /// tests do not each trigger a separate OAuth2 token fetch against the
    /// sso-gateway.
    static PERMISSION_CLIENT_CACHE: OnceCell<tokio::sync::Mutex<PermissionClientCache>> =
        OnceCell::const_new();

    /// Shared identity clients keyed by service credentials, for the same
    /// token-fetch deduplication as the permission cache.
    static IDENTITY_CLIENT_CACHE: OnceCell<tokio::sync::Mutex<IdentityClientCache>> =
        OnceCell::const_new();

    /// Set when the shared bootstrap panics inside the `SHARED` initializer.
    /// A panicked `OnceCell::get_or_init` leaves the cell empty, so without
    /// this guard every subsequent test would re-pay the full container
    /// bootstrap (and leak another container set) only to fail at the same
    /// place.
    static BOOTSTRAP_FAILED: AtomicBool = AtomicBool::new(false);

    // ── Startup reaper (KANBAN-036) ────────────────────────────────────────
    //
    // `SHARED` is a static and statics are never dropped, so the container
    // handles in `SharedInfra` never run their `Drop` impls: every
    // `cargo test` process leaks its whole stack (containers plus the
    // sso-gateway Docker network), and repeated runs exhaust Docker's address
    // pools. The reaper runs once per process at bootstrap, before any new
    // container is created, and removes leftovers from previous runs. The
    // 30-minute age cutoff is what makes this safe with concurrent
    // `cargo test` processes: their containers are younger than the cutoff
    // and are never touched.

    /// Only resources older than this are reaped, so a concurrent test run
    /// never loses its live containers. 10 minutes far exceeds any full-suite
    /// duration (~2–4 min), so overlapping runs are still safe while
    /// iteration loops stop accumulating stacks.
    const REAP_MIN_AGE_SECS: u64 = 10 * 60;

    /// Best-effort removal of stale containers and networks from previous
    /// test runs. Every failure is logged and swallowed; reaping must never
    /// abort the bootstrap.
    fn reap_stale_resources() {
        reap_stale_containers();
        reap_stale_sso_networks();
    }

    /// Remove testcontainers-managed and sso-gateway-stack containers older
    /// than [`REAP_MIN_AGE_SECS`]. The sdk `SsoGateway` stack containers are
    /// testcontainers-managed too, but listing them by name as well keeps the
    /// reaper working even if the label convention changes.
    fn reap_stale_containers() {
        let until = format!("until={}m", REAP_MIN_AGE_SECS / 60);
        let mut ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for filter in [
            "label=org.testcontainers.managed-by=testcontainers",
            // sdk `SsoGateway` stack containers are named
            // `sso<hex nanos>-{postgres,hydra,kratos,keto,openfga,gateway}`.
            "name=^sso[0-9a-f]+-",
        ] {
            match docker_lines(&["ps", "-aq", "--filter", filter, "--filter", &until]) {
                Ok(found) => ids.extend(found),
                Err(e) => tracing::warn!(error = %e, "reaper: failed to list stale containers"),
            }
        }

        if ids.is_empty() {
            tracing::debug!("reaper: no stale containers from previous test runs");
            return;
        }

        let count = ids.len();
        match std::process::Command::new("docker")
            .arg("rm")
            .arg("-f")
            .args(ids.iter().map(String::as_str))
            .output()
        {
            Ok(out) if out.status.success() => {
                tracing::info!(count, "reaped stale containers from previous test runs");
            }
            // `docker rm` removes what it can even when it exits non-zero.
            Ok(out) => tracing::warn!(
                count,
                stderr = %String::from_utf8_lossy(&out.stderr),
                "reaper: docker rm exited non-zero"
            ),
            Err(e) => tracing::warn!(error = %e, count, "reaper: failed to run docker rm"),
        }
    }

    /// Remove orphaned sso-gateway stack networks (`sso<hex nanos>-net`) that
    /// are older than [`REAP_MIN_AGE_SECS`] and have no attached containers.
    /// The age is decoded from the nanosecond timestamp the sdk embeds in the
    /// name, so no Docker timestamp parsing is needed. Networks that cannot
    /// be identified as harness-created are left alone; never run a blanket
    /// `docker network prune` (it would delete unrelated user networks).
    fn reap_stale_sso_networks() {
        let names = match docker_lines(&[
            "network",
            "ls",
            "--filter",
            "name=^sso[0-9a-f]+-net$",
            "--format",
            "{{.Name}}",
        ]) {
            Ok(names) => names,
            Err(e) => {
                tracing::warn!(error = %e, "reaper: failed to list stale networks");
                return;
            }
        };

        let mut reaped = 0usize;
        for name in names {
            if !sso_name_is_stale(&name, "-net") {
                continue;
            }
            // Only remove networks with no attached containers; `{{json
            // .Containers}}` renders as `null` (or `{}`) when empty.
            match docker_lines(&["network", "inspect", "-f", "{{json .Containers}}", &name]) {
                Ok(lines) => {
                    let empty = lines.is_empty() || lines.iter().all(|l| l == "null" || l == "{}");
                    if !empty {
                        continue;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        network = %name,
                        "reaper: failed to inspect network"
                    );
                    continue;
                }
            }
            match std::process::Command::new("docker")
                .args(["network", "rm", &name])
                .output()
            {
                Ok(out) if out.status.success() => reaped += 1,
                Ok(out) => tracing::warn!(
                    network = %name,
                    stderr = %String::from_utf8_lossy(&out.stderr),
                    "reaper: docker network rm exited non-zero"
                ),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        network = %name,
                        "reaper: failed to run docker network rm"
                    );
                }
            }
        }

        if reaped > 0 {
            tracing::info!(
                reaped,
                "reaped stale sso-gateway networks from previous test runs"
            );
        } else {
            tracing::debug!("reaper: no stale sso-gateway networks");
        }
    }

    /// Decode the age of an sdk `SsoGateway` stack resource from its name.
    /// The stack names everything `sso<hex nanos since UNIX_EPOCH><suffix>`,
    /// so the creation time is embedded in the name itself. Returns false for
    /// anything that does not match the convention exactly.
    fn sso_name_is_stale(name: &str, suffix: &str) -> bool {
        let Some(hex) = name
            .strip_prefix("sso")
            .and_then(|rest| rest.strip_suffix(suffix))
        else {
            return false;
        };
        let Ok(nanos) = u128::from_str_radix(hex, 16) else {
            return false;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        now.saturating_sub(nanos) > u128::from(REAP_MIN_AGE_SECS) * 1_000_000_000
    }

    /// Run a read-only `docker` CLI command and return its non-empty stdout
    /// lines. Used by the startup reaper; the daemon endpoint comes from
    /// `DOCKER_HOST`, which `ensure_docker_host` has already resolved.
    fn docker_lines(args: &[&str]) -> Result<Vec<String>, String> {
        let output = std::process::Command::new("docker")
            .args(args)
            .output()
            .map_err(|e| format!("failed to spawn docker: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "docker {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect())
    }

    // ── Transient-failure retries (KANBAN-037) ─────────────────────────────
    //
    // Under parallel test load the freshly started sso-gateway (and its
    // Hydra/OpenFGA backends) answers some early calls with transient errors
    // — "permission expand failed", OAuth2 serialization conflicts, dropped
    // connections while a backend warms up. These always pass on retry, so
    // the bootstrap wraps every gateway-touching step in `with_retry`.

    /// Run `f` until it succeeds, with exponential backoff between attempts.
    ///
    /// `what` names the operation for logs; `attempts` caps the total number
    /// of tries. Backoff starts at 250 ms and doubles after each failure.
    /// Retries are logged at warn; the final failure is returned to the
    /// caller.
    async fn with_retry<T, E, F, Fut>(what: &str, attempts: u32, f: F) -> Result<T, E>
    where
        E: std::fmt::Display,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match f().await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if attempt >= attempts {
                        return Err(e);
                    }
                    let backoff = Duration::from_millis(250u64 << (attempt - 1));
                    tracing::warn!(
                        what = %what,
                        attempt,
                        backoff_ms = backoff.as_millis(),
                        error = %e,
                        "transient failure; retrying"
                    );
                    sleep(backoff).await;
                }
            }
        }
    }

    /// Set up the shared test infrastructure and return a fresh `TestInfra`
    /// for the calling test.
    pub async fn setup() -> TestInfra {
        ensure_docker_host();

        if BOOTSTRAP_FAILED.load(Ordering::SeqCst) {
            panic!("shared test infrastructure bootstrap previously failed; not retrying");
        }

        let infra = SHARED
            .get_or_init(|| async {
                // `tokio::OnceCell` wakes a waiting caller to re-run the
                // initializer when the previous attempt panics. Fail fast
                // instead of leaking a second container stack into the VM.
                if BOOTSTRAP_FAILED.load(Ordering::SeqCst) {
                    panic!("shared test infrastructure bootstrap previously failed; not retrying");
                }
                let bootstrap = async {
                    if let Some(infra) = from_env().await {
                        infra
                    } else {
                        start_containers().await
                    }
                };
                match std::panic::AssertUnwindSafe(bootstrap).catch_unwind().await {
                    Ok(infra) => infra,
                    Err(payload) => {
                        BOOTSTRAP_FAILED.store(true, Ordering::SeqCst);
                        std::panic::resume_unwind(payload);
                    }
                }
            })
            .await;

        let (database_url, nats_url, sso_gateway_url, tenant_id, client_id, client_secret) =
            infra.urls();

        build_test_infra(
            &database_url,
            &nats_url,
            &sso_gateway_url,
            &tenant_id,
            &client_id,
            &client_secret,
        )
        .await
    }

    /// Use externally-provided services when the standard env vars are set.
    ///
    /// The caller is expected to have bootstrapped an OAuth2 application with
    /// `permission:admin`, `tenant:admin`, and `identity:read` and to expose
    /// its credentials via `SSO_GATEWAY_CLIENT_ID` and
    /// `SSO_GATEWAY_CLIENT_SECRET`. The tenant must also have a default
    /// identity schema registered (the container harness registers one; with
    /// external services, call `ensure_default_identity_schema` once).
    async fn from_env() -> Option<SharedInfra> {
        let database_url = std::env::var("DATABASE_URL").ok()?;
        let nats_url = std::env::var("NATS_URL").ok()?;
        let sso_gateway_url = std::env::var("SSO_GATEWAY_URL").ok()?;
        let tenant_id = std::env::var("KANBAN_TEST_TENANT_ID").unwrap_or_default();
        let client_id = std::env::var("SSO_GATEWAY_CLIENT_ID").unwrap_or_default();
        let client_secret = std::env::var("SSO_GATEWAY_CLIENT_SECRET").unwrap_or_default();

        // Ensure an S3 bucket exists even when the rest of the stack is external.
        let _ = ensure_minio().await;

        Some(SharedInfra {
            database_url,
            nats_url,
            sso_gateway_url,
            tenant_id,
            client_id,
            client_secret,
            _pg: None,
            _nats: None,
            _sso_gateway: None,
            _minio: None,
            _opensearch: None,
        })
    }

    /// Start all containers, run migrations, and return the shared URLs/handles.
    async fn start_containers() -> SharedInfra {
        // Reap containers/networks leaked by previous test processes before
        // creating new ones (KANBAN-036). Blocking docker CLI calls run off
        // the async worker; all failures are logged inside the reaper and
        // never abort the bootstrap.
        if let Err(e) = tokio::task::spawn_blocking(reap_stale_resources).await {
            tracing::warn!(error = %e, "reaper task failed to join");
        }

        eprintln!(
            "[testcontainers] starting Postgres, NATS, sso-gateway, MinIO, and OpenSearch..."
        );

        let startup_timeout = Duration::from_secs(600);

        let pg = start_postgres(startup_timeout).await;
        let (pg_host, pg_port) = host_port(&pg, 5432).await;
        let database_url = format!("postgres://sunbeam:sunbeam@{pg_host}:{pg_port}/kanban");

        let nats = start_nats(startup_timeout).await;
        let (nats_host, nats_port) = host_port(&nats, 4222).await;
        let nats_url = format!("nats://{nats_host}:{nats_port}");

        let (sso_gateway, sso_gateway_url) = start_sso_gateway_stack().await;

        let minio = start_minio(startup_timeout).await;
        let (minio_host, minio_port) = host_port(&minio, 9000).await;
        let s3_endpoint = format!("http://{minio_host}:{minio_port}");

        let opensearch = start_opensearch(startup_timeout).await;
        let (opensearch_host, opensearch_port) = host_port(&opensearch, 9200).await;
        let opensearch_url = format!("http://{opensearch_host}:{opensearch_port}");

        // Export the service URLs as environment variables so that tests that
        // spin up their own clients (e.g. auth::permission_dispatch integration tests)
        // can find the running containers.
        // SAFETY: runs once inside the SHARED OnceCell initializer, before any
        // test receives its infra handles and reads these variables.
        unsafe {
            std::env::set_var("DATABASE_URL", &database_url);
            std::env::set_var("NATS_URL", &nats_url);
            std::env::set_var("SSO_GATEWAY_URL", &sso_gateway_url);
            std::env::set_var("SSO_GATEWAY_PERMISSION_URL", &sso_gateway_url);
            std::env::set_var(
                "SSO_GATEWAY_TOKEN_URL",
                format!("{sso_gateway_url}/oauth2/token"),
            );
            std::env::set_var(
                "SSO_GATEWAY_INTROSPECTION_URL",
                format!("{sso_gateway_url}/oauth2/introspect"),
            );
            std::env::set_var("OPENSEARCH_URL", &opensearch_url);
        }

        // Bootstrap a dedicated tenant + application with `permission:admin`.
        // Do this once per shared stack so parallel tests reuse the same
        // service credentials.
        let (tenant_id, client_id, client_secret) = bootstrap_kanban_app(&sso_gateway_url)
            .await
            .expect("failed to bootstrap sso-gateway tenant/application");
        // SAFETY: same OnceCell initialization as above; single writer.
        unsafe {
            std::env::set_var("SSO_GATEWAY_CLIENT_ID", &client_id);
            std::env::set_var("SSO_GATEWAY_CLIENT_SECRET", &client_secret);
            std::env::set_var("KANBAN_TEST_TENANT_ID", &tenant_id);
        }

        // Register the Kanban permission namespace + authorization model in
        // the kanban-test tenant (the service app's home tenant). Idempotent on
        // the gateway side. Retried: the freshly started gateway/OpenFGA can
        // fail early calls transiently (KANBAN-037).
        let provisioner = PermissionClient::new(&PermissionClientConfig {
            base_url: sso_gateway_url.clone(),
            token_url: format!("{sso_gateway_url}/oauth2/token"),
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
        })
        .expect("failed to build permission client for namespace provisioning");
        with_retry("ensure Kanban permission namespace", 3, || {
            provisioner.ensure_kanban_namespace()
        })
        .await
        .expect("failed to ensure Kanban permission namespace");

        // Readiness settle: one retrying permission round-trip against the
        // freshly provisioned namespace, so the parallel test stampede that
        // follows bootstrap does not hit a half-warm gateway/OpenFGA.
        with_retry("permission settle round-trip", 3, || {
            provisioner.get_namespace(KANBAN_NAMESPACE)
        })
        .await
        .expect("sso-gateway permission backend did not settle after provisioning");

        // Run migrations against the fresh Postgres instance.
        {
            let pool = connect_pool(&database_url).await;
            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("failed to run database migrations");
        }

        // Create the MinIO bucket used by attachment tests.
        // SAFETY: same OnceCell initialization as above; single writer.
        unsafe {
            std::env::set_var("S3_ENDPOINT", &s3_endpoint);
            std::env::set_var("S3_REGION", "us-east-1");
            std::env::set_var("S3_ACCESS_KEY", "minioadmin");
            std::env::set_var("S3_SECRET_KEY", "minioadmin");
            std::env::set_var("S3_BUCKET", MINIO_BUCKET);
        }
        let s3 = S3Client::new(S3Config::from_env());
        s3.create_bucket()
            .await
            .expect("failed to create MinIO bucket");

        eprintln!("[testcontainers] infrastructure ready");

        SharedInfra {
            database_url,
            nats_url,
            sso_gateway_url,
            tenant_id,
            client_id,
            client_secret,
            _pg: Some(pg),
            _nats: Some(nats),
            _sso_gateway: Some(sso_gateway),
            _minio: Some(minio),
            _opensearch: Some(opensearch),
        }
    }

    /// Return the host and published port for a running container.
    ///
    /// With native Docker (including lima-docker) ports are published to the
    /// host, so tests can connect via `localhost:<mapped-port>` instead of the
    /// container bridge IP.
    async fn host_port(container: &ContainerAsync<GenericImage>, port: u16) -> (String, u16) {
        let host = container
            .get_host()
            .await
            .expect("failed to resolve container host");
        let mapped = container
            .get_host_port_ipv4(ContainerPort::Tcp(port))
            .await
            .expect("failed to resolve container host port");
        (host.to_string(), mapped)
    }

    async fn start_postgres(timeout: Duration) -> ContainerAsync<GenericImage> {
        let image = postgres_image();
        let parts: Vec<&str> = image.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (image, "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(5432.tcp())
            .with_env_var("POSTGRES_USER", "sunbeam")
            .with_env_var("POSTGRES_PASSWORD", "sunbeam")
            .with_env_var("POSTGRES_DB", "kanban")
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start Postgres container");

        let (host, port) = host_port(&container, 5432).await;

        // Poll until Postgres accepts connections.
        for _ in 0..120 {
            if tokio::net::TcpStream::connect((host.as_str(), port))
                .await
                .is_ok()
            {
                let url = format!("postgres://sunbeam:sunbeam@{host}:{port}/kanban");
                if let Ok(pool) = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(2))
                    .connect(&url)
                    .await
                    && sqlx::query("SELECT 1").fetch_optional(&pool).await.is_ok()
                {
                    return container;
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        panic!("Postgres did not become ready in time");
    }

    async fn start_nats(timeout: Duration) -> ContainerAsync<GenericImage> {
        let image = nats_image();
        let parts: Vec<&str> = image.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (image, "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(4222.tcp())
            .with_cmd(vec!["-js"])
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start NATS container");

        let (host, port) = host_port(&container, 4222).await;

        for _ in 0..120 {
            if tokio::net::TcpStream::connect((host.as_str(), port))
                .await
                .is_ok()
            {
                return container;
            }
            sleep(Duration::from_millis(250)).await;
        }

        panic!("NATS did not become ready in time");
    }

    async fn start_sso_gateway_stack() -> (SsoGatewayHandle, String) {
        let image = sso_gateway_image();
        let parts: Vec<&str> = image.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (image, "latest".to_string()),
        };

        let handle = SsoGateway::new()
            .with_image(name, tag)
            .with_env(
                "SYSTEM_BOOTSTRAP_CLIENT_SECRET",
                SYSTEM_BOOTSTRAP_CLIENT_SECRET,
            )
            .start()
            .await
            .expect("failed to start sso-gateway stack");

        let url = handle.endpoint().to_string();
        (handle, url)
    }

    async fn start_minio(timeout: Duration) -> ContainerAsync<GenericImage> {
        let image = minio_image();
        let parts: Vec<&str> = image.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (image, "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(9000.tcp())
            .with_env_var("MINIO_ROOT_USER", "minioadmin")
            .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
            .with_cmd(vec!["server", "/data"])
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start MinIO container");

        let (host, port) = host_port(&container, 9000).await;

        for _ in 0..120 {
            let url = format!("http://{host}:{port}/minio/health/live");
            if let Ok(resp) = reqwest::get(&url).await
                && resp.status().is_success()
            {
                return container;
            }
            sleep(Duration::from_millis(250)).await;
        }

        panic!("MinIO did not become ready in time");
    }

    async fn start_opensearch(timeout: Duration) -> ContainerAsync<GenericImage> {
        let image = opensearch_image();
        let parts: Vec<&str> = image.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (image, "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(9200.tcp())
            .with_env_var("discovery.type", "single-node")
            .with_env_var("plugins.security.disabled", "true")
            .with_env_var("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "OpenSearchTest123!")
            .with_env_var("DISABLE_PERFORMANCE_ANALYZER_AGENT_CLI", "true")
            .with_env_var(
                "OPENSEARCH_JAVA_OPTS",
                "-Xms512m -Xmx512m -Dopensearch.transport.cname_in_publish_address=true",
            )
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start OpenSearch container");

        let (host, port) = host_port(&container, 9200).await;

        for _ in 0..240 {
            let url = format!("http://{host}:{port}/_cluster/health");
            if let Ok(resp) = reqwest::get(&url).await
                && resp.status().is_success()
                && let Ok(body) = resp.text().await
                && (body.contains("\"status\":\"green\"") || body.contains("\"status\":\"yellow\""))
            {
                return container;
            }
            sleep(Duration::from_millis(500)).await;
        }

        panic!("OpenSearch did not become ready in time");
    }

    /// Ensure an S3-compatible endpoint is available for attachments tests.
    ///
    /// If `S3_ENDPOINT` is already set the harness reuses it and returns it.
    /// Otherwise it starts a MinIO container and returns the endpoint.
    async fn ensure_minio() -> String {
        if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
            return endpoint;
        }

        let minio = start_minio(Duration::from_secs(600)).await;
        let (host, port) = host_port(&minio, 9000).await;
        let endpoint = format!("http://{host}:{port}");

        // SAFETY: called from within the SHARED OnceCell initializer; single writer.
        unsafe {
            std::env::set_var("S3_ENDPOINT", &endpoint);
            std::env::set_var("S3_REGION", "us-east-1");
            std::env::set_var("S3_ACCESS_KEY", "minioadmin");
            std::env::set_var("S3_SECRET_KEY", "minioadmin");
            std::env::set_var("S3_BUCKET", MINIO_BUCKET);
        }

        let s3 = S3Client::new(S3Config::from_env());
        s3.create_bucket()
            .await
            .expect("failed to create MinIO bucket");

        endpoint
    }

    /// Build a fresh `TestInfra` from the shared URLs/credentials so each test
    /// owns its own connections and is isolated from other parallel tests.
    async fn build_test_infra(
        database_url: &str,
        nats_url: &str,
        sso_gateway_url: &str,
        _tenant_id: &str,
        client_id: &str,
        client_secret: &str,
    ) -> TestInfra {
        let pool = connect_pool(database_url).await;
        let nats = connect_nats(nats_url).await;

        let cache = PERMISSION_CLIENT_CACHE
            .get_or_init(|| async { tokio::sync::Mutex::new(PermissionClientCache::new()) })
            .await;
        let key = (
            sso_gateway_url.to_string(),
            client_id.to_string(),
            client_secret.to_string(),
        );
        let mut clients = cache.lock().await;
        let permission = clients.entry(key.clone()).or_insert_with(|| {
            Arc::new(
                PermissionClient::new(&PermissionClientConfig {
                    base_url: sso_gateway_url.to_string(),
                    token_url: format!("{sso_gateway_url}/oauth2/token"),
                    client_id: client_id.to_string(),
                    client_secret: client_secret.to_string(),
                })
                .expect("failed to build permission client"),
            )
        });
        let permission = Arc::clone(permission);
        drop(clients);

        let identity_cache = IDENTITY_CLIENT_CACHE
            .get_or_init(|| async { tokio::sync::Mutex::new(IdentityClientCache::new()) })
            .await;
        let mut clients = identity_cache.lock().await;
        let identity = clients.entry(key).or_insert_with(|| {
            Arc::new(
                IdentityClient::new(&IdentityClientConfig {
                    base_url: sso_gateway_url.to_string(),
                    token_url: format!("{sso_gateway_url}/oauth2/token"),
                    client_id: client_id.to_string(),
                    client_secret: client_secret.to_string(),
                })
                .expect("failed to build identity client"),
            )
        });
        let identity = Arc::clone(identity);
        drop(clients);

        TestInfra {
            pool,
            nats,
            permission,
            identity,
            sso_gateway_url: sso_gateway_url.to_string(),
        }
    }

    /// Create the well-known `kanban-test` tenant and its Kanban service
    /// application with `permission:admin`, `tenant:admin`, and `identity:read`.
    async fn bootstrap_kanban_app(
        sso_gateway_url: &str,
    ) -> Result<(String, String, String), Box<dyn std::error::Error + Send + Sync>> {
        let (tenant_id, client_id, client_secret) =
            create_tenant_app(sso_gateway_url, "kanban-test", "Kanban Test Tenant").await?;
        // Assignee-validation tests create identities in this tenant; the
        // gateway requires a default identity schema before CreateIdentity.
        ensure_default_identity_schema(sso_gateway_url, &tenant_id).await?;
        Ok((tenant_id, client_id, client_secret))
    }

    /// Create a tenant using the system bootstrap client and return its id.
    pub async fn create_tenant(
        sso_gateway_url: &str,
        slug: &str,
        display_name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let base_uri = sso_gateway_url
            .parse::<http::Uri>()
            .map_err(|e| format!("invalid sso-gateway URL: {e}"))?;

        let admin_token = fetch_bootstrap_token(sso_gateway_url, "tenant:admin")
            .await
            .map_err(|e| format!("failed to fetch admin token: {e}"))?;

        let client = ClientBuilder::new(sso_gateway_url)
            .auth(sunbeam_g2v::client::BearerToken::new(admin_token))
            .build()
            .map_err(|e| format!("failed to build IAM admin client: {e}"))?;
        let transport = ConnectTransport::new(client, base_uri.clone());
        let config = connectrpc::client::ClientConfig::new(base_uri);

        let tenant_client = TenantServiceClient::new(transport, config);

        // Retried: a half-warm gateway can fail the first calls transiently
        // (KANBAN-037). Slug conflicts only occur if a previous attempt
        // actually created the tenant, which cannot happen on a fresh stack.
        let tenant = with_retry("create tenant", 3, || async {
            tenant_client
                .create_tenant(CreateTenantRequest {
                    slug: slug.to_string(),
                    display_name: display_name.to_string(),
                    settings: Default::default(),
                    __buffa_unknown_fields: Default::default(),
                })
                .await
                .map_err(|e| format!("CreateTenant failed: {e}"))
                .map(|r| r.into_owned())
        })
        .await?;

        Ok(tenant.id)
    }

    /// Create a tenant and a Kanban service application with `permission:admin`,
    /// `tenant:admin`, and `identity:read` inside it, returning `(tenant_id,
    /// client_id, client_secret)`.
    ///
    /// The system bootstrap client first exchanges its credentials for an access
    /// token; that token is used to call the IAM Connect-RPC endpoints. The new
    /// application uses `client_secret_post` so that
    /// `sunbeam_g2v::client::OAuth2ClientCredentials` can fetch tokens with
    /// form-encoded credentials. The application is created with
    /// `cross_tenant: true` so it can target other tenants via `x-tenant-id`.
    pub async fn create_tenant_app(
        sso_gateway_url: &str,
        slug: &str,
        display_name: &str,
    ) -> Result<(String, String, String), Box<dyn std::error::Error + Send + Sync>> {
        let tenant_id = create_tenant(sso_gateway_url, slug, display_name).await?;

        let base_uri = sso_gateway_url
            .parse::<http::Uri>()
            .map_err(|e| format!("invalid sso-gateway URL: {e}"))?;

        let admin_token = fetch_bootstrap_token(sso_gateway_url, "application:admin")
            .await
            .map_err(|e| format!("failed to fetch admin token: {e}"))?;

        let client = ClientBuilder::new(sso_gateway_url)
            .auth(sunbeam_g2v::client::BearerToken::new(admin_token))
            .build()
            .map_err(|e| format!("failed to build IAM admin client: {e}"))?;
        let transport = ConnectTransport::new(client, base_uri.clone());
        let config = connectrpc::client::ClientConfig::new(base_uri);

        let app_client = ApplicationServiceClient::new(transport, config);

        let tenant_options =
            connectrpc::client::CallOptions::default().with_header("x-tenant-id", &tenant_id);

        // Hydra serializes OAuth2 client writes; parallel tests can lose the
        // race with a transient "Unable to serialize access" conflict, and a
        // half-warm gateway can fail early calls outright (KANBAN-037). Retry
        // only the app provisioning — the tenant already exists at this point.
        let secret = with_retry("provision kanban-service application", 5, || async {
            let app = app_client
                .create_application_with_options(
                    CreateApplicationRequest {
                        name: "kanban-service".to_string(),
                        redirect_uris: vec![],
                        grant_types: vec!["client_credentials".to_string()],
                        response_types: vec!["token".to_string()],
                        scope: vec![
                            "permission:admin".to_string(),
                            "tenant:admin".to_string(),
                            "identity:read".to_string(),
                        ],
                        token_endpoint_auth_method: "client_secret_post".to_string(),
                        cross_tenant: true,
                        skip_consent: false,
                        __buffa_unknown_fields: Default::default(),
                    },
                    tenant_options.clone(),
                )
                .await
                .map_err(|e| format!("CreateApplication failed: {e}"))?
                .into_owned();
            eprintln!(
                "[kanban-test] created app id={} tenant={} cross_tenant={}",
                app.id, tenant_id, app.cross_tenant
            );

            app_client
                .rotate_secret_with_options(
                    RotateSecretRequest {
                        id: app.id,
                        __buffa_unknown_fields: Default::default(),
                    },
                    tenant_options.clone(),
                )
                .await
                .map_err(|e| format!("RotateSecret failed: {e}"))
                .map(|r| r.into_owned())
        })
        .await?;

        Ok((tenant_id, secret.client_id, secret.client_secret))
    }

    /// Build an `identity:admin`-scoped gateway client for `tenant_id` using
    /// the system bootstrap credentials.
    async fn identity_admin_client(
        sso_gateway_url: &str,
        tenant_id: &str,
    ) -> Result<sdk::auth::AuthClient, Box<dyn std::error::Error + Send + Sync>> {
        let admin_token = fetch_bootstrap_token(sso_gateway_url, "identity:admin")
            .await
            .map_err(|e| format!("failed to fetch admin token: {e}"))?;

        let client = sdk::auth::AuthClient::builder(sso_gateway_url)
            .auth(sunbeam_g2v::client::BearerToken::new(admin_token))
            .build()
            .map_err(|e| format!("failed to build identity admin client: {e}"))?;
        let base_uri = sso_gateway_url
            .parse::<http::Uri>()
            .map_err(|e| format!("invalid sso-gateway URL: {e}"))?;
        sdk::auth::AuthClient::new(client, base_uri)
            .map(|c| c.with_tenant(tenant_id))
            .map_err(|e| format!("failed to build identity admin client: {e}").into())
    }

    /// Register the minimal email-password identity schema as the tenant
    /// default. The gateway rejects `CreateIdentity` until one exists.
    ///
    /// Idempotent: both the `AlreadyExists` RPC error and the raw duplicate-key
    /// violation the gateway surfaces as `internal` (parallel first-time
    /// registration race) are treated as success.
    pub async fn ensure_default_identity_schema(
        sso_gateway_url: &str,
        tenant_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let auth = identity_admin_client(sso_gateway_url, tenant_id).await?;

        let schema_json: buffa_types::google::protobuf::Struct =
            serde_json::from_value(serde_json::json!({
                "$id": "https://schemas.ory.sh/presets/kratos/quickstart/email-password/identity.schema.json",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Person",
                "type": "object",
                "properties": {
                    "traits": {
                        "type": "object",
                        "properties": {
                            "email": {
                                "type": "string",
                                "format": "email",
                                "title": "E-Mail",
                                "ory.sh/kratos": {
                                    "credentials": { "password": { "identifier": true } },
                                    "recovery": { "via": "email" },
                                    "verification": { "via": "email" }
                                }
                            },
                            // Optional name traits, mirroring the deployed
                            // `employee` schema so display-name hydration
                            // (KANBAN-024) is testable.
                            "given_name": { "type": "string" },
                            "family_name": { "type": "string" }
                        },
                        "required": ["email"],
                        "additionalProperties": false
                    }
                }
            }))
            .map_err(|e| format!("failed to build identity schema: {e}"))?;

        // Retried (KANBAN-037): a half-warm gateway can fail the first calls
        // transiently. The duplicate tolerance stays inside the retried
        // closure so an `AlreadyExists` race still counts as success.
        with_retry("register default identity schema", 3, || async {
            let result = auth
                .identity()
                .create_identity_schema(sdk::auth::v1::CreateIdentitySchemaRequest {
                    schema_id: "default".to_string(),
                    schema_json: buffa::MessageField::some(schema_json.clone()),
                    is_default: true,
                    ..Default::default()
                })
                .await;

            match result {
                Ok(_) => Ok(()),
                Err(e)
                    if e.code == connectrpc::ErrorCode::AlreadyExists
                        || e.to_string()
                            .contains("tenant_identity_schemas_tenant_id_schema_id_key") =>
                {
                    Ok(())
                }
                Err(e) => Err(format!("CreateIdentitySchema failed: {e}")),
            }
        })
        .await?;

        Ok(())
    }

    /// Register an identity with the given email in `tenant_id`'s user
    /// directory and return its gateway identity ULID.
    ///
    /// Uses the system bootstrap client with `identity:admin`.
    pub async fn create_identity(
        sso_gateway_url: &str,
        tenant_id: &str,
        email: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        ensure_default_identity_schema(sso_gateway_url, tenant_id).await?;
        let auth = identity_admin_client(sso_gateway_url, tenant_id).await?;

        let traits: buffa_types::google::protobuf::Struct =
            serde_json::from_value(serde_json::json!({ "email": email }))
                .map_err(|e| format!("failed to build identity traits: {e}"))?;

        let identity = auth
            .identity()
            .create_identity(sdk::auth::v1::CreateIdentityRequest {
                schema_id: String::new(),
                traits: buffa::MessageField::some(traits),
                password: String::new(),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("CreateIdentity failed: {e}"))?
            .into_owned();

        Ok(identity.id)
    }

    /// Register an identity with email and name traits (mirroring the
    /// deployed `employee` schema) and return its gateway identity ULID.
    pub async fn create_identity_named(
        sso_gateway_url: &str,
        tenant_id: &str,
        email: &str,
        given_name: &str,
        family_name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        ensure_default_identity_schema(sso_gateway_url, tenant_id).await?;
        let auth = identity_admin_client(sso_gateway_url, tenant_id).await?;

        let traits: buffa_types::google::protobuf::Struct =
            serde_json::from_value(serde_json::json!({
                "email": email,
                "given_name": given_name,
                "family_name": family_name,
            }))
            .map_err(|e| format!("failed to build identity traits: {e}"))?;

        let identity = auth
            .identity()
            .create_identity(sdk::auth::v1::CreateIdentityRequest {
                schema_id: String::new(),
                traits: buffa::MessageField::some(traits),
                password: String::new(),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("CreateIdentity failed: {e}"))?
            .into_owned();

        Ok(identity.id)
    }

    /// Exchange the system bootstrap client credentials for an access token with
    /// the requested scope.
    ///
    /// The gateway's OAuth2 token endpoint expects HTTP Basic authentication
    /// (`client_secret_basic`) rather than form-encoded credentials.
    async fn fetch_bootstrap_token(sso_gateway_url: &str, scope: &str) -> reqwest::Result<String> {
        // Retried (KANBAN-037): the token endpoint can 5xx or drop connections
        // while Hydra warms up; token fetches are idempotent.
        with_retry("fetch bootstrap token", 3, || async {
            let client = reqwest::Client::new();
            let resp: serde_json::Value = client
                .post(format!("{sso_gateway_url}/oauth2/token"))
                .basic_auth(
                    "system-bootstrap-client",
                    Some(SYSTEM_BOOTSTRAP_CLIENT_SECRET),
                )
                .form(&[("grant_type", "client_credentials"), ("scope", scope)])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            Ok(resp["access_token"].as_str().unwrap_or("").to_string())
        })
        .await
    }

    async fn connect_pool(database_url: &str) -> sqlx::PgPool {
        PgPoolOptions::new()
            .max_connections(10)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .connect(database_url)
            .await
            .expect("failed to connect to Postgres")
    }

    async fn connect_nats(nats_url: &str) -> Arc<NatsClient> {
        Arc::new(
            NatsClient::connect(&NatsConfig {
                url: nats_url.to_string(),
                jetstream: true,
                lease_duration: 30,
                auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
            })
            .await
            .expect("NATS connect failed"),
        )
    }
}
