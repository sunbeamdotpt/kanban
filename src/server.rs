// SPDX-License-Identifier: AGPL-3.0-or-later
//! Bootstrap for the Kanban service.
//!
//! This is where the whole process comes together: load configuration from the
//! environment, set up OpenTelemetry tracing, connect to Postgres and run
//! migrations, bootstrap the NATS JetStream board-events stream, check
//! sso-gateway readiness, register Prometheus metrics, build the axum router,
//! and finally start listening on `KANBAN_PORT` until the process receives a
//! shutdown signal.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    Router, extract::State, http::StatusCode, middleware, response::IntoResponse, routing::get,
};
use clap::Parser;
use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};
use sqlx::postgres::PgPoolOptions;
use tower_http::trace::TraceLayer;
use tracing::info;

use sunbeam_g2v::client::OAuth2ClientCredentials;
use sunbeam_g2v::config::NatsConfig;
use sunbeam_g2v::middleware::auth::{AuthMiddlewareState, auth_middleware};
use sunbeam_g2v::mq::NatsClient;

use crate::auth::permission_client::{PermissionClient, PermissionClientConfig};
use crate::auth::session_client::SsoGatewaySessionClient;

use crate::auth::permission_dispatch::{DispatchState, dispatch};
use crate::cpb::sunbeam::kanban::v1::{
    AggregatedBoardServiceExt as _, AttachmentServiceExt as _, BoardServiceExt as _,
    CardServiceExt as _, GithubLinkServiceExt as _, ProjectServiceExt as _,
    PublicBoardServiceExt as _, SearchServiceExt as _, TemplatesServiceExt as _,
};
use crate::id::Id;
use crate::integrations::opensearch::{OpenSearchClient, OpenSearchConfig};
use crate::integrations::s3::{S3Client, S3Config};
use crate::realtime::registry::BoardSubscriberRegistry;
use crate::services::{
    aggregated_boards::AggregatedBoardServiceImpl, attachments::AttachmentServiceImpl,
    boards::BoardServiceImpl, cards::CardServiceImpl, github::GitHubServiceImpl,
    projects::ProjectServiceImpl, public_boards::PublicBoardServiceImpl, search::SearchServiceImpl,
    templates::TemplatesServiceImpl,
};
use sunbeam_g2v::router::ServiceRouter;

// ── JetStream stream name & config ──────────────────────────────────────────
//
// Naming and config live in realtime::jetstream_bootstrap; server.rs only
// calls the bootstrap function at startup.

// ── Prometheus metrics registry shared across handlers ──────────────────────

#[derive(Clone)]
pub struct KanbanMetrics {
    pub registry: Arc<Registry>,
    /// Counter for permission checks, labeled `result` (allow, deny, or error).
    pub permission_check_total: CounterVec,
    /// Number of active board subscription streams.
    pub subscribe_active_streams: prometheus::Gauge,
    /// JetStream consumer lag in seconds, labeled by board.
    pub jet_stream_lag_seconds: GaugeVec,
    /// Fraction of `project_member_view` rows that differ from the permission backend.
    pub mirror_drift_ratio: prometheus::Gauge,
    /// Histogram of gRPC handler durations, labeled by service and method.
    pub rpc_duration_seconds: HistogramVec,
    /// Total gRPC requests, labeled by service, method, and status.
    pub rpc_total: CounterVec,
}

impl KanbanMetrics {
    pub fn new() -> Result<Self> {
        Self::with_buckets(vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ])
    }

    pub fn with_buckets(buckets: Vec<f64>) -> Result<Self> {
        let registry = Arc::new(Registry::new());

        let permission_check_total = CounterVec::new(
            Opts::new("kanban_permission_check_total", "Permission check results"),
            &["result"],
        )?;
        registry.register(Box::new(permission_check_total.clone()))?;

        let subscribe_active_streams = prometheus::Gauge::with_opts(Opts::new(
            "kanban_subscribe_active_streams",
            "Active board subscription streams",
        ))?;
        registry.register(Box::new(subscribe_active_streams.clone()))?;

        let jet_stream_lag_seconds = GaugeVec::new(
            Opts::new(
                "kanban_jet_stream_lag_seconds",
                "JetStream consumer lag in seconds per board",
            ),
            &["board_id"],
        )?;
        registry.register(Box::new(jet_stream_lag_seconds.clone()))?;

        let mirror_drift_ratio = prometheus::Gauge::with_opts(Opts::new(
            "kanban_mirror_drift_ratio",
            "Fraction of project_member_view rows that differ from the permission backend (Stage 7a)",
        ))?;
        registry.register(Box::new(mirror_drift_ratio.clone()))?;

        let rpc_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "kanban_rpc_duration_seconds",
                "gRPC handler duration in seconds",
            )
            .buckets(buckets),
            &["service", "method"],
        )?;
        registry.register(Box::new(rpc_duration_seconds.clone()))?;

        let rpc_total = CounterVec::new(
            Opts::new(
                "kanban_rpc_total",
                "Total gRPC requests by service/method/status",
            ),
            &["service", "method", "status"],
        )?;
        registry.register(Box::new(rpc_total.clone()))?;

        Ok(Self {
            registry,
            permission_check_total,
            subscribe_active_streams,
            jet_stream_lag_seconds,
            mirror_drift_ratio,
            rpc_duration_seconds,
            rpc_total,
        })
    }
}

// ── App state threaded into health handlers ──────────────────────────────────

#[derive(Clone)]
struct AppState {
    sso_gateway_url: String,
    metrics: Arc<KanbanMetrics>,
}

// ── Health handlers ──────────────────────────────────────────────────────────

async fn healthz_live() -> impl IntoResponse {
    StatusCode::OK
}

/// Ready probe. Returns 200 when the sso-gateway ready endpoint responds
/// successfully; otherwise returns 503.
async fn healthz_ready(State(state): State<AppState>) -> impl IntoResponse {
    let url = format!("{}/health/ready", state.sso_gateway_url);
    match reqwest::get(&url).await {
        Ok(resp) if resp.status().is_success() => StatusCode::OK,
        Ok(resp) => {
            tracing::warn!(status = %resp.status(), "readiness probe: sso-gateway not ready");
            StatusCode::SERVICE_UNAVAILABLE
        }
        Err(e) => {
            tracing::error!(error = %e, "readiness probe: sso-gateway ready check failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// Serve Prometheus metrics in text format.
async fn metrics_handler(State(state): State<AppState>) -> (StatusCode, String) {
    let encoder = TextEncoder::new();
    let mf = state.metrics.registry.gather();
    match encoder.encode_to_string(&mf) {
        Ok(body) => (StatusCode::OK, body),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metrics encode error: {e}"),
        ),
    }
}

// ── Main entry point ─────────────────────────────────────────────────────────

// ── CLI / Configuration ───────────────────────────────────────────────────────

/// Kanban service configuration.
///
/// All fields can be set via environment variables (the `env` attribute on each
/// arg) or command-line flags. Command-line flags take precedence over env vars.
#[derive(Clone, Debug, Parser)]
#[command(name = "kanban", about = "Sunbeam Kanban backend")]
pub struct Cli {
    /// Host address to bind to.
    #[arg(long, env = "KANBAN_HOST", default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on.
    #[arg(long, env = "KANBAN_PORT", default_value = "8080")]
    port: u16,

    /// PostgreSQL connection string.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Maximum Postgres pool size.
    #[arg(long, env = "KANBAN_DATABASE_MAX_CONNECTIONS", default_value = "20")]
    database_max_connections: u32,

    /// Postgres connection acquire timeout in seconds.
    #[arg(
        long,
        env = "KANBAN_DATABASE_ACQUIRE_TIMEOUT_SECS",
        default_value = "10"
    )]
    database_acquire_timeout_secs: u64,

    /// sso-gateway OAuth2 token introspection endpoint URL.
    #[arg(
        long,
        env = "SSO_GATEWAY_INTROSPECTION_URL",
        default_value = "http://localhost:4445/oauth2/introspect"
    )]
    sso_gateway_introspection_url: String,

    /// OAuth2 client ID for the service client-credentials used to call the
    /// sso-gateway (introspection and permission service).
    #[arg(long, env = "SSO_GATEWAY_CLIENT_ID", default_value = "")]
    sso_gateway_client_id: String,

    /// OAuth2 client secret for the service client-credentials used to call
    /// the sso-gateway (introspection and permission service).
    #[arg(long, env = "SSO_GATEWAY_CLIENT_SECRET", default_value = "")]
    sso_gateway_client_secret: String,

    /// System tenant id used for globally-shared resources (e.g. system-wide
    /// board/card templates). Must be a valid tenant id in the sso-gateway.
    #[arg(long, env = "KANBAN_SYSTEM_TENANT_ID", default_value = "system")]
    system_tenant_id: String,

    /// NATS server URL.
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Optional NATS authentication token (supports NATS auth callout).
    #[arg(long, env = "NATS_AUTH_TOKEN")]
    nats_auth_token: Option<String>,

    /// OpenSearch base URL.
    #[arg(long, env = "OPENSEARCH_URL", default_value = "http://localhost:9200")]
    opensearch_url: String,

    /// OpenSearch index name for card search.
    #[arg(
        long,
        env = "KANBAN_OPENSEARCH_INDEX",
        default_value = "sunbeam-kanban-cards-v1"
    )]
    opensearch_index_name: String,

    /// Optional S3-compatible endpoint for attachments.
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    /// Optional public S3 endpoint used only for presigned URLs. Set this when
    /// `S3_ENDPOINT` is cluster-internal so browsers receive reachable URLs.
    #[arg(long, env = "S3_PUBLIC_ENDPOINT")]
    s3_public_endpoint: Option<String>,

    /// S3 region.
    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// S3 access key.
    #[arg(long, env = "S3_ACCESS_KEY", default_value = "")]
    s3_access_key: String,

    /// S3 secret key.
    #[arg(long, env = "S3_SECRET_KEY", default_value = "")]
    s3_secret_key: String,

    /// S3 bucket name.
    #[arg(long, env = "S3_BUCKET", default_value = "sunbeam-kanban")]
    s3_bucket: String,

    /// Stable pod identity used when naming NATS consumers.
    #[arg(long, env = "POD_NAME")]
    pod_name: Option<String>,

    /// Prometheus RPC duration histogram buckets in seconds (comma-separated).
    #[arg(
        long,
        env = "KANBAN_RPC_DURATION_BUCKETS_SECS",
        value_delimiter = ',',
        default_value = "0.005,0.01,0.025,0.05,0.1,0.25,0.5,1.0,2.5,5.0"
    )]
    rpc_duration_buckets_secs: Vec<f64>,

    // ── NATS / JetStream tunables ─────────────────────────────────────────────
    /// NATS consumer lease duration in seconds.
    #[arg(long, env = "KANBAN_NATS_LEASE_DURATION_SECS", default_value = "30")]
    nats_lease_duration_secs: u64,

    /// JetStream stream replica count.
    #[arg(long, env = "KANBAN_NATS_REPLICAS", default_value = "1")]
    stream_replicas: i32,

    /// JetStream stream max age in seconds.
    #[arg(long, env = "KANBAN_STREAM_MAX_AGE_SECS", default_value = "86400")]
    stream_max_age_secs: u64,

    /// Max messages retained per subject.
    #[arg(
        long,
        env = "KANBAN_STREAM_MAX_MSGS_PER_SUBJECT",
        default_value = "10000"
    )]
    stream_max_msgs_per_subject: i64,

    /// JetStream retention policy: limits, interest, or work_queue.
    #[arg(long, env = "KANBAN_STREAM_RETENTION", default_value = "limits")]
    stream_retention: String,

    /// JetStream storage backend: file or memory.
    #[arg(long, env = "KANBAN_STREAM_STORAGE", default_value = "file")]
    stream_storage: String,

    // ── Outbox dispatcher ─────────────────────────────────────────────────────
    /// Outbox poll interval in milliseconds.
    #[arg(long, env = "KANBAN_OUTBOX_POLL_INTERVAL_MS", default_value = "250")]
    outbox_poll_interval_ms: u64,

    /// Outbox drain batch size.
    #[arg(long, env = "KANBAN_OUTBOX_BATCH_SIZE", default_value = "256")]
    outbox_batch_size: i64,

    // ── Board subscriber registry ─────────────────────────────────────────────
    /// Per-board broadcast channel capacity.
    #[arg(
        long,
        env = "KANBAN_REGISTRY_BROADCAST_CAPACITY",
        default_value = "256"
    )]
    registry_broadcast_capacity: usize,

    /// NATS ephemeral consumer inactive threshold in seconds.
    #[arg(
        long,
        env = "KANBAN_REGISTRY_INACTIVE_THRESHOLD_SECS",
        default_value = "30"
    )]
    registry_inactive_threshold_secs: u64,

    // ── Live subscription streams ─────────────────────────────────────────────
    /// Heartbeat interval for live streams in milliseconds.
    #[arg(long, env = "KANBAN_HEARTBEAT_INTERVAL_MS", default_value = "15000")]
    heartbeat_interval_ms: u64,

    /// Permission recheck interval for live streams in milliseconds.
    #[arg(
        long,
        env = "KANBAN_PERMISSION_RECHECK_INTERVAL_MS",
        default_value = "30000"
    )]
    permission_recheck_interval_ms: u64,

    /// Cutover tracker deduplication capacity.
    #[arg(long, env = "KANBAN_CUTOVER_SEEN_CAPACITY", default_value = "1024")]
    cutover_seen_capacity: usize,

    // ── Attachment presigned URLs ─────────────────────────────────────────────
    /// Presigned PUT URL lifetime in seconds.
    #[arg(long, env = "KANBAN_UPLOAD_EXPIRES_SECS", default_value = "900")]
    upload_expires_secs: u64,

    /// Presigned GET URL lifetime in seconds.
    #[arg(long, env = "KANBAN_DOWNLOAD_EXPIRES_SECS", default_value = "300")]
    download_expires_secs: u64,
}

impl Cli {
    /// Convert parsed CLI / env configuration into the internal `AppConfig`.
    pub fn into_config(self) -> Result<AppConfig> {
        let addr: SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .context("failed to parse KANBAN_HOST:KANBAN_PORT as a socket address")?;

        let stream_retention = self
            .stream_retention
            .parse::<crate::realtime::jetstream_bootstrap::Retention>()
            .map_err(|e| {
                anyhow::anyhow!(
                    "KANBAN_STREAM_RETENTION must be one of: limits, interest, work_queue: {e}"
                )
            })?;
        let stream_storage = self
            .stream_storage
            .parse::<crate::realtime::jetstream_bootstrap::Storage>()
            .map_err(|e| {
                anyhow::anyhow!("KANBAN_STREAM_STORAGE must be one of: file, memory: {e}")
            })?;

        let sso_gateway_url = self
            .sso_gateway_introspection_url
            .strip_suffix("/oauth2/introspect")
            .map(String::from)
            .unwrap_or_else(|| self.sso_gateway_introspection_url.clone());

        Ok(AppConfig {
            addr,
            host: self.host,
            sso_gateway_introspection_url: self.sso_gateway_introspection_url,
            sso_gateway_client_id: self.sso_gateway_client_id,
            sso_gateway_client_secret: self.sso_gateway_client_secret,
            system_tenant_id: self.system_tenant_id,
            database_url: self.database_url,
            database_max_connections: self.database_max_connections,
            database_acquire_timeout_secs: self.database_acquire_timeout_secs,
            nats_url: self.nats_url,
            nats_auth_token: self.nats_auth_token,
            nats_lease_duration_secs: self.nats_lease_duration_secs,
            sso_gateway_url: sso_gateway_url.clone(),
            sso_gateway_permission_url: sso_gateway_url.clone(),
            sso_gateway_token_url: format!("{sso_gateway_url}/oauth2/token"),
            opensearch_url: self.opensearch_url,
            opensearch_index_name: self.opensearch_index_name,
            s3_endpoint: self.s3_endpoint,
            s3_public_endpoint: self.s3_public_endpoint,
            s3_region: self.s3_region,
            s3_access_key: self.s3_access_key,
            s3_secret_key: self.s3_secret_key,
            s3_bucket: self.s3_bucket,
            pod_name: self.pod_name,
            rpc_duration_buckets_secs: self.rpc_duration_buckets_secs,
            stream_replicas: self.stream_replicas,
            stream_max_age_secs: self.stream_max_age_secs,
            stream_max_msgs_per_subject: self.stream_max_msgs_per_subject,
            stream_retention,
            stream_storage,
            outbox_poll_interval_ms: self.outbox_poll_interval_ms,
            outbox_batch_size: self.outbox_batch_size,
            registry_broadcast_capacity: self.registry_broadcast_capacity,
            registry_inactive_threshold_secs: self.registry_inactive_threshold_secs,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            permission_recheck_interval_ms: self.permission_recheck_interval_ms,
            cutover_seen_capacity: self.cutover_seen_capacity,
            upload_expires_secs: self.upload_expires_secs,
            download_expires_secs: self.download_expires_secs,
        })
    }
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub addr: SocketAddr,
    pub host: String,
    pub sso_gateway_introspection_url: String,
    pub sso_gateway_client_id: String,
    pub sso_gateway_client_secret: String,
    pub system_tenant_id: String,
    pub database_url: String,
    pub database_max_connections: u32,
    pub database_acquire_timeout_secs: u64,
    pub nats_url: String,
    pub nats_auth_token: Option<String>,
    pub nats_lease_duration_secs: u64,
    pub sso_gateway_url: String,
    pub sso_gateway_permission_url: String,
    pub sso_gateway_token_url: String,
    pub opensearch_url: String,
    pub opensearch_index_name: String,
    pub s3_endpoint: Option<String>,
    pub s3_public_endpoint: Option<String>,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_bucket: String,
    pub pod_name: Option<String>,
    pub rpc_duration_buckets_secs: Vec<f64>,
    pub stream_replicas: i32,
    pub stream_max_age_secs: u64,
    pub stream_max_msgs_per_subject: i64,
    pub stream_retention: crate::realtime::jetstream_bootstrap::Retention,
    pub stream_storage: crate::realtime::jetstream_bootstrap::Storage,
    pub outbox_poll_interval_ms: u64,
    pub outbox_batch_size: i64,
    pub registry_broadcast_capacity: usize,
    pub registry_inactive_threshold_secs: u64,
    pub heartbeat_interval_ms: u64,
    pub permission_recheck_interval_ms: u64,
    pub cutover_seen_capacity: usize,
    pub upload_expires_secs: u64,
    pub download_expires_secs: u64,
}

pub fn load_config() -> Result<AppConfig> {
    Cli::parse().into_config()
}

// ── Main entry point ─────────────────────────────────────────────────────────

pub async fn run() -> Result<()> {
    let config = load_config()?;

    // OTel tracing init.
    init_otel_tracing()?;

    info!(addr = %config.addr, "kanban service starting");

    let listener = tokio::net::TcpListener::bind(config.addr)
        .await
        .with_context(|| format!("failed to bind to {}", config.addr))?;

    run_with_config(config, listener, shutdown_signal()).await
}

pub async fn run_with_config(
    config: AppConfig,
    listener: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    // ── 3. Postgres + migrations ────────────────────────────────────────────
    let pg_pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_secs(config.database_acquire_timeout_secs))
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;

    sqlx::migrate!("./migrations")
        .run(&pg_pool)
        .await
        .context("sqlx migrations failed")?;

    info!("Postgres connected and migrations applied");

    // ── 3b. System migrations (data fixes across Postgres, the permission backend, OpenSearch) ─
    let opensearch_client_for_migrations = Arc::new(OpenSearchClient::new(OpenSearchConfig {
        url: config.opensearch_url.clone(),
    }));
    let permission_config = PermissionClientConfig {
        base_url: config.sso_gateway_permission_url.clone(),
        token_url: config.sso_gateway_token_url.clone(),
        client_id: config.sso_gateway_client_id.clone(),
        client_secret: config.sso_gateway_client_secret.clone(),
    };
    let permission_for_migrations = Arc::new(
        PermissionClient::new(&permission_config)
            .context("failed to build permission client for migrations")?,
    );

    // Register the Kanban permission namespace and authorization model with
    // the sso-gateway. Idempotent: an identical model is a no-op, a changed
    // model publishes a new version into the existing store without touching
    // tuples. Fatal on failure — without the namespace no check can succeed.
    permission_for_migrations
        .ensure_kanban_namespace()
        .await
        .context("fatal: failed to ensure Kanban permission namespace")?;
    info!("Kanban permission namespace ensured");

    crate::system_migrations::MigrationRunner::new(crate::system_migrations::all_migrations())
        .run_all(&crate::system_migrations::MigrationContext::new(
            pg_pool.clone(),
            permission_for_migrations,
            opensearch_client_for_migrations,
            config.opensearch_index_name.clone(),
        ))
        .await
        .context("system migrations failed")?;
    info!("system migrations applied");

    // ── 4. NATS + JetStream bootstrap (fatal on failure) ───────────────────
    let nats = Arc::new(
        NatsClient::connect(&NatsConfig {
            url: config.nats_url,
            jetstream: true,
            lease_duration: config.nats_lease_duration_secs,
            auth_token: config.nats_auth_token.clone(),
        })
        .await
        .context("failed to connect to NATS")?,
    );

    let stream_config = crate::realtime::jetstream_bootstrap::StreamConfig {
        name: crate::realtime::jetstream_bootstrap::STREAM_NAME,
        subjects: &[crate::realtime::jetstream_bootstrap::STREAM_WILDCARD_SUBJECT],
        retention: config.stream_retention,
        max_age_secs: config.stream_max_age_secs,
        max_msgs_per_subject: config.stream_max_msgs_per_subject,
        replicas: config.stream_replicas,
        storage: config.stream_storage,
    };
    crate::realtime::jetstream_bootstrap::ensure_kanban_stream(&nats, &stream_config)
        .await
        .context("fatal: failed to bootstrap KANBAN_BOARD_EVENTS JetStream stream")?;

    info!(
        stream = crate::realtime::jetstream_bootstrap::STREAM_NAME,
        "JetStream stream bootstrapped"
    );

    // Stable pod identity used by the outbox dispatcher and NATS consumers.
    let pod_id = config.pod_name.unwrap_or_else(|| Id::new().to_string());

    // ── 4c. OpenSearch client (search service + outbox indexing) ──────────────
    let opensearch_client = Arc::new(OpenSearchClient::new(OpenSearchConfig {
        url: config.opensearch_url.clone(),
    }));
    info!(
        "OpenSearch client initialised (url={})",
        config.opensearch_url
    );

    // ── 4d. Outbox dispatcher (event_log → JetStream) ──────────────────────
    // Drains undispatched event_log rows to NATS JetStream at 250ms poll
    // cadence. Hold the handle so the task is not immediately dropped.
    // TODO: graceful shutdown — plumb a CancellationToken and abort on SIGTERM.
    let _outbox_handle = crate::realtime::outbox::OutboxDispatcher::new(
        pg_pool.clone(),
        Arc::clone(&nats),
        crate::realtime::outbox::OutboxConfig {
            poll_interval: Duration::from_millis(config.outbox_poll_interval_ms),
            batch_size: config.outbox_batch_size,
            pod_name: pod_id.clone(),
        },
    )
    .with_opensearch(
        Arc::clone(&opensearch_client),
        config.opensearch_index_name.clone(),
    )
    .spawn();

    info!("outbox dispatcher spawned");

    // ── 4c. BoardSubscriberRegistry — per-pod NATS push consumer fanout ────
    let board_registry = Arc::new(BoardSubscriberRegistry::new_with_config(
        Arc::clone(&nats),
        pod_id,
        crate::realtime::registry::RegistryConfig {
            broadcast_capacity: config.registry_broadcast_capacity,
            inactive_threshold_secs: config.registry_inactive_threshold_secs,
        },
    ));

    info!("BoardSubscriberRegistry constructed");

    // ── 5. Permission client for service handlers ───────────────────────────
    let permission = Arc::new(
        PermissionClient::new(&permission_config).context("failed to build permission client")?,
    );

    info!("Permission client initialised");

    // ── 7. Prometheus metrics ───────────────────────────────────────────────
    let metrics = Arc::new(
        KanbanMetrics::with_buckets(config.rpc_duration_buckets_secs.clone())
            .context("failed to register metrics")?,
    );

    info!("Prometheus metrics declared");

    // ── 8. S3 client for AttachmentService ─────────────────────────────────
    let s3_endpoint = config
        .s3_endpoint
        .clone()
        .unwrap_or_else(|| "http://seaweedfs-filer.storage.svc.cluster.local:8333".to_string());
    let s3_client = Arc::new(S3Client::new(S3Config {
        endpoint: s3_endpoint.clone(),
        public_endpoint: config.s3_public_endpoint.clone(),
        region: config.s3_region.clone(),
        access_key: config.s3_access_key.clone(),
        secret_key: config.s3_secret_key.clone(),
        bucket: config.s3_bucket.clone(),
    }));
    info!("S3 client initialised (endpoint={})", s3_endpoint);

    // ── Build Connect-RPC router (sunbeam-g2v serving stack) ─────────────────
    let connect_router = connectrpc::Router::new();
    let connect_router = Arc::new(AttachmentServiceImpl {
        pool: pg_pool.clone(),
        s3: Arc::clone(&s3_client),
        upload_expires_secs: config.upload_expires_secs,
        download_expires_secs: config.download_expires_secs,
    })
    .register(connect_router);
    let connect_router = Arc::new(BoardServiceImpl {
        pool: pg_pool.clone(),
        permission: Arc::clone(&permission),
        registry: Arc::clone(&board_registry),
        heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
        permission_recheck_interval: Duration::from_millis(config.permission_recheck_interval_ms),
        cutover_seen_capacity: config.cutover_seen_capacity,
    })
    .register(connect_router);
    let connect_router = Arc::new(CardServiceImpl {
        pool: pg_pool.clone(),
        permission: Arc::clone(&permission),
    })
    .register(connect_router);
    let connect_router = Arc::new(GitHubServiceImpl).register(connect_router);
    let connect_router = Arc::new(AggregatedBoardServiceImpl {
        pool: pg_pool.clone(),
        permission: Arc::clone(&permission),
        registry: Arc::clone(&board_registry),
        heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
        permission_recheck_interval: Duration::from_millis(config.permission_recheck_interval_ms),
        cutover_seen_capacity: config.cutover_seen_capacity,
    })
    .register(connect_router);
    let connect_router = Arc::new(ProjectServiceImpl {
        pool: pg_pool.clone(),
        permission: Arc::clone(&permission),
        registry: Arc::clone(&board_registry),
    })
    .register(connect_router);
    let connect_router = Arc::new(SearchServiceImpl {
        pool: pg_pool.clone(),
        permission: Arc::clone(&permission),
        opensearch: Arc::clone(&opensearch_client),
        index_name: Some(config.opensearch_index_name.clone()),
    })
    .register(connect_router);
    let connect_router = Arc::new(TemplatesServiceImpl {
        pool: pg_pool.clone(),
        permission: Arc::clone(&permission),
    })
    .register(connect_router);
    let grpc_axum = ServiceRouter::from_router(connect_router)
        .into_inner()
        .into_axum_router();

    // Public, unauthenticated RPCs (no introspection / permission middleware).
    // Registered on their own router and mounted on explicit paths so the
    // resulting axum Router has no fallback; this lets us merge it with the
    // main RPC router (which does have a fallback) without a runtime panic.
    let public_router = connectrpc::Router::new();
    let public_router = Arc::new(PublicBoardServiceImpl {
        pool: pg_pool.clone(),
    })
    .register(public_router);
    let public_svc = connectrpc::ConnectRpcService::new(public_router);
    let public_axum = Router::new()
        .route_service(
            "/sunbeam.kanban.v1.PublicBoardService/GetPublicBoard",
            public_svc.clone(),
        )
        .route_service(
            "/sunbeam.kanban.v1.PublicBoardService/ListPublicBoards",
            public_svc,
        )
        .layer(TraceLayer::new_for_http());

    // ── 9. Middleware stack (outer → inner) + axum Router ───────────────────
    //
    //   Layer order in axum is last-applied = outermost:
    //     .layer(A).layer(B).layer(C)  →  A wraps B wraps C wraps handler
    //
    //   We want: tracing → prometheus → IntrospectionLayer → permission_dispatch → routing
    //   So apply in reverse: permission_dispatch first, then IntrospectionLayer, then
    //   prometheus placeholder, then tracing.

    let dispatch_state = Arc::new(DispatchState {
        permission: Arc::clone(&permission),
    });

    let auth_state = AuthMiddlewareState::new(Arc::new(
        SsoGatewaySessionClient::new(
            &config.sso_gateway_introspection_url,
            OAuth2ClientCredentials::new(
                &config.sso_gateway_token_url,
                &config.sso_gateway_client_id,
                &config.sso_gateway_client_secret,
            )
            .with_scope("tenant:admin"),
        )
        .context("failed to build sso-gateway session client")?,
    ));

    let app_state = AppState {
        sso_gateway_url: config.sso_gateway_url,
        metrics: Arc::clone(&metrics),
    };

    // Non-RPC routes
    let aux_router = Router::new()
        .route("/healthz/live", get(healthz_live))
        .route("/healthz/ready", get(healthz_ready))
        .route("/metrics", get(metrics_handler))
        .with_state(app_state);

    // Apply auth layers only to the gRPC router. Health/metrics routes live
    // on their own unauthenticated router, and public RPCs are merged after the
    // auth layers so they remain unauthenticated.
    let grpc_auth = grpc_axum
        // permission_dispatch (innermost applied = innermost executed)
        .layer(middleware::from_fn_with_state(dispatch_state, dispatch))
        // auth_middleware — validates Bearer with the sso-gateway, inserts Extension<AuthContext>
        .layer(middleware::from_fn_with_state(auth_state, auth_middleware))
        // Tracing / OTel propagation (outermost)
        .layer(TraceLayer::new_for_http());

    let app = Router::new()
        .merge(aux_router.layer(TraceLayer::new_for_http()))
        .merge(grpc_auth)
        .merge(public_axum);

    info!(addr = %listener.local_addr().unwrap_or(config.addr), "kanban listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .context("axum serve error")?;

    info!("kanban shutdown complete");
    Ok(())
}

// ── OTel init ────────────────────────────────────────────────────────────────

fn init_otel_tracing() -> Result<()> {
    use opentelemetry::global;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    // Build OTel pipeline only when an endpoint is configured.
    if let Some(endpoint) = otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .context("failed to build OTLP span exporter")?;

        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();

        global::set_tracer_provider(provider);

        let otel_layer = tracing_opentelemetry::layer();

        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
            .with(tracing_subscriber::fmt::layer().json())
            .with(otel_layer)
            .try_init()
            .ok(); // ignore if a subscriber is already set (e.g. in tests)
    } else {
        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
            .with(tracing_subscriber::fmt::layer())
            .try_init()
            .ok();
    }

    Ok(())
}

// ── Graceful shutdown ────────────────────────────────────────────────────────

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("received shutdown signal");
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::realtime::jetstream_bootstrap::{Retention, Storage};
    use crate::test_support::containers;

    #[test]
    fn kanban_metrics_new_succeeds() {
        let metrics = KanbanMetrics::new();
        assert!(metrics.is_ok(), "metrics registration should succeed");
    }

    #[tokio::test]
    async fn healthz_ready_returns_ok_when_sso_gateway_ready() {
        let infra = containers::setup().await;

        let metrics = Arc::new(KanbanMetrics::new().unwrap());
        let state = AppState {
            sso_gateway_url: infra.sso_gateway_url.clone(),
            metrics,
        };

        let resp = healthz_ready(State(state)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_handler_renders_prometheus_text() {
        let infra = containers::setup().await;
        let metrics = Arc::new(KanbanMetrics::new().unwrap());
        // Increment the counter so the metric family is emitted by the text
        // encoder regardless of test ordering.
        metrics
            .permission_check_total
            .with_label_values(&["allow"])
            .inc();
        let state = AppState {
            sso_gateway_url: infra.sso_gateway_url.clone(),
            metrics,
        };

        let (status, body) = metrics_handler(State(state)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("kanban_permission_check_total"),
            "metrics body should contain kanban_permission_check_total: {body}"
        );
    }

    #[test]
    fn init_otel_tracing_without_endpoint_succeeds() {
        // If a subscriber is already set (e.g. from another test), the function
        // returns Ok because it ignores the duplicate-init error.
        let result = init_otel_tracing();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn healthz_live_returns_ok() {
        let resp = healthz_live().await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    fn parse_config_args(args: &[&str]) -> Result<AppConfig, clap::Error> {
        let mut full_args = vec!["kanban"];
        full_args.extend(args);
        Cli::try_parse_from(full_args).map(|cli| cli.into_config().expect("into_config"))
    }

    /// Parse CLI arguments with environment variables temporarily cleared.
    ///
    /// Integration tests set service endpoints in the environment, which would
    /// otherwise leak into these unit tests and change defaults / required-arg
    /// behaviour.
    fn parse_config_args_isolated(args: &[&str]) -> Result<AppConfig, clap::Error> {
        use crate::config::ENV_LOCK;
        let _guard = ENV_LOCK.lock().unwrap();

        let prefixes: &[&str] = &[
            "DATABASE_URL",
            "SSO_GATEWAY_",
            "NATS_",
            "OPENSEARCH_URL",
            "KANBAN_",
            "S3_",
            "POD_NAME",
        ];

        let snapshot: Vec<(String, Option<String>)> = std::env::vars()
            .filter(|(k, _)| prefixes.iter().any(|p| k == *p || k.starts_with(p)))
            .map(|(k, v)| (k, Some(v)))
            .collect();

        for (k, _) in &snapshot {
            // SAFETY: test-only env manipulation serialized through ENV_LOCK.
            unsafe { std::env::remove_var(k) };
        }

        let result = parse_config_args(args);

        for (k, v) in snapshot {
            if let Some(val) = v {
                // SAFETY: test-only env manipulation serialized through ENV_LOCK.
                unsafe { std::env::set_var(k, val) };
            }
        }

        result
    }

    #[test]
    fn cli_parses_all_fields() {
        let cfg = parse_config_args(&[
            "--port=1234",
            "--host=127.0.0.1",
            "--sso-gateway-introspection-url=http://sso-gateway:4445/oauth2/introspect",
            "--sso-gateway-client-id=kanban-client",
            "--sso-gateway-client-secret=kanban-secret",
            "--database-url=postgres://db",
            "--database-max-connections=50",
            "--database-acquire-timeout-secs=5",
            "--nats-url=nats://nats",
            "--nats-auth-token=callout-token",
            "--nats-lease-duration-secs=60",
            "--opensearch-url=http://opensearch",
            "--opensearch-index-name=custom-index",
            "--s3-endpoint=http://s3",
            "--s3-region=us-west-2",
            "--s3-access-key=access",
            "--s3-secret-key=secret",
            "--s3-bucket=my-bucket",
            "--pod-name=pod-1",
            "--rpc-duration-buckets-secs=0.1,0.2",
            "--stream-replicas=3",
            "--stream-max-age-secs=3600",
            "--stream-max-msgs-per-subject=5000",
            "--stream-retention=interest",
            "--stream-storage=memory",
            "--outbox-poll-interval-ms=100",
            "--outbox-batch-size=128",
            "--registry-broadcast-capacity=512",
            "--registry-inactive-threshold-secs=60",
            "--heartbeat-interval-ms=7000",
            "--permission-recheck-interval-ms=15000",
            "--cutover-seen-capacity=2048",
            "--upload-expires-secs=600",
            "--download-expires-secs=120",
        ])
        .expect("config should parse");

        assert_eq!(cfg.addr, "127.0.0.1:1234".parse().unwrap());
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(
            cfg.sso_gateway_introspection_url,
            "http://sso-gateway:4445/oauth2/introspect"
        );
        assert_eq!(cfg.sso_gateway_client_id, "kanban-client");
        assert_eq!(cfg.sso_gateway_client_secret, "kanban-secret");
        assert_eq!(cfg.database_url, "postgres://db");
        assert_eq!(cfg.database_max_connections, 50);
        assert_eq!(cfg.database_acquire_timeout_secs, 5);
        assert_eq!(cfg.nats_url, "nats://nats");
        assert_eq!(cfg.nats_auth_token, Some("callout-token".into()));
        assert_eq!(cfg.nats_lease_duration_secs, 60);
        assert_eq!(cfg.sso_gateway_url, "http://sso-gateway:4445");
        assert_eq!(cfg.opensearch_url, "http://opensearch");
        assert_eq!(cfg.opensearch_index_name, "custom-index");
        assert_eq!(cfg.s3_endpoint, Some("http://s3".into()));
        assert_eq!(cfg.s3_region, "us-west-2");
        assert_eq!(cfg.s3_access_key, "access");
        assert_eq!(cfg.s3_secret_key, "secret");
        assert_eq!(cfg.s3_bucket, "my-bucket");
        assert_eq!(cfg.pod_name, Some("pod-1".into()));
        assert_eq!(cfg.rpc_duration_buckets_secs, vec![0.1, 0.2]);
        assert_eq!(cfg.stream_replicas, 3);
        assert_eq!(cfg.stream_max_age_secs, 3600);
        assert_eq!(cfg.stream_max_msgs_per_subject, 5000);
        assert_eq!(cfg.stream_retention, Retention::Interest);
        assert_eq!(cfg.stream_storage, Storage::Memory);
        assert_eq!(cfg.outbox_poll_interval_ms, 100);
        assert_eq!(cfg.outbox_batch_size, 128);
        assert_eq!(cfg.registry_broadcast_capacity, 512);
        assert_eq!(cfg.registry_inactive_threshold_secs, 60);
        assert_eq!(cfg.heartbeat_interval_ms, 7000);
        assert_eq!(cfg.permission_recheck_interval_ms, 15000);
        assert_eq!(cfg.cutover_seen_capacity, 2048);
        assert_eq!(cfg.upload_expires_secs, 600);
        assert_eq!(cfg.download_expires_secs, 120);
    }

    #[test]
    fn cli_uses_defaults() {
        let cfg = parse_config_args_isolated(&["--database-url=postgres://db"])
            .expect("config should parse with defaults");

        assert_eq!(cfg.addr.port(), 8080);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(
            cfg.sso_gateway_introspection_url,
            "http://localhost:4445/oauth2/introspect"
        );
        assert_eq!(cfg.sso_gateway_client_id, "");
        assert_eq!(cfg.sso_gateway_client_secret, "");
        assert_eq!(cfg.database_max_connections, 20);
        assert_eq!(cfg.database_acquire_timeout_secs, 10);
        assert_eq!(cfg.nats_url, "nats://localhost:4222");
        assert!(cfg.nats_auth_token.is_none());
        assert_eq!(cfg.nats_lease_duration_secs, 30);
        assert_eq!(cfg.sso_gateway_url, "http://localhost:4445");
        assert_eq!(cfg.opensearch_url, "http://localhost:9200");
        assert_eq!(cfg.opensearch_index_name, "sunbeam-kanban-cards-v1");
        assert!(cfg.s3_endpoint.is_none());
        assert_eq!(cfg.s3_region, "us-east-1");
        assert_eq!(cfg.s3_bucket, "sunbeam-kanban");
        assert!(cfg.pod_name.is_none());
        assert_eq!(
            cfg.rpc_duration_buckets_secs,
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        );
        assert_eq!(cfg.stream_replicas, 1);
        assert_eq!(cfg.stream_max_age_secs, 86_400);
        assert_eq!(cfg.stream_max_msgs_per_subject, 10_000);
        assert_eq!(cfg.stream_retention, Retention::Limits);
        assert_eq!(cfg.stream_storage, Storage::File);
        assert_eq!(cfg.outbox_poll_interval_ms, 250);
        assert_eq!(cfg.outbox_batch_size, 256);
        assert_eq!(cfg.registry_broadcast_capacity, 256);
        assert_eq!(cfg.registry_inactive_threshold_secs, 30);
        assert_eq!(cfg.heartbeat_interval_ms, 15_000);
        assert_eq!(cfg.permission_recheck_interval_ms, 30_000);
        assert_eq!(cfg.cutover_seen_capacity, 1024);
        assert_eq!(cfg.upload_expires_secs, 900);
        assert_eq!(cfg.download_expires_secs, 300);
    }

    #[test]
    fn cli_requires_database_url() {
        let err = parse_config_args_isolated(&[]).unwrap_err();
        assert!(
            err.to_string().contains("database-url") || err.to_string().contains("DATABASE_URL"),
            "error should mention database-url/DATABASE_URL: {err}"
        );
    }

    #[test]
    fn cli_rejects_invalid_port() {
        let err =
            parse_config_args(&["--database-url=postgres://db", "--port=not-a-port"]).unwrap_err();
        assert!(
            err.to_string().contains("port") || err.to_string().contains("KANBAN_PORT"),
            "error should mention port/KANBAN_PORT: {err}"
        );
    }

    #[tokio::test]
    async fn run_with_config_starts_and_serves_health() {
        use std::time::Duration;

        use sqlx::postgres::PgPoolOptions;

        use crate::id::Id;

        let _infra = containers::setup().await;

        // `run_with_config` runs system migrations which drop and recreate
        // foreign keys, so point it at an isolated database instead of the
        // shared test database.
        let shared_pool = crate::test_support::setup_pool().await;
        let base_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let db_name = format!(
            "kanban_server_test_{}",
            Id::new().to_string().to_lowercase()
        );
        sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
            .execute(&shared_pool)
            .await
            .expect("failed to create isolated server test database");

        let mut isolated_url = url::Url::parse(&base_url).expect("invalid DATABASE_URL");
        isolated_url.set_path(&format!("/{db_name}"));
        let isolated_pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect(isolated_url.as_str())
            .await
            .expect("failed to connect to isolated server test database");

        sqlx::migrate!("./migrations")
            .run(&isolated_pool)
            .await
            .expect("failed to run schema migrations on isolated database");

        let config = AppConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            host: "127.0.0.1".into(),
            sso_gateway_introspection_url: "http://localhost:4445/oauth2/introspect".into(),
            // Boot provisions the permission namespace fail-fast, so the test
            // needs the harness-provisioned service credentials.
            sso_gateway_client_id: std::env::var("SSO_GATEWAY_CLIENT_ID").unwrap_or_default(),
            sso_gateway_client_secret: std::env::var("SSO_GATEWAY_CLIENT_SECRET")
                .unwrap_or_default(),
            system_tenant_id: std::env::var("KANBAN_SYSTEM_TENANT_ID")
                .unwrap_or_else(|_| "system".into()),
            database_url: isolated_url.to_string(),
            database_max_connections: 20,
            database_acquire_timeout_secs: 10,
            nats_url: std::env::var("NATS_URL").expect("NATS_URL"),
            nats_auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
            nats_lease_duration_secs: 30,
            sso_gateway_url: std::env::var("SSO_GATEWAY_URL")
                .unwrap_or_else(|_| "http://localhost:8080".into()),
            sso_gateway_permission_url: std::env::var("SSO_GATEWAY_PERMISSION_URL")
                .unwrap_or_else(|_| "http://localhost:8080".into()),
            sso_gateway_token_url: std::env::var("SSO_GATEWAY_TOKEN_URL")
                .unwrap_or_else(|_| "http://localhost:8080/oauth2/token".into()),
            opensearch_url: std::env::var("OPENSEARCH_URL")
                .unwrap_or_else(|_| "http://localhost:9200".into()),
            opensearch_index_name: "sunbeam-kanban-cards-v1".into(),
            s3_endpoint: std::env::var("S3_ENDPOINT").ok(),
            s3_public_endpoint: std::env::var("S3_PUBLIC_ENDPOINT").ok(),
            s3_region: "us-east-1".into(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_bucket: "sunbeam-kanban".into(),
            pod_name: Some("test-pod".into()),
            rpc_duration_buckets_secs: vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ],
            stream_replicas: 1,
            stream_max_age_secs: 86_400,
            stream_max_msgs_per_subject: 10_000,
            stream_retention: Retention::Limits,
            stream_storage: Storage::File,
            outbox_poll_interval_ms: 250,
            outbox_batch_size: 256,
            registry_broadcast_capacity: 256,
            registry_inactive_threshold_secs: 30,
            heartbeat_interval_ms: 15_000,
            permission_recheck_interval_ms: 30_000,
            cutover_seen_capacity: 1024,
            upload_expires_secs: 900,
            download_expires_secs: 300,
        };

        let listener = tokio::net::TcpListener::bind(config.addr)
            .await
            .expect("test listener should bind");
        let local_addr = listener.local_addr().expect("local addr");

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown = async {
            rx.await.ok();
        };

        let handle = tokio::spawn(async move { run_with_config(config, listener, shutdown).await });

        // Wait for migrations + service startup, polling the health endpoint.
        let health_url = format!("http://{local_addr}/healthz/live");
        let mut resp = None;
        for _ in 0..50 {
            if let Ok(r) = reqwest::get(&health_url).await {
                resp = Some(r);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let resp = resp.expect("should reach health endpoint within 10s");
        assert_eq!(resp.status(), StatusCode::OK);

        tx.send(()).ok();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .ok();
    }
}
