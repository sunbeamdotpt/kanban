// SPDX-License-Identifier: AGPL-3.0-or-later
//! Bootstrap for the Kanban service.
//!
//! This is where the whole process comes together: load configuration from the
//! environment, set up OpenTelemetry tracing, connect to Postgres and run
//! migrations, bootstrap the NATS JetStream board-events stream, warm up the
//! Keto readiness tuple, register Prometheus metrics, build the axum router,
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
use tonic::service::Routes as TonicRoutes;
use tower_http::trace::TraceLayer;
use tracing::info;

use sunbeam_g2v::config::HydraConfig;
use sunbeam_g2v::config::NatsConfig;
use sunbeam_g2v::middleware::auth::introspection::{IntrospectionClient, IntrospectionLayer};
use sunbeam_g2v::middleware::auth::keto::{KetoClient, KetoConfig};
use sunbeam_g2v::mq::NatsClient;

use crate::auth::keto_dispatch::{DispatchState, dispatch};
use crate::integrations::opensearch::{OpenSearchClient, OpenSearchConfig};
use crate::integrations::s3::{S3Client, S3Config};
use crate::pb::{
    aggregated_board_service_server::AggregatedBoardServiceServer,
    attachment_service_server::AttachmentServiceServer, board_service_server::BoardServiceServer,
    card_service_server::CardServiceServer, github_link_service_server::GithubLinkServiceServer,
    project_service_server::ProjectServiceServer,
    public_board_service_server::PublicBoardServiceServer,
    search_service_server::SearchServiceServer, templates_service_server::TemplatesServiceServer,
};
use crate::realtime::registry::BoardSubscriberRegistry;
use crate::services::{
    aggregated_boards::AggregatedBoardServiceImpl, attachments::AttachmentServiceImpl,
    boards::BoardServiceImpl, cards::CardServiceImpl, github::GitHubServiceImpl,
    projects::ProjectServiceImpl, public_boards::PublicBoardServiceImpl, search::SearchServiceImpl,
    templates::TemplatesServiceImpl,
};

// ── JetStream stream name & config ──────────────────────────────────────────
//
// Naming and config live in realtime::jetstream_bootstrap; server.rs only
// calls the bootstrap function at startup.

// ── Prometheus metrics registry shared across handlers ──────────────────────

#[derive(Clone)]
pub struct KanbanMetrics {
    pub registry: Arc<Registry>,
    /// Counter for Keto permission checks, labeled `result` (allow, deny, or error).
    pub keto_check_total: CounterVec,
    /// Number of active board subscription streams.
    pub subscribe_active_streams: prometheus::Gauge,
    /// JetStream consumer lag in seconds, labeled by board.
    pub jet_stream_lag_seconds: GaugeVec,
    /// Fraction of `project_member_view` rows that differ from Keto.
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

        let keto_check_total = CounterVec::new(
            Opts::new("kanban_keto_check_total", "Keto permission check results"),
            &["result"],
        )?;
        registry.register(Box::new(keto_check_total.clone()))?;

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
            "Fraction of project_member_view rows that differ from Keto (Stage 7a)",
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
            keto_check_total,
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
    keto: Arc<KetoClient>,
    metrics: Arc<KanbanMetrics>,
}

// ── Health handlers ──────────────────────────────────────────────────────────

async fn healthz_live() -> impl IntoResponse {
    StatusCode::OK
}

/// Ready probe. Returns 200 once Keto confirms the synthetic `_kanban_health`
/// tuple is present; otherwise returns 503.
async fn healthz_ready(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .keto
        .check_permission(
            "_kanban_health",
            "probe",
            "health",
            &crate::auth::keto_retry::keto_subject_id("user:_kanban_startup_probe"),
        )
        .await
    {
        Ok(true) => StatusCode::OK,
        Ok(false) => {
            tracing::warn!("readiness probe: Keto health tuple missing");
            StatusCode::SERVICE_UNAVAILABLE
        }
        Err(e) => {
            tracing::error!(error = %e, "readiness probe: Keto check failed");
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

    /// Hydra token introspection endpoint URL.
    #[arg(
        long,
        env = "HYDRA_INTROSPECTION_URL",
        default_value = "http://localhost:4445/oauth2/introspect"
    )]
    hydra_introspection_url: String,

    /// Hydra OAuth2 client ID used for introspection Basic authentication.
    #[arg(long, env = "HYDRA_CLIENT_ID", default_value = "")]
    hydra_client_id: String,

    /// Hydra OAuth2 client secret used for introspection Basic authentication.
    #[arg(long, env = "HYDRA_CLIENT_SECRET", default_value = "")]
    hydra_client_secret: String,

    /// NATS server URL.
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Optional NATS authentication token (supports NATS auth callout).
    #[arg(long, env = "NATS_AUTH_TOKEN")]
    nats_auth_token: Option<String>,

    /// Keto read API endpoint.
    #[arg(long, env = "KETO_READ_ADDR", default_value = "http://localhost:4466")]
    keto_read_addr: String,

    /// Keto write API endpoint.
    #[arg(long, env = "KETO_WRITE_ADDR", default_value = "http://localhost:4467")]
    keto_write_addr: String,

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

    /// Keto permission recheck interval for live streams in milliseconds.
    #[arg(long, env = "KANBAN_KETO_RECHECK_INTERVAL_MS", default_value = "30000")]
    keto_recheck_interval_ms: u64,

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

        Ok(AppConfig {
            addr,
            host: self.host,
            hydra_introspection_url: self.hydra_introspection_url,
            hydra_client_id: self.hydra_client_id,
            hydra_client_secret: self.hydra_client_secret,
            database_url: self.database_url,
            database_max_connections: self.database_max_connections,
            database_acquire_timeout_secs: self.database_acquire_timeout_secs,
            nats_url: self.nats_url,
            nats_auth_token: self.nats_auth_token,
            nats_lease_duration_secs: self.nats_lease_duration_secs,
            keto_read_addr: self.keto_read_addr,
            keto_write_addr: self.keto_write_addr,
            opensearch_url: self.opensearch_url,
            opensearch_index_name: self.opensearch_index_name,
            s3_endpoint: self.s3_endpoint,
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
            keto_recheck_interval_ms: self.keto_recheck_interval_ms,
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
    pub hydra_introspection_url: String,
    pub hydra_client_id: String,
    pub hydra_client_secret: String,
    pub database_url: String,
    pub database_max_connections: u32,
    pub database_acquire_timeout_secs: u64,
    pub nats_url: String,
    pub nats_auth_token: Option<String>,
    pub nats_lease_duration_secs: u64,
    pub keto_read_addr: String,
    pub keto_write_addr: String,
    pub opensearch_url: String,
    pub opensearch_index_name: String,
    pub s3_endpoint: Option<String>,
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
    pub keto_recheck_interval_ms: u64,
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
    let pod_id = config
        .pod_name
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());

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

    // ── 5. Keto client + synthetic readiness tuple ──────────────────────────
    let keto = Arc::new(KetoClient::new(KetoConfig {
        grpc_endpoint: config.keto_read_addr,
        write_grpc_endpoint: config.keto_write_addr,
    }));

    ensure_keto_health_tuple(&keto).await?;

    info!("Keto client initialised and health tuple present");

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
        region: config.s3_region.clone(),
        access_key: config.s3_access_key.clone(),
        secret_key: config.s3_secret_key.clone(),
        bucket: config.s3_bucket.clone(),
    }));
    info!("S3 client initialised (endpoint={})", s3_endpoint);

    // ── 8b. OpenSearch client for SearchService ─────────────────────────────
    let opensearch_client = Arc::new(OpenSearchClient::new(OpenSearchConfig {
        url: config.opensearch_url.clone(),
    }));
    info!(
        "OpenSearch client initialised (url={})",
        config.opensearch_url
    );

    // ── Build tonic gRPC router ─────────────────────────────────────────────
    let grpc_axum = TonicRoutes::new(AttachmentServiceServer::new(AttachmentServiceImpl {
        pool: pg_pool.clone(),
        s3: Arc::clone(&s3_client),
        upload_expires_secs: config.upload_expires_secs,
        download_expires_secs: config.download_expires_secs,
    }))
    .add_service(BoardServiceServer::new(BoardServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
        registry: Arc::clone(&board_registry),
        heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
        keto_recheck_interval: Duration::from_millis(config.keto_recheck_interval_ms),
        cutover_seen_capacity: config.cutover_seen_capacity,
    }))
    .add_service(CardServiceServer::new(CardServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
    }))
    .add_service(GithubLinkServiceServer::new(GitHubServiceImpl))
    .add_service(AggregatedBoardServiceServer::new(
        AggregatedBoardServiceImpl {
            pool: pg_pool.clone(),
            keto: Arc::clone(&keto),
            registry: Arc::clone(&board_registry),
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
            keto_recheck_interval: Duration::from_millis(config.keto_recheck_interval_ms),
            cutover_seen_capacity: config.cutover_seen_capacity,
        },
    ))
    .add_service(ProjectServiceServer::new(ProjectServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
        registry: Arc::clone(&board_registry),
    }))
    .add_service(SearchServiceServer::new(SearchServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
        opensearch: Arc::clone(&opensearch_client),
        index_name: Some(config.opensearch_index_name.clone()),
    }))
    .add_service(TemplatesServiceServer::new(TemplatesServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
    }))
    .into_axum_router();

    // Public, unauthenticated RPCs (no introspection / Keto middleware).
    // We route each method directly to the tonic service so the resulting
    // axum Router has no fallback; this lets us merge it with the main gRPC
    // router (which does have a fallback) without a runtime panic.
    let public_svc = PublicBoardServiceServer::new(PublicBoardServiceImpl {
        pool: pg_pool.clone(),
    });
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
    //   We want: tracing → prometheus → IntrospectionLayer → keto_dispatch → routing
    //   So apply in reverse: keto_dispatch first, then IntrospectionLayer, then
    //   prometheus placeholder, then tracing.

    let dispatch_state = Arc::new(DispatchState {
        keto: Arc::clone(&keto),
    });

    let introspection = IntrospectionLayer::new(IntrospectionClient::new(HydraConfig {
        introspection_url: config.hydra_introspection_url,
        client_id: config.hydra_client_id,
        client_secret: config.hydra_client_secret,
    }));

    let app_state = AppState {
        keto: Arc::clone(&keto),
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
        // keto_dispatch (innermost applied = innermost executed)
        .layer(middleware::from_fn_with_state(dispatch_state, dispatch))
        // IntrospectionLayer — validates Bearer with Hydra, inserts Extension<AuthContext>
        .layer(introspection)
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

// ── Keto health tuple ────────────────────────────────────────────────────────

/// Make sure the synthetic `_kanban_health` readiness tuple exists in Keto.
///
/// Reads first and writes only if the tuple is missing. A write failure is
/// treated as fatal and stops startup.
async fn ensure_keto_health_tuple(keto: &KetoClient) -> Result<()> {
    let probe_subject = crate::auth::keto_retry::keto_subject_id("user:_kanban_startup_probe");

    let present = keto
        .check_permission("_kanban_health", "probe", "health", &probe_subject)
        .await
        .context("Keto health-tuple check failed at boot")?;

    if !present {
        tracing::info!("_kanban_health tuple absent — writing once");
        keto.grant("_kanban_health", "probe", "health", &probe_subject)
            .await
            .context("fatal: failed to write Keto _kanban_health readiness tuple")?;
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
    async fn healthz_ready_returns_ok_when_tuple_present() {
        let infra = containers::setup().await;

        // Ensure the synthetic health tuple exists.
        ensure_keto_health_tuple(&infra.keto)
            .await
            .expect("health tuple should be writable");

        let metrics = Arc::new(KanbanMetrics::new().unwrap());
        let state = AppState {
            keto: Arc::clone(&infra.keto),
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
        metrics.keto_check_total.with_label_values(&["allow"]).inc();
        let state = AppState {
            keto: Arc::clone(&infra.keto),
            metrics,
        };

        let (status, body) = metrics_handler(State(state)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("kanban_keto_check_total"),
            "metrics body should contain kanban_keto_check_total: {body}"
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
            "HYDRA_",
            "NATS_",
            "KETO_",
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
            unsafe { std::env::remove_var(k) };
        }

        let result = parse_config_args(args);

        for (k, v) in snapshot {
            if let Some(val) = v {
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
            "--hydra-introspection-url=http://hydra:4445/oauth2/introspect",
            "--hydra-client-id=kanban-client",
            "--hydra-client-secret=kanban-secret",
            "--database-url=postgres://db",
            "--database-max-connections=50",
            "--database-acquire-timeout-secs=5",
            "--nats-url=nats://nats",
            "--nats-auth-token=callout-token",
            "--nats-lease-duration-secs=60",
            "--keto-read-addr=http://keto-read",
            "--keto-write-addr=http://keto-write",
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
            "--keto-recheck-interval-ms=15000",
            "--cutover-seen-capacity=2048",
            "--upload-expires-secs=600",
            "--download-expires-secs=120",
        ])
        .expect("config should parse");

        assert_eq!(cfg.addr, "127.0.0.1:1234".parse().unwrap());
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(
            cfg.hydra_introspection_url,
            "http://hydra:4445/oauth2/introspect"
        );
        assert_eq!(cfg.hydra_client_id, "kanban-client");
        assert_eq!(cfg.hydra_client_secret, "kanban-secret");
        assert_eq!(cfg.database_url, "postgres://db");
        assert_eq!(cfg.database_max_connections, 50);
        assert_eq!(cfg.database_acquire_timeout_secs, 5);
        assert_eq!(cfg.nats_url, "nats://nats");
        assert_eq!(cfg.nats_auth_token, Some("callout-token".into()));
        assert_eq!(cfg.nats_lease_duration_secs, 60);
        assert_eq!(cfg.keto_read_addr, "http://keto-read");
        assert_eq!(cfg.keto_write_addr, "http://keto-write");
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
        assert_eq!(cfg.keto_recheck_interval_ms, 15000);
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
            cfg.hydra_introspection_url,
            "http://localhost:4445/oauth2/introspect"
        );
        assert_eq!(cfg.hydra_client_id, "");
        assert_eq!(cfg.hydra_client_secret, "");
        assert_eq!(cfg.database_max_connections, 20);
        assert_eq!(cfg.database_acquire_timeout_secs, 10);
        assert_eq!(cfg.nats_url, "nats://localhost:4222");
        assert!(cfg.nats_auth_token.is_none());
        assert_eq!(cfg.nats_lease_duration_secs, 30);
        assert_eq!(cfg.keto_read_addr, "http://localhost:4466");
        assert_eq!(cfg.keto_write_addr, "http://localhost:4467");
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
        assert_eq!(cfg.keto_recheck_interval_ms, 30_000);
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
        let _infra = containers::setup().await;

        let config = AppConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            host: "127.0.0.1".into(),
            hydra_introspection_url: "http://localhost:4445/oauth2/introspect".into(),
            hydra_client_id: String::new(),
            hydra_client_secret: String::new(),
            database_url: std::env::var("DATABASE_URL").expect("DATABASE_URL"),
            database_max_connections: 20,
            database_acquire_timeout_secs: 10,
            nats_url: std::env::var("NATS_URL").expect("NATS_URL"),
            nats_auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
            nats_lease_duration_secs: 30,
            keto_read_addr: std::env::var("KETO_READ_ADDR").expect("KETO_READ_ADDR"),
            keto_write_addr: std::env::var("KETO_WRITE_ADDR").expect("KETO_WRITE_ADDR"),
            opensearch_url: std::env::var("OPENSEARCH_URL")
                .unwrap_or_else(|_| "http://localhost:9200".into()),
            opensearch_index_name: "sunbeam-kanban-cards-v1".into(),
            s3_endpoint: std::env::var("S3_ENDPOINT").ok(),
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
            keto_recheck_interval_ms: 30_000,
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
