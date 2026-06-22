// SPDX-License-Identifier: AGPL-3.0-or-later
//! Bootstrap for the Kanban service.
//!
//! This is where the whole process comes together: load configuration from the
//! environment, set up OpenTelemetry tracing, connect to Postgres and run
//! migrations, bootstrap the NATS JetStream board-events stream, start the
//! Valkey-backed logout watermark, warm up the Keto readiness tuple, register
//! Prometheus metrics, build the axum router, and finally start listening on
//! `KANBAN_PORT` until the process receives a shutdown signal.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    Router, extract::State, http::StatusCode, middleware, response::IntoResponse, routing::get,
};
use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};
use sqlx::postgres::PgPoolOptions;
use tonic::service::Routes as TonicRoutes;
use tower_http::trace::TraceLayer;
use tracing::info;

use sunbeam_g2v::config::AuthConfig;
use sunbeam_g2v::config::NatsConfig;
use sunbeam_g2v::middleware::auth::jwt::{JwtLayer, JwtValidator};
use sunbeam_g2v::middleware::auth::keto::{KetoClient, KetoConfig};
use sunbeam_g2v::mq::NatsClient;

use crate::auth::keto_dispatch::{DispatchState, dispatch};
use crate::auth::logout_watermark::LogoutWatermark;
use crate::integrations::opensearch::{OpenSearchClient, OpenSearchConfig};
use crate::integrations::s3::{S3Client, S3Config};
use crate::pb::{
    aggregated_board_service_server::AggregatedBoardServiceServer,
    attachment_service_server::AttachmentServiceServer, auth_service_server::AuthServiceServer,
    board_service_server::BoardServiceServer, card_service_server::CardServiceServer,
    github_link_service_server::GithubLinkServiceServer,
    project_service_server::ProjectServiceServer,
    public_board_service_server::PublicBoardServiceServer,
    search_service_server::SearchServiceServer, templates_service_server::TemplatesServiceServer,
};
use crate::realtime::registry::BoardSubscriberRegistry;
use crate::services::{
    aggregated_boards::AggregatedBoardServiceImpl, attachments::AttachmentServiceImpl,
    auth::AuthServiceImpl, boards::BoardServiceImpl, cards::CardServiceImpl,
    github::GitHubServiceImpl, projects::ProjectServiceImpl, public_boards::PublicBoardServiceImpl,
    search::SearchServiceImpl, templates::TemplatesServiceImpl,
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
    /// Total errors while reading the logout watermark from Valkey.
    pub logout_watermark_errors_total: prometheus::Counter,
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
        let registry = Arc::new(Registry::new());

        let keto_check_total = CounterVec::new(
            Opts::new("kanban_keto_check_total", "Keto permission check results"),
            &["result"],
        )?;
        registry.register(Box::new(keto_check_total.clone()))?;

        let logout_watermark_errors_total = prometheus::Counter::with_opts(Opts::new(
            "kanban_logout_watermark_errors_total",
            "Errors reading logout watermark from Valkey",
        ))?;
        registry.register(Box::new(logout_watermark_errors_total.clone()))?;

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
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
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
            logout_watermark_errors_total,
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
            "user:_kanban_startup_probe",
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

// ── Configuration ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub addr: SocketAddr,
    pub jwt_secret: String,
    pub database_url: String,
    pub nats_url: String,
    pub valkey_url: String,
    pub keto_read_addr: String,
    pub keto_write_addr: String,
    pub opensearch_url: String,
    pub s3_endpoint: Option<String>,
    pub pod_name: Option<String>,
}

pub fn load_config() -> Result<AppConfig> {
    load_config_with(|key| std::env::var(key).ok())
}

pub fn load_config_with<F>(get_env: F) -> Result<AppConfig>
where
    F: Fn(&str) -> Option<String>,
{
    let port: u16 = get_env("KANBAN_PORT")
        .unwrap_or_else(|| "8080".into())
        .parse()
        .context("KANBAN_PORT must be a valid port number")?;
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;

    let jwt_secret = get_env("JWT_SECRET").unwrap_or_else(|| "change-me".into());

    let database_url = get_env("DATABASE_URL").context("DATABASE_URL is required")?;

    let nats_url = get_env("NATS_URL").unwrap_or_else(|| "nats://localhost:4222".into());

    let valkey_url = get_env("VALKEY_URL").unwrap_or_else(|| "redis://localhost:6379".into());

    let keto_read_addr =
        get_env("KETO_READ_ADDR").unwrap_or_else(|| "http://localhost:4466".into());

    let keto_write_addr =
        get_env("KETO_WRITE_ADDR").unwrap_or_else(|| "http://localhost:4467".into());

    let opensearch_url =
        get_env("OPENSEARCH_URL").unwrap_or_else(|| "http://localhost:9200".into());

    let s3_endpoint = get_env("S3_ENDPOINT");
    let pod_name = get_env("POD_NAME");

    Ok(AppConfig {
        addr,
        jwt_secret,
        database_url,
        nats_url,
        valkey_url,
        keto_read_addr,
        keto_write_addr,
        opensearch_url,
        s3_endpoint,
        pod_name,
    })
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
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
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
            lease_duration: 30,
        })
        .await
        .context("failed to connect to NATS")?,
    );

    crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
        &nats,
        &crate::realtime::jetstream_bootstrap::default_config(),
    )
    .await
    .context("fatal: failed to bootstrap KANBAN_BOARD_EVENTS JetStream stream")?;

    info!(
        stream = crate::realtime::jetstream_bootstrap::STREAM_NAME,
        "JetStream stream bootstrapped"
    );

    // ── 4d. Outbox dispatcher (event_log → JetStream) ──────────────────────
    // Drains undispatched event_log rows to NATS JetStream at 250ms poll
    // cadence. Hold the handle so the task is not immediately dropped.
    // TODO: graceful shutdown — plumb a CancellationToken and abort on SIGTERM.
    let _outbox_handle =
        crate::realtime::outbox::OutboxDispatcher::new(pg_pool.clone(), Arc::clone(&nats)).spawn();

    info!("outbox dispatcher spawned");

    // ── 4c. BoardSubscriberRegistry — per-pod NATS push consumer fanout ────
    let pod_id = config
        .pod_name
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let board_registry = Arc::new(BoardSubscriberRegistry::new(Arc::clone(&nats), pod_id));

    info!("BoardSubscriberRegistry constructed");

    // ── 5. Valkey / logout watermark ────────────────────────────────────────
    let watermark = Arc::new(
        LogoutWatermark::new(&config.valkey_url).context("failed to construct LogoutWatermark")?,
    );

    info!("Valkey client initialised");

    // ── 6. Keto client + synthetic readiness tuple ──────────────────────────
    let keto = Arc::new(KetoClient::new(KetoConfig {
        grpc_endpoint: config.keto_read_addr,
        write_grpc_endpoint: config.keto_write_addr,
    }));

    ensure_keto_health_tuple(&keto).await?;

    info!("Keto client initialised and health tuple present");

    // ── 7. Prometheus metrics ───────────────────────────────────────────────
    let metrics = Arc::new(KanbanMetrics::new().context("failed to register metrics")?);

    info!("Prometheus metrics declared");

    // ── 8. S3 client for AttachmentService ─────────────────────────────────
    let s3_client = Arc::new(S3Client::new(S3Config::from_env()));
    info!(
        "S3 client initialised (endpoint={})",
        config.s3_endpoint.unwrap_or_else(|| "<default>".into())
    );

    // ── 8b. OpenSearch client for SearchService ─────────────────────────────
    let opensearch_client = Arc::new(OpenSearchClient::new(OpenSearchConfig::from_env()));
    info!(
        "OpenSearch client initialised (url={})",
        config.opensearch_url
    );

    // ── Build tonic gRPC router ─────────────────────────────────────────────
    let grpc_axum = TonicRoutes::new(AuthServiceServer::new(AuthServiceImpl {
        watermark: Arc::clone(&watermark),
    }))
    .add_service(AttachmentServiceServer::new(AttachmentServiceImpl {
        pool: pg_pool.clone(),
        s3: Arc::clone(&s3_client),
    }))
    .add_service(BoardServiceServer::new(BoardServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
        registry: Arc::clone(&board_registry),
        watermark: Arc::clone(&watermark),
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
            watermark: Arc::clone(&watermark),
        },
    ))
    .add_service(ProjectServiceServer::new(ProjectServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
        registry: Arc::clone(&board_registry),
        watermark: Arc::clone(&watermark),
    }))
    .add_service(SearchServiceServer::new(SearchServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
        opensearch: Arc::clone(&opensearch_client),
        index_name: None,
    }))
    .add_service(TemplatesServiceServer::new(TemplatesServiceImpl {
        pool: pg_pool.clone(),
        keto: Arc::clone(&keto),
    }))
    .into_axum_router();

    // Public, unauthenticated RPCs (no JWT / Keto middleware).
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
    //   We want: tracing → prometheus → JwtLayer → keto_dispatch → routing
    //   So apply in reverse: keto_dispatch first, then JwtLayer, then
    //   prometheus placeholder, then tracing.

    let dispatch_state = Arc::new(DispatchState {
        keto: Arc::clone(&keto),
        watermark: Arc::clone(&watermark),
    });

    let jwt_validator = JwtValidator::new(AuthConfig {
        jwt_secret: config.jwt_secret.clone(),
        token_expiry: 3600,
    });

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
        // JwtLayer — validates Bearer, inserts Extension<AuthContext>
        .layer(JwtLayer::new(jwt_validator))
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
    let present = keto
        .check_permission(
            "_kanban_health",
            "probe",
            "health",
            "user:_kanban_startup_probe",
        )
        .await
        .context("Keto health-tuple check failed at boot")?;

    if !present {
        tracing::info!("_kanban_health tuple absent — writing once");
        keto.grant(
            "_kanban_health",
            "probe",
            "health",
            "user:_kanban_startup_probe",
        )
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
        let state = AppState {
            keto: Arc::clone(&infra.keto),
            metrics,
        };

        let (status, body) = metrics_handler(State(state)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("kanban_logout_watermark_errors_total"),
            "metrics body should contain kanban_logout_watermark_errors_total: {body}"
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

    #[test]
    fn load_config_with_parses_all_fields() {
        let cfg = load_config_with(|key| match key {
            "KANBAN_PORT" => Some("1234".into()),
            "JWT_SECRET" => Some("secret".into()),
            "DATABASE_URL" => Some("postgres://db".into()),
            "NATS_URL" => Some("nats://nats".into()),
            "VALKEY_URL" => Some("redis://valkey".into()),
            "KETO_READ_ADDR" => Some("http://keto-read".into()),
            "KETO_WRITE_ADDR" => Some("http://keto-write".into()),
            "OPENSEARCH_URL" => Some("http://opensearch".into()),
            "S3_ENDPOINT" => Some("http://s3".into()),
            "POD_NAME" => Some("pod-1".into()),
            _ => None,
        })
        .expect("config should parse");

        assert_eq!(cfg.addr.port(), 1234);
        assert_eq!(cfg.jwt_secret, "secret");
        assert_eq!(cfg.database_url, "postgres://db");
        assert_eq!(cfg.nats_url, "nats://nats");
        assert_eq!(cfg.valkey_url, "redis://valkey");
        assert_eq!(cfg.keto_read_addr, "http://keto-read");
        assert_eq!(cfg.keto_write_addr, "http://keto-write");
        assert_eq!(cfg.opensearch_url, "http://opensearch");
        assert_eq!(cfg.s3_endpoint, Some("http://s3".into()));
        assert_eq!(cfg.pod_name, Some("pod-1".into()));
    }

    #[test]
    fn load_config_with_uses_defaults() {
        let cfg = load_config_with(|key| match key {
            "DATABASE_URL" => Some("postgres://db".into()),
            _ => None,
        })
        .expect("config should parse with defaults");

        assert_eq!(cfg.addr.port(), 8080);
        assert_eq!(cfg.jwt_secret, "change-me");
        assert_eq!(cfg.nats_url, "nats://localhost:4222");
        assert_eq!(cfg.valkey_url, "redis://localhost:6379");
        assert_eq!(cfg.keto_read_addr, "http://localhost:4466");
        assert_eq!(cfg.keto_write_addr, "http://localhost:4467");
        assert_eq!(cfg.opensearch_url, "http://localhost:9200");
        assert!(cfg.pod_name.is_none());
    }

    #[test]
    fn load_config_with_requires_database_url() {
        let err = load_config_with(|_| None).unwrap_err();
        assert!(
            err.to_string().contains("DATABASE_URL is required"),
            "error should mention DATABASE_URL: {err}"
        );
    }

    #[test]
    fn load_config_with_rejects_invalid_port() {
        let err = load_config_with(|key| match key {
            "DATABASE_URL" => Some("postgres://db".into()),
            "KANBAN_PORT" => Some("not-a-port".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("KANBAN_PORT"),
            "error should mention KANBAN_PORT: {err}"
        );
    }

    #[tokio::test]
    async fn run_with_config_starts_and_serves_health() {
        let _infra = containers::setup().await;

        let config = AppConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            jwt_secret: "test-secret".into(),
            database_url: std::env::var("DATABASE_URL").expect("DATABASE_URL"),
            nats_url: std::env::var("NATS_URL").expect("NATS_URL"),
            valkey_url: std::env::var("VALKEY_URL").expect("VALKEY_URL"),
            keto_read_addr: std::env::var("KETO_READ_ADDR").expect("KETO_READ_ADDR"),
            keto_write_addr: std::env::var("KETO_WRITE_ADDR").expect("KETO_WRITE_ADDR"),
            opensearch_url: std::env::var("OPENSEARCH_URL")
                .unwrap_or_else(|_| "http://localhost:9200".into()),
            s3_endpoint: std::env::var("S3_ENDPOINT").ok(),
            pod_name: Some("test-pod".into()),
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
