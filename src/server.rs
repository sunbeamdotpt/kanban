//! Kanban service bootstrap — Stage 2c.
//!
//! Boot sequence:
//!   1. Load config from env.
//!   2. Init OTel tracing + metrics.
//!   3. Connect to Postgres; run sqlx migrations.
//!   4. Connect to NATS; bootstrap KANBAN_BOARD_EVENTS stream (fatal on failure).
//!   5. Connect to Valkey; construct LogoutWatermark.
//!   6. Connect to Keto; synthetic readiness tuple write-if-absent.
//!   7. Declare Prometheus metrics.
//!   8. Build axum Router with health, metrics, and tonic gRPC fallback.
//!   9. Apply middleware stack (outer→inner): tracing → prometheus → JwtLayer →
//!      keto_dispatch → service routing.
//!  10. Bind to KANBAN_PORT; serve until SIGTERM.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::get,
    Router,
};
use prometheus::{
    CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
};
use sqlx::postgres::PgPoolOptions;
use tonic::service::Routes as TonicRoutes;
use tower_http::trace::TraceLayer;
use tracing::info;

use sunbeam_g2v::config::AuthConfig;
use sunbeam_g2v::middleware::auth::jwt::{JwtLayer, JwtValidator};
use sunbeam_g2v::middleware::auth::keto::{KetoClient, KetoConfig};
use sunbeam_g2v::mq::NatsClient;
use sunbeam_g2v::config::NatsConfig;

use crate::auth::keto_dispatch::{dispatch, DispatchState};
use crate::auth::logout_watermark::LogoutWatermark;
use crate::integrations::opensearch::{OpenSearchClient, OpenSearchConfig};
use crate::integrations::s3::{S3Client, S3Config};
use crate::pb::{
    attachment_service_server::AttachmentServiceServer,
    auth_service_server::AuthServiceServer,
    board_service_server::BoardServiceServer,
    card_service_server::CardServiceServer,
    forgejo_link_service_server::ForgejoLinkServiceServer,
    project_service_server::ProjectServiceServer,
    search_service_server::SearchServiceServer,
};
use crate::services::{
    attachments::AttachmentServiceImpl, auth::AuthServiceImpl,
    boards::BoardServiceImpl, cards::CardServiceImpl,
    forgejo::ForgejoServiceImpl, projects::ProjectServiceImpl,
    search::SearchServiceImpl,
};

// ── JetStream stream name & config ──────────────────────────────────────────
//
// Naming and config live in realtime::jetstream_bootstrap; server.rs only
// calls the bootstrap function at startup.

// ── Prometheus metrics registry shared across handlers ──────────────────────

#[derive(Clone)]
pub struct KanbanMetrics {
    pub registry: Arc<Registry>,
    /// `kanban_keto_check_total{result}` — result ∈ {allow, deny, error}
    pub keto_check_total: CounterVec,
    /// `kanban_logout_watermark_errors_total`
    pub logout_watermark_errors_total: prometheus::Counter,
    /// `kanban_subscribe_active_streams`
    pub subscribe_active_streams: prometheus::Gauge,
    /// `kanban_jet_stream_lag_seconds`
    pub jet_stream_lag_seconds: GaugeVec,
    /// `kanban_mirror_drift_ratio`
    pub mirror_drift_ratio: prometheus::Gauge,
    /// `kanban_rpc_duration_seconds{service,method}`
    pub rpc_duration_seconds: HistogramVec,
    /// `kanban_rpc_total{service,method,status}`
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
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
            &["service", "method"],
        )?;
        registry.register(Box::new(rpc_duration_seconds.clone()))?;

        let rpc_total = CounterVec::new(
            Opts::new("kanban_rpc_total", "Total gRPC requests by service/method/status"),
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

/// `/healthz/ready` — 200 only after the synthetic `_kanban_health` Keto tuple
/// check passes. 503 otherwise.
async fn healthz_ready(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .keto
        .check_permission("_kanban_health", "probe", "health", "user:_kanban_startup_probe")
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

/// `/metrics` — render Prometheus text format.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
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

pub async fn run() -> Result<()> {
    // ── 1. Config from env ──────────────────────────────────────────────────
    let port: u16 = std::env::var("KANBAN_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .context("KANBAN_PORT must be a valid port number")?;
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;

    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "change-me".into());

    let database_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL is required")?;

    let nats_url = std::env::var("NATS_URL")
        .unwrap_or_else(|_| "nats://localhost:4222".into());

    let valkey_url = std::env::var("VALKEY_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".into());

    let keto_read_addr = std::env::var("KETO_READ_ADDR")
        .unwrap_or_else(|_| "http://localhost:4466".into());

    let keto_write_addr = std::env::var("KETO_WRITE_ADDR")
        .unwrap_or_else(|_| "http://localhost:4467".into());

    // ── 2. OTel tracing init ────────────────────────────────────────────────
    init_otel_tracing()?;

    info!(addr = %addr, "kanban service starting");

    // ── 3. Postgres + migrations ────────────────────────────────────────────
    let pg_pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;

    sqlx::migrate!("./migrations")
        .run(&pg_pool)
        .await
        .context("sqlx migrations failed")?;

    info!("Postgres connected and migrations applied");

    // ── 4. NATS + JetStream bootstrap (fatal on failure) ───────────────────
    let nats = NatsClient::connect(&NatsConfig {
        url: nats_url,
        jetstream: true,
        lease_duration: 30,
    })
    .await
    .context("failed to connect to NATS")?;

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

    // ── 5. Valkey / logout watermark ────────────────────────────────────────
    let watermark = Arc::new(
        LogoutWatermark::new(&valkey_url)
            .context("failed to construct LogoutWatermark")?,
    );

    info!("Valkey client initialised");

    // ── 6. Keto client + synthetic readiness tuple ──────────────────────────
    let keto = Arc::new(KetoClient::new(KetoConfig {
        grpc_endpoint: keto_read_addr,
        write_grpc_endpoint: keto_write_addr,
    }));

    ensure_keto_health_tuple(&keto).await?;

    info!("Keto client initialised and health tuple present");

    // ── 7. Prometheus metrics ───────────────────────────────────────────────
    let metrics = Arc::new(KanbanMetrics::new().context("failed to register metrics")?);

    info!("Prometheus metrics declared");

    // ── 8. S3 client for AttachmentService ─────────────────────────────────
    let s3_client = Arc::new(S3Client::new(S3Config::from_env()));
    info!("S3 client initialised (endpoint={})", std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "<default>".into()));

    // ── 8b. OpenSearch client for SearchService ─────────────────────────────
    let opensearch_client = Arc::new(OpenSearchClient::new(OpenSearchConfig::from_env()));
    info!("OpenSearch client initialised (url={})", std::env::var("OPENSEARCH_URL").unwrap_or_else(|_| "http://localhost:9200".into()));

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
        }))
        .add_service(CardServiceServer::new(CardServiceImpl {
            pool: pg_pool.clone(),
            keto: Arc::clone(&keto),
        }))
        .add_service(ForgejoLinkServiceServer::new(ForgejoServiceImpl))
        .add_service(ProjectServiceServer::new(ProjectServiceImpl {
            pool: pg_pool.clone(),
            keto: Arc::clone(&keto),
        }))
        .add_service(SearchServiceServer::new(SearchServiceImpl {
            keto: Arc::clone(&keto),
            opensearch: Arc::clone(&opensearch_client),
        }))
        .into_axum_router();

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
        jwt_secret: jwt_secret.clone(),
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

    // Non-RPC routes serve health/metrics; unmatched paths fall through to
    // the tonic gRPC axum router.
    let app = aux_router
        .merge(grpc_axum)
        // keto_dispatch (innermost applied = innermost executed)
        .layer(middleware::from_fn_with_state(
            dispatch_state,
            dispatch,
        ))
        // JwtLayer — validates Bearer, inserts Extension<AuthContext>
        .layer(JwtLayer::new(jwt_validator))
        // Prometheus metrics wrapper (records duration + status) — placeholder
        // for Stage 3; the KanbanMetrics struct is declared and scrape-able now.
        // Tower-level per-request instrumentation wired in Stage 3.
        //
        // Tracing / OTel propagation (outermost)
        .layer(TraceLayer::new_for_http());

    // ── 10. Bind and serve until SIGTERM ────────────────────────────────────
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {addr}"))?;

    info!(addr = %addr, "kanban listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
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
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

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

/// Ensure the synthetic `_kanban_health` readiness tuple exists.
///
/// Tries to read first; if absent, writes once. If write fails, returns Err
/// (which panics the service — correct per the plan's fatal-on-failure policy).
async fn ensure_keto_health_tuple(keto: &KetoClient) -> Result<()> {
    let present = keto
        .check_permission("_kanban_health", "probe", "health", "user:_kanban_startup_probe")
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
