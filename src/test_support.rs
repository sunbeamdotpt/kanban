//! Shared test helpers for kanban integration tests.
//!
//! All helpers here are `#[cfg(test)]` — they are compiled only during
//! `cargo test`. The module is declared as `#[cfg(test)] pub(crate) mod
//! test_support` in `lib.rs` so every service module can re-use the seeders
//! without duplicating the FK-chain SQL.
//!
//! # Seeder contract
//!
//! Each `seed_*` function allocates fresh UUIDs so parallel test tasks
//! (nextest runs each test in isolation) never collide on unique constraints.
//!
//! # Teardown
//!
//! Tests are responsible for their own cleanup. The seeders do not run
//! inside a transaction that gets rolled back — we hit a shared dev
//! Postgres instance and rely on unique UUIDs to keep tests independent.

use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Seed a minimal project row. Returns the new `project_id`.
pub(crate) async fn seed_project(pool: &PgPool) -> Uuid {
    let project_id = Uuid::new_v4();
    let suffix = project_id.simple().to_string();

    sqlx::query(
        "INSERT INTO projects (id, name, slug, owner_id, created_at, updated_at)
         VALUES ($1, $2, $3, 'user:test', now(), now())",
    )
    .bind(project_id)
    .bind(format!("test-proj-{}", &suffix[0..8]))
    .bind(suffix[0..12].to_lowercase())
    .execute(pool)
    .await
    .expect("seed_project: INSERT failed");

    project_id
}

/// Seed a minimal board row under `project_id`. Returns the new `board_id`.
pub(crate) async fn seed_board(pool: &PgPool, project_id: Uuid) -> Uuid {
    let board_id = Uuid::new_v4();
    let suffix = board_id.simple().to_string();

    sqlx::query(
        "INSERT INTO boards (id, project_id, name, slug, created_at, updated_at)
         VALUES ($1, $2, $3, $4, now(), now())",
    )
    .bind(board_id)
    .bind(project_id)
    .bind(format!("test-board-{}", &suffix[0..8]))
    .bind(suffix[0..12].to_lowercase())
    .execute(pool)
    .await
    .expect("seed_board: INSERT failed");

    board_id
}

/// Seed a minimal column row under `board_id`. Returns the new `column_id`.
pub(crate) async fn seed_column(pool: &PgPool, board_id: Uuid) -> Uuid {
    let column_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO columns (id, board_id, title, position, created_at, updated_at)
         VALUES ($1, $2, 'todo', 0, now(), now())",
    )
    .bind(column_id)
    .bind(board_id)
    .execute(pool)
    .await
    .expect("seed_column: INSERT failed");

    column_id
}

/// Seed a minimal card row under `board_id` / `column_id` / `project_id`.
/// Returns the new `card_id`.
pub(crate) async fn seed_card(
    pool: &PgPool,
    board_id: Uuid,
    column_id: Uuid,
    project_id: Uuid,
) -> Uuid {
    let card_id = Uuid::new_v4();
    let card_suffix = card_id.simple().to_string();

    sqlx::query(
        "INSERT INTO cards \
         (id, board_id, column_id, project_id, ref, title, position, revision, \
          created_by, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, 'test-card', 0, 0, 'user:test', now(), now())",
    )
    .bind(card_id)
    .bind(board_id)
    .bind(column_id)
    .bind(project_id)
    .bind(format!("T{}", &card_suffix[0..6].to_uppercase()))
    .execute(pool)
    .await
    .expect("seed_card: INSERT failed");

    card_id
}

/// Seed a full project → board → column → card chain.
/// Returns `(project_id, board_id, column_id, card_id)`.
///
/// Each call is independent (fresh UUIDs) so parallel tests don't collide.
pub(crate) async fn seed_card_chain(pool: &PgPool) -> (Uuid, Uuid, Uuid, Uuid) {
    let project_id = seed_project(pool).await;
    let board_id = seed_board(pool, project_id).await;
    let column_id = seed_column(pool, board_id).await;
    let card_id = seed_card(pool, board_id, column_id, project_id).await;
    (project_id, board_id, column_id, card_id)
}

/// Insert a raw `event_log` row for testing the outbox dispatcher.
///
/// Returns the inserted row's `id` (UUID).
pub(crate) async fn seed_event_log(pool: &PgPool, board_id: Uuid, event_type: &str) -> Uuid {
    let row = sqlx::query(
        "INSERT INTO event_log (id, board_id, event_type, payload, created_at)
         VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, now())
         RETURNING id",
    )
    .bind(board_id)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("seed_event_log: INSERT failed");

    row.get("id")
}

/// Insert a raw `event_log` row that is already dispatched (nats_seq set).
/// Returns the inserted row's `id`.
pub(crate) async fn seed_event_log_dispatched(
    pool: &PgPool,
    board_id: Uuid,
    event_type: &str,
    nats_seq: i64,
) -> Uuid {
    let row = sqlx::query(
        "INSERT INTO event_log \
         (id, board_id, event_type, payload, nats_seq, dispatched_at, created_at)
         VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, $3, now(), now())
         RETURNING id",
    )
    .bind(board_id)
    .bind(event_type)
    .bind(nats_seq)
    .fetch_one(pool)
    .await
    .expect("seed_event_log_dispatched: INSERT failed");

    row.get("id")
}

/// Standard DATABASE_URL resolver for integration tests.
/// Falls back to a sensible dev-compose default.
pub(crate) fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sunbeam:sunbeam@localhost:5432/kanban".to_string())
}

/// Standard NATS URL resolver for integration tests.
pub(crate) fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

// ── Testcontainers-backed dependency harness ───────────────────────────────
//
// This module starts Postgres, NATS (JetStream), Valkey, and Ory Keto in
// throwaway containers when `containers::setup()` is first called. It is
// designed to work with Docker-compatible runtimes; on macOS the project
// expects `DOCKER_HOST=unix://$HOME/.socktainer/container.sock`.
//
// If the standard environment variables are already set (DATABASE_URL, etc.)
// the harness skips container creation and uses the provided services.

#[cfg(test)]
pub(crate) mod containers {
    use std::sync::Arc;
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use sunbeam_g2v::config::NatsConfig;
    use sunbeam_g2v::middleware::auth::keto::KetoClient;
    use sunbeam_g2v::mq::NatsClient;
    use sunbeam_test::Keto;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, ImageExt};
    use testcontainers_modules::minio::MinIO;
    use testcontainers_modules::nats::{Nats, NatsServerCmd};
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::valkey::Valkey;
    use tokio::sync::OnceCell;

    use crate::auth::logout_watermark::LogoutWatermark;
    use crate::integrations::s3::{S3Client, S3Config};

    /// Live dependency clients used by integration tests.
    ///
    /// The optional `_*` fields keep testcontainers containers alive for the
    /// lifetime of the test process. They are `None` when the harness reuses
    /// externally-provided services via environment variables.
    pub struct TestInfra {
        pub pool: sqlx::PgPool,
        pub nats: Arc<NatsClient>,
        pub keto: Arc<KetoClient>,
        pub watermark: Arc<LogoutWatermark>,
        _pg: Option<ContainerAsync<Postgres>>,
        _nats: Option<ContainerAsync<Nats>>,
        _valkey: Option<ContainerAsync<Valkey>>,
        _keto: Option<ContainerAsync<testcontainers::GenericImage>>,
        _minio: Option<ContainerAsync<MinIO>>,
    }

    static INFRA: OnceCell<TestInfra> = OnceCell::const_new();

    /// Resolve the bridge IP of a running container.
    async fn bridge_ip(container: &ContainerAsync<impl testcontainers::Image>) -> String {
        container
            .get_bridge_ip_address()
            .await
            .expect("container bridge IP unavailable")
            .to_string()
    }

    /// Return a Keto YAML that declares the namespaces used by kanban.
    ///
    /// The in-memory Keto instance uses legacy namespace declarations so any
    /// relation/permission can be checked as a direct tuple. Tests that rely on
    /// derived permissions (e.g. board `view` via project access) must write the
    /// explicit `view` tuple themselves.
    fn keto_config() -> String {
        r#"
dsn: memory
namespaces:
  - id: 0
    name: KanbanProject
  - id: 1
    name: KanbanBoard
  - id: 2
    name: KanbanCard
  - id: 3
    name: KanbanAggregatedBoard
  - id: 4
    name: _kanban_health
serve:
  read:
    host: 0.0.0.0
    port: 4466
  write:
    host: 0.0.0.0
    port: 4467
  metrics:
    host: 0.0.0.0
    port: 4468
"#
        .to_string()
    }

    /// Build or reuse the shared test infrastructure.
    pub async fn setup() -> &'static TestInfra {
        INFRA
            .get_or_init(|| async {
                if let Some(infra) = from_env().await {
                    return infra;
                }
                start_containers().await
            })
            .await
    }

    /// Use externally-provided services when the standard env vars are set.
    async fn from_env() -> Option<TestInfra> {
        let database_url = std::env::var("DATABASE_URL").ok()?;
        let nats_url = std::env::var("NATS_URL").ok()?;
        let valkey_url = std::env::var("VALKEY_URL").ok()?;
        let keto_read_url = std::env::var("KETO_GRPC_URL")
            .or_else(|_| std::env::var("KETO_READ_ADDR"))
            .ok()?;
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .or_else(|_| std::env::var("KETO_WRITE_ADDR"))
            .ok()?;

        let pool = connect_pool(&database_url).await;
        let nats = connect_nats(&nats_url).await;
        let watermark = Arc::new(LogoutWatermark::new(&valkey_url).expect("LogoutWatermark::new"));
        let keto = Arc::new(KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_read_url.clone(),
                write_grpc_endpoint: keto_write_url.clone(),
            },
        ));

        // If the caller did not provide an S3 endpoint, start a throwaway MinIO
        // bucket for attachments tests.
        let minio = ensure_minio().await;

        Some(TestInfra {
            pool,
            nats,
            keto,
            watermark,
            _pg: None,
            _nats: None,
            _valkey: None,
            _keto: None,
            _minio: minio,
        })
    }

    /// Start all containers and run migrations.
    async fn start_containers() -> TestInfra {
        eprintln!("[testcontainers] starting Postgres, NATS, Valkey, and Keto...");

        let startup_timeout = Duration::from_secs(600);

        let pg_fut = tokio::spawn(
            Postgres::default()
                .with_db_name("kanban")
                .with_user("sunbeam")
                .with_password("sunbeam")
                .with_startup_timeout(startup_timeout)
                .start(),
        );

        let nats_cmd = NatsServerCmd::default().with_jetstream();
        let nats_fut = tokio::spawn(
            Nats::default()
                .with_cmd(&nats_cmd)
                .with_startup_timeout(startup_timeout)
                .start(),
        );

        let valkey_fut = tokio::spawn(
            Valkey::default()
                .with_startup_timeout(startup_timeout)
                .start(),
        );

        let keto_fut = tokio::spawn(Keto::new().with_config(keto_config()).start());

        let pg = pg_fut
            .await
            .expect("Postgres startup task panicked")
            .expect("failed to start Postgres container");
        let nats_container = nats_fut
            .await
            .expect("NATS startup task panicked")
            .expect("failed to start NATS container");
        let valkey = valkey_fut
            .await
            .expect("Valkey startup task panicked")
            .expect("failed to start Valkey container");
        let keto_container = keto_fut
            .await
            .expect("Keto startup task panicked")
            .expect("failed to start Keto container");

        eprintln!("[testcontainers] containers started; resolving bridge IPs...");

        let pg_ip = bridge_ip(&pg).await;
        let nats_ip = bridge_ip(&nats_container).await;
        let valkey_ip = bridge_ip(&valkey).await;
        let keto_ip = bridge_ip(&keto_container).await;

        let database_url = format!("postgres://sunbeam:sunbeam@{pg_ip}:5432/kanban");
        let nats_url = format!("nats://{nats_ip}:4222");
        let valkey_url = format!("redis://{valkey_ip}:6379");
        let keto_read_url = format!("http://{keto_ip}:4466");
        let keto_write_url = format!("http://{keto_ip}:4467");

        let pool = connect_pool(&database_url).await;

        eprintln!("[testcontainers] running database migrations...");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("failed to run database migrations");
        eprintln!("[testcontainers] infrastructure ready");

        let nats = connect_nats(&nats_url).await;
        let watermark = Arc::new(LogoutWatermark::new(&valkey_url).expect("LogoutWatermark::new"));
        let keto = Arc::new(KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_read_url.clone(),
                write_grpc_endpoint: keto_write_url.clone(),
            },
        ));

        // Start a throwaway MinIO bucket for attachments tests.
        let minio = ensure_minio().await;

        TestInfra {
            pool,
            nats,
            keto,
            watermark,
            _pg: Some(pg),
            _nats: Some(nats_container),
            _valkey: Some(valkey),
            _keto: Some(keto_container),
            _minio: minio,
        }
    }

    const MINIO_BUCKET: &str = "sunbeam-kanban";

    /// Ensure an S3-compatible endpoint is available for attachments tests.
    ///
    /// If `S3_ENDPOINT` is already set the harness reuses it and returns `None`.
    /// Otherwise it starts a MinIO container, sets the standard S3 env vars, and
    /// creates the bucket.
    async fn ensure_minio() -> Option<ContainerAsync<MinIO>> {
        if std::env::var("S3_ENDPOINT").is_ok() {
            return None;
        }

        eprintln!("[testcontainers] starting MinIO...");
        let minio = MinIO::default()
            .with_startup_timeout(Duration::from_secs(600))
            .start()
            .await
            .expect("failed to start MinIO container");

        let ip = bridge_ip(&minio).await;
        let endpoint = format!("http://{ip}:9000");

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
        eprintln!("[testcontainers] MinIO ready at {endpoint}");

        Some(minio)
    }

    async fn connect_pool(database_url: &str) -> sqlx::PgPool {
        PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
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
            })
            .await
            .expect("NATS connect failed"),
        )
    }
}
