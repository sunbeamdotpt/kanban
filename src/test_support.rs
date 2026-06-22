// SPDX-License-Identifier: AGPL-3.0-or-later
//! Shared helpers for integration tests.
//!
//! These helpers are only compiled during `cargo test`. They live in a
//! `#[cfg(test)]` module so every service can reuse seeders without copying the
//! foreign-key SQL needed to set up a test row.
//!
//! # Seeder contract
//!
//! Every `seed_*` function generates fresh UUIDs, so tests running in parallel
//! (for example under nextest) never collide on unique constraints.
//!
//! # Teardown
//!
//! Tests clean up after themselves. The seeders do not wrap inserts in a
//! transaction that gets rolled back; instead we rely on random UUIDs to keep
//! tests isolated on a shared Postgres instance.

use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Create a minimal project and return its id.
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

/// Create a minimal board under `project_id` and return its id.
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

/// Create a minimal column under `board_id` and return its id.
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

/// Create a minimal card under `board_id`, `column_id`, and `project_id` and
/// return its id.
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

/// Create a complete project → board → column → card chain.
///
/// Returns `(project_id, board_id, column_id, card_id)` with fresh UUIDs so
/// parallel tests stay independent.
pub(crate) async fn seed_card_chain(pool: &PgPool) -> (Uuid, Uuid, Uuid, Uuid) {
    let project_id = seed_project(pool).await;
    let board_id = seed_board(pool, project_id).await;
    let column_id = seed_column(pool, board_id).await;
    let card_id = seed_card(pool, board_id, column_id, project_id).await;
    (project_id, board_id, column_id, card_id)
}

/// Insert a raw `event_log` row for testing the outbox dispatcher.
///
/// Returns the inserted row's id.
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

/// Insert a raw `event_log` row that is already marked as dispatched.
///
/// Returns the inserted row's id.
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

/// Return the `DATABASE_URL` for integration tests, falling back to a sensible
/// dev-compose default.
pub(crate) fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sunbeam:sunbeam@localhost:5432/kanban".to_string())
}

/// Return the `NATS_URL` for integration tests, falling back to a sensible
/// dev-compose default.
pub(crate) fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

// ── Testcontainers-backed dependency harness ───────────────────────────────
//
// This module starts Postgres, NATS (JetStream), Valkey, and Ory Keto in
// throwaway containers when `containers::setup()` is first called. The
// implementation uses testcontainers' GenericImage directly instead of
// module-specific wrappers because Apple Container's Docker-compatible API
// (accessed via socktainer) does not reliably stream container logs, so
// log-based readiness strategies hang. Instead we start containers with no
// wait strategy and poll readiness manually.
//
// Because socktainer does not publish container ports to the host, we connect
// to each container via its bridge IP address and the original internal port.
// `sunbeam_test::container_bridge_ip` resolves that IP by inspecting
// `NetworkSettings.Networks`, which works with this runtime.

#[cfg(test)]
pub(crate) mod containers {
    use std::sync::Arc;
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use sunbeam_g2v::config::NatsConfig;
    use sunbeam_g2v::middleware::auth::keto::KetoClient;
    use sunbeam_g2v::mq::NatsClient;
    use sunbeam_test::container_bridge_ip;
    use testcontainers::core::{ContainerPort, IntoContainerPort};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    use crate::auth::logout_watermark::LogoutWatermark;
    use crate::integrations::s3::{S3Client, S3Config};

    /// Default container images for the harness. Override them with environment variables if needed.
    const POSTGRES_IMAGE: &str = match option_env!("KANBAN_TEST_POSTGRES_IMAGE") {
        Some(s) => s,
        None => "mirror.gcr.io/library/postgres:16-alpine",
    };
    const NATS_IMAGE: &str = match option_env!("KANBAN_TEST_NATS_IMAGE") {
        Some(s) => s,
        None => "nats:2.10-alpine",
    };
    const VALKEY_IMAGE: &str = match option_env!("KANBAN_TEST_VALKEY_IMAGE") {
        Some(s) => s,
        None => "valkey/valkey:8.0.2-alpine",
    };
    const KETO_IMAGE: &str = match option_env!("KANBAN_TEST_KETO_IMAGE") {
        Some(s) => s,
        None => "oryd/keto:v26.2.0",
    };
    const MINIO_IMAGE: &str = match option_env!("KANBAN_TEST_MINIO_IMAGE") {
        Some(s) => s,
        None => "minio/minio:RELEASE.2025-02-28T09-55-16Z",
    };
    const OPENSEARCH_IMAGE: &str = match option_env!("KANBAN_TEST_OPENSEARCH_IMAGE") {
        Some(s) => s,
        None => "opensearchproject/opensearch:2.19.1",
    };

    const MINIO_BUCKET: &str = "sunbeam-kanban";

    /// Clients for a single test's dependencies.
    ///
    /// A fresh `TestInfra` is returned on every call so that each test owns
    /// its own Postgres pool and clients. The heavy container handles are kept
    /// alive in the static `SHARED` singleton and stopped when the test process
    /// exits.
    pub struct TestInfra {
        pub pool: sqlx::PgPool,
        pub nats: Arc<NatsClient>,
        pub keto: Arc<KetoClient>,
        pub watermark: Arc<LogoutWatermark>,
    }

    /// Connection URLs and container handles shared by the whole test process.
    struct SharedInfra {
        database_url: String,
        nats_url: String,
        valkey_url: String,
        keto_read_url: String,
        keto_write_url: String,
        _pg: Option<ContainerAsync<GenericImage>>,
        _nats: Option<ContainerAsync<GenericImage>>,
        _valkey: Option<ContainerAsync<GenericImage>>,
        _keto: Option<ContainerAsync<GenericImage>>,
        _minio: Option<ContainerAsync<GenericImage>>,
        _opensearch: Option<ContainerAsync<GenericImage>>,
    }

    /// Clonable snapshot of the shared service URLs.
    struct SharedUrls {
        database_url: String,
        nats_url: String,
        valkey_url: String,
        keto_read_url: String,
        keto_write_url: String,
    }

    impl SharedInfra {
        fn urls(&self) -> SharedUrls {
            SharedUrls {
                database_url: self.database_url.clone(),
                nats_url: self.nats_url.clone(),
                valkey_url: self.valkey_url.clone(),
                keto_read_url: self.keto_read_url.clone(),
                keto_write_url: self.keto_write_url.clone(),
            }
        }
    }

    static SHARED: Mutex<Option<SharedInfra>> = Mutex::const_new(None);
    static CONTAINER_IDS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    /// Set up the shared test infrastructure and return a fresh `TestInfra`
    /// for the calling test.
    pub async fn setup() -> TestInfra {
        let urls = {
            let mut guard = SHARED.lock().await;
            if guard.is_none() {
                let infra = if let Some(infra) = from_env().await {
                    infra
                } else {
                    start_containers().await
                };
                *guard = Some(infra);
            }
            let infra = guard.as_ref().expect("shared infra initialized above");
            infra.urls()
        };

        build_test_infra(
            &urls.database_url,
            &urls.nats_url,
            &urls.valkey_url,
            &urls.keto_read_url,
            &urls.keto_write_url,
        )
        .await
    }

    /// Stop and remove every container started by this harness.
    ///
    /// This is synchronous so it can be called from `#[ctor::dtor]` at process
    /// exit. Containers are removed with `container rm -f` (falling back to
    /// `docker rm -f`) so the runtime's async context does not matter.
    pub fn teardown() {
        let ids: Vec<String> = match CONTAINER_IDS.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };

        for id in ids {
            remove_container(&id);
        }
    }

    fn remove_container(id: &str) {
        // Prefer the Docker-compatible API over the local CLI. The Apple
        // Container `container` binary uses UUIDs in its UI that do not match
        // the ids returned by testcontainers, but the unix socket still speaks
        // the Docker Engine API and accepts the id we have.
        if let Ok(docker_host) = std::env::var("DOCKER_HOST") {
            if docker_host.starts_with("unix://") {
                let path = &docker_host["unix://".len()..];
                if api_remove(path, id) {
                    eprintln!("[testcontainers] removed container {id}");
                    return;
                }
            }
        }

        // Fall back to whichever container CLI is available.
        for binary in ["container", "docker"] {
            let output = std::process::Command::new(binary)
                .args(["rm", "-f", id])
                .output();
            if let Ok(output) = output {
                if output.status.success() {
                    eprintln!("[testcontainers] removed container {id}");
                    return;
                }
            }
        }
        eprintln!("[testcontainers] failed to remove container {id}");
    }

    fn api_remove(socket_path: &str, id: &str) -> bool {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let request = format!(
            "DELETE /containers/{id}?force=true HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        );
        let mut stream = match UnixStream::connect(socket_path) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if stream.write_all(request.as_bytes()).is_err() {
            return false;
        }
        let mut response = String::new();
        if stream.read_to_string(&mut response).is_err() {
            return false;
        }
        response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.1 404")
    }

    /// Use externally-provided services when the standard env vars are set.
    async fn from_env() -> Option<SharedInfra> {
        let database_url = std::env::var("DATABASE_URL").ok()?;
        let nats_url = std::env::var("NATS_URL").ok()?;
        let valkey_url = std::env::var("VALKEY_URL").ok()?;
        let keto_read_url = std::env::var("KETO_GRPC_URL")
            .or_else(|_| std::env::var("KETO_READ_ADDR"))
            .ok()?;
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .or_else(|_| std::env::var("KETO_WRITE_ADDR"))
            .ok()?;

        // Ensure an S3 bucket exists even when the rest of the stack is external.
        let _ = ensure_minio().await;

        Some(SharedInfra {
            database_url,
            nats_url,
            valkey_url,
            keto_read_url,
            keto_write_url,
            _pg: None,
            _nats: None,
            _valkey: None,
            _keto: None,
            _minio: None,
            _opensearch: None,
        })
    }

    /// Start all containers, run migrations, and return the shared URLs/handles.
    async fn start_containers() -> SharedInfra {
        eprintln!(
            "[testcontainers] starting Postgres, NATS, Valkey, Keto, MinIO, and OpenSearch..."
        );

        let startup_timeout = Duration::from_secs(600);

        let pg = start_postgres(startup_timeout).await;
        let pg_ip = bridge_ip(&pg).await;
        let database_url = format!("postgres://sunbeam:sunbeam@{pg_ip}:5432/kanban");

        let nats = start_nats(startup_timeout).await;
        let nats_ip = bridge_ip(&nats).await;
        let nats_url = format!("nats://{nats_ip}:4222");

        let valkey = start_valkey(startup_timeout).await;
        let valkey_ip = bridge_ip(&valkey).await;
        let valkey_url = format!("redis://{valkey_ip}:6379");

        let keto = start_keto(startup_timeout).await;
        let keto_ip = bridge_ip(&keto).await;
        let keto_read_url = format!("http://{keto_ip}:4466");
        let keto_write_url = format!("http://{keto_ip}:4467");

        let minio = start_minio(startup_timeout).await;
        let minio_ip = bridge_ip(&minio).await;
        let s3_endpoint = format!("http://{minio_ip}:9000");

        let opensearch = start_opensearch(startup_timeout).await;
        let opensearch_ip = bridge_ip(&opensearch).await;
        let opensearch_url = format!("http://{opensearch_ip}:9200");

        {
            let mut ids = CONTAINER_IDS.lock().unwrap();
            ids.push(pg.id().to_string());
            ids.push(nats.id().to_string());
            ids.push(valkey.id().to_string());
            ids.push(keto.id().to_string());
            ids.push(minio.id().to_string());
            ids.push(opensearch.id().to_string());
        }

        // Export the service URLs as environment variables so that tests that
        // spin up their own clients (e.g. auth::keto_dispatch integration tests)
        // can find the running containers.
        unsafe {
            std::env::set_var("DATABASE_URL", &database_url);
            std::env::set_var("NATS_URL", &nats_url);
            std::env::set_var("VALKEY_URL", &valkey_url);
            std::env::set_var("KETO_GRPC_URL", &keto_read_url);
            std::env::set_var("KETO_READ_ADDR", &keto_read_url);
            std::env::set_var("KETO_WRITE_GRPC_URL", &keto_write_url);
            std::env::set_var("KETO_WRITE_ADDR", &keto_write_url);
            std::env::set_var("OPENSEARCH_URL", &opensearch_url);
        }

        // Run migrations against the fresh Postgres instance.
        {
            let pool = connect_pool(&database_url).await;
            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("failed to run database migrations");
        }

        // Create the MinIO bucket used by attachment tests.
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
            valkey_url,
            keto_read_url,
            keto_write_url,
            _pg: Some(pg),
            _nats: Some(nats),
            _valkey: Some(valkey),
            _keto: Some(keto),
            _minio: Some(minio),
            _opensearch: Some(opensearch),
        }
    }

    /// Return the bridge IP address of a running container.
    ///
    /// Uses `sunbeam_test::container_bridge_ip`, which inspects
    /// `NetworkSettings.Networks` directly and therefore works with runtimes
    /// such as socktainer that do not expose host port mappings.
    async fn bridge_ip(container: &ContainerAsync<GenericImage>) -> String {
        container_bridge_ip(container.id())
            .await
            .expect("failed to resolve container bridge IP")
    }

    async fn start_postgres(timeout: Duration) -> ContainerAsync<GenericImage> {
        let parts: Vec<&str> = POSTGRES_IMAGE.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (POSTGRES_IMAGE.to_string(), "latest".to_string()),
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

        let host = bridge_ip(&container).await;

        // Poll until Postgres accepts connections.
        for _ in 0..120 {
            match tokio::net::TcpStream::connect((&*host, 5432)).await {
                Ok(_) => {
                    // Also verify we can run a simple query.
                    let url = format!("postgres://sunbeam:sunbeam@{host}:5432/kanban");
                    if let Ok(pool) = PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(Duration::from_secs(2))
                        .connect(&url)
                        .await
                    {
                        if sqlx::query("SELECT 1").fetch_optional(&pool).await.is_ok() {
                            return container;
                        }
                    }
                }
                Err(_) => {}
            }
            sleep(Duration::from_millis(500)).await;
        }

        panic!("Postgres did not become ready in time");
    }

    async fn start_nats(timeout: Duration) -> ContainerAsync<GenericImage> {
        let parts: Vec<&str> = NATS_IMAGE.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (NATS_IMAGE.to_string(), "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(4222.tcp())
            .with_cmd(vec!["-js"])
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start NATS container");

        let host = bridge_ip(&container).await;

        for _ in 0..120 {
            if tokio::net::TcpStream::connect((&*host, 4222)).await.is_ok() {
                return container;
            }
            sleep(Duration::from_millis(250)).await;
        }

        panic!("NATS did not become ready in time");
    }

    async fn start_valkey(timeout: Duration) -> ContainerAsync<GenericImage> {
        let parts: Vec<&str> = VALKEY_IMAGE.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (VALKEY_IMAGE.to_string(), "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(6379.tcp())
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start Valkey container");

        let host = bridge_ip(&container).await;

        for _ in 0..120 {
            let url = format!("redis://{host}:6379");
            if let Ok(client) = redis::Client::open(url.as_str()) {
                if client.get_multiplexed_async_connection().await.is_ok() {
                    return container;
                }
            }
            sleep(Duration::from_millis(250)).await;
        }

        panic!("Valkey did not become ready in time");
    }

    async fn start_keto(timeout: Duration) -> ContainerAsync<GenericImage> {
        // Write the Keto config to a host path and bind-mount it into the
        // container. testcontainers' `with_copy_to` copies files before the
        // container starts, which socktainer does not support ("Rootfs not
        // found"); a bind mount works because the runtime resolves it at start.
        let config_path =
            std::env::temp_dir().join(format!("kanban-keto-config-{}.yml", std::process::id()));
        std::fs::write(&config_path, keto_config()).expect("write keto config");

        let parts: Vec<&str> = KETO_IMAGE.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (KETO_IMAGE.to_string(), "latest".to_string()),
        };

        let container = GenericImage::new(name, tag)
            .with_exposed_port(ContainerPort::Tcp(4466))
            .with_exposed_port(ContainerPort::Tcp(4467))
            .with_exposed_port(ContainerPort::Tcp(4468))
            .with_host_config_modifier(move |host_config| {
                let bind = format!("{}:/home/ory/keto.yml", config_path.display());
                host_config.binds = Some(vec![bind]);
            })
            .with_cmd(vec!["serve", "-c", "/home/ory/keto.yml"])
            .with_startup_timeout(timeout)
            .start()
            .await
            .expect("failed to start Keto container");

        let host = bridge_ip(&container).await;

        let client = KetoClient::new(sunbeam_g2v::middleware::auth::keto::KetoConfig {
            grpc_endpoint: format!("http://{host}:4466"),
            write_grpc_endpoint: format!("http://{host}:4467"),
        });

        for _ in 0..120 {
            if client
                .check_permission("_kanban_health", "probe", "health", "probe-subject")
                .await
                .is_ok()
            {
                return container;
            }
            sleep(Duration::from_millis(250)).await;
        }

        panic!("Keto did not become ready in time");
    }

    async fn start_minio(timeout: Duration) -> ContainerAsync<GenericImage> {
        let parts: Vec<&str> = MINIO_IMAGE.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (MINIO_IMAGE.to_string(), "latest".to_string()),
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

        let host = bridge_ip(&container).await;

        for _ in 0..120 {
            let url = format!("http://{host}:9000/minio/health/live");
            if let Ok(resp) = reqwest::get(&url).await {
                if resp.status().is_success() {
                    return container;
                }
            }
            sleep(Duration::from_millis(250)).await;
        }

        panic!("MinIO did not become ready in time");
    }

    async fn start_opensearch(timeout: Duration) -> ContainerAsync<GenericImage> {
        let parts: Vec<&str> = OPENSEARCH_IMAGE.rsplitn(2, ':').collect();
        let (name, tag) = match parts.as_slice() {
            [tag, name] => (name.to_string(), tag.to_string()),
            _ => (OPENSEARCH_IMAGE.to_string(), "latest".to_string()),
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

        let host = bridge_ip(&container).await;

        for _ in 0..240 {
            let url = format!("http://{host}:9200/_cluster/health");
            if let Ok(resp) = reqwest::get(&url).await {
                if resp.status().is_success() {
                    if let Ok(body) = resp.text().await {
                        if body.contains("\"status\":\"green\"")
                            || body.contains("\"status\":\"yellow\"")
                        {
                            return container;
                        }
                    }
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        panic!("OpenSearch did not become ready in time");
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

    /// Ensure an S3-compatible endpoint is available for attachments tests.
    ///
    /// If `S3_ENDPOINT` is already set the harness reuses it and returns it.
    /// Otherwise it starts a MinIO container and returns the endpoint.
    async fn ensure_minio() -> String {
        if let Some(endpoint) = std::env::var("S3_ENDPOINT").ok() {
            return endpoint;
        }

        let minio = start_minio(Duration::from_secs(600)).await;
        let host = bridge_ip(&minio).await;
        let endpoint = format!("http://{host}:9000");

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

    /// Build a fresh `TestInfra` from the shared URLs so each test owns its own
    /// connections and is isolated from other parallel tests.
    async fn build_test_infra(
        database_url: &str,
        nats_url: &str,
        valkey_url: &str,
        keto_read_url: &str,
        keto_write_url: &str,
    ) -> TestInfra {
        let pool = connect_pool(database_url).await;
        let nats = connect_nats(nats_url).await;
        let watermark = Arc::new(LogoutWatermark::new(valkey_url).expect("LogoutWatermark::new"));
        let keto = Arc::new(KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_read_url.to_string(),
                write_grpc_endpoint: keto_write_url.to_string(),
            },
        ));

        TestInfra {
            pool,
            nats,
            keto,
            watermark,
        }
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
            })
            .await
            .expect("NATS connect failed"),
        )
    }
}
