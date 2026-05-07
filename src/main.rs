#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kanban::server::run().await
}
