// SPDX-License-Identifier: AGPL-3.0-or-later
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kanban::server::run().await
}
