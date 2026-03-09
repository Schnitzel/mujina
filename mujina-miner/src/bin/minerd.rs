//! Main entry point for the mujina-miner daemon.

use mujina_miner::{config::Config, daemon::Daemon, tracing};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing::init_journald_or_stdout();

    let config = Config::load_optional()?;
    let daemon = Daemon::new(config);
    daemon.run().await
}
