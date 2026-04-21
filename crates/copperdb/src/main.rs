use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use copperdb_server::{build_router, AppState};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "copperdb")]
#[command(about = "Run the copperdb HTTP server")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:3000")]
    listen_addr: SocketAddr,

    #[arg(long, default_value = "copperdb")]
    db_name: String,

    #[arg(long)]
    static_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let state = Arc::new(AppState {
        db_name: cli.db_name,
        static_dir: cli.static_dir,
        ..Default::default()
    });

    let app = build_router(state);
    let listener = TcpListener::bind(cli.listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", cli.listen_addr))?;

    info!(listen_addr = %cli.listen_addr, "copperdb HTTP server listening");

    axum::serve(listener, app)
        .await
        .context("server exited unexpectedly")
}