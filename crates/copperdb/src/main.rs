use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use copperdb_bolt::server::BoltServer;
use copperdb_config::{load_with_precedence, ConfigOverrides};
use copperdb_otel::Telemetry;
use copperdb_server::{build_router, AppState};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "copperdb")]
#[command(about = "Run the copperdb HTTP and Bolt servers")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    address: Option<String>,

    #[arg(long)]
    bolt_address: Option<String>,

    #[arg(long)]
    http_address: Option<String>,

    #[arg(long)]
    bolt_port: Option<u16>,

    #[arg(long)]
    http_port: Option<u16>,

    #[arg(long)]
    headless: bool,

    #[arg(long)]
    base_path: Option<String>,

    #[arg(long, default_value = "copperdb")]
    db_name: String,

    #[arg(long)]
    static_dir: Option<String>,
}

#[derive(Debug, Clone)]
struct StartupConfig {
    db_name: String,
    http_address: String,
    bolt_address: String,
    http_enabled: bool,
    bolt_enabled: bool,
    headless: bool,
    base_path: String,
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
    let startup = resolve_startup_config(&cli)?;
    let telemetry = Arc::new(Telemetry::new());
    telemetry.seed_zero_catalog_metrics();
    let state = Arc::new(AppState {
        db_name: startup.db_name.clone(),
        static_dir: startup.static_dir.clone(),
        base_path: startup.base_path.clone(),
        headless: startup.headless,
        telemetry: Arc::clone(&telemetry),
        ..Default::default()
    });

    let app = build_router(state);

    let http_task = if startup.http_enabled {
        let http_addr: SocketAddr = startup
            .http_address
            .parse()
            .with_context(|| format!("invalid HTTP listen address {}", startup.http_address))?;
        Some(tokio::spawn(async move {
            let listener = TcpListener::bind(http_addr)
                .await
                .with_context(|| format!("failed to bind {}", http_addr))?;
            info!(listen_addr = %http_addr, "copperdb HTTP server listening");
            axum::serve(listener, app)
                .await
                .context("http server exited unexpectedly")
        }))
    } else {
        None
    };

    let bolt_task = if startup.bolt_enabled {
        let bolt_server = BoltServer::new(startup.bolt_address.clone(), telemetry);
        let bolt_addr = startup.bolt_address.clone();
        Some(tokio::spawn(async move {
            info!(listen_addr = %bolt_addr, "copperdb Bolt server listening");
            bolt_server
                .serve()
                .await
                .context("bolt server exited unexpectedly")
        }))
    } else {
        None
    };

    match (http_task, bolt_task) {
        (Some(http_task), Some(bolt_task)) => {
            tokio::try_join!(flatten_task(http_task), flatten_task(bolt_task))?;
        }
        (Some(http_task), None) => {
            flatten_task(http_task).await?;
        }
        (None, Some(bolt_task)) => {
            flatten_task(bolt_task).await?;
        }
        (None, None) => anyhow::bail!("both HTTP and Bolt listeners are disabled"),
    }

    Ok(())
}

async fn flatten_task(handle: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    handle.await.context("startup task panicked")?
}

fn resolve_startup_config(cli: &Cli) -> Result<StartupConfig> {
    let config = load_with_precedence(cli.config.as_deref(), &cli_config_overrides(cli))?;
    let listeners = config.listener_config();

    Ok(StartupConfig {
        db_name: cli.db_name.clone(),
        http_address: listeners.http_address,
        bolt_address: listeners.bolt_address,
        http_enabled: listeners.http_enabled,
        bolt_enabled: listeners.bolt_enabled,
        headless: listeners.headless,
        base_path: listeners.base_path,
        static_dir: cli
            .static_dir
            .clone()
            .or(listeners.static_dir)
            .or_else(find_default_ui_dist),
    })
}

fn find_default_ui_dist() -> Option<String> {
    let path = PathBuf::from("./ui/dist");
    path.exists().then(|| path.display().to_string())
}

fn cli_config_overrides(cli: &Cli) -> ConfigOverrides {
    ConfigOverrides {
        address: cli.address.clone(),
        http_address: cli.http_address.clone(),
        bolt_address: cli.bolt_address.clone(),
        http_port: cli.http_port,
        bolt_port: cli.bolt_port,
        headless: cli.headless.then_some(true),
        base_path: cli.base_path.clone(),
        static_dir: cli.static_dir.clone(),
        ..Default::default()
    }
}
