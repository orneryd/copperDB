use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use copperdb_bolt::server::BoltServer;
use copperdb_config::{load_toml, load_yaml, Config};
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
    let state = Arc::new(AppState {
        db_name: startup.db_name.clone(),
        static_dir: startup.static_dir.clone(),
        base_path: startup.base_path.clone(),
        headless: startup.headless,
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
        let bolt_server = BoltServer::new(startup.bolt_address.clone());
        let bolt_addr = startup.bolt_address.clone();
        Some(tokio::spawn(async move {
            info!(listen_addr = %bolt_addr, "copperdb Bolt server listening");
            bolt_server.serve().await.context("bolt server exited unexpectedly")
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
    let mut config = load_config_with_fallback(cli.config.as_deref())?;

    apply_env_overrides(&mut config);
    apply_cli_overrides(&mut config, cli);

    let base_address = config.server.address.clone();
    let http_address = config
        .server
        .http_address
        .clone()
        .unwrap_or_else(|| format!("{}:{}", base_address, config.server.http_port));
    let bolt_address = config
        .server
        .bolt_address
        .clone()
        .unwrap_or_else(|| format!("{}:{}", base_address, config.server.bolt_port));

    Ok(StartupConfig {
        db_name: cli.db_name.clone(),
        http_address,
        bolt_address,
        http_enabled: config.server.http_enabled,
        bolt_enabled: config.server.bolt_enabled,
        headless: config.server.headless,
        base_path: config.server.base_path.clone(),
        static_dir: cli
            .static_dir
            .clone()
            .or_else(|| config.server.static_dir.clone())
            .or_else(find_default_ui_dist),
    })
}

fn load_config_with_fallback(explicit_path: Option<&Path>) -> Result<Config> {
    let config_path = explicit_path
        .map(PathBuf::from)
        .or_else(|| env::var_os("COPPERDB_CONFIG").map(PathBuf::from))
        .or_else(find_default_config_path);

    if let Some(path) = config_path {
        load_config_file(&path)
    } else {
        Ok(Config::default())
    }
}

fn find_default_config_path() -> Option<PathBuf> {
    ["./copperdb.yaml", "./copperdb.yml", "./copperdb.toml"]
        .iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
}

fn find_default_ui_dist() -> Option<String> {
    let path = PathBuf::from("./ui/dist");
    path.exists().then(|| path.display().to_string())
}

fn load_config_file(path: &Path) -> Result<Config> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("yaml") | Some("yml") => {
            load_yaml(path).with_context(|| format!("failed to load config from {}", path.display()))
        }
        Some("toml") => {
            load_toml(path).with_context(|| format!("failed to load config from {}", path.display()))
        }
        _ => anyhow::bail!("unsupported config format for {}", path.display()),
    }
}

fn apply_env_overrides(config: &mut Config) {
    set_if_present(env::var("COPPERDB_ADDRESS").ok(), |value| config.server.address = value);
    set_if_present(env::var("COPPERDB_HTTP_ADDRESS").ok(), |value| config.server.http_address = Some(value));
    set_if_present(env::var("COPPERDB_BOLT_ADDRESS").ok(), |value| config.server.bolt_address = Some(value));
    set_if_present(
        parse_env_u16("COPPERDB_HTTP_PORT")
            .or_else(|| parse_env_u16("NEO4J_dbms_connector_http_listen__address_port")),
        |value| config.server.http_port = value,
    );
    set_if_present(
        parse_env_u16("COPPERDB_BOLT_PORT")
            .or_else(|| parse_env_u16("NEO4J_dbms_connector_bolt_listen__address_port")),
        |value| config.server.bolt_port = value,
    );
    set_if_present(parse_env_bool("COPPERDB_HTTP_ENABLED"), |value| config.server.http_enabled = value);
    set_if_present(parse_env_bool("COPPERDB_BOLT_ENABLED"), |value| config.server.bolt_enabled = value);
    set_if_present(parse_env_bool("COPPERDB_HEADLESS"), |value| config.server.headless = value);
    set_if_present(env::var("COPPERDB_BASE_PATH").ok(), |value| config.server.base_path = value);
    set_if_present(env::var("COPPERDB_STATIC_DIR").ok(), |value| config.server.static_dir = Some(value));
}

fn apply_cli_overrides(config: &mut Config, cli: &Cli) {
    if let Some(address) = &cli.address {
        config.server.address = address.clone();
    }
    if let Some(address) = &cli.http_address {
        config.server.http_address = Some(address.clone());
    }
    if let Some(address) = &cli.bolt_address {
        config.server.bolt_address = Some(address.clone());
    }
    if let Some(port) = cli.http_port {
        config.server.http_port = port;
    }
    if let Some(port) = cli.bolt_port {
        config.server.bolt_port = port;
    }
    if cli.headless {
        config.server.headless = true;
    }
    if let Some(base_path) = &cli.base_path {
        config.server.base_path = base_path.clone();
    }
}

fn parse_env_u16(name: &str) -> Option<u16> {
    env::var(name).ok()?.parse().ok()
}

fn parse_env_bool(name: &str) -> Option<bool> {
    let value = env::var(name).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn set_if_present<T>(value: Option<T>, apply: impl FnOnce(T)) {
    if let Some(value) = value {
        apply(value);
    }
}