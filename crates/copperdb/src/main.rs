use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::Router;
use clap::Parser;
use copperdb_bolt::server::BoltServer;
use copperdb_buildinfo::display_version;
use copperdb_config::{load_with_precedence, ConfigOverrides};
use copperdb_lifecycle::{BoxError, Component, Supervisor};
use copperdb_otel::Telemetry;
use copperdb_server::{build_local_nornic_replica_service, build_router, AppState};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

fn ensure_tls_crypto_provider() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

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
    grpc_address: Option<String>,

    #[arg(long)]
    grpc_tls_enabled: bool,

    #[arg(long)]
    grpc_tls_cert: Option<String>,

    #[arg(long)]
    grpc_tls_key: Option<String>,

    #[arg(long)]
    grpc_tls_ca_cert: Option<String>,

    #[arg(long)]
    grpc_tls_domain_name: Option<String>,

    #[arg(long)]
    grpc_tls_client_cert: Option<String>,

    #[arg(long)]
    grpc_tls_client_key: Option<String>,

    #[arg(long)]
    grpc_tls_client_auth_ca_cert: Option<String>,

    #[arg(long)]
    grpc_tls_client_auth_optional: bool,

    #[arg(long)]
    http_address: Option<String>,

    #[arg(long)]
    bolt_port: Option<u16>,

    #[arg(long)]
    grpc_port: Option<u16>,

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
    runtime_config: Arc<copperdb_config::Config>,
    http_address: String,
    bolt_address: String,
    grpc_address: String,
    http_enabled: bool,
    bolt_enabled: bool,
    grpc_enabled: bool,
    headless: bool,
    base_path: String,
    static_dir: Option<String>,
}

#[derive(Debug)]
struct HttpComponent {
    listen_addr: String,
    app: Router,
}

#[async_trait]
impl Component for HttpComponent {
    fn name(&self) -> &str {
        "http"
    }

    async fn start(&self, token: CancellationToken) -> Result<(), BoxError> {
        let http_addr: SocketAddr = self
            .listen_addr
            .parse()
            .with_context(|| format!("invalid HTTP listen address {}", self.listen_addr))?;
        let listener = TcpListener::bind(http_addr)
            .await
            .with_context(|| format!("failed to bind {}", http_addr))?;
        info!(listen_addr = %http_addr, "copperdb HTTP server listening");
        let server = axum::serve(listener, self.app.clone());
        tokio::select! {
            result = server => result.context("http server exited unexpectedly")?,
            _ = token.cancelled() => {}
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), BoxError> {
        Ok(())
    }
}

#[derive(Debug)]
struct BoltComponent {
    server: BoltServer,
}

#[derive(Clone)]
struct GrpcComponent {
    listen_addr: String,
    state: Arc<AppState>,
}

impl std::fmt::Debug for GrpcComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrpcComponent")
            .field("listen_addr", &self.listen_addr)
            .finish()
    }
}

#[async_trait]
impl Component for BoltComponent {
    fn name(&self) -> &str {
        "bolt"
    }

    async fn start(&self, token: CancellationToken) -> Result<(), BoxError> {
        info!(listen_addr = %self.server.listen_addr, "copperdb Bolt server listening");
        tokio::select! {
            result = self.server.serve() => result.context("bolt server exited unexpectedly")?,
            _ = token.cancelled() => {}
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), BoxError> {
        Ok(())
    }
}

#[async_trait]
impl Component for GrpcComponent {
    fn name(&self) -> &str {
        "grpc"
    }

    async fn start(&self, token: CancellationToken) -> Result<(), BoxError> {
        let grpc_addr: SocketAddr = self
            .listen_addr
            .parse()
            .with_context(|| format!("invalid gRPC listen address {}", self.listen_addr))?;
        info!(listen_addr = %grpc_addr, "copperdb gRPC server listening");
        let service = build_local_nornic_replica_service(Arc::clone(&self.state)).into_server();
        let mut builder = Server::builder();
        if self.state.runtime_config.server.grpc_tls_enabled {
            ensure_tls_crypto_provider();
            let cert_path = self
                .state
                .runtime_config
                .server
                .grpc_tls_cert
                .as_deref()
                .context("gRPC TLS is enabled but server.grpc_tls_cert is not set")?;
            let key_path = self
                .state
                .runtime_config
                .server
                .grpc_tls_key
                .as_deref()
                .context("gRPC TLS is enabled but server.grpc_tls_key is not set")?;
            let cert = std::fs::read(cert_path)
                .with_context(|| format!("failed to read gRPC TLS certificate {cert_path}"))?;
            let key = std::fs::read(key_path)
                .with_context(|| format!("failed to read gRPC TLS private key {key_path}"))?;
            let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
            if let Some(client_ca_cert_path) = self
                .state
                .runtime_config
                .server
                .grpc_tls_client_auth_ca_cert
                .as_deref()
            {
                let client_ca_cert = std::fs::read(client_ca_cert_path).with_context(|| {
                    format!("failed to read gRPC TLS client-auth CA certificate {client_ca_cert_path}")
                })?;
                tls = tls.client_ca_root(Certificate::from_pem(client_ca_cert));
                if self.state.runtime_config.server.grpc_tls_client_auth_optional {
                    tls = tls.client_auth_optional(true);
                }
            }
            builder = builder.tls_config(tls).context("failed to configure gRPC TLS")?;
        }
        builder
            .add_service(service)
            .serve_with_shutdown(grpc_addr, async move {
                token.cancelled().await;
            })
            .await
            .context("gRPC server exited unexpectedly")?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), BoxError> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let startup = resolve_startup_config(&cli).await?;
    info!(version = %display_version(), "starting copperdb");
    let telemetry = Arc::new(Telemetry::new());
    telemetry.seed_zero_catalog_metrics();
    let state = Arc::new(AppState {
        db_name: startup.db_name.clone(),
        runtime_config: Arc::clone(&startup.runtime_config),
        static_dir: startup.static_dir.clone(),
        base_path: startup.base_path.clone(),
        headless: startup.headless,
        telemetry: Arc::clone(&telemetry),
        ..Default::default()
    });

    let app = build_router(Arc::clone(&state));
    let mut supervisor = Supervisor::new();

    if startup.http_enabled {
        supervisor.register(HttpComponent {
            listen_addr: startup.http_address.clone(),
            app,
        });
    }

    if startup.bolt_enabled {
        supervisor.register(BoltComponent {
            server: BoltServer::new(startup.bolt_address.clone(), telemetry),
        });
    }

    if startup.grpc_enabled {
        supervisor.register(GrpcComponent {
            listen_addr: startup.grpc_address.clone(),
            state,
        });
    }

    if supervisor.components().is_empty() {
        anyhow::bail!("HTTP, Bolt, and gRPC listeners are all disabled");
    }

    supervisor
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|errors| anyhow::anyhow!("lifecycle supervision failed: {errors:?}"))?;

    Ok(())
}

async fn resolve_startup_config(cli: &Cli) -> Result<StartupConfig> {
    let config = load_with_precedence(cli.config.as_deref(), &cli_config_overrides(cli))?;
    config.validate()?;
    let config = Arc::new(config);
    let listeners = config.listener_config();

    Ok(StartupConfig {
        db_name: cli.db_name.clone(),
        runtime_config: Arc::clone(&config),
        http_address: listeners.http_address,
        bolt_address: listeners.bolt_address,
        grpc_address: listeners.grpc_address,
        http_enabled: listeners.http_enabled,
        bolt_enabled: listeners.bolt_enabled,
        grpc_enabled: listeners.grpc_enabled,
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
        grpc_address: cli.grpc_address.clone(),
        grpc_tls_enabled: cli.grpc_tls_enabled.then_some(true),
        grpc_tls_cert: cli.grpc_tls_cert.clone(),
        grpc_tls_key: cli.grpc_tls_key.clone(),
        grpc_tls_ca_cert: cli.grpc_tls_ca_cert.clone(),
        grpc_tls_domain_name: cli.grpc_tls_domain_name.clone(),
        grpc_tls_client_cert: cli.grpc_tls_client_cert.clone(),
        grpc_tls_client_key: cli.grpc_tls_client_key.clone(),
        grpc_tls_client_auth_ca_cert: cli.grpc_tls_client_auth_ca_cert.clone(),
        grpc_tls_client_auth_optional: cli.grpc_tls_client_auth_optional.then_some(true),
        http_port: cli.http_port,
        bolt_port: cli.bolt_port,
        grpc_port: cli.grpc_port,
        headless: cli.headless.then_some(true),
        base_path: cli.base_path.clone(),
        static_dir: cli.static_dir.clone(),
        ..Default::default()
    }
}
