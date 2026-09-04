use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::Router;
use clap::Parser;
use copperdb_bolt::server::BoltServer;
use copperdb_buildinfo::display_version;
use copperdb_config::{ConfigOverrides, load_with_precedence};
use copperdb_lifecycle::{BoxError, Component, Supervisor};
use copperdb_localization::{ProcessPreferences, resolve_process_preferences};
use copperdb_multidb::DatabaseStatus;
use copperdb_otel::{
    Health, ObservabilityConfig, ServiceInfo, TelemetryProvider, install_global_telemetry,
    resolve_instance_id,
};
use copperdb_server::{
    AppState, AppStateBoltExecutor, build_local_nornic_replica_service, build_observability_router,
    build_router, execute_local_mcp_request,
};
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

    /// Disable authentication for this process after normal config resolution.
    #[arg(long)]
    no_auth: bool,

    #[arg(long)]
    base_path: Option<String>,

    #[arg(long, default_value = "copperdb")]
    db_name: String,

    #[arg(long)]
    static_dir: Option<String>,

    /// Run MCP over stdin/stdout without starting network listeners.
    #[arg(long)]
    mcp_stdio: bool,
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

struct PackageComponent {
    state: Arc<AppState>,
}

impl std::fmt::Debug for PackageComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PackageComponent").finish()
    }
}

#[async_trait]
impl Component for PackageComponent {
    fn name(&self) -> &str {
        "packages"
    }

    async fn start(&self, token: CancellationToken) -> Result<(), BoxError> {
        token.cancelled().await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), BoxError> {
        self.state.shutdown_packages().await.map_err(Into::into)
    }
}

#[derive(Debug)]
struct TelemetryComponent {
    listen_addr: String,
    app: Router,
    provider: Arc<TelemetryProvider>,
    flush_timeout: std::time::Duration,
}

#[async_trait]
impl Component for TelemetryComponent {
    fn name(&self) -> &str {
        "telemetry"
    }

    async fn start(&self, token: CancellationToken) -> Result<(), BoxError> {
        let listener = TcpListener::bind(&self.listen_addr)
            .await
            .with_context(|| format!("failed to bind telemetry listener {}", self.listen_addr))?;
        info!(listen_addr = %self.listen_addr, "copperdb telemetry listener listening");
        let server = axum::serve(listener, self.app.clone());
        tokio::select! {
            result = server => result.context("telemetry listener exited unexpectedly")?,
            _ = token.cancelled() => {}
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), BoxError> {
        self.provider
            .shutdown(self.flush_timeout)
            .map_err(Into::into)
    }
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

struct BoltComponent {
    server: BoltServer,
}

impl std::fmt::Debug for BoltComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltComponent")
            .field("listen_addr", &self.server.listen_addr)
            .finish()
    }
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
                    format!(
                        "failed to read gRPC TLS client-auth CA certificate {client_ca_cert_path}"
                    )
                })?;
                tls = tls.client_ca_root(Certificate::from_pem(client_ca_cert));
                if self
                    .state
                    .runtime_config
                    .server
                    .grpc_tls_client_auth_optional
                {
                    tls = tls.client_auth_optional(true);
                }
            }
            builder = builder
                .tls_config(tls)
                .context("failed to configure gRPC TLS")?;
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
    let cli = Cli::parse();
    let env = std::env::vars().collect();
    let mut observability_config = ObservabilityConfig::default();
    observability_config.apply_env_map(&env);
    let telemetry_provider = Arc::new(TelemetryProvider::initialize(
        &observability_config,
        &ServiceInfo {
            name: "copperdb".into(),
            version: copperdb_buildinfo::version().into(),
            component: None,
            node_id: None,
            cluster_mode: None,
            replication_role: None,
        },
        &env,
    ));
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("copperdb=info,fjall=warn,info"));
    let otel_layer = telemetry_provider
        .tracer()
        .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer));
    let log_writer = if cli.mcp_stdio {
        BoxMakeWriter::new(std::io::stderr)
    } else {
        BoxMakeWriter::new(std::io::stdout)
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_writer(log_writer)
                .compact(),
        )
        .with(otel_layer)
        .init();

    let startup = resolve_startup_config(&cli).await?;
    let process_preferences = resolve_startup_preferences(&startup)?;
    if let Some(error) = telemetry_provider.tracing_error() {
        warn!(
            error,
            "OTLP trace exporter unavailable; continuing without export"
        );
    }
    info!(version = %display_version(), "starting copperdb");
    if cli.no_auth {
        warn!("authentication disabled by explicit --no-auth override");
    } else {
        info!(
            auth_enabled = startup.runtime_config.auth.enabled,
            "resolved authentication configuration"
        );
    }
    let telemetry = telemetry_provider.telemetry();
    let _ = install_global_telemetry(Arc::clone(&telemetry));
    let auth = copperdb_server::AuthState::from_runtime_config(startup.runtime_config.as_ref())
        .context("failed to initialize configured authentication")?;
    let mut state = AppState::with_auth_and_storage(auth, &startup.runtime_config.storage.path);
    state.language_preferences = process_preferences.preferences.clone();
    state.localizer = Arc::new(copperdb_localization::Manager::new(
        &process_preferences.preferences,
    ));
    state.db_name = startup.db_name.clone();
    state
        .configure_runtime(Arc::clone(&startup.runtime_config))
        .await
        .context("failed to resolve configured packages")?;
    state.static_dir = startup.static_dir.clone();
    state.base_path = startup.base_path.clone();
    state.headless = startup.headless;
    state.bolt_enabled = startup.bolt_enabled;
    state.telemetry = Arc::clone(&telemetry);
    let state = Arc::new(state);
    if state.db_manager.get(&startup.db_name).is_none() {
        let storage_path = state.db_manager.default_storage_path(&startup.db_name);
        state
            .db_manager
            .create(startup.db_name.clone(), storage_path)
            .with_context(|| format!("failed to create home database {}", startup.db_name))?;
    }

    if cli.mcp_stdio {
        let dispatch_state = Arc::clone(&state);
        let transport_result = copperdb_mcp::serve_stdio_with(
            BufReader::new(tokio::io::stdin()),
            tokio::io::stdout(),
            move |request| execute_local_mcp_request(Arc::clone(&dispatch_state), request),
        )
        .await
        .context("MCP stdio transport failed");
        state
            .shutdown_packages()
            .await
            .context("failed to shut down packages")?;
        telemetry_provider
            .shutdown(observability_config.tracing.timeout)
            .map_err(anyhow::Error::msg)
            .context("failed to shut down telemetry")?;
        return transport_result;
    }

    let app = build_router(Arc::clone(&state));
    let mut supervisor = Supervisor::new();
    supervisor.register(PackageComponent {
        state: Arc::clone(&state),
    });

    let readiness = Arc::new(Health::new());
    let storage_state = Arc::clone(&state);
    readiness.register("storage", false, move || {
        storage_state
            .db_manager
            .get(&storage_state.db_name)
            .is_some_and(|database| database.status == DatabaseStatus::Online)
            .then_some(())
            .ok_or_else(|| "default database is unavailable".into())
    });
    supervisor.register(TelemetryComponent {
        listen_addr: observability_config.metrics.listen,
        app: build_observability_router(
            readiness,
            Arc::clone(&telemetry),
            observability_config.metrics.enabled,
            resolve_instance_id(None, &env).0,
        ),
        provider: Arc::clone(&telemetry_provider),
        flush_timeout: observability_config.tracing.timeout,
    });

    if startup.http_enabled {
        supervisor.register(HttpComponent {
            listen_addr: startup.http_address.clone(),
            app,
        });
    }

    if startup.bolt_enabled {
        let executor = Arc::new(AppStateBoltExecutor::new(Arc::clone(&state)));
        let auth_provider = Arc::clone(&executor);
        supervisor.register(BoltComponent {
            server: BoltServer::new(startup.bolt_address.clone(), telemetry, executor)
                .with_auth_enabled(state.auth.security_enabled)
                .with_auth_provider(auth_provider)
                .with_runtime_counters(Arc::clone(&state.bolt_counters))
                .with_language_preferences(state.language_preferences.clone()),
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
    let mut config = load_with_precedence(cli.config.as_deref(), &cli_config_overrides(cli))?;
    if cli.no_auth {
        config.auth.enabled = false;
    }
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

fn resolve_startup_preferences(startup: &StartupConfig) -> Result<ProcessPreferences> {
    resolve_process_preferences(&startup.runtime_config.localization.language, || {
        sys_locale::get_locale()
            .map(|locale| vec![locale])
            .ok_or_else(|| "operating system language was not detected".to_string())
    })
    .context("failed to resolve process language")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn telemetry_component_reports_listener_bind_failure() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = occupied.local_addr().unwrap();
        let provider = Arc::new(TelemetryProvider::test());
        let component = TelemetryComponent {
            listen_addr: address.to_string(),
            app: build_observability_router(
                Arc::new(Health::new()),
                provider.telemetry(),
                true,
                "test-instance".into(),
            ),
            provider,
            flush_timeout: std::time::Duration::from_millis(50),
        };

        let error = component.start(CancellationToken::new()).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to bind telemetry listener")
        );
    }

    #[tokio::test]
    async fn telemetry_component_shutdown_is_bounded_and_idempotent() {
        let component = TelemetryComponent {
            listen_addr: "127.0.0.1:0".into(),
            app: Router::new(),
            provider: Arc::new(TelemetryProvider::test()),
            flush_timeout: std::time::Duration::from_millis(50),
        };

        tokio::time::timeout(std::time::Duration::from_millis(100), component.shutdown())
            .await
            .expect("telemetry shutdown exceeded its bound")
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(100), component.shutdown())
            .await
            .expect("repeated telemetry shutdown exceeded its bound")
            .unwrap();
    }

    #[tokio::test]
    async fn startup_config_enables_auth_by_default() {
        let cli = Cli::parse_from(["copperdb"]);

        let startup = resolve_startup_config(&cli).await.unwrap();

        assert!(startup.runtime_config.auth.enabled);
    }

    #[tokio::test]
    async fn startup_config_defaults_localization_to_os_detection() {
        let cli = Cli::parse_from(["copperdb"]);
        let startup = resolve_startup_config(&cli).await.unwrap();

        assert_eq!(startup.runtime_config.localization.language, "auto");
    }

    #[test]
    fn mcp_stdio_is_an_explicit_local_mode() {
        let cli = Cli::parse_from(["copperdb", "--mcp-stdio"]);

        assert!(cli.mcp_stdio);
    }

    #[tokio::test]
    async fn no_auth_overrides_resolved_auth_configuration() {
        let config_path = std::env::temp_dir().join(format!(
            "copperdb-no-auth-test-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&config_path, "[auth]\nenabled = true\n").unwrap();
        let cli = Cli::parse_from([
            "copperdb",
            "--config",
            config_path.to_str().unwrap(),
            "--no-auth",
        ]);

        let startup = resolve_startup_config(&cli).await.unwrap();

        assert!(!startup.runtime_config.auth.enabled);
        std::fs::remove_file(config_path).unwrap();
    }
}
