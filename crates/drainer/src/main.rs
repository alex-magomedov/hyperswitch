use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use drainer::{
    errors::DrainerResult,
    logger,
    services,
    settings::{self, AppState, Settings},
    start_drainer, start_web_server,
};
use futures::future::try_join_all;
use router_env::tracing::{error, info, warn, Instrument};
use tokio::signal;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Maximum concurrent tenant store initializations
const MAX_CONCURRENT_STORES: usize = 10;

/// Grace period for shutdown operations
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<()> {
    let result = bootstrap_and_run().await;
    if let Err(ref e) = result {
        eprintln!("Application failed: {:#}", e);
        std::process::exit(1);
    }
    Ok(())
}

/// Bootstrap the application and run until shutdown signal
async fn bootstrap_and_run() -> Result<()> {
    // Parse command line configuration
    let cmd_line = settings::CmdLineConf::parse();
    
    // Load and validate configuration
    let conf = Settings::with_config_path(cmd_line.config_path)
        .context("Unable to construct application configuration")?;
    
    conf.validate()
        .context("Failed to validate drainer configuration")?;
    
    // Setup logging first
    let _guard = setup_logging(&conf)?;
    
    // Log startup with version info (safe config logging)
    log_startup_info(&conf)?;
    
    // Create application state
    let state = AppState::new(conf.clone())
        .await
        .context("Failed to create application state")?;
    
    // Initialize stores in parallel with bounded concurrency
    let stores = initialize_stores_parallel(&state, &conf)
        .instrument(tracing::info_span!("store_initialization"))
        .await
        .context("Failed to initialize tenant stores")?;
    
    // Create cancellation token for graceful shutdown
    let cancellation_token = CancellationToken::new();
    
    // Run the application
    run_application(state, stores, cancellation_token).await
}

/// Setup logging and return guard
fn setup_logging(conf: &Settings) -> Result<router_env::LoggerGuard> {
    let guard = router_env::setup(
        &conf.log,
        router_env::service_name!(),
        [router_env::service_name!()],
    )
    .context("Failed to setup logging")?;
    Ok(guard)
}

/// Log startup information with safe config redaction
fn log_startup_info(conf: &Settings) -> Result<()> {
    #[cfg(feature = "vergen")]
    {
        info!("Starting drainer (Version: {})", router_env::git_tag!());
    }
    #[cfg(not(feature = "vergen"))]
    {
        info!("Starting drainer");
    }
    
    // Log safe configuration details (redact sensitive info)
    info!(
        tenant_count = conf.multitenancy.get_tenants().len(),
        log_level = ?conf.log.level,
        "Drainer configuration loaded"
    );
    
    Ok(())
}

/// Initialize tenant stores in parallel with bounded concurrency
async fn initialize_stores_parallel(
    state: &AppState,
    conf: &Settings,
) -> Result<HashMap<String, Arc<services::Store>>> {
    let tenants: Vec<_> = conf.multitenancy.get_tenants().collect();
    let tenant_count = tenants.len();
    
    info!(tenant_count, "Initializing tenant stores");
    
    // Pre-allocate HashMap with capacity
    let mut stores = HashMap::with_capacity(tenant_count);
    
    if tenant_count == 0 {
        warn!("No tenants configured");
        return Ok(stores);
    }
    
    // Create semaphore for bounded concurrency
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_STORES.min(tenant_count)));
    
    // Create futures for parallel initialization
    let store_futures: Vec<Pin<Box<dyn Future<Output = Result<(String, Arc<services::Store>)>> + Send>>> = tenants
        .into_iter()
        .map(|(tenant_name, tenant)| {
            let semaphore = Arc::clone(&semaphore);
            let state_conf = state.conf.clone();
            let tenant_name = tenant_name.clone();
            
            Box::pin(async move {
                let _permit = semaphore.acquire().await
                    .map_err(|_| anyhow!("Failed to acquire semaphore permit"))?;
                
                let span = tracing::info_span!("store_init", tenant = %tenant_name);
                async move {
                    info!("Initializing store for tenant: {}", tenant_name);
                    
                    let store = Arc::new(
                        services::Store::new(&state_conf, false, tenant)
                            .await
                            .with_context(|| format!("Failed to create store for tenant: {}", tenant_name))?
                    );
                    
                    info!("Successfully initialized store for tenant: {}", tenant_name);
                    Ok((tenant_name, store))
                }
                .instrument(span)
                .await
            }) as Pin<Box<dyn Future<Output = Result<(String, Arc<services::Store>)>> + Send>>
        })
        .collect();
    
    // Execute all store initializations concurrently
    let results = try_join_all(store_futures)
        .await
        .context("Failed to initialize one or more tenant stores")?;
    
    // Collect results into HashMap
    for (tenant_name, store) in results {
        stores.insert(tenant_name, store);
    }
    
    info!(initialized_stores = stores.len(), "All tenant stores initialized successfully");
    Ok(stores)
}

/// Run the main application with graceful shutdown handling
async fn run_application(
    state: AppState,
    stores: HashMap<String, Arc<services::Store>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let app_span = tracing::info_span!("drainer_application");
    
    async move {
        info!("Starting drainer application");
        
        // Setup signal handling for graceful shutdown
        let signal_token = cancellation_token.clone();
        let signal_handler = tokio::spawn(async move {
            wait_for_shutdown_signal().await;
            info!("Shutdown signal received, initiating graceful shutdown");
            signal_token.cancel();
        });
        
        // Start web server for health checks
        let health_server_token = cancellation_token.clone();
        let health_server = tokio::spawn(async move {
            if let Err(e) = start_web_server(health_server_token.clone()).await {
                error!(error = ?e, "Health server failed");
                // Health server failure is critical - trigger shutdown
                health_server_token.cancel();
                return Err(anyhow!("Health server failed: {:#}", e));
            }
            Ok(())
        });
        
        // Start main drainer service
        let drainer_token = cancellation_token.clone();
        let drainer_service = tokio::spawn(async move {
            start_drainer(state, stores, drainer_token)
                .await
                .context("Drainer service failed")
        });
        
        // Wait for any task to complete (shutdown signal, health server failure, or drainer completion)
        let shutdown_result = tokio::select! {
            result = health_server => {
                match result {
                    Ok(Ok(())) => {
                        info!("Health server completed successfully");
                        Ok(())
                    },
                    Ok(Err(e)) => {
                        error!(error = ?e, "Health server failed critically");
                        Err(e)
                    },
                    Err(e) => {
                        error!(error = ?e, "Health server task panicked");
                        Err(anyhow!("Health server task panicked: {:#}", e))
                    }
                }
            },
            result = drainer_service => {
                match result {
                    Ok(Ok(())) => {
                        info!("Drainer service completed successfully");
                        Ok(())
                    },
                    Ok(Err(e)) => {
                        error!(error = ?e, "Drainer service failed");
                        Err(e.into())
                    },
                    Err(e) => {
                        error!(error = ?e, "Drainer service task panicked");
                        Err(anyhow!("Drainer service task panicked: {:#}", e))
                    }
                }
            },
            _ = cancellation_token.cancelled() => {
                info!("Cancellation requested, shutting down gracefully");
                Ok(())
            }
        };
        
        // Ensure all tasks are cancelled
        cancellation_token.cancel();
        
        // Wait for graceful shutdown with timeout
        info!("Waiting for graceful shutdown (timeout: {:?})", SHUTDOWN_GRACE_PERIOD);
        
        let shutdown_timeout = tokio::time::timeout(
            SHUTDOWN_GRACE_PERIOD,
            async {
                // Wait for signal handler to complete
                if let Err(e) = signal_handler.await {
                    warn!(error = ?e, "Signal handler task failed during shutdown");
                }
                
                // Note: We don't wait for health_server and drainer_service here
                // as they should respond to the cancellation token
            }
        );
        
        match shutdown_timeout.await {
            Ok(()) => {
                info!("Graceful shutdown completed successfully");
            },
            Err(_) => {
                warn!("Graceful shutdown timed out after {:?}", SHUTDOWN_GRACE_PERIOD);
            }
        }
        
        shutdown_result
    }
    .instrument(app_span)
    .await
}

/// Wait for shutdown signals (SIGINT, SIGTERM)
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };
    
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };
    
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    
    tokio::select! {
        _ = ctrl_c => {
            info!("Received SIGINT (Ctrl+C)");
        },
        _ = terminate => {
            info!("Received SIGTERM");
        },
    }
}
