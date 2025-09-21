//! Integration tests for the drainer startup and shutdown logic

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use drainer::{
    errors::DrainerResult,
    settings::{AppState, Settings},
    test_support::{MockServer, MockStore},
};
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Test helper to create a basic configuration for testing
fn create_test_config() -> Settings {
    // This would normally load from a config file, but for tests we create a minimal config
    Settings::default_for_testing()
}

/// Test helper to create application state with mocked dependencies
async fn create_test_app_state() -> Result<AppState> {
    let conf = create_test_config();
    AppState::new(conf).await
}

#[tokio::test]
async fn test_startup_success_uses_mock_store() {
    // Arrange
    let state = create_test_app_state().await.expect("Failed to create test app state");
    let mut stores = HashMap::new();
    let mock_store = Arc::new(MockStore::new_success());
    stores.insert("test_tenant".to_string(), mock_store.clone());
    
    let cancellation_token = CancellationToken::new();
    let cancel_handle = cancellation_token.clone();
    
    // Setup mock services
    let (health_tx, health_rx) = oneshot::channel();
    let (drainer_tx, drainer_rx) = oneshot::channel();
    
    // Mock successful health server
    MockServer::set_health_server_mock(move |_token| {
        Box::pin(async move {
            let _ = health_rx.await;
            Ok(())
        })
    });
    
    // Mock successful drainer
    MockServer::set_drainer_mock(move |_state, _stores, _token| {
        Box::pin(async move {
            let _ = drainer_rx.await;
            Ok(())
        })
    });
    
    // Act
    let app_handle = tokio::spawn(async move {
        drainer::run_application(state, stores, cancellation_token).await
    });
    
    // Allow some time for startup
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Signal shutdown
    cancel_handle.cancel();
    
    // Complete the mock services
    let _ = health_tx.send(());
    let _ = drainer_tx.send(());
    
    // Assert
    let result = app_handle.await.expect("App task should not panic");
    assert!(result.is_ok(), "Application should start and shutdown successfully");
    
    // Verify mock store was used
    assert!(mock_store.was_created(), "Mock store should have been created");
}

#[tokio::test]
async fn test_health_server_failure_triggers_shutdown() {
    // Arrange
    let state = create_test_app_state().await.expect("Failed to create test app state");
    let stores = HashMap::new();
    let cancellation_token = CancellationToken::new();
    
    let (drainer_tx, drainer_rx) = oneshot::channel();
    
    // Mock failing health server
    MockServer::set_health_server_mock(|_token| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Err(anyhow!("Health server failed"))
        })
    });
    
    // Mock drainer that waits for signal
    MockServer::set_drainer_mock(move |_state, _stores, _token| {
        Box::pin(async move {
            let _ = drainer_rx.await;
            Ok(())
        })
    });
    
    // Act
    let app_handle = tokio::spawn(async move {
        drainer::run_application(state, stores, cancellation_token).await
    });
    
    // Wait for health server failure and shutdown
    let result = app_handle.await.expect("App task should not panic");
    
    // Complete drainer mock
    let _ = drainer_tx.send(());
    
    // Assert
    assert!(result.is_err(), "Application should fail when health server fails");
    assert!(
        result.unwrap_err().to_string().contains("Health server failed"),
        "Error should indicate health server failure"
    );
}

#[tokio::test]
async fn test_ctrl_c_graceful_shutdown() {
    // Arrange
    let state = create_test_app_state().await.expect("Failed to create test app state");
    let stores = HashMap::new();
    let cancellation_token = CancellationToken::new();
    let shutdown_signal = cancellation_token.clone();
    
    let (health_tx, health_rx) = oneshot::channel();
    let (drainer_tx, drainer_rx) = oneshot::channel();
    
    // Mock services that wait for cancellation
    MockServer::set_health_server_mock(move |token| {
        Box::pin(async move {
            tokio::select! {
                _ = token.cancelled() => Ok(()),
                _ = health_rx => Ok(()),
            }
        })
    });
    
    MockServer::set_drainer_mock(move |_state, _stores, token| {
        Box::pin(async move {
            tokio::select! {
                _ = token.cancelled() => Ok(()),
                _ = drainer_rx => Ok(()),
            }
        })
    });
    
    // Act
    let app_handle = tokio::spawn(async move {
        drainer::run_application(state, stores, cancellation_token).await
    });
    
    // Allow some time for startup
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Simulate Ctrl+C (graceful shutdown signal)
    shutdown_signal.cancel();
    
    // Wait for graceful shutdown
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        app_handle
    ).await.expect("App should shutdown within timeout");
    
    // Clean up mock channels
    let _ = health_tx.send(());
    let _ = drainer_tx.send(());
    
    // Assert
    let app_result = result.expect("App task should not panic");
    assert!(app_result.is_ok(), "Application should shutdown gracefully on cancellation signal");
}

#[tokio::test]
async fn test_concurrent_store_initialization() {
    // Arrange
    let conf = create_test_config();
    let state = AppState::new(conf.clone()).await.expect("Failed to create app state");
    
    // Mock multiple tenants
    let tenant_count = 5;
    MockStore::set_creation_delay(Duration::from_millis(100));
    
    // Act
    let start_time = std::time::Instant::now();
    let stores = drainer::initialize_stores_parallel(&state, &conf).await;
    let elapsed = start_time.elapsed();
    
    // Assert
    assert!(stores.is_ok(), "Store initialization should succeed");
    let stores = stores.unwrap();
    assert_eq!(stores.len(), tenant_count, "Should initialize all tenant stores");
    
    // With parallel initialization, it should take much less time than sequential
    // (5 * 100ms = 500ms sequential vs ~100-200ms parallel)
    assert!(
        elapsed < Duration::from_millis(300),
        "Parallel initialization should be faster than sequential. Took: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_store_initialization_failure_handling() {
    // Arrange
    let conf = create_test_config();
    let state = AppState::new(conf.clone()).await.expect("Failed to create app state");
    
    // Mock store creation failure
    MockStore::set_creation_behavior(drainer::test_support::CreationBehavior::Fail);
    
    // Act
    let result = drainer::initialize_stores_parallel(&state, &conf).await;
    
    // Assert
    assert!(result.is_err(), "Store initialization should fail when store creation fails");
    assert!(
        result.unwrap_err().to_string().contains("Failed to initialize"),
        "Error should indicate initialization failure"
    );
    
    // Reset mock behavior for other tests
    MockStore::set_creation_behavior(drainer::test_support::CreationBehavior::Success);
}
