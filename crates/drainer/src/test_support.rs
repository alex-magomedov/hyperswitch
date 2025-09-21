//! Test support utilities and mocks for the drainer crate
//!
//! This module provides mock implementations and test utilities that allow
//! testing of the drainer application components in isolation.
//!
//! The module is conditionally compiled only when the `test-support` feature
//! is enabled, which should be used during testing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::{
    errors::DrainerResult,
    services::Store,
    settings::{AppState, Settings},
};

/// Mock server functionality for testing
pub struct MockServer;

/// Mock store implementation for testing
pub struct MockStore {
    created: Arc<Mutex<bool>>,
    creation_delay: Arc<Mutex<Duration>>,
    creation_behavior: Arc<Mutex<CreationBehavior>>,
}

/// Behavior configuration for mock store creation
#[derive(Clone, Debug)]
pub enum CreationBehavior {
    Success,
    Fail,
}

/// Type alias for health server mock function
type HealthServerMockFn = fn(CancellationToken) -> BoxFuture<'static, Result<()>>;

/// Type alias for drainer mock function
type DrainerMockFn = fn(
    AppState,
    HashMap<String, Arc<Store>>,
    CancellationToken,
) -> BoxFuture<'static, Result<()>>;

/// Global storage for mock functions
static HEALTH_SERVER_MOCK: Mutex<Option<HealthServerMockFn>> = Mutex::new(None);
static DRAINER_MOCK: Mutex<Option<DrainerMockFn>> = Mutex::new(None);

/// Global configuration for mock store behavior
static MOCK_STORE_CREATION_DELAY: Mutex<Duration> = Mutex::new(Duration::from_millis(0));
static MOCK_STORE_CREATION_BEHAVIOR: Mutex<CreationBehavior> = Mutex::new(CreationBehavior::Success);

impl MockServer {
    /// Set the mock function for the health server
    pub fn set_health_server_mock(mock_fn: HealthServerMockFn) {
        let mut mock = HEALTH_SERVER_MOCK.lock().unwrap();
        *mock = Some(mock_fn);
    }

    /// Set the mock function for the drainer service
    pub fn set_drainer_mock(mock_fn: DrainerMockFn) {
        let mut mock = DRAINER_MOCK.lock().unwrap();
        *mock = Some(mock_fn);
    }

    /// Clear all mocks (useful for test cleanup)
    pub fn clear_mocks() {
        {
            let mut health_mock = HEALTH_SERVER_MOCK.lock().unwrap();
            *health_mock = None;
        }
        {
            let mut drainer_mock = DRAINER_MOCK.lock().unwrap();
            *drainer_mock = None;
        }
    }
}

impl MockStore {
    /// Create a new mock store that indicates successful creation
    pub fn new_success() -> Self {
        Self {
            created: Arc::new(Mutex::new(true)),
            creation_delay: Arc::new(Mutex::new(Duration::from_millis(0))),
            creation_behavior: Arc::new(Mutex::new(CreationBehavior::Success)),
        }
    }

    /// Check if the mock store was created (for test assertions)
    pub fn was_created(&self) -> bool {
        *self.created.lock().unwrap()
    }

    /// Set the creation delay for all mock stores (global setting)
    pub fn set_creation_delay(delay: Duration) {
        let mut global_delay = MOCK_STORE_CREATION_DELAY.lock().unwrap();
        *global_delay = delay;
    }

    /// Set the creation behavior for all mock stores (global setting)
    pub fn set_creation_behavior(behavior: CreationBehavior) {
        let mut global_behavior = MOCK_STORE_CREATION_BEHAVIOR.lock().unwrap();
        *global_behavior = behavior;
    }

    /// Create a mock store with the configured behavior
    pub async fn create_mock(_config: &Settings, _tenant: &str) -> Result<Self> {
        let delay = {
            let delay = MOCK_STORE_CREATION_DELAY.lock().unwrap();
            *delay
        };
        
        let behavior = {
            let behavior = MOCK_STORE_CREATION_BEHAVIOR.lock().unwrap();
            behavior.clone()
        };

        // Simulate creation delay
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        // Simulate creation behavior
        match behavior {
            CreationBehavior::Success => Ok(Self::new_success()),
            CreationBehavior::Fail => Err(anyhow!("Mock store creation failed")),
        }
    }
}

/// Mock implementation of the start_web_server function
/// 
/// This function is called by tests to provide controlled behavior
/// for the health server during testing.
pub async fn mock_start_web_server(cancellation_token: CancellationToken) -> Result<()> {
    let mock_fn = {
        let mock = HEALTH_SERVER_MOCK.lock().unwrap();
        mock.clone()
    };

    match mock_fn {
        Some(mock_fn) => mock_fn(cancellation_token).await,
        None => {
            // Default behavior: wait for cancellation
            cancellation_token.cancelled().await;
            Ok(())
        }
    }
}

/// Mock implementation of the start_drainer function
/// 
/// This function is called by tests to provide controlled behavior
/// for the drainer service during testing.
pub async fn mock_start_drainer(
    app_state: AppState,
    stores: HashMap<String, Arc<Store>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let mock_fn = {
        let mock = DRAINER_MOCK.lock().unwrap();
        mock.clone()
    };

    match mock_fn {
        Some(mock_fn) => mock_fn(app_state, stores, cancellation_token).await,
        None => {
            // Default behavior: wait for cancellation
            cancellation_token.cancelled().await;
            Ok(())
        }
    }
}

/// Extension trait for Settings to provide test-specific configurations
pub trait SettingsTestExt {
    /// Create a default settings instance suitable for testing
    fn default_for_testing() -> Self;
}

impl SettingsTestExt for Settings {
    fn default_for_testing() -> Self {
        // This would create a minimal configuration suitable for testing
        // In a real implementation, this might load from a test config file
        // or create a minimal in-memory configuration
        Settings::new_for_testing()
    }
}

/// Mock implementation extension for Settings
impl Settings {
    /// Create a new Settings instance configured for testing
    fn new_for_testing() -> Self {
        // This is a placeholder - in a real implementation, you would
        // create a minimal configuration that allows the tests to run
        // without requiring external dependencies like Redis, databases, etc.
        Settings::default()
    }
}

/// Test utilities module
pub mod test_utils {
    use super::*;
    use std::sync::Once;

    static INIT_LOGGER: Once = Once::new();

    /// Initialize logging for tests (call once per test suite)
    pub fn init_test_logging() {
        INIT_LOGGER.call_once(|| {
            // Initialize a simple logger for tests
            let _ = env_logger::builder()
                .filter_level(log::LevelFilter::Debug)
                .is_test(true)
                .try_init();
        });
    }

    /// Create a test configuration with the given number of tenants
    pub fn create_test_config_with_tenants(tenant_count: usize) -> Settings {
        let mut config = Settings::default_for_testing();
        
        // Configure the specified number of tenants for testing
        // This would modify the multitenancy configuration
        // to include the requested number of test tenants
        
        config
    }

    /// Cleanup function to be called after each test
    pub fn cleanup_test_state() {
        MockServer::clear_mocks();
        MockStore::set_creation_behavior(CreationBehavior::Success);
        MockStore::set_creation_delay(Duration::from_millis(0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_mock_store_creation_success() {
        MockStore::set_creation_behavior(CreationBehavior::Success);
        
        let config = Settings::default_for_testing();
        let result = MockStore::create_mock(&config, "test_tenant").await;
        
        assert!(result.is_ok());
        let store = result.unwrap();
        assert!(store.was_created());
    }

    #[tokio::test]
    async fn test_mock_store_creation_failure() {
        MockStore::set_creation_behavior(CreationBehavior::Fail);
        
        let config = Settings::default_for_testing();
        let result = MockStore::create_mock(&config, "test_tenant").await;
        
        assert!(result.is_err());
        
        // Reset for other tests
        MockStore::set_creation_behavior(CreationBehavior::Success);
    }

    #[tokio::test]
    async fn test_mock_store_creation_delay() {
        let delay = Duration::from_millis(100);
        MockStore::set_creation_delay(delay);
        
        let config = Settings::default_for_testing();
        let start = std::time::Instant::now();
        let result = MockStore::create_mock(&config, "test_tenant").await;
        let elapsed = start.elapsed();
        
        assert!(result.is_ok());
        assert!(elapsed >= delay);
        
        // Reset for other tests
        MockStore::set_creation_delay(Duration::from_millis(0));
    }

    #[tokio::test]
    async fn test_mock_health_server_default_behavior() {
        let token = CancellationToken::new();
        let token_clone = token.clone();
        
        // Start mock server
        let server_task = tokio::spawn(async move {
            mock_start_web_server(token_clone).await
        });
        
        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Cancel and check that it completes
        token.cancel();
        let result = timeout(Duration::from_millis(100), server_task).await;
        
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_cleanup_resets_state() {
        // Set some non-default values
        MockStore::set_creation_delay(Duration::from_millis(100));
        MockStore::set_creation_behavior(CreationBehavior::Fail);
        
        // Cleanup
        test_utils::cleanup_test_state();
        
        // Verify reset to defaults
        let delay = *MOCK_STORE_CREATION_DELAY.lock().unwrap();
        let behavior = MOCK_STORE_CREATION_BEHAVIOR.lock().unwrap().clone();
        
        assert_eq!(delay, Duration::from_millis(0));
        assert!(matches!(behavior, CreationBehavior::Success));
    }
}
