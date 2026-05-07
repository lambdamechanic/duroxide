//! SQLite Stress Tests for Duroxide
//!
//! This library provides SQLite-specific stress test implementations for Duroxide,
//! using the provider stress test infrastructure from the main crate.
//!
//! # Quick Start
//!
//! Run the parallel orchestrations stress test:
//!
//! ```bash
//! cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress [DURATION]
//! ```
//!
//! Run the large payload stress test:
//!
//! ```bash
//! cargo run --release --package duroxide-sqlite-stress --bin large-payload-stress [DURATION]
//! ```
//!
//! Or use from the workspace root:
//!
//! ```bash
//! ./run-stress-tests.sh [DURATION]
//! ```

use duroxide::provider_stress_tests::parallel_orchestrations::{
    run_parallel_orchestrations_test_with_config, ProviderStressFactory,
};
use duroxide::provider_stress_tests::{print_comparison_table, StressTestConfig};
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::providers::turso::{TursoJournalMode, TursoOptions, TursoProvider, TursoTransactionMode};
use duroxide::providers::Provider;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

// Re-export the stress test infrastructure for convenience
pub use duroxide::provider_stress_tests::{StressTestConfig as Config, StressTestResult};

fn temp_db_path(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("/tmp/duroxide_{prefix}_stress_{}_{}.db", std::process::id(), timestamp)
}

fn cleanup_db_files(db_path: &str) {
    for suffix in ["", "-wal", "-shm"] {
        let path = format!("{db_path}{suffix}");
        if let Err(error) = std::fs::remove_file(&path) {
            if error.kind() != ErrorKind::NotFound {
                tracing::warn!("Failed to remove temp DB file {}: {}", path, error);
            }
        }
    }
}

fn stress_config(duration_secs: u64, orch_conc: usize, worker_conc: usize) -> StressTestConfig {
    StressTestConfig {
        max_concurrent: 20,
        duration_secs,
        tasks_per_instance: 5,
        activity_delay_ms: 10,
        orch_concurrency: orch_conc,
        worker_concurrency: worker_conc,
        wait_timeout_secs: 60,
    }
}

/// Factory for creating in-memory SQLite providers for stress testing
pub struct InMemorySqliteFactory;

#[async_trait::async_trait]
impl ProviderStressFactory for InMemorySqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            SqliteProvider::new_in_memory()
                .await
                .expect("Failed to create in-memory SQLite provider"),
        )
    }
}

/// Factory for creating file-based SQLite providers for stress testing
pub struct FileSqliteFactory {
    db_path: String,
}

impl FileSqliteFactory {
    pub fn new() -> Self {
        let db_path = temp_db_path("sqlite");
        // Create the file to ensure it exists
        if let Err(e) = std::fs::File::create(&db_path) {
            tracing::warn!("Failed to pre-create DB file {}: {}", db_path, e);
        }
        Self { db_path }
    }

    pub fn cleanup(&self) {
        cleanup_db_files(&self.db_path);
    }
}

#[async_trait::async_trait]
impl ProviderStressFactory for FileSqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            SqliteProvider::new(&format!("sqlite:{}", self.db_path), None)
                .await
                .expect("Failed to create file-based SQLite provider"),
        )
    }
}

/// Factory for creating in-memory Turso providers for stress testing
pub struct InMemoryTursoFactory {
    options: Option<TursoOptions>,
}

impl InMemoryTursoFactory {
    pub fn new(options: Option<TursoOptions>) -> Self {
        Self { options }
    }
}

#[async_trait::async_trait]
impl ProviderStressFactory for InMemoryTursoFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            TursoProvider::new_in_memory_with_options(self.options.clone())
                .await
                .expect("Failed to create in-memory Turso provider"),
        )
    }
}

/// Factory for creating file-based Turso providers for stress testing
pub struct FileTursoFactory {
    db_path: String,
    options: Option<TursoOptions>,
}

impl FileTursoFactory {
    pub fn immediate() -> Self {
        Self::new(None)
    }

    pub fn mvcc_concurrent() -> Self {
        Self::new(Some(TursoOptions {
            journal_mode: TursoJournalMode::Mvcc,
            transaction_mode: TursoTransactionMode::Concurrent,
            ..Default::default()
        }))
    }

    pub fn new(options: Option<TursoOptions>) -> Self {
        Self {
            db_path: temp_db_path("turso"),
            options,
        }
    }

    pub fn cleanup(&self) {
        cleanup_db_files(&self.db_path);
    }
}

#[async_trait::async_trait]
impl ProviderStressFactory for FileTursoFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            TursoProvider::new(&format!("turso:{}", self.db_path), self.options.clone())
                .await
                .expect("Failed to create file-based Turso provider"),
        )
    }
}

/// Run the parallel orchestrations stress test suite across SQLite providers and configurations
pub async fn run_test_suite(duration_secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    info!("=== Duroxide SQLite Stress Test Suite ===");
    info!("Duration: {} seconds per test", duration_secs);

    let concurrency_combos = vec![(1, 1), (2, 2)];
    let mut results = Vec::new();

    // Test in-memory SQLite
    info!("\n--- Testing In-Memory SQLite Provider ---");
    let in_memory_factory = InMemorySqliteFactory;

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = stress_config(duration_secs, *orch_conc, *worker_conc);

        match run_parallel_orchestrations_test_with_config(&in_memory_factory, config).await {
            Ok(result) => {
                results.push((
                    "In-Memory SQLite".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
            }
        }
    }

    // Test file-based SQLite
    info!("\n--- Testing File-Based SQLite Provider ---");

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = stress_config(duration_secs, *orch_conc, *worker_conc);

        let file_factory = FileSqliteFactory::new();
        match run_parallel_orchestrations_test_with_config(&file_factory, config).await {
            Ok(result) => {
                results.push((
                    "File SQLite".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
                file_factory.cleanup();
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
                file_factory.cleanup();
            }
        }
    }

    // Print comparison table
    print_comparison_table(&results);

    Ok(())
}

/// Run the parallel orchestrations stress test suite across Turso modes.
pub async fn run_turso_test_suite(duration_secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    info!("=== Duroxide Turso Stress Test Suite ===");
    info!("Duration: {} seconds per test", duration_secs);

    let concurrency_combos = vec![(1, 1), (2, 2)];
    let mut results = Vec::new();

    info!("\n--- Testing In-Memory Turso Provider ---");
    let in_memory_factory = InMemoryTursoFactory::new(None);

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = stress_config(duration_secs, *orch_conc, *worker_conc);

        match run_parallel_orchestrations_test_with_config(&in_memory_factory, config).await {
            Ok(result) => {
                results.push((
                    "In-Memory Turso".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
            }
        }
    }

    info!("\n--- Testing File-Based Turso Provider (BEGIN IMMEDIATE) ---");

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = stress_config(duration_secs, *orch_conc, *worker_conc);
        let file_factory = FileTursoFactory::immediate();

        match run_parallel_orchestrations_test_with_config(&file_factory, config).await {
            Ok(result) => {
                results.push((
                    "File Turso".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
            }
        }

        file_factory.cleanup();
    }

    info!("\n--- Testing File-Based Turso Provider (MVCC + BEGIN CONCURRENT) ---");

    for (orch_conc, worker_conc) in &concurrency_combos {
        let config = stress_config(duration_secs, *orch_conc, *worker_conc);
        let file_factory = FileTursoFactory::mvcc_concurrent();

        match run_parallel_orchestrations_test_with_config(&file_factory, config).await {
            Ok(result) => {
                results.push((
                    "Turso MVCC".to_string(),
                    format!("{}/{}", orch_conc, worker_conc),
                    result,
                ));
                info!("✓ Test completed");
            }
            Err(e) => {
                info!("✗ Test failed: {}", e);
            }
        }

        file_factory.cleanup();
    }

    print_comparison_table(&results);

    Ok(())
}
