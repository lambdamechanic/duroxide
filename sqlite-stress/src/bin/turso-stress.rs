//! Turso Stress Test Binary
//!
//! This binary runs the generic provider stress test suite across Turso modes.
//!
//! Usage:
//!   cargo run --release --package duroxide-sqlite-stress --bin turso-stress [DURATION_SECS]

use duroxide_sqlite_stress;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let duration_secs = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<u64>().ok())
        .unwrap_or(30);

    duroxide_sqlite_stress::run_turso_test_suite(duration_secs).await?;

    Ok(())
}
