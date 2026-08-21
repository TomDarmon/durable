//! Shared helpers for durable end-to-end tests.

use durable_s3::{S3Backend, S3BackendConfig};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Returns the RustFS/S3 configuration used by local integration tests.
pub fn rustfs_config() -> S3BackendConfig {
    S3BackendConfig {
        endpoint: std::env::var("DURABLE_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".into()),
        region: "us-east-1".into(),
        bucket: std::env::var("DURABLE_RUSTFS_BUCKET").unwrap_or_else(|_| "durable-dev".into()),
        access_key: std::env::var("DURABLE_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "durable".into()),
        secret_key: std::env::var("DURABLE_RUSTFS_SECRET_KEY")
            .unwrap_or_else(|_| "durable-secret".into()),
        create_bucket_if_missing: true,
    }
}

/// Waits for local RustFS to accept S3 requests.
pub async fn wait_for_rustfs() -> S3Backend {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match S3Backend::new(rustfs_config()).await {
            Ok(backend) => return backend,
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for RustFS: {error}");
                thread::sleep(Duration::from_millis(500));
            }
            Err(error) => panic!("RustFS did not become ready: {error}"),
        }
    }
}
