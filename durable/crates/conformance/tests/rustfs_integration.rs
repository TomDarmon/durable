use conformance::run_backend_conformance;
use s3::{S3Backend, S3BackendConfig};
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

fn config() -> S3BackendConfig {
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

async fn wait_backend() -> S3Backend {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match S3Backend::new(config()).await {
            Ok(backend) => return backend,
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for RustFS: {error}");
                thread::sleep(Duration::from_millis(500));
            }
            Err(error) => panic!("RustFS did not become ready: {error}"),
        }
    }
}

#[tokio::test]
#[ignore = "requires local RustFS from `make integration-up`"]
async fn rustfs_backend_satisfies_storage_contract() {
    let backend = Arc::new(wait_backend().await);
    let report = run_backend_conformance(backend).await.unwrap();
    assert!(report.create_if_absent);
    assert!(report.stale_version_rejected);
    assert!(report.racing_cas_single_winner);
    assert!(report.fresh_logical_revisions);
    assert!(report.read_after_write);
    assert!(report.range_reads);
    assert!(report.distinguishable_failures);
}
