use durable_conformance::run_backend_conformance;
use durable_core::{
    compute_object_id, DatasetId, Durability, EncryptionDomainId, ExpectedRevision,
    ImmutableObjects, ObjectFormat, PublishOutcome, RootName, RootRegister, ScopedStorage,
    StorageScope, TenantId,
};
use durable_s3::{S3Backend, S3BackendConfig};
use std::{
    path::PathBuf,
    process::Command,
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

fn durable_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate should be under durable/crates")
        .to_path_buf()
}

#[tokio::test]
#[ignore = "requires local RustFS from `make integration-up`"]
async fn rustfs_backend_conforms_and_survives_restart() {
    let backend = Arc::new(wait_backend().await);
    let report = run_backend_conformance(backend.clone()).await.unwrap();
    assert!(report.create_if_absent);
    assert!(report.stale_version_rejected);
    assert!(report.racing_cas_single_winner);
    assert!(report.fresh_logical_revisions);
    assert!(report.read_after_write);
    assert!(report.range_reads);
    assert!(report.distinguishable_failures);

    let scope = StorageScope::new(
        TenantId::new("restart-tenant"),
        DatasetId::new(uuid::Uuid::new_v4().to_string()),
        EncryptionDomainId::new("edek"),
    );
    let storage = ScopedStorage::new(scope.clone(), backend);
    let bytes = b"survives rustfs restart";
    let id = compute_object_id(&ObjectFormat::Raw, bytes);
    let reference = storage
        .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
        .await
        .unwrap();
    let root = RootName::new("restart-root");
    let published = storage
        .compare_exchange(&root, ExpectedRevision::Missing, b"root".to_vec())
        .await
        .unwrap();
    assert!(matches!(published, PublishOutcome::Applied(_)));

    let status = Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(durable_root())
        .args(["restart", "rustfs"])
        .status()
        .expect("failed to run docker compose restart rustfs");
    assert!(status.success());

    let restarted = Arc::new(wait_backend().await);
    let storage_after_restart = ScopedStorage::new(scope, restarted);
    assert_eq!(
        ImmutableObjects::read(&storage_after_restart, &reference)
            .await
            .unwrap(),
        bytes
    );
    assert_eq!(
        RootRegister::read(&storage_after_restart, &root)
            .await
            .unwrap()
            .unwrap()
            .value(),
        b"root"
    );
}
