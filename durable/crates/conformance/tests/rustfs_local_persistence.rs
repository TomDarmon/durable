use s3::{S3Backend, S3BackendConfig};
use std::{
    path::PathBuf,
    process::Command,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use substrate::{
    compute_object_id, DatasetId, Durability, EncryptionDomainId, ExpectedRevision,
    ImmutableObjects, ObjectFormat, PublishOutcome, RootName, RootRegister, ScopedStorage,
    StorageScope, TenantId,
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

fn restart_rustfs() {
    let status = Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(durable_root())
        .args(["restart", "rustfs"])
        .status()
        .expect("failed to run docker compose restart rustfs");
    assert!(status.success());
}

#[tokio::test]
#[ignore = "local RustFS Docker persistence smoke test; not an S3 semantics test"]
async fn local_rustfs_volume_preserves_objects_and_roots_after_container_restart() {
    let backend = Arc::new(wait_backend().await);
    let scope = StorageScope::new(
        TenantId::new("rustfs-local-persistence"),
        DatasetId::new(uuid::Uuid::new_v4().to_string()),
        EncryptionDomainId::new("edek"),
    );
    let storage = ScopedStorage::new(scope.clone(), backend);
    let bytes = b"survives local rustfs container restart";
    let id = compute_object_id(&ObjectFormat::Raw, bytes);
    let reference = storage
        .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
        .await
        .unwrap();
    let root = RootName::new("local-persistence-root");
    let published = storage
        .compare_exchange(&root, ExpectedRevision::Missing, b"root".to_vec())
        .await
        .unwrap();
    assert!(matches!(published, PublishOutcome::Applied(_)));

    restart_rustfs();

    let reopened = Arc::new(wait_backend().await);
    let storage_after_restart = ScopedStorage::new(scope, reopened);
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
