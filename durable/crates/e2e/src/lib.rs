//! Shared helpers for durable end-to-end tests.

use cache::{
    CachedObjects, DefaultLoadPolicy, DiskCacheStore, LruEviction, MemoryCacheStore, SizeAdmission,
};
use s3::{S3Backend, S3BackendConfig};
use std::{
    path::PathBuf,
    process::Command,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use substrate::{
    compute_object_id, DatasetId, Durability, DurableError, DurableObjectRef, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, RootName, RootRegister,
    ScopedStorage, StorageScope, TenantId,
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

/// Returns the durable workspace root, where `docker-compose.yml` lives.
pub fn durable_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate should be under durable/crates")
        .to_path_buf()
}

/// Restarts RustFS through the local compose stack.
pub fn restart_rustfs() {
    let status = Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(durable_root())
        .args(["restart", "rustfs"])
        .status()
        .expect("failed to restart RustFS");
    assert!(status.success());
}

/// Creates a fresh scope-bound RustFS storage handle.
pub async fn fresh_storage(label: &str) -> Arc<ScopedStorage<S3Backend>> {
    let scope = StorageScope::new(
        TenantId::new(format!("{label}-tenant")),
        DatasetId::new(uuid::Uuid::new_v4().to_string()),
        EncryptionDomainId::new("edek"),
    );
    storage_for_scope(scope).await
}

/// Creates a RustFS storage handle for an existing scope.
pub async fn storage_for_scope(scope: StorageScope) -> Arc<ScopedStorage<S3Backend>> {
    Arc::new(ScopedStorage::new(scope, Arc::new(wait_for_rustfs().await)))
}

/// Writes raw bytes as a verified immutable object.
pub async fn put_raw(
    storage: &ScopedStorage<S3Backend>,
    bytes: &[u8],
) -> Result<DurableObjectRef, DurableError> {
    storage
        .put(
            compute_object_id(&ObjectFormat::Raw, bytes),
            ObjectFormat::Raw,
            bytes,
            Durability::BackendDefault,
        )
        .await
}

/// Builds a memory+disk read-through cache for one scope.
pub fn cached_objects(
    storage: Arc<ScopedStorage<S3Backend>>,
    disk_path: PathBuf,
) -> CachedObjects<ScopedStorage<S3Backend>> {
    CachedObjects::new(
        storage.scope_id(),
        storage,
        Arc::new(MemoryCacheStore::new(
            4 * 1024 * 1024,
            Arc::new(LruEviction),
        )),
        Arc::new(DiskCacheStore::new(disk_path)),
        Arc::new(DefaultLoadPolicy),
        Arc::new(SizeAdmission::new(4 * 1024 * 1024)),
    )
}

/// Publishes a root value, retrying benign CAS conflicts until visible.
pub async fn publish_until_visible(
    storage: &ScopedStorage<S3Backend>,
    root: &RootName,
    value: Vec<u8>,
) -> Result<(), DurableError> {
    for _ in 0..8 {
        let expected = RootRegister::read(storage, root)
            .await?
            .map_or(ExpectedRevision::Missing, |state| {
                ExpectedRevision::Exact(state.revision())
            });
        match storage
            .compare_exchange(root, expected, value.clone())
            .await?
        {
            PublishOutcome::Applied(state) if state.value() == value => return Ok(()),
            PublishOutcome::Applied(_) => {
                return Err(DurableError::Corrupt(
                    "published unexpected root value".into(),
                ));
            }
            PublishOutcome::Conflict {
                current: Some(state),
            } if state.value() == value => {
                return Ok(());
            }
            PublishOutcome::Conflict { .. } | PublishOutcome::OutcomeUnknown => {
                if RootRegister::read(storage, root)
                    .await?
                    .is_some_and(|state| state.value() == value)
                {
                    return Ok(());
                }
            }
        }
    }
    Err(DurableError::Unavailable(
        "root publication did not become visible".into(),
    ))
}
