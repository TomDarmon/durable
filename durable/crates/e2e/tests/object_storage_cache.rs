use e2e::{cached_objects, fresh_storage, put_raw, restart_rustfs, storage_for_scope};
use std::error::Error;
use substrate::{ImmutableObjects, RawRanges, RootName, RootRegister};
use tempfile::TempDir;

#[tokio::test]
#[ignore = "requires local RustFS from `make integration-up`"]
async fn immutable_objects_survive_cache_loss_and_rustfs_restart() -> Result<(), Box<dyn Error>> {
    let storage = fresh_storage("object-cache").await;
    let scope = storage.scope().clone();
    let bytes = b"0123456789abcdef";

    let reference = put_raw(storage.as_ref(), bytes).await?;

    assert_eq!(
        ImmutableObjects::read(storage.as_ref(), &reference).await?,
        bytes
    );
    assert_eq!(
        RawRanges::read_range(storage.as_ref(), &reference, 3..8).await?,
        b"34567"
    );

    let cache_dir = TempDir::new()?;
    let cached = cached_objects(storage.clone(), cache_dir.path().to_path_buf());
    assert_eq!(cached.read(&reference).await?, bytes);

    let cache_after_memory_loss = cached_objects(storage.clone(), cache_dir.path().to_path_buf());
    assert_eq!(cache_after_memory_loss.read(&reference).await?, bytes);

    let cache_dir_path = cache_dir.keep();
    std::fs::remove_dir_all(&cache_dir_path)?;
    assert_eq!(
        ImmutableObjects::read(storage.as_ref(), &reference).await?,
        bytes
    );

    restart_rustfs();
    let restarted_storage = storage_for_scope(scope).await;
    assert_eq!(
        ImmutableObjects::read(restarted_storage.as_ref(), &reference).await?,
        bytes
    );
    assert!(
        RootRegister::read(restarted_storage.as_ref(), &RootName::new("unused-root"))
            .await?
            .is_none()
    );

    Ok(())
}
