mod support;

use e2e::{cached_objects, fresh_storage, publish_until_visible, put_raw, storage_for_scope};
use journal::{AppendRequest, Journal, JournalPosition, PagedJournal, StreamId};
use retention::{RetentionEpochs, RetentionError};
use std::{error::Error, sync::Arc};
use substrate::{ImmutableObjects, RootName};
use support::assert_root_value;
use tempfile::TempDir;

#[tokio::test]
#[ignore = "requires local RustFS from `make integration-up`"]
async fn composed_library_state_is_visible_to_fresh_client() -> Result<(), Box<dyn Error>> {
    let storage = fresh_storage("full-stack").await;
    let scope = storage.scope().clone();
    let root = RootName::new("application-root");
    let retention = RetentionEpochs::new(storage.scope_id());
    let epoch = retention.enter(1, 10);

    assert_eq!(
        retention.require_delete_proof().unwrap_err(),
        RetentionError::PhysicalDeletionDisabled
    );

    let object_ref = put_raw(storage.as_ref(), b"recover me").await?;
    let cache_dir = TempDir::new()?;
    assert_eq!(
        cached_objects(storage.clone(), cache_dir.path().to_path_buf())
            .read(&object_ref)
            .await?,
        b"recover me"
    );

    let journal = Arc::new(PagedJournal::new(
        StreamId::new(format!("full-stack-stream-{}", uuid::Uuid::new_v4())),
        storage.clone(),
    ));
    let records = vec![object_ref.clone()];
    let digest = journal::digest_records(&records)?;
    let token = journal.issue_request_token(journal::ClientRequestKey::new("recovery"), digest, 1);
    journal.append(AppendRequest { token, records }, 1).await?;
    publish_until_visible(storage.as_ref(), &root, b"published".to_vec()).await?;
    retention.release(&epoch)?;

    let reopened_storage = storage_for_scope(scope).await;
    let reopened_journal = PagedJournal::new(journal.stream_id().clone(), reopened_storage.clone());

    assert_eq!(
        reopened_journal
            .scan_from(JournalPosition::FIRST)
            .await?
            .records
            .len(),
        1
    );
    assert_root_value(reopened_storage.as_ref(), &root, b"published").await?;
    assert_eq!(
        ImmutableObjects::read(reopened_storage.as_ref(), &object_ref).await?,
        b"recover me"
    );

    Ok(())
}
