use durable_cache::{
    CachedObjects, DefaultLoadPolicy, DiskCacheStore, LruEviction, MemoryCacheStore, SizeAdmission,
};
use durable_core::{
    compute_object_id, DatasetId, Durability, DurableError, DurableObjectRef, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, RootName, RootRegister,
    ScopedStorage, StorageScope, TenantId,
};
use durable_e2e::wait_for_rustfs;
use durable_journal::{
    digest_records, AppendOutcome, AppendRequest, Journal, JournalPosition, PagedJournal, StreamId,
};
use durable_queue::{ClaimedJob, QueueConfig, WorkQueue};
use durable_retention::{RetentionEpochs, RetentionError};
use durable_s3::S3Backend;
use durable_worker::{
    JobContext, JobDisposition, StaticHandler, WorkerConfig, WorkerError, WorkerRuntime,
};
use serde_json::json;
use std::{error::Error, path::PathBuf, process::Command, sync::Arc};
use tempfile::TempDir;

#[tokio::test]
#[ignore = "requires local RustFS from `make integration-up`"]
async fn rustfs_full_stack_worker_journal_cache_and_recovery() -> Result<(), Box<dyn Error>> {
    let backend = Arc::new(wait_for_rustfs().await);
    let scope = StorageScope::new(
        TenantId::new("e2e-tenant"),
        DatasetId::new(uuid::Uuid::new_v4().to_string()),
        EncryptionDomainId::new("edek"),
    );
    let storage = Arc::new(ScopedStorage::new(scope.clone(), backend));
    let root = RootName::new("application-root");

    let retention = RetentionEpochs::new(storage.scope_id());
    let epoch = retention.enter(1, 10);
    assert!(RootRegister::read(storage.as_ref(), &root).await?.is_none());
    retention.renew(&epoch, 2, 10)?;
    assert_eq!(
        retention.require_delete_proof().unwrap_err(),
        RetentionError::PhysicalDeletionDisabled
    );

    let first_ref = put_raw(storage.as_ref(), b"first durable object").await?;
    let second_ref = put_raw(storage.as_ref(), b"second durable object").await?;
    let refs = Arc::new(vec![first_ref.clone(), second_ref.clone()]);

    let cache_dir = TempDir::new()?;
    let cached = cached_objects(storage.clone(), cache_dir.path().to_path_buf());
    assert_eq!(cached.read(&first_ref).await?, b"first durable object");
    let cached_after_memory_loss = cached_objects(storage.clone(), cache_dir.path().to_path_buf());
    assert_eq!(
        cached_after_memory_loss.read(&first_ref).await?,
        b"first durable object"
    );

    let journal = Arc::new(PagedJournal::new(
        StreamId::new(format!("e2e-stream-{}", uuid::Uuid::new_v4())),
        storage.clone(),
    ));
    let queue = WorkQueue::new(QueueConfig {
        lease_ticks: 5,
        max_attempts: 3,
        retry_backoff_ticks: 1,
    });
    let first_job = queue
        .enqueue(
            json!({"client_key": "job-one", "record_index": 0, "root_value": "job-one-applied"}),
            1,
        )
        .await?;
    let second_job = queue
        .enqueue(
            json!({"client_key": "job-two", "record_index": 1, "root_value": "job-two-applied"}),
            1,
        )
        .await?;

    let handler_storage = storage.clone();
    let handler_journal = journal.clone();
    let handler_refs = refs.clone();
    let handler_root = root.clone();
    let handler = StaticHandler::new(move |job: ClaimedJob, _context: JobContext| {
        let storage = handler_storage.clone();
        let journal = handler_journal.clone();
        let refs = handler_refs.clone();
        let root = handler_root.clone();
        async move {
            let record_index = job
                .payload
                .get("record_index")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| WorkerError::Handler("missing record_index".into()))?
                as usize;
            let client_key = job
                .payload
                .get("client_key")
                .and_then(|value| value.as_str())
                .ok_or_else(|| WorkerError::Handler("missing client_key".into()))?;
            let root_value = job
                .payload
                .get("root_value")
                .and_then(|value| value.as_str())
                .ok_or_else(|| WorkerError::Handler("missing root_value".into()))?
                .as_bytes()
                .to_vec();
            let record = refs
                .get(record_index)
                .ok_or_else(|| WorkerError::Handler("record_index out of range".into()))?
                .clone();
            let records = vec![record];
            let digest = digest_records(&records)
                .map_err(|error| WorkerError::Handler(error.to_string()))?;
            let token = journal.issue_request_token(
                durable_journal::ClientRequestKey::new(client_key),
                digest,
                1,
            );
            match journal
                .append(AppendRequest { token, records }, 1)
                .await
                .map_err(|error| WorkerError::Handler(error.to_string()))?
            {
                AppendOutcome::Appended(_) | AppendOutcome::AlreadyAppended(_) => {}
                AppendOutcome::OutcomeUnknown => {
                    return Err(WorkerError::Handler("unexpected journal ambiguity".into()));
                }
            }
            publish_until_visible(storage.as_ref(), &root, root_value)
                .await
                .map_err(|error| WorkerError::Handler(error.to_string()))?;
            Ok(JobDisposition::Completed)
        }
    });

    let runtime = WorkerRuntime::new(
        queue.clone(),
        handler,
        WorkerConfig {
            worker_id: "e2e-worker".into(),
            concurrency: 2,
            ..WorkerConfig::default()
        },
    );
    assert_eq!(runtime.run_once(1).await?, 2);
    assert_eq!(
        queue.terminal_state(&first_job).await,
        Some(durable_queue::TerminalState::Acked)
    );
    assert_eq!(
        queue.terminal_state(&second_job).await,
        Some(durable_queue::TerminalState::Acked)
    );

    let snapshot = journal.scan_from(JournalPosition::FIRST).await?;
    assert_eq!(snapshot.records.len(), 2);
    assert_eq!(
        cached_after_memory_loss.read(&snapshot.records[0]).await?,
        b"first durable object"
    );
    assert_eq!(
        cached_after_memory_loss.read(&snapshot.records[1]).await?,
        b"second durable object"
    );
    assert_eq!(
        RootRegister::read(storage.as_ref(), &root)
            .await?
            .expect("root should be published")
            .value(),
        b"job-two-applied"
    );
    retention.release(&epoch)?;

    let status = Command::new("docker")
        .arg("compose")
        .arg("--project-directory")
        .arg(durable_root())
        .args(["restart", "rustfs"])
        .status()
        .expect("failed to restart RustFS");
    assert!(status.success());

    let restarted = Arc::new(wait_for_rustfs().await);
    let restarted_storage = Arc::new(ScopedStorage::new(scope, restarted));
    let restarted_journal =
        PagedJournal::new(journal.stream_id().clone(), restarted_storage.clone());
    let recovered_snapshot = restarted_journal.scan_from(JournalPosition::FIRST).await?;
    assert_eq!(recovered_snapshot.records.len(), 2);
    assert_eq!(
        RootRegister::read(restarted_storage.as_ref(), &root)
            .await?
            .expect("root should survive restart")
            .value(),
        b"job-two-applied"
    );
    assert_eq!(
        ImmutableObjects::read(restarted_storage.as_ref(), &first_ref).await?,
        b"first durable object"
    );

    Ok(())
}

fn durable_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate should be under durable/crates")
        .to_path_buf()
}

async fn put_raw(
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

fn cached_objects(
    storage: Arc<ScopedStorage<S3Backend>>,
    disk_path: std::path::PathBuf,
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

async fn publish_until_visible(
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
