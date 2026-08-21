#[path = "support/journaling_worker.rs"]
mod journaling_worker;
mod support;

use e2e::{fresh_storage, put_raw};
use journal::{JournalPosition, PagedJournal, StreamId};
use journaling_worker::journaling_worker;
use queue::{QueueConfig, TerminalState, WorkQueue};
use serde_json::json;
use std::{error::Error, sync::Arc};
use substrate::RootName;
use support::assert_root_value;
use worker::{WorkerConfig, WorkerRuntime};

#[tokio::test]
#[ignore = "requires local RustFS from `make integration-up`"]
async fn worker_acks_jobs_after_journal_append_and_root_publish() -> Result<(), Box<dyn Error>> {
    let storage = fresh_storage("worker-journal").await;
    let root = RootName::new("application-root");
    let records = Arc::new(vec![
        put_raw(storage.as_ref(), b"first durable object").await?,
        put_raw(storage.as_ref(), b"second durable object").await?,
    ]);
    let journal = Arc::new(PagedJournal::new(
        StreamId::new(format!("worker-stream-{}", uuid::Uuid::new_v4())),
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

    let runtime = WorkerRuntime::new(
        queue.clone(),
        journaling_worker(storage.clone(), journal.clone(), records, root.clone()),
        WorkerConfig {
            worker_id: "e2e-worker".into(),
            concurrency: 2,
            ..WorkerConfig::default()
        },
    );

    assert_eq!(runtime.run_once(1).await?, 2);
    assert_eq!(
        queue.terminal_state(&first_job).await,
        Some(TerminalState::Acked)
    );
    assert_eq!(
        queue.terminal_state(&second_job).await,
        Some(TerminalState::Acked)
    );

    let snapshot = journal.scan_from(JournalPosition::FIRST).await?;
    assert_eq!(snapshot.records.len(), 2);
    assert_root_value(storage.as_ref(), &root, b"job-two-applied").await?;

    Ok(())
}
