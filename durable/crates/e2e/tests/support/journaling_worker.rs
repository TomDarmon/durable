use e2e::publish_until_visible;
use journal::{digest_records, AppendOutcome, AppendRequest, ClientRequestKey, Journal};
use queue::ClaimedJob;
use std::{future::Future, pin::Pin, sync::Arc};
use substrate::RootName;
use worker::{JobContext, JobDisposition, StaticHandler, WorkerError};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<JobDisposition, WorkerError>> + Send>>;

pub fn journaling_worker(
    storage: Arc<substrate::ScopedStorage<s3::S3Backend>>,
    journal: Arc<journal::PagedJournal<substrate::ScopedStorage<s3::S3Backend>>>,
    records: Arc<Vec<substrate::DurableObjectRef>>,
    root: RootName,
) -> StaticHandler<impl Fn(ClaimedJob, JobContext) -> HandlerFuture + Send + Sync> {
    StaticHandler::new(move |job: ClaimedJob, _context: JobContext| {
        let storage = storage.clone();
        let journal = journal.clone();
        let records = records.clone();
        let root = root.clone();
        let future: HandlerFuture = Box::pin(async move {
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
            let record = records
                .get(record_index)
                .ok_or_else(|| WorkerError::Handler("record_index out of range".into()))?
                .clone();
            let records = vec![record];
            let digest = digest_records(&records)
                .map_err(|error| WorkerError::Handler(error.to_string()))?;
            let token = journal.issue_request_token(ClientRequestKey::new(client_key), digest, 1);
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
        });
        future
    })
}
