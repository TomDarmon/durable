//! Minimal at-least-once worker runtime.

use async_trait::async_trait;
use durable_queue::{ClaimedJob, QueueError, WorkQueue};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Worker result type.
pub type Result<T> = std::result::Result<T, WorkerError>;

/// Worker runtime errors.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Queue operation failed.
    #[error(transparent)]
    Queue(#[from] QueueError),
    /// Handler failed.
    #[error("handler failed: {0}")]
    Handler(String),
}

/// Handler disposition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDisposition {
    /// Handler completed after observing its authoritative effect.
    Completed,
    /// Job was superseded by newer authoritative state.
    Superseded,
    /// Job should retry after backoff.
    Retry,
    /// Job should be released for reconciliation.
    Reconcile,
    /// Job permanently failed.
    PermanentFailure(String),
}

/// Resource budget passed to handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBudget {
    /// Maximum bytes the handler may use for scratch work.
    pub max_bytes: u64,
}

/// Handler context.
#[derive(Debug, Clone)]
pub struct JobContext {
    /// Resource budget.
    pub budget: ResourceBudget,
    /// Cancellation token.
    pub cancellation: CancellationToken,
}

/// At-least-once job handler.
#[async_trait]
pub trait JobHandler: Send + Sync {
    /// Handles a claimed job. External effects must be idempotent or reconciled.
    async fn handle(&self, job: ClaimedJob, context: JobContext) -> Result<JobDisposition>;
}

/// Metrics or tracing observer.
pub trait WorkerObserver: Send + Sync {
    /// Called when a job starts.
    fn job_started(&self, _job: &ClaimedJob) {}
    /// Called when a job finishes.
    fn job_finished(&self, _job: &ClaimedJob, _disposition: &JobDisposition) {}
}

/// Worker runtime configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Worker identity.
    pub worker_id: String,
    /// Logical concurrency limit.
    pub concurrency: usize,
    /// Handler resource budget.
    pub budget: ResourceBudget,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker".into(),
            concurrency: 1,
            budget: ResourceBudget {
                max_bytes: 64 * 1024 * 1024,
            },
        }
    }
}

/// Small reusable worker runtime.
pub struct WorkerRuntime<H, O = NoopObserver> {
    queue: WorkQueue,
    handler: H,
    observer: O,
    config: WorkerConfig,
    cancellation: CancellationToken,
}

impl<H> WorkerRuntime<H, NoopObserver> {
    /// Creates a runtime with a no-op observer.
    pub fn new(queue: WorkQueue, handler: H, config: WorkerConfig) -> Self {
        Self {
            queue,
            handler,
            observer: NoopObserver,
            config,
            cancellation: CancellationToken::new(),
        }
    }
}

impl<H, O> WorkerRuntime<H, O>
where
    H: JobHandler,
    O: WorkerObserver,
{
    /// Creates a runtime with an observer.
    pub fn with_observer(queue: WorkQueue, handler: H, observer: O, config: WorkerConfig) -> Self {
        Self {
            queue,
            handler,
            observer,
            config,
            cancellation: CancellationToken::new(),
        }
    }

    /// Cancels future work.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Claims and processes at most `concurrency` jobs once.
    pub async fn run_once(&self, now_tick: u64) -> Result<usize> {
        let mut processed = 0;
        for _ in 0..self.config.concurrency.max(1) {
            if self.cancellation.is_cancelled() {
                break;
            }
            let Some(job) = self.queue.claim(&self.config.worker_id, now_tick).await? else {
                break;
            };
            self.observer.job_started(&job);
            let context = JobContext {
                budget: self.config.budget.clone(),
                cancellation: self.cancellation.clone(),
            };
            let disposition = self.handler.handle(job.clone(), context).await?;
            self.apply_disposition(&job, &disposition, now_tick).await?;
            self.observer.job_finished(&job, &disposition);
            tracing::debug!(job_id = %job.id.as_uuid(), ?disposition, "job handled");
            processed += 1;
        }
        Ok(processed)
    }

    async fn apply_disposition(
        &self,
        job: &ClaimedJob,
        disposition: &JobDisposition,
        now_tick: u64,
    ) -> Result<()> {
        match disposition {
            JobDisposition::Completed | JobDisposition::Superseded => {
                self.queue.ack(&job.token).await?;
            }
            JobDisposition::Retry | JobDisposition::Reconcile => {
                let _ = self.queue.retry(&job.token, now_tick).await;
            }
            JobDisposition::PermanentFailure(_) => {
                self.queue.permanent_failure(&job.token).await?;
            }
        }
        Ok(())
    }
}

/// No-op observer.
#[derive(Debug, Clone, Copy)]
pub struct NoopObserver;

impl WorkerObserver for NoopObserver {}

/// Convenience handler from a closure-like value.
pub struct StaticHandler<F> {
    f: F,
}

impl<F> StaticHandler<F> {
    /// Creates a static handler.
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

#[async_trait]
impl<F, Fut> JobHandler for StaticHandler<F>
where
    F: Fn(ClaimedJob, JobContext) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<JobDisposition>> + Send,
{
    async fn handle(&self, job: ClaimedJob, context: JobContext) -> Result<JobDisposition> {
        (self.f)(job, context).await
    }
}

/// Helper for tests that only need the job payload.
pub fn payload_string(payload: &Value, key: &str) -> Option<String> {
    payload.get(key)?.as_str().map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_queue::{QueueConfig, WorkQueue};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[tokio::test]
    async fn completed_job_is_acked() {
        let queue = WorkQueue::new(QueueConfig::default());
        let id = queue
            .enqueue(serde_json::json!({"job": "ok"}), 1)
            .await
            .unwrap();
        let handler = StaticHandler::new(|_job, _context| async { Ok(JobDisposition::Completed) });
        let runtime = WorkerRuntime::new(queue.clone(), handler, WorkerConfig::default());

        assert_eq!(runtime.run_once(1).await.unwrap(), 1);

        assert_eq!(
            queue.terminal_state(&id).await,
            Some(durable_queue::TerminalState::Acked)
        );
    }

    #[tokio::test]
    async fn retry_disposition_allows_safe_reexecution() {
        let queue = WorkQueue::new(QueueConfig {
            lease_ticks: 2,
            max_attempts: 3,
            retry_backoff_ticks: 1,
        });
        queue.enqueue(serde_json::json!({}), 1).await.unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let handler_attempts = attempts.clone();
        let handler = StaticHandler::new(move |_job, _context| {
            let attempts = handler_attempts.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::Relaxed);
                if current == 0 {
                    Ok(JobDisposition::Retry)
                } else {
                    Ok(JobDisposition::Completed)
                }
            }
        });
        let runtime = WorkerRuntime::new(queue, handler, WorkerConfig::default());

        assert_eq!(runtime.run_once(1).await.unwrap(), 1);
        assert_eq!(runtime.run_once(2).await.unwrap(), 1);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }
}
