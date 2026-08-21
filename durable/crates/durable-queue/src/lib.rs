//! At-least-once work queue with fenced claim tokens.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use thiserror::Error;
use uuid::Uuid;

/// Queue result type.
pub type Result<T> = std::result::Result<T, QueueError>;

/// Queue error taxonomy.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueueError {
    /// The job is missing.
    #[error("missing queue job")]
    Missing,
    /// A stale claim token attempted to mutate a reassigned job.
    #[error("stale claim token")]
    Fenced,
    /// The job can no longer be retried.
    #[error("job reached maximum attempts")]
    MaxAttempts,
    /// Queue state was unavailable.
    #[error("queue unavailable: {0}")]
    Unavailable(String),
}

/// Queue job identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JobId(Uuid);

impl JobId {
    /// Returns the UUID backing this job ID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// Fenced claim token with a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimToken {
    job_id: JobId,
    generation: u64,
    worker_id: String,
}

impl ClaimToken {
    /// Returns the claimed job ID.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Returns the claim generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Claimed job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedJob {
    /// Job ID.
    pub id: JobId,
    /// Payload.
    pub payload: Value,
    /// Attempt number starting at one.
    pub attempt: u32,
    /// Fenced claim token.
    pub token: ClaimToken,
}

/// Terminal job state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TerminalState {
    /// Job completed.
    Acked,
    /// Job permanently failed.
    DeadLetter,
}

/// Queue configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    /// Lease duration in logical ticks.
    pub lease_ticks: u64,
    /// Maximum attempts before dead-letter.
    pub max_attempts: u32,
    /// Retry backoff in logical ticks.
    pub retry_backoff_ticks: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            lease_ticks: 10,
            max_attempts: 3,
            retry_backoff_ticks: 1,
        }
    }
}

/// Reusable queue API.
#[derive(Debug, Clone)]
pub struct WorkQueue {
    config: QueueConfig,
    state: Arc<Mutex<QueueState>>,
}

#[derive(Debug, Default)]
struct QueueState {
    ready: VecDeque<JobId>,
    jobs: BTreeMap<JobId, JobState>,
}

#[derive(Debug, Clone)]
struct JobState {
    payload: Value,
    attempts: u32,
    next_visible_tick: u64,
    claim: Option<ActiveClaim>,
    terminal: Option<TerminalState>,
}

#[derive(Debug, Clone)]
struct ActiveClaim {
    token: ClaimToken,
    lease_deadline_tick: u64,
}

impl WorkQueue {
    /// Creates an empty queue.
    pub fn new(config: QueueConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(QueueState::default())),
        }
    }

    /// Enqueues a job.
    pub async fn enqueue(&self, payload: Value, now_tick: u64) -> Result<JobId> {
        let id = JobId(Uuid::new_v4());
        let mut state = self.state.lock().expect("queue mutex poisoned");
        state.jobs.insert(
            id.clone(),
            JobState {
                payload,
                attempts: 0,
                next_visible_tick: now_tick,
                claim: None,
                terminal: None,
            },
        );
        state.ready.push_back(id.clone());
        Ok(id)
    }

    /// Claims the next visible job.
    pub async fn claim(
        &self,
        worker_id: impl Into<String>,
        now_tick: u64,
    ) -> Result<Option<ClaimedJob>> {
        let worker_id = worker_id.into();
        let mut state = self.state.lock().expect("queue mutex poisoned");
        requeue_expired(&mut state, now_tick);

        let mut skipped = VecDeque::new();
        let claimed = loop {
            let Some(id) = state.ready.pop_front() else {
                break None;
            };
            let Some(job) = state.jobs.get_mut(&id) else {
                continue;
            };
            if job.terminal.is_some() || job.next_visible_tick > now_tick || job.claim.is_some() {
                skipped.push_back(id);
                continue;
            }
            if job.attempts >= self.config.max_attempts {
                job.terminal = Some(TerminalState::DeadLetter);
                continue;
            }
            job.attempts = job.attempts.saturating_add(1);
            let token = ClaimToken {
                job_id: id.clone(),
                generation: job.attempts as u64,
                worker_id: worker_id.clone(),
            };
            job.claim = Some(ActiveClaim {
                token: token.clone(),
                lease_deadline_tick: now_tick.saturating_add(self.config.lease_ticks),
            });
            break Some(ClaimedJob {
                id,
                payload: job.payload.clone(),
                attempt: job.attempts,
                token,
            });
        };
        state.ready.extend(skipped);
        Ok(claimed)
    }

    /// Renews a live claim.
    pub async fn heartbeat(&self, token: &ClaimToken, now_tick: u64) -> Result<()> {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        let job = state
            .jobs
            .get_mut(&token.job_id)
            .ok_or(QueueError::Missing)?;
        let claim = job.claim.as_mut().ok_or(QueueError::Fenced)?;
        if claim.token != *token {
            return Err(QueueError::Fenced);
        }
        claim.lease_deadline_tick = now_tick.saturating_add(self.config.lease_ticks);
        Ok(())
    }

    /// Acknowledges a live claim.
    pub async fn ack(&self, token: &ClaimToken) -> Result<()> {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        let job = state
            .jobs
            .get_mut(&token.job_id)
            .ok_or(QueueError::Missing)?;
        let claim = job.claim.as_ref().ok_or(QueueError::Fenced)?;
        if claim.token != *token {
            return Err(QueueError::Fenced);
        }
        job.claim = None;
        job.terminal = Some(TerminalState::Acked);
        Ok(())
    }

    /// Releases a job for retry or dead-letter.
    pub async fn retry(&self, token: &ClaimToken, now_tick: u64) -> Result<()> {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        let job = state
            .jobs
            .get_mut(&token.job_id)
            .ok_or(QueueError::Missing)?;
        let claim = job.claim.as_ref().ok_or(QueueError::Fenced)?;
        if claim.token != *token {
            return Err(QueueError::Fenced);
        }
        job.claim = None;
        if job.attempts >= self.config.max_attempts {
            job.terminal = Some(TerminalState::DeadLetter);
            return Err(QueueError::MaxAttempts);
        }
        job.next_visible_tick = now_tick.saturating_add(self.config.retry_backoff_ticks);
        state.ready.push_back(token.job_id.clone());
        Ok(())
    }

    /// Marks a job permanently failed.
    pub async fn permanent_failure(&self, token: &ClaimToken) -> Result<()> {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        let job = state
            .jobs
            .get_mut(&token.job_id)
            .ok_or(QueueError::Missing)?;
        let claim = job.claim.as_ref().ok_or(QueueError::Fenced)?;
        if claim.token != *token {
            return Err(QueueError::Fenced);
        }
        job.claim = None;
        job.terminal = Some(TerminalState::DeadLetter);
        Ok(())
    }

    /// Returns terminal state for diagnostics.
    pub async fn terminal_state(&self, job_id: &JobId) -> Option<TerminalState> {
        self.state
            .lock()
            .expect("queue mutex poisoned")
            .jobs
            .get(job_id)
            .and_then(|job| job.terminal.clone())
    }

    /// Reconciles expired leases.
    pub async fn reconcile(&self, now_tick: u64) {
        let mut state = self.state.lock().expect("queue mutex poisoned");
        requeue_expired(&mut state, now_tick);
    }
}

fn requeue_expired(state: &mut QueueState, now_tick: u64) {
    let expired: Vec<JobId> = state
        .jobs
        .iter_mut()
        .filter_map(|(id, job)| {
            let claim = job.claim.as_ref()?;
            if job.terminal.is_none() && claim.lease_deadline_tick < now_tick {
                job.claim = None;
                Some(id.clone())
            } else {
                None
            }
        })
        .collect();
    state.ready.extend(expired);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lease_expiry_allows_another_worker_to_reclaim() {
        let queue = WorkQueue::new(QueueConfig {
            lease_ticks: 2,
            max_attempts: 3,
            retry_backoff_ticks: 1,
        });
        queue
            .enqueue(serde_json::json!({"work": true}), 1)
            .await
            .unwrap();
        let first = queue.claim("worker-a", 1).await.unwrap().unwrap();

        queue.reconcile(4).await;
        let second = queue.claim("worker-b", 4).await.unwrap().unwrap();

        assert_eq!(first.id, second.id);
        assert_ne!(first.token, second.token);
        assert_eq!(second.attempt, 2);
    }

    #[tokio::test]
    async fn old_worker_heartbeat_and_ack_are_rejected() {
        let queue = WorkQueue::new(QueueConfig {
            lease_ticks: 2,
            max_attempts: 3,
            retry_backoff_ticks: 1,
        });
        queue.enqueue(serde_json::json!({}), 1).await.unwrap();
        let first = queue.claim("worker-a", 1).await.unwrap().unwrap();
        queue.reconcile(4).await;
        let second = queue.claim("worker-b", 4).await.unwrap().unwrap();

        assert_eq!(
            queue.heartbeat(&first.token, 4).await.unwrap_err(),
            QueueError::Fenced
        );
        assert_eq!(
            queue.ack(&first.token).await.unwrap_err(),
            QueueError::Fenced
        );
        queue.ack(&second.token).await.unwrap();
    }

    #[tokio::test]
    async fn ack_loss_causes_safe_reexecution_after_lease_expiry() {
        let queue = WorkQueue::new(QueueConfig {
            lease_ticks: 2,
            max_attempts: 3,
            retry_backoff_ticks: 1,
        });
        queue
            .enqueue(serde_json::json!({"idempotent": true}), 1)
            .await
            .unwrap();
        let first = queue.claim("worker-a", 1).await.unwrap().unwrap();

        queue.reconcile(4).await;
        let second = queue.claim("worker-b", 4).await.unwrap().unwrap();

        assert_eq!(first.id, second.id);
        assert_eq!(second.attempt, 2);
        queue.ack(&second.token).await.unwrap();
    }

    #[tokio::test]
    async fn maximum_attempts_dead_letters_job() {
        let queue = WorkQueue::new(QueueConfig {
            lease_ticks: 2,
            max_attempts: 1,
            retry_backoff_ticks: 1,
        });
        let id = queue.enqueue(serde_json::json!({}), 1).await.unwrap();
        let claimed = queue.claim("worker", 1).await.unwrap().unwrap();

        assert_eq!(
            queue.retry(&claimed.token, 1).await.unwrap_err(),
            QueueError::MaxAttempts
        );
        assert_eq!(
            queue.terminal_state(&id).await,
            Some(TerminalState::DeadLetter)
        );
    }
}
