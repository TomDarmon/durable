//! Safe retention epochs with physical deletion disabled.

use durable_core::ScopeId;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use uuid::Uuid;

/// Retention result type.
pub type Result<T> = std::result::Result<T, RetentionError>;

/// Retention errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RetentionError {
    /// Epoch token was not found.
    #[error("retention epoch is missing")]
    MissingEpoch,
    /// Epoch token belongs to another scope.
    #[error("retention epoch belongs to another scope")]
    ScopeMismatch,
    /// Physical deletion is disabled in iteration 1.
    #[error("physical deletion is disabled")]
    PhysicalDeletionDisabled,
}

/// Reader epoch token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderEpoch {
    id: Uuid,
    scope_id: ScopeId,
}

impl ReaderEpoch {
    /// Returns the epoch scope.
    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
}

/// Scope-bound retention epoch manager.
#[derive(Debug, Clone)]
pub struct RetentionEpochs {
    scope_id: ScopeId,
    state: Arc<Mutex<RetentionState>>,
}

#[derive(Debug, Default)]
struct RetentionState {
    epochs: BTreeMap<Uuid, EpochState>,
}

#[derive(Debug, Clone)]
struct EpochState {
    expires_at_tick: u64,
}

impl RetentionEpochs {
    /// Creates a scope-bound manager.
    pub fn new(scope_id: ScopeId) -> Self {
        Self {
            scope_id,
            state: Arc::new(Mutex::new(RetentionState::default())),
        }
    }

    /// Enters a reader epoch before reading a root.
    pub fn enter(&self, now_tick: u64, ttl_ticks: u64) -> ReaderEpoch {
        let epoch = ReaderEpoch {
            id: Uuid::new_v4(),
            scope_id: self.scope_id,
        };
        self.state
            .lock()
            .expect("retention mutex poisoned")
            .epochs
            .insert(
                epoch.id,
                EpochState {
                    expires_at_tick: now_tick.saturating_add(ttl_ticks),
                },
            );
        epoch
    }

    /// Renews a reader epoch.
    pub fn renew(&self, epoch: &ReaderEpoch, now_tick: u64, ttl_ticks: u64) -> Result<()> {
        self.check_scope(epoch)?;
        let mut state = self.state.lock().expect("retention mutex poisoned");
        let existing = state
            .epochs
            .get_mut(&epoch.id)
            .ok_or(RetentionError::MissingEpoch)?;
        existing.expires_at_tick = now_tick.saturating_add(ttl_ticks);
        Ok(())
    }

    /// Releases a reader epoch.
    pub fn release(&self, epoch: &ReaderEpoch) -> Result<()> {
        self.check_scope(epoch)?;
        self.state
            .lock()
            .expect("retention mutex poisoned")
            .epochs
            .remove(&epoch.id)
            .ok_or(RetentionError::MissingEpoch)?;
        Ok(())
    }

    /// Returns the first tick no longer protected by live epochs.
    pub fn safe_before(&self, now_tick: u64) -> u64 {
        let mut state = self.state.lock().expect("retention mutex poisoned");
        state
            .epochs
            .retain(|_, epoch| epoch.expires_at_tick >= now_tick);
        state
            .epochs
            .values()
            .map(|epoch| epoch.expires_at_tick)
            .min()
            .unwrap_or(now_tick)
    }

    /// Always rejects physical deletion in iteration 1.
    pub fn require_delete_proof(&self) -> Result<()> {
        Err(RetentionError::PhysicalDeletionDisabled)
    }

    fn check_scope(&self, epoch: &ReaderEpoch) -> Result<()> {
        if epoch.scope_id != self.scope_id {
            Err(RetentionError::ScopeMismatch)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_core::{DatasetId, EncryptionDomainId, StorageScope, TenantId};

    fn scope_id(name: &str) -> ScopeId {
        StorageScope::new(
            TenantId::new(name),
            DatasetId::new("dataset"),
            EncryptionDomainId::new("edek"),
        )
        .scope_id()
    }

    #[test]
    fn reader_epoch_can_be_renewed_and_released() {
        let retention = RetentionEpochs::new(scope_id("a"));
        let epoch = retention.enter(10, 5);

        assert_eq!(retention.safe_before(11), 15);
        retention.renew(&epoch, 12, 10).unwrap();
        assert_eq!(retention.safe_before(13), 22);
        retention.release(&epoch).unwrap();
        assert_eq!(retention.safe_before(13), 13);
    }

    #[test]
    fn physical_delete_is_disabled() {
        let retention = RetentionEpochs::new(scope_id("a"));
        assert_eq!(
            retention.require_delete_proof().unwrap_err(),
            RetentionError::PhysicalDeletionDisabled
        );
    }
}
