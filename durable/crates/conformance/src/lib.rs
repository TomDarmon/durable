//! Reusable backend conformance checks.

use std::sync::Arc;
use substrate::{
    compute_object_id, BackendVersion, DatasetId, Durability, DurableError, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, PutObjectResult, RawBackend,
    RawRanges, RootName, RootRegister, RootState, ScopedStorage, StorageScope, TenantId,
};
use thiserror::Error;

/// Conformance result type.
pub type Result<T> = std::result::Result<T, ConformanceError>;

/// Conformance errors.
#[derive(Debug, Error)]
pub enum ConformanceError {
    /// Durable storage failed.
    #[error(transparent)]
    Durable(#[from] DurableError),
    /// A required property failed.
    #[error("conformance failure: {0}")]
    Failed(String),
}

/// Successful conformance probe report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceReport {
    /// Create-if-absent was atomic.
    pub create_if_absent: bool,
    /// Stale root versions are rejected.
    pub stale_version_rejected: bool,
    /// Racing CAS has one winner.
    pub racing_cas_single_winner: bool,
    /// Logical revisions are fresh.
    pub fresh_logical_revisions: bool,
    /// Exact-key reads observe writes.
    pub read_after_write: bool,
    /// Range reads are exact.
    pub range_reads: bool,
    /// Missing/precondition errors are distinguishable.
    pub distinguishable_failures: bool,
}

/// Runs core backend conformance checks.
pub async fn run_backend_conformance<B>(backend: Arc<B>) -> Result<ConformanceReport>
where
    B: RawBackend + 'static,
{
    let scope = StorageScope::new(
        TenantId::new("conformance-tenant"),
        DatasetId::new(uuid::Uuid::new_v4().to_string()),
        EncryptionDomainId::new("edek"),
    );
    let storage = Arc::new(ScopedStorage::new(scope, backend.clone()));
    let bytes = b"0123456789abcdef";
    let object_id = compute_object_id(&ObjectFormat::Raw, bytes);
    let reference = storage
        .put(
            object_id,
            ObjectFormat::Raw,
            bytes,
            Durability::BackendDefault,
        )
        .await?;
    let second_put = backend
        .put_object_if_absent(storage.scope_id(), object_id, b"different")
        .await?;
    if second_put != PutObjectResult::AlreadyExists {
        return Err(ConformanceError::Failed(
            "create-if-absent allowed duplicate object".into(),
        ));
    }
    if ImmutableObjects::read(storage.as_ref(), &reference).await? != bytes {
        return Err(ConformanceError::Failed("read-after-write failed".into()));
    }
    if storage.read_range(&reference, 3..7).await? != b"3456" {
        return Err(ConformanceError::Failed("range read failed".into()));
    }

    let missing = backend
        .read_object(
            storage.scope_id(),
            compute_object_id(&ObjectFormat::Raw, b"missing"),
        )
        .await
        .unwrap_err();
    if !matches!(missing, DurableError::Missing(_)) {
        return Err(ConformanceError::Failed(
            "missing object was not distinct".into(),
        ));
    }

    let stale_root_rejected = !backend
        .compare_exchange_root(
            storage.scope_id(),
            &RootName::new("stale"),
            Some(&BackendVersion("stale".into())),
            &RootState::new(substrate::Revision::fresh(), b"x".to_vec())?,
        )
        .await?;
    if !stale_root_rejected {
        return Err(ConformanceError::Failed(
            "stale version was accepted".into(),
        ));
    }

    let race_root = RootName::new("race");
    let first = storage.compare_exchange(&race_root, ExpectedRevision::Missing, b"a".to_vec());
    let second = storage.compare_exchange(&race_root, ExpectedRevision::Missing, b"b".to_vec());
    let (first, second) = tokio::join!(first, second);
    let winners = [first?, second?]
        .iter()
        .filter(|outcome| matches!(outcome, PublishOutcome::Applied(_)))
        .count();
    if winners != 1 {
        return Err(ConformanceError::Failed(format!(
            "expected one CAS winner, got {winners}"
        )));
    }

    let same_root = RootName::new("same-bytes");
    let root_a = match storage
        .compare_exchange(&same_root, ExpectedRevision::Missing, b"same".to_vec())
        .await?
    {
        PublishOutcome::Applied(state) => state,
        other => {
            return Err(ConformanceError::Failed(format!(
                "first root publish failed: {other:?}"
            )));
        }
    };
    let root_b = match storage
        .compare_exchange(
            &same_root,
            ExpectedRevision::Exact(root_a.revision()),
            b"same".to_vec(),
        )
        .await?
    {
        PublishOutcome::Applied(state) => state,
        other => {
            return Err(ConformanceError::Failed(format!(
                "second root publish failed: {other:?}"
            )));
        }
    };
    if root_a.revision() == root_b.revision() {
        return Err(ConformanceError::Failed(
            "same logical bytes reused a revision".into(),
        ));
    }

    Ok(ConformanceReport {
        create_if_absent: true,
        stale_version_rejected: true,
        racing_cas_single_winner: true,
        fresh_logical_revisions: true,
        read_after_write: true,
        range_reads: true,
        distinguishable_failures: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrate::InMemoryBackend;

    #[tokio::test]
    async fn in_memory_backend_conforms() {
        let report = run_backend_conformance(Arc::new(InMemoryBackend::new()))
            .await
            .unwrap();

        assert!(report.create_if_absent);
        assert!(report.racing_cas_single_winner);
    }
}
