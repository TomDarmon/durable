//! Deterministic model and fault injection utilities for durable protocols.

use rand::{rngs::StdRng, SeedableRng};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use substrate::{
    compute_object_id, Durability, DurableError, DurableObjectRef, ExpectedRevision,
    ImmutableObjects, ObjectFormat, ObjectId, PublishOutcome, Result, RootName, RootRegister,
    RootState, ScopeId, StorageScope,
};

/// Deterministic fault point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultPoint {
    /// Before object persistence.
    BeforeObjectPersistence,
    /// After object persistence before ACK.
    AfterObjectPersistenceBeforeAck,
    /// Before root condition evaluation.
    BeforeRootConditionEvaluation,
    /// After root commit before ACK.
    AfterRootCommitBeforeAck,
    /// After journal page persistence before head CAS.
    AfterJournalPagePersistenceBeforeHeadCas,
    /// After journal head CAS before ACK.
    AfterJournalHeadCasBeforeAck,
    /// During cache fill.
    DuringCacheFill,
    /// During queue ACK.
    DuringQueueAck,
    /// During worker heartbeat.
    DuringWorkerHeartbeat,
}

/// Deterministic fault action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultAction {
    /// Return unavailable.
    ReturnUnavailable,
    /// Return outcome unknown.
    ReturnOutcomeUnknown,
    /// Drop the response.
    DropResponse,
    /// Pause at a deterministic barrier.
    Pause,
    /// Simulate a process crash.
    SimulateProcessCrash,
    /// Corrupt a cached read.
    CorruptCachedRead,
}

/// Reproducible command trace for randomized tests.
#[derive(Debug, Clone)]
pub struct CommandTrace {
    /// Seed used for the run.
    pub seed: u64,
    /// Commands executed by the model.
    pub commands: Vec<String>,
}

/// Deterministic fault injector.
#[derive(Debug)]
pub struct FaultInjector {
    faults: Mutex<BTreeMap<FaultPoint, VecDeque<FaultAction>>>,
}

impl FaultInjector {
    /// Creates an empty injector.
    pub fn new() -> Self {
        Self {
            faults: Mutex::new(BTreeMap::new()),
        }
    }

    /// Schedules a fault action.
    pub fn push(&self, point: FaultPoint, action: FaultAction) {
        self.faults
            .lock()
            .expect("fault injector mutex poisoned")
            .entry(point)
            .or_default()
            .push_back(action);
    }

    /// Takes the next action for a point, if any.
    pub fn take(&self, point: FaultPoint) -> Option<FaultAction> {
        self.faults
            .lock()
            .expect("fault injector mutex poisoned")
            .get_mut(&point)
            .and_then(VecDeque::pop_front)
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

/// Synchronous deterministic reference model.
#[derive(Debug)]
pub struct DeterministicModel {
    scope: StorageScope,
    objects: BTreeMap<substrate::ObjectId, Vec<u8>>,
    roots: BTreeMap<RootName, RootState>,
    trace: CommandTrace,
    faults: Arc<FaultInjector>,
    _rng: StdRng,
}

/// Reference type used only by the synchronous deterministic model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelObjectRef {
    scope_id: ScopeId,
    object_id: ObjectId,
    format: ObjectFormat,
    size: u64,
}

impl ModelObjectRef {
    /// Returns the model scope identity.
    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    /// Returns the model object ID.
    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Returns the model object format.
    pub fn format(&self) -> &ObjectFormat {
        &self.format
    }

    /// Returns the model object size.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl DeterministicModel {
    /// Creates a model with a reproducible seed.
    pub fn new(scope: StorageScope, seed: u64, faults: Arc<FaultInjector>) -> Self {
        Self {
            scope,
            objects: BTreeMap::new(),
            roots: BTreeMap::new(),
            trace: CommandTrace {
                seed,
                commands: Vec::new(),
            },
            faults,
            _rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Returns the private scope ID.
    pub fn scope_id(&self) -> ScopeId {
        self.scope.scope_id()
    }

    /// Returns a reproducible trace.
    pub fn trace(&self) -> &CommandTrace {
        &self.trace
    }

    /// Synchronously puts an immutable object.
    pub fn put(
        &mut self,
        id: ObjectId,
        format: ObjectFormat,
        bytes: &[u8],
    ) -> Result<ModelObjectRef> {
        self.trace.commands.push(format!("put {}", id.to_hex()));
        if matches!(
            self.faults.take(FaultPoint::BeforeObjectPersistence),
            Some(FaultAction::ReturnUnavailable)
        ) {
            return Err(DurableError::Unavailable(
                "fault before object persistence".into(),
            ));
        }
        if compute_object_id(&format, bytes) != id {
            return Err(DurableError::ObjectHashMismatch);
        }
        if let Some(existing) = self.objects.get(&id) {
            if existing != bytes {
                return Err(DurableError::Corrupt(
                    "different bytes under same ID".into(),
                ));
            }
        } else {
            self.objects.insert(id, bytes.to_vec());
        }
        if matches!(
            self.faults
                .take(FaultPoint::AfterObjectPersistenceBeforeAck),
            Some(FaultAction::ReturnUnavailable | FaultAction::DropResponse)
        ) {
            return Err(DurableError::Unavailable(
                "fault after object persistence".into(),
            ));
        }
        Ok(model_ref(self.scope_id(), id, format, bytes.len() as u64))
    }

    /// Synchronously reads an immutable object.
    pub fn read(&self, reference: &ModelObjectRef) -> Result<Vec<u8>> {
        if reference.scope_id() != self.scope_id() {
            return Err(DurableError::ScopeMismatch);
        }
        self.objects
            .get(&reference.object_id())
            .cloned()
            .ok_or_else(|| DurableError::Missing("model object".into()))
    }

    /// Synchronously publishes a root.
    pub fn compare_exchange(
        &mut self,
        name: &RootName,
        expected: ExpectedRevision,
        value: Vec<u8>,
    ) -> Result<PublishOutcome> {
        self.trace
            .commands
            .push(format!("cas-root {}", name.as_str()));
        if matches!(
            self.faults.take(FaultPoint::BeforeRootConditionEvaluation),
            Some(FaultAction::ReturnUnavailable)
        ) {
            return Err(DurableError::Unavailable(
                "fault before root condition".into(),
            ));
        }
        let current = self.roots.get(name);
        let matches = match (expected, current) {
            (ExpectedRevision::Missing, None) => true,
            (ExpectedRevision::Missing, Some(_)) => false,
            (ExpectedRevision::Exact(revision), Some(state)) => revision == state.revision(),
            (ExpectedRevision::Exact(_), None) => false,
        };
        if !matches {
            return Ok(PublishOutcome::Conflict {
                current: current.cloned(),
            });
        }
        let next = RootState::new(substrate::Revision::fresh(), value)?;
        self.roots.insert(name.clone(), next.clone());
        if matches!(
            self.faults.take(FaultPoint::AfterRootCommitBeforeAck),
            Some(FaultAction::ReturnOutcomeUnknown | FaultAction::DropResponse)
        ) {
            return Ok(PublishOutcome::OutcomeUnknown);
        }
        Ok(PublishOutcome::Applied(next))
    }
}

fn model_ref(scope_id: ScopeId, id: ObjectId, format: ObjectFormat, size: u64) -> ModelObjectRef {
    ModelObjectRef {
        scope_id,
        object_id: id,
        format,
        size,
    }
}

/// Async bridge used by tests that need public durable references.
pub async fn put_with_core_handle<S>(
    storage: &S,
    format: ObjectFormat,
    bytes: &[u8],
) -> Result<DurableObjectRef>
where
    S: ImmutableObjects,
{
    let id = compute_object_id(&format, bytes);
    storage
        .put(id, format, bytes, Durability::BackendDefault)
        .await
}

/// Root fault wrapper for production handles.
pub struct FaultyRootRegister<R> {
    inner: R,
    faults: Arc<FaultInjector>,
}

impl<R> FaultyRootRegister<R> {
    /// Creates a root fault wrapper.
    pub fn new(inner: R, faults: Arc<FaultInjector>) -> Self {
        Self { inner, faults }
    }
}

#[async_trait::async_trait]
impl<R> RootRegister for FaultyRootRegister<R>
where
    R: RootRegister + Send + Sync,
{
    async fn read(&self, name: &RootName) -> Result<Option<RootState>> {
        self.inner.read(name).await
    }

    async fn compare_exchange(
        &self,
        name: &RootName,
        expected: ExpectedRevision,
        value: Vec<u8>,
    ) -> Result<PublishOutcome> {
        if matches!(
            self.faults.take(FaultPoint::BeforeRootConditionEvaluation),
            Some(FaultAction::ReturnUnavailable)
        ) {
            return Err(DurableError::Unavailable(
                "fault before root condition".into(),
            ));
        }
        let outcome = self.inner.compare_exchange(name, expected, value).await?;
        if matches!(outcome, PublishOutcome::Applied(_))
            && matches!(
                self.faults.take(FaultPoint::AfterRootCommitBeforeAck),
                Some(FaultAction::ReturnOutcomeUnknown | FaultAction::DropResponse)
            )
        {
            return Ok(PublishOutcome::OutcomeUnknown);
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrate::{DatasetId, EncryptionDomainId, StorageScope, TenantId};

    #[test]
    fn trace_includes_seed_and_commands() {
        let faults = Arc::new(FaultInjector::new());
        let mut model = DeterministicModel::new(
            StorageScope::new(
                TenantId::new("tenant"),
                DatasetId::new("dataset"),
                EncryptionDomainId::new("edek"),
            ),
            42,
            faults,
        );
        let root = RootName::new("main");
        let _ = model
            .compare_exchange(&root, ExpectedRevision::Missing, b"value".to_vec())
            .unwrap();

        assert_eq!(model.trace().seed, 42);
        assert_eq!(model.trace().commands, vec!["cas-root main"]);
    }

    #[tokio::test]
    async fn root_commit_ack_loss_returns_unknown_but_state_is_visible() {
        use substrate::{
            DatasetId, EncryptionDomainId, InMemoryBackend, ScopedStorage, StorageScope, TenantId,
        };

        let storage = ScopedStorage::new(
            StorageScope::new(
                TenantId::new("tenant"),
                DatasetId::new("dataset"),
                EncryptionDomainId::new("edek"),
            ),
            Arc::new(InMemoryBackend::new()),
        );
        let faults = Arc::new(FaultInjector::new());
        faults.push(
            FaultPoint::AfterRootCommitBeforeAck,
            FaultAction::ReturnOutcomeUnknown,
        );
        let faulty = FaultyRootRegister::new(storage, faults);
        let root = RootName::new("main");

        let outcome = faulty
            .compare_exchange(&root, ExpectedRevision::Missing, b"value".to_vec())
            .await
            .unwrap();
        let visible = faulty.read(&root).await.unwrap().unwrap();

        assert!(matches!(outcome, PublishOutcome::OutcomeUnknown));
        assert_eq!(visible.value(), b"value");
    }
}
