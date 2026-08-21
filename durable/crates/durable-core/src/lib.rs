//! Core durable storage contracts.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    ops::Range,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use uuid::Uuid;

/// Core result type.
pub type Result<T> = std::result::Result<T, DurableError>;

/// Distinct tenant identity used to derive private storage scopes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TenantId(String);

impl TenantId {
    /// Creates a tenant identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable textual identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Distinct dataset identity used to derive private storage scopes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DatasetId(String);

impl DatasetId {
    /// Creates a dataset identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable textual identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Distinct encryption-domain identity used to fence durable references.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EncryptionDomainId(String);

impl EncryptionDomainId {
    /// Creates an encryption-domain identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable textual identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// User-visible storage scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageScope {
    tenant: TenantId,
    dataset: DatasetId,
    encryption_domain: EncryptionDomainId,
}

impl StorageScope {
    /// Creates a scope from distinct identity components.
    pub fn new(
        tenant: TenantId,
        dataset: DatasetId,
        encryption_domain: EncryptionDomainId,
    ) -> Self {
        Self {
            tenant,
            dataset,
            encryption_domain,
        }
    }

    /// Returns the tenant component.
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Returns the dataset component.
    pub fn dataset(&self) -> &DatasetId {
        &self.dataset
    }

    /// Returns the encryption-domain component.
    pub fn encryption_domain(&self) -> &EncryptionDomainId {
        &self.encryption_domain
    }

    /// Returns the private scope identity used in durable keys and references.
    pub fn scope_id(&self) -> ScopeId {
        let mut hasher = Sha256::new();
        hasher.update(b"durable.scope.v1\0");
        hasher.update(self.tenant.as_str().as_bytes());
        hasher.update(b"\0");
        hasher.update(self.dataset.as_str().as_bytes());
        hasher.update(b"\0");
        hasher.update(self.encryption_domain.as_str().as_bytes());
        ScopeId(hasher.finalize().into())
    }
}

/// Private, stable identity for a full storage scope.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopeId([u8; 32]);

impl ScopeId {
    /// Returns the hexadecimal representation used in backend keys.
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    /// Returns the raw identity bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ScopeId").field(&self.to_hex()).finish()
    }
}

/// Fixed 32-byte substrate object identity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectId([u8; 32]);

impl ObjectId {
    /// Creates an object ID from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns raw ID bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the hexadecimal object ID.
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ObjectId").field(&self.to_hex()).finish()
    }
}

/// Canonical object format domain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ObjectFormat {
    /// Unstructured bytes.
    Raw,
    /// Canonical JSON bytes.
    Json,
    /// Durable journal page bytes.
    JournalPage,
    /// Explicit future format.
    Custom(String),
}

impl ObjectFormat {
    /// Returns the domain separator used in object hashing.
    pub fn domain_separator(&self) -> Vec<u8> {
        match self {
            Self::Raw => b"durable.object.raw.v1\0".to_vec(),
            Self::Json => b"durable.object.json.v1\0".to_vec(),
            Self::JournalPage => b"durable.object.journal-page.v1\0".to_vec(),
            Self::Custom(value) => {
                let mut out = b"durable.object.custom.v1\0".to_vec();
                out.extend_from_slice(value.as_bytes());
                out.push(0);
                out
            }
        }
    }
}

/// Computes the canonical substrate object ID for format and bytes.
pub fn compute_object_id(format: &ObjectFormat, canonical_bytes: &[u8]) -> ObjectId {
    let mut hasher = Sha256::new();
    hasher.update(format.domain_separator());
    hasher.update(canonical_bytes);
    ObjectId(hasher.finalize().into())
}

/// Durability domain identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DurabilityDomainId(String);

impl DurabilityDomainId {
    /// Creates a durability domain.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Durability requirement for writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Durability {
    /// Use the backend default durability.
    BackendDefault,
    /// Require a specific durability domain.
    Required(DurabilityDomainId),
}

/// Scope-bound durable reference created after verified persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableObjectRef {
    scope_id: ScopeId,
    object_id: ObjectId,
    format: ObjectFormat,
    size: u64,
}

impl DurableObjectRef {
    fn verified(scope_id: ScopeId, object_id: ObjectId, format: ObjectFormat, size: u64) -> Self {
        Self {
            scope_id,
            object_id,
            format,
            size,
        }
    }

    /// Returns the scope identity that fences the reference.
    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    /// Returns the object identity.
    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Returns the object format.
    pub fn format(&self) -> &ObjectFormat {
        &self.format
    }

    /// Returns the verified object size.
    pub fn size(&self) -> u64 {
        self.size
    }
}

/// Name of a small mutable root inside one scope.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RootName(String);

impl RootName {
    /// Creates a root name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the root name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque logical root revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Revision(Uuid);

impl Revision {
    /// Creates a fresh logical revision.
    pub fn fresh() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the UUID backing this logical revision.
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Current root state returned by root reads and publications.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootState {
    revision: Revision,
    value: Vec<u8>,
}

impl RootState {
    /// Creates a root state.
    pub fn new(revision: Revision, value: Vec<u8>) -> Result<Self> {
        if value.len() > MAX_ROOT_VALUE_BYTES {
            return Err(DurableError::RootTooLarge {
                size: value.len(),
                max: MAX_ROOT_VALUE_BYTES,
            });
        }
        Ok(Self { revision, value })
    }

    /// Returns the logical revision.
    pub fn revision(&self) -> Revision {
        self.revision
    }

    /// Returns the small opaque root value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Expected revision for a root compare-and-swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    /// The root must not exist.
    Missing,
    /// The root must have exactly this logical revision.
    Exact(Revision),
}

/// Result of root publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The publication linearized and returned the new state.
    Applied(RootState),
    /// The expected revision did not match. Current state is included when known.
    Conflict { current: Option<RootState> },
    /// The backend may or may not have applied the publication.
    OutcomeUnknown,
}

/// Maximum root value size in iteration 1.
pub const MAX_ROOT_VALUE_BYTES: usize = 4 * 1024;

/// Core error taxonomy.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DurableError {
    /// The requested durable item is missing.
    #[error("missing durable item: {0}")]
    Missing(String),
    /// A stored value or caller-supplied value is corrupt.
    #[error("corrupt durable item: {0}")]
    Corrupt(String),
    /// The backend is unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    /// A compare-and-swap or create-if-absent precondition failed.
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
    /// A reference was used with the wrong scope.
    #[error("cross-scope durable reference rejected")]
    ScopeMismatch,
    /// Caller supplied bytes do not match the object ID.
    #[error("object hash mismatch")]
    ObjectHashMismatch,
    /// Root value exceeded the supported size.
    #[error("root value is too large: {size} > {max}")]
    RootTooLarge { size: usize, max: usize },
}

/// Result of a create-if-absent immutable object write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutObjectResult {
    /// The object was newly written.
    Written,
    /// The key already existed.
    AlreadyExists,
}

/// Version token used internally by root backends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendVersion(pub String);

/// Stored root envelope plus private backend version token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedRoot {
    /// Encoded root state.
    pub state: RootState,
    /// Private backend CAS token.
    pub version: BackendVersion,
}

/// Raw backend contract used by the core handles.
#[async_trait]
pub trait RawBackend: Send + Sync {
    /// Creates an object only when absent.
    async fn put_object_if_absent(
        &self,
        scope_id: ScopeId,
        object_id: ObjectId,
        bytes: &[u8],
    ) -> Result<PutObjectResult>;

    /// Reads a full object.
    async fn read_object(&self, scope_id: ScopeId, object_id: ObjectId) -> Result<Vec<u8>>;

    /// Reads an exact object byte range.
    async fn read_object_range(
        &self,
        scope_id: ScopeId,
        object_id: ObjectId,
        range: Range<u64>,
    ) -> Result<Vec<u8>>;

    /// Reads a root and private CAS token.
    async fn read_root(&self, scope_id: ScopeId, name: &RootName) -> Result<Option<VersionedRoot>>;

    /// Conditionally writes a root envelope.
    async fn compare_exchange_root(
        &self,
        scope_id: ScopeId,
        name: &RootName,
        expected: Option<&BackendVersion>,
        state: &RootState,
    ) -> Result<bool>;
}

/// Immutable object API.
#[async_trait]
pub trait ImmutableObjects: Send + Sync {
    /// Verifies and persists canonical bytes.
    async fn put(
        &self,
        id: ObjectId,
        format: ObjectFormat,
        canonical_bytes: &[u8],
        durability: Durability,
    ) -> Result<DurableObjectRef>;

    /// Reads the full object addressed by a verified reference.
    async fn read(&self, reference: &DurableObjectRef) -> Result<Vec<u8>>;
}

/// Raw range API for immutable objects.
#[async_trait]
pub trait RawRanges: Send + Sync {
    /// Reads an exact byte range from a verified reference.
    async fn read_range(&self, reference: &DurableObjectRef, range: Range<u64>) -> Result<Vec<u8>>;
}

/// Root register API.
#[async_trait]
pub trait RootRegister: Send + Sync {
    /// Reads the current root.
    async fn read(&self, name: &RootName) -> Result<Option<RootState>>;

    /// Publishes a new root value by compare-and-swap.
    async fn compare_exchange(
        &self,
        name: &RootName,
        expected: ExpectedRevision,
        value: Vec<u8>,
    ) -> Result<PublishOutcome>;
}

/// Scope-bound storage handle over a raw backend.
pub struct ScopedStorage<B> {
    scope: StorageScope,
    backend: Arc<B>,
}

impl<B> Clone for ScopedStorage<B> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            backend: self.backend.clone(),
        }
    }
}

impl<B> ScopedStorage<B> {
    /// Creates a scope-bound handle.
    pub fn new(scope: StorageScope, backend: Arc<B>) -> Self {
        Self { scope, backend }
    }

    /// Returns the user-visible scope.
    pub fn scope(&self) -> &StorageScope {
        &self.scope
    }

    /// Returns the private scope identity.
    pub fn scope_id(&self) -> ScopeId {
        self.scope.scope_id()
    }
}

#[async_trait]
impl<B> ImmutableObjects for ScopedStorage<B>
where
    B: RawBackend + 'static,
{
    async fn put(
        &self,
        id: ObjectId,
        format: ObjectFormat,
        canonical_bytes: &[u8],
        _durability: Durability,
    ) -> Result<DurableObjectRef> {
        if compute_object_id(&format, canonical_bytes) != id {
            return Err(DurableError::ObjectHashMismatch);
        }

        let scope_id = self.scope_id();
        match self
            .backend
            .put_object_if_absent(scope_id, id, canonical_bytes)
            .await?
        {
            PutObjectResult::Written => {}
            PutObjectResult::AlreadyExists => {
                let existing = self.backend.read_object(scope_id, id).await?;
                if existing != canonical_bytes {
                    return Err(DurableError::Corrupt(format!(
                        "object {} exists with different bytes",
                        id.to_hex()
                    )));
                }
            }
        }

        Ok(DurableObjectRef::verified(
            scope_id,
            id,
            format,
            canonical_bytes.len() as u64,
        ))
    }

    async fn read(&self, reference: &DurableObjectRef) -> Result<Vec<u8>> {
        if reference.scope_id != self.scope_id() {
            return Err(DurableError::ScopeMismatch);
        }
        let bytes = self
            .backend
            .read_object(reference.scope_id, reference.object_id)
            .await?;
        if compute_object_id(&reference.format, &bytes) != reference.object_id {
            return Err(DurableError::Corrupt(format!(
                "object {} failed integrity check",
                reference.object_id.to_hex()
            )));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl<B> RawRanges for ScopedStorage<B>
where
    B: RawBackend + 'static,
{
    async fn read_range(&self, reference: &DurableObjectRef, range: Range<u64>) -> Result<Vec<u8>> {
        if reference.scope_id != self.scope_id() {
            return Err(DurableError::ScopeMismatch);
        }
        if range.start > range.end || range.end > reference.size {
            return Err(DurableError::Missing(
                "object range is outside object".into(),
            ));
        }
        self.backend
            .read_object_range(reference.scope_id, reference.object_id, range)
            .await
    }
}

#[async_trait]
impl<B> RootRegister for ScopedStorage<B>
where
    B: RawBackend + 'static,
{
    async fn read(&self, name: &RootName) -> Result<Option<RootState>> {
        Ok(self
            .backend
            .read_root(self.scope_id(), name)
            .await?
            .map(|root| root.state))
    }

    async fn compare_exchange(
        &self,
        name: &RootName,
        expected: ExpectedRevision,
        value: Vec<u8>,
    ) -> Result<PublishOutcome> {
        let current = self.backend.read_root(self.scope_id(), name).await?;
        let expected_version = match (expected, current.as_ref()) {
            (ExpectedRevision::Missing, None) => None,
            (ExpectedRevision::Missing, Some(root)) => {
                return Ok(PublishOutcome::Conflict {
                    current: Some(root.state.clone()),
                });
            }
            (ExpectedRevision::Exact(revision), Some(root))
                if root.state.revision() == revision =>
            {
                Some(root.version.clone())
            }
            (ExpectedRevision::Exact(_), Some(root)) => {
                return Ok(PublishOutcome::Conflict {
                    current: Some(root.state.clone()),
                });
            }
            (ExpectedRevision::Exact(_), None) => {
                return Ok(PublishOutcome::Conflict { current: None });
            }
        };

        let new_state = RootState::new(Revision::fresh(), value)?;
        let applied = self
            .backend
            .compare_exchange_root(self.scope_id(), name, expected_version.as_ref(), &new_state)
            .await?;

        if applied {
            Ok(PublishOutcome::Applied(new_state))
        } else {
            Ok(PublishOutcome::Conflict {
                current: self
                    .backend
                    .read_root(self.scope_id(), name)
                    .await?
                    .map(|root| root.state),
            })
        }
    }
}

/// In-memory backend for deterministic tests and local models.
#[derive(Debug, Default)]
pub struct InMemoryBackend {
    inner: Mutex<InMemoryState>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    objects: BTreeMap<(ScopeId, ObjectId), Vec<u8>>,
    roots: BTreeMap<(ScopeId, RootName), StoredRoot>,
    next_version: u64,
}

#[derive(Debug, Clone)]
struct StoredRoot {
    state: RootState,
    version: u64,
}

impl InMemoryBackend {
    /// Creates an empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Corrupts an existing object for fault-oriented tests.
    pub fn corrupt_object(&self, scope_id: ScopeId, object_id: ObjectId, bytes: Vec<u8>) {
        self.inner
            .lock()
            .expect("in-memory backend mutex poisoned")
            .objects
            .insert((scope_id, object_id), bytes);
    }
}

#[async_trait]
impl RawBackend for InMemoryBackend {
    async fn put_object_if_absent(
        &self,
        scope_id: ScopeId,
        object_id: ObjectId,
        bytes: &[u8],
    ) -> Result<PutObjectResult> {
        let mut inner = self.inner.lock().expect("in-memory backend mutex poisoned");
        let key = (scope_id, object_id);
        if let std::collections::btree_map::Entry::Vacant(entry) = inner.objects.entry(key) {
            entry.insert(bytes.to_vec());
            Ok(PutObjectResult::Written)
        } else {
            Ok(PutObjectResult::AlreadyExists)
        }
    }

    async fn read_object(&self, scope_id: ScopeId, object_id: ObjectId) -> Result<Vec<u8>> {
        let inner = self.inner.lock().expect("in-memory backend mutex poisoned");
        inner
            .objects
            .get(&(scope_id, object_id))
            .cloned()
            .ok_or_else(|| DurableError::Missing(format!("object {}", object_id.to_hex())))
    }

    async fn read_object_range(
        &self,
        scope_id: ScopeId,
        object_id: ObjectId,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        let bytes = self.read_object(scope_id, object_id).await?;
        let start = usize::try_from(range.start)
            .map_err(|_| DurableError::Missing("range start overflows usize".into()))?;
        let end = usize::try_from(range.end)
            .map_err(|_| DurableError::Missing("range end overflows usize".into()))?;
        bytes
            .get(start..end)
            .map(ToOwned::to_owned)
            .ok_or_else(|| DurableError::Missing("range is outside object".into()))
    }

    async fn read_root(&self, scope_id: ScopeId, name: &RootName) -> Result<Option<VersionedRoot>> {
        let inner = self.inner.lock().expect("in-memory backend mutex poisoned");
        Ok(inner
            .roots
            .get(&(scope_id, name.clone()))
            .map(|stored| VersionedRoot {
                state: stored.state.clone(),
                version: BackendVersion(stored.version.to_string()),
            }))
    }

    async fn compare_exchange_root(
        &self,
        scope_id: ScopeId,
        name: &RootName,
        expected: Option<&BackendVersion>,
        state: &RootState,
    ) -> Result<bool> {
        let mut inner = self.inner.lock().expect("in-memory backend mutex poisoned");
        let key = (scope_id, name.clone());
        let current = inner.roots.get(&key).map(|root| root.version.to_string());
        let matches = match (
            current.as_deref(),
            expected.map(|version| version.0.as_str()),
        ) {
            (None, None) => true,
            (Some(current), Some(expected)) => current == expected,
            _ => false,
        };
        if !matches {
            return Ok(false);
        }
        inner.next_version = inner.next_version.saturating_add(1);
        let version = inner.next_version;
        inner.roots.insert(
            key,
            StoredRoot {
                state: state.clone(),
                version,
            },
        );
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_scope(name: &str) -> StorageScope {
        StorageScope::new(
            TenantId::new(format!("tenant-{name}")),
            DatasetId::new("dataset"),
            EncryptionDomainId::new("edek"),
        )
    }

    #[tokio::test]
    async fn identical_object_put_is_idempotent() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend);
        let bytes = b"hello durable";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);

        let first = store
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        let second = store
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();

        assert_eq!(first.object_id(), second.object_id());
        assert_eq!(ImmutableObjects::read(&store, &first).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn object_hash_mismatch_is_rejected() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend);
        let id = compute_object_id(&ObjectFormat::Raw, b"expected");

        let error = store
            .put(id, ObjectFormat::Raw, b"actual", Durability::BackendDefault)
            .await
            .unwrap_err();

        assert_eq!(error, DurableError::ObjectHashMismatch);
    }

    #[tokio::test]
    async fn different_bytes_under_same_id_are_rejected() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend.clone());
        let bytes = b"expected";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = store
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        backend.corrupt_object(store.scope_id(), reference.object_id(), b"other".to_vec());

        let error = store
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap_err();

        assert!(matches!(error, DurableError::Corrupt(_)));
    }

    #[tokio::test]
    async fn cross_scope_object_access_is_rejected() {
        let backend = Arc::new(InMemoryBackend::new());
        let first = ScopedStorage::new(test_scope("a"), backend.clone());
        let second = ScopedStorage::new(test_scope("b"), backend);
        let bytes = b"scoped";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = first
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();

        let error = ImmutableObjects::read(&second, &reference)
            .await
            .unwrap_err();

        assert_eq!(error, DurableError::ScopeMismatch);
    }

    #[tokio::test]
    async fn range_reads_return_exact_bytes() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend);
        let bytes = b"0123456789";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = store
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();

        assert_eq!(
            store.read_range(&reference, 2..6).await.unwrap(),
            b"2345".to_vec()
        );
    }

    #[tokio::test]
    async fn root_publish_race_has_one_winner() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend);
        let root = RootName::new("main");

        let first = store
            .compare_exchange(&root, ExpectedRevision::Missing, b"one".to_vec())
            .await
            .unwrap();
        let second = store
            .compare_exchange(&root, ExpectedRevision::Missing, b"two".to_vec())
            .await
            .unwrap();

        assert!(matches!(first, PublishOutcome::Applied(_)));
        assert!(matches!(
            second,
            PublishOutcome::Conflict { current: Some(_) }
        ));
    }

    #[tokio::test]
    async fn same_root_bytes_receive_fresh_revision() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend);
        let root = RootName::new("main");
        let first = match store
            .compare_exchange(&root, ExpectedRevision::Missing, b"value".to_vec())
            .await
            .unwrap()
        {
            PublishOutcome::Applied(state) => state,
            other => panic!("unexpected outcome: {other:?}"),
        };
        let second = match store
            .compare_exchange(
                &root,
                ExpectedRevision::Exact(first.revision()),
                b"value".to_vec(),
            )
            .await
            .unwrap()
        {
            PublishOutcome::Applied(state) => state,
            other => panic!("unexpected outcome: {other:?}"),
        };

        assert_ne!(first.revision(), second.revision());
        assert_eq!(second.value(), b"value");
    }

    #[tokio::test]
    async fn root_conflict_returns_actual_current_root() {
        let backend = Arc::new(InMemoryBackend::new());
        let store = ScopedStorage::new(test_scope("a"), backend);
        let root = RootName::new("main");
        let first = match store
            .compare_exchange(&root, ExpectedRevision::Missing, b"current".to_vec())
            .await
            .unwrap()
        {
            PublishOutcome::Applied(state) => state,
            other => panic!("unexpected outcome: {other:?}"),
        };

        let conflict = store
            .compare_exchange(
                &root,
                ExpectedRevision::Exact(Revision::fresh()),
                b"next".to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            conflict,
            PublishOutcome::Conflict {
                current: Some(first)
            }
        );
    }
}
