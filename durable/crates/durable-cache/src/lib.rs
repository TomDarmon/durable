//! Disposable immutable-object cache tiers.

use async_trait::async_trait;
use durable_core::{
    DurableError, DurableObjectRef, ImmutableObjects, ObjectId, RawRanges, Result, ScopeId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

/// Cache storage tier contract.
#[async_trait]
pub trait CacheStore: Send + Sync {
    /// Reads a cache entry.
    async fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>>;
    /// Writes a cache entry.
    async fn put(&self, key: CacheKey, bytes: Vec<u8>) -> Result<()>;
    /// Removes a cache entry.
    async fn evict(&self, key: &CacheKey) -> Result<()>;
}

/// Load decision for an immutable object request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadPlan {
    /// Load the full object through cache.
    Full,
    /// Load a raw range through cache.
    RawRange(Range<u64>),
    /// Bypass the cache and read authoritatively.
    Bypass,
    /// Prefetch the full object.
    Prefetch,
}

/// Policy deciding how a request should load.
pub trait LoadPolicy: Send + Sync {
    /// Returns the plan for a request.
    fn plan(&self, request: &ReadRequest) -> LoadPlan;
}

/// Policy deciding whether to admit an entry.
pub trait AdmissionPolicy: Send + Sync {
    /// Returns true if the entry should be cached.
    fn admit(&self, key: &CacheKey, bytes: &[u8]) -> bool;
}

/// Policy deciding which entry to evict.
pub trait EvictionPolicy: Send + Sync {
    /// Selects an entry to evict from metadata.
    fn victim(&self, entries: &BTreeMap<CacheKey, CacheEntryMetadata>) -> Option<CacheKey>;
}

/// Cache read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRequest {
    /// Referenced durable object.
    pub reference: DurableObjectRef,
    /// Optional requested range.
    pub range: Option<Range<u64>>,
    /// Whether the caller wants an authoritative bypass.
    pub bypass: bool,
}

/// Cache key with scope, representation, extent, and integrity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    scope_id: ScopeId,
    object_id: ObjectId,
    representation: String,
    extent: CacheExtent,
    integrity: IntegrityLevel,
}

impl CacheKey {
    fn full(reference: &DurableObjectRef) -> Self {
        Self {
            scope_id: reference.scope_id(),
            object_id: reference.object_id(),
            representation: "canonical".into(),
            extent: CacheExtent::Full,
            integrity: IntegrityLevel::Verified,
        }
    }

    fn range(reference: &DurableObjectRef, range: Range<u64>) -> Self {
        Self {
            scope_id: reference.scope_id(),
            object_id: reference.object_id(),
            representation: "canonical".into(),
            extent: CacheExtent::Range {
                start: range.start,
                end: range.end,
            },
            integrity: IntegrityLevel::Verified,
        }
    }
}

/// Cached extent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CacheExtent {
    /// Entire object.
    Full,
    /// Half-open byte range.
    Range { start: u64, end: u64 },
}

/// Required integrity for a cached entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IntegrityLevel {
    /// Entry came from an authoritative verified loader.
    Verified,
}

/// Metadata tracked for eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntryMetadata {
    /// Entry size in bytes.
    pub size: u64,
    /// Monotonic access counter.
    pub last_access: u64,
    /// Access frequency.
    pub hits: u64,
}

/// Simple LRU eviction policy.
#[derive(Debug, Default)]
pub struct LruEviction;

impl EvictionPolicy for LruEviction {
    fn victim(&self, entries: &BTreeMap<CacheKey, CacheEntryMetadata>) -> Option<CacheKey> {
        entries
            .iter()
            .min_by_key(|(_, metadata)| metadata.last_access)
            .map(|(key, _)| key.clone())
    }
}

/// Size-aware admission policy.
#[derive(Debug, Clone)]
pub struct SizeAdmission {
    max_entry_bytes: u64,
}

impl SizeAdmission {
    /// Creates a size-aware policy.
    pub fn new(max_entry_bytes: u64) -> Self {
        Self { max_entry_bytes }
    }
}

impl AdmissionPolicy for SizeAdmission {
    fn admit(&self, _key: &CacheKey, bytes: &[u8]) -> bool {
        bytes.len() as u64 <= self.max_entry_bytes
    }
}

/// Default policy that follows the request exactly.
#[derive(Debug, Default)]
pub struct DefaultLoadPolicy;

impl LoadPolicy for DefaultLoadPolicy {
    fn plan(&self, request: &ReadRequest) -> LoadPlan {
        if request.bypass {
            LoadPlan::Bypass
        } else if let Some(range) = request.range.clone() {
            LoadPlan::RawRange(range)
        } else {
            LoadPlan::Full
        }
    }
}

/// Process-local memory tier.
pub struct MemoryCacheStore {
    max_bytes: u64,
    current_bytes: Mutex<u64>,
    tick: AtomicU64,
    entries: Mutex<BTreeMap<CacheKey, Vec<u8>>>,
    metadata: Mutex<BTreeMap<CacheKey, CacheEntryMetadata>>,
    eviction: Arc<dyn EvictionPolicy>,
}

impl MemoryCacheStore {
    /// Creates a memory tier.
    pub fn new(max_bytes: u64, eviction: Arc<dyn EvictionPolicy>) -> Self {
        Self {
            max_bytes,
            current_bytes: Mutex::new(0),
            tick: AtomicU64::new(1),
            entries: Mutex::new(BTreeMap::new()),
            metadata: Mutex::new(BTreeMap::new()),
            eviction,
        }
    }
}

#[async_trait]
impl CacheStore for MemoryCacheStore {
    async fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>> {
        let found = self
            .entries
            .lock()
            .expect("memory cache mutex poisoned")
            .get(key)
            .cloned();
        if found.is_some() {
            let mut metadata = self
                .metadata
                .lock()
                .expect("memory cache metadata mutex poisoned");
            if let Some(entry) = metadata.get_mut(key) {
                entry.last_access = self.tick.fetch_add(1, Ordering::Relaxed);
                entry.hits = entry.hits.saturating_add(1);
            }
        }
        Ok(found)
    }

    async fn put(&self, key: CacheKey, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() as u64 > self.max_bytes {
            return Ok(());
        }
        loop {
            let current = *self
                .current_bytes
                .lock()
                .expect("memory cache size mutex poisoned");
            if current + bytes.len() as u64 <= self.max_bytes {
                break;
            }
            let victim = {
                let metadata = self
                    .metadata
                    .lock()
                    .expect("memory cache metadata mutex poisoned");
                self.eviction.victim(&metadata)
            };
            let Some(victim) = victim else { break };
            self.evict(&victim).await?;
        }

        let previous = self
            .entries
            .lock()
            .expect("memory cache mutex poisoned")
            .insert(key.clone(), bytes.clone());
        let previous_len = previous.map_or(0, |value| value.len() as u64);
        let mut current = self
            .current_bytes
            .lock()
            .expect("memory cache size mutex poisoned");
        *current = current
            .saturating_sub(previous_len)
            .saturating_add(bytes.len() as u64);
        self.metadata
            .lock()
            .expect("memory cache metadata mutex poisoned")
            .insert(
                key,
                CacheEntryMetadata {
                    size: bytes.len() as u64,
                    last_access: self.tick.fetch_add(1, Ordering::Relaxed),
                    hits: 0,
                },
            );
        Ok(())
    }

    async fn evict(&self, key: &CacheKey) -> Result<()> {
        let removed = self
            .entries
            .lock()
            .expect("memory cache mutex poisoned")
            .remove(key);
        if let Some(bytes) = removed {
            let mut current = self
                .current_bytes
                .lock()
                .expect("memory cache size mutex poisoned");
            *current = current.saturating_sub(bytes.len() as u64);
        }
        self.metadata
            .lock()
            .expect("memory cache metadata mutex poisoned")
            .remove(key);
        Ok(())
    }
}

/// Node-local filesystem cache tier.
#[derive(Debug, Clone)]
pub struct DiskCacheStore {
    root: PathBuf,
}

impl DiskCacheStore {
    /// Creates a disk tier rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, key: &CacheKey) -> Result<PathBuf> {
        let encoded = serde_json::to_vec(key)
            .map_err(|error| DurableError::Corrupt(format!("cache key encode failed: {error}")))?;
        let mut hasher = Sha256::new();
        hasher.update(encoded);
        Ok(self.root.join(hex::encode(hasher.finalize())))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskEntry {
    checksum: [u8; 32],
    bytes: Vec<u8>,
}

#[async_trait]
impl CacheStore for DiskCacheStore {
    async fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        if !Path::new(&path).exists() {
            return Ok(None);
        }
        let raw = fs::read(&path)
            .map_err(|error| DurableError::Unavailable(format!("cache read failed: {error}")))?;
        let entry: DiskEntry = match serde_json::from_slice(&raw) {
            Ok(entry) => entry,
            Err(_) => {
                let _ = fs::remove_file(&path);
                return Ok(None);
            }
        };
        let actual: [u8; 32] = Sha256::digest(&entry.bytes).into();
        if actual != entry.checksum {
            let _ = fs::remove_file(&path);
            return Ok(None);
        }
        Ok(Some(entry.bytes))
    }

    async fn put(&self, key: CacheKey, bytes: Vec<u8>) -> Result<()> {
        fs::create_dir_all(&self.root)
            .map_err(|error| DurableError::Unavailable(format!("cache mkdir failed: {error}")))?;
        let entry = DiskEntry {
            checksum: Sha256::digest(&bytes).into(),
            bytes,
        };
        let raw = serde_json::to_vec(&entry)
            .map_err(|error| DurableError::Corrupt(format!("cache encode failed: {error}")))?;
        fs::write(self.path(&key)?, raw)
            .map_err(|error| DurableError::Unavailable(format!("cache write failed: {error}")))?;
        Ok(())
    }

    async fn evict(&self, key: &CacheKey) -> Result<()> {
        let path = self.path(key)?;
        let _ = fs::remove_file(path);
        Ok(())
    }
}

/// Read-through cached object handle bound to one immutable scope.
pub struct CachedObjects<L> {
    scope_id: ScopeId,
    loader: Arc<L>,
    memory: Arc<dyn CacheStore>,
    disk: Arc<dyn CacheStore>,
    load_policy: Arc<dyn LoadPolicy>,
    admission: Arc<dyn AdmissionPolicy>,
}

impl<L> CachedObjects<L>
where
    L: ImmutableObjects + RawRanges + 'static,
{
    /// Creates a cached object stack.
    pub fn new(
        scope_id: ScopeId,
        loader: Arc<L>,
        memory: Arc<dyn CacheStore>,
        disk: Arc<dyn CacheStore>,
        load_policy: Arc<dyn LoadPolicy>,
        admission: Arc<dyn AdmissionPolicy>,
    ) -> Self {
        Self {
            scope_id,
            loader,
            memory,
            disk,
            load_policy,
            admission,
        }
    }

    /// Reads the full object through cache.
    pub async fn read(&self, reference: &DurableObjectRef) -> Result<Vec<u8>> {
        self.read_request(ReadRequest {
            reference: reference.clone(),
            range: None,
            bypass: false,
        })
        .await
    }

    /// Reads a range through cache.
    pub async fn read_range(
        &self,
        reference: &DurableObjectRef,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        self.read_request(ReadRequest {
            reference: reference.clone(),
            range: Some(range),
            bypass: false,
        })
        .await
    }

    /// Reads from the authoritative loader without filling cache.
    pub async fn bypass(&self, reference: &DurableObjectRef) -> Result<Vec<u8>> {
        self.read_request(ReadRequest {
            reference: reference.clone(),
            range: None,
            bypass: true,
        })
        .await
    }

    /// Prefetches a full object into cache.
    pub async fn prefetch(&self, reference: &DurableObjectRef) -> Result<()> {
        let bytes = self.loader.read(reference).await?;
        self.fill(CacheKey::full(reference), bytes).await
    }

    async fn read_request(&self, request: ReadRequest) -> Result<Vec<u8>> {
        if request.reference.scope_id() != self.scope_id {
            return Err(DurableError::ScopeMismatch);
        }
        match self.load_policy.plan(&request) {
            LoadPlan::Bypass => self.loader.read(&request.reference).await,
            LoadPlan::Full | LoadPlan::Prefetch => {
                let key = CacheKey::full(&request.reference);
                self.lookup_or_load(key, || async { self.loader.read(&request.reference).await })
                    .await
            }
            LoadPlan::RawRange(range) => {
                let key = CacheKey::range(&request.reference, range.clone());
                self.lookup_or_load(key, || async {
                    self.loader.read_range(&request.reference, range).await
                })
                .await
            }
        }
    }

    async fn lookup_or_load<F, Fut>(&self, key: CacheKey, load: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>>>,
    {
        if let Some(bytes) = self.memory.get(&key).await? {
            return Ok(bytes);
        }
        if let Some(bytes) = self.disk.get(&key).await? {
            self.memory.put(key, bytes.clone()).await?;
            return Ok(bytes);
        }
        let bytes = load().await?;
        self.fill(key, bytes.clone()).await?;
        Ok(bytes)
    }

    async fn fill(&self, key: CacheKey, bytes: Vec<u8>) -> Result<()> {
        if self.admission.admit(&key, &bytes) {
            self.memory.put(key.clone(), bytes.clone()).await?;
            self.disk.put(key, bytes).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_core::{
        compute_object_id, DatasetId, Durability, EncryptionDomainId, InMemoryBackend,
        ObjectFormat, ScopedStorage, StorageScope, TenantId,
    };
    use std::sync::atomic::AtomicUsize;

    struct CountingLoader {
        inner: ScopedStorage<InMemoryBackend>,
        reads: AtomicUsize,
    }

    #[async_trait]
    impl ImmutableObjects for CountingLoader {
        async fn put(
            &self,
            id: ObjectId,
            format: ObjectFormat,
            canonical_bytes: &[u8],
            durability: Durability,
        ) -> Result<DurableObjectRef> {
            self.inner
                .put(id, format, canonical_bytes, durability)
                .await
        }

        async fn read(&self, reference: &DurableObjectRef) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            ImmutableObjects::read(&self.inner, reference).await
        }
    }

    #[async_trait]
    impl RawRanges for CountingLoader {
        async fn read_range(
            &self,
            reference: &DurableObjectRef,
            range: Range<u64>,
        ) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read_range(reference, range).await
        }
    }

    fn scope() -> StorageScope {
        StorageScope::new(
            TenantId::new("tenant"),
            DatasetId::new("dataset"),
            EncryptionDomainId::new("edek"),
        )
    }

    fn cache_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("durable-cache-{name}-{}", std::process::id()))
    }

    #[tokio::test]
    async fn memory_cache_hit_avoids_authoritative_read() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = ScopedStorage::new(scope(), backend);
        let bytes = b"cache me";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = storage
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        let loader = Arc::new(CountingLoader {
            inner: storage.clone(),
            reads: AtomicUsize::new(0),
        });
        let cache = CachedObjects::new(
            storage.scope_id(),
            loader.clone(),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(cache_dir("memory-hit"))),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );

        assert_eq!(cache.read(&reference).await.unwrap(), bytes);
        assert_eq!(cache.read(&reference).await.unwrap(), bytes);
        assert_eq!(loader.reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn disk_cache_survives_memory_loss() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = ScopedStorage::new(scope(), backend);
        let bytes = b"disk cache";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = storage
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        let dir = cache_dir("disk");
        let _ = fs::remove_dir_all(&dir);
        let loader = Arc::new(CountingLoader {
            inner: storage.clone(),
            reads: AtomicUsize::new(0),
        });
        let first = CachedObjects::new(
            storage.scope_id(),
            loader.clone(),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(&dir)),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );
        assert_eq!(first.read(&reference).await.unwrap(), bytes);
        let second = CachedObjects::new(
            storage.scope_id(),
            loader.clone(),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(&dir)),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );
        assert_eq!(second.read(&reference).await.unwrap(), bytes);
        assert_eq!(loader.reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn corrupt_disk_cache_entry_is_evicted_and_reloaded() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = ScopedStorage::new(scope(), backend);
        let bytes = b"disk corruption";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = storage
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        let dir = cache_dir("corrupt");
        let _ = fs::remove_dir_all(&dir);
        let loader = Arc::new(CountingLoader {
            inner: storage.clone(),
            reads: AtomicUsize::new(0),
        });
        let first = CachedObjects::new(
            storage.scope_id(),
            loader.clone(),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(&dir)),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );
        assert_eq!(first.read(&reference).await.unwrap(), bytes);
        for entry in fs::read_dir(&dir).unwrap() {
            fs::write(entry.unwrap().path(), b"corrupt").unwrap();
        }
        let second = CachedObjects::new(
            storage.scope_id(),
            loader.clone(),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(&dir)),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );

        assert_eq!(second.read(&reference).await.unwrap(), bytes);
        assert_eq!(loader.reads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn cache_scope_isolation_is_enforced() {
        let backend = Arc::new(InMemoryBackend::new());
        let first = ScopedStorage::new(scope(), backend.clone());
        let second_scope = StorageScope::new(
            TenantId::new("other"),
            DatasetId::new("dataset"),
            EncryptionDomainId::new("edek"),
        );
        let second = ScopedStorage::new(second_scope, backend);
        let bytes = b"other scope";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = second
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        let cache = CachedObjects::new(
            first.scope_id(),
            Arc::new(first),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(cache_dir("scope"))),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );

        assert_eq!(
            cache.read(&reference).await.unwrap_err(),
            DurableError::ScopeMismatch
        );
    }

    #[tokio::test]
    async fn removing_cache_does_not_lose_durable_data() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = ScopedStorage::new(scope(), backend);
        let bytes = b"authoritative";
        let id = compute_object_id(&ObjectFormat::Raw, bytes);
        let reference = storage
            .put(id, ObjectFormat::Raw, bytes, Durability::BackendDefault)
            .await
            .unwrap();
        let dir = cache_dir("delete");
        let cache = CachedObjects::new(
            storage.scope_id(),
            Arc::new(storage.clone()),
            Arc::new(MemoryCacheStore::new(1024, Arc::new(LruEviction))),
            Arc::new(DiskCacheStore::new(&dir)),
            Arc::new(DefaultLoadPolicy),
            Arc::new(SizeAdmission::new(1024)),
        );
        assert_eq!(cache.read(&reference).await.unwrap(), bytes);
        let _ = fs::remove_dir_all(&dir);

        assert_eq!(
            ImmutableObjects::read(&storage, &reference).await.unwrap(),
            bytes
        );
    }
}
