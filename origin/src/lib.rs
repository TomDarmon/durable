//! Origin-like Git storage built on durable primitives.
//!
//! The durable library stays Git-agnostic. This crate owns Git repository
//! identity, bare-repository materialization, ref publication, and recovery.

mod catalog;
mod git_cache;
mod wal;

use axum::{extract::DefaultBodyLimit, Json};
use axum::{middleware::Next, response::IntoResponse};
pub use catalog::{OriginCatalog, RepositoryCatalog, RepositoryCatalogEntry};
use catalog::{CATALOG_DATASET, CATALOG_ENCRYPTION_DOMAIN, CATALOG_TENANT};
#[cfg(test)]
use git_cache::command;
use git_cache::{
    cache_marker_matches, capture_git_objects, capture_git_refs, command_output,
    command_output_with_input, git, git_bytes_output, git_output, is_git_oid,
    materialize_empty_bare_repo, pack_index_locations, path_str, safe_join, validate_path_segment,
    validate_repository_name, write_cache_marker, write_loose_refs,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use substrate::{
    compute_object_id, DatasetId, Durability, DurableError, DurableObjectRef, EncryptionDomainId,
    ExpectedRevision, ImmutableObjects, ObjectFormat, PublishOutcome, RawBackend, RawRanges,
    RootName, RootRegister, RootState, ScopedStorage, StorageScope, TenantId,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use wal::{git_wal_event_digest, git_wal_index_digest, publication_digest, GitWalRootPointer};
pub use wal::{
    GitCompactionWalEntry, GitObjectEntry, GitObjectLocation, GitPackEntry, GitPublication,
    GitPushWalEntry, GitRefEntry, GitWalEvent, GitWalEventKind, GitWalIndex, GitWalIndexEntry,
    PublicationResolution,
};

const BROWSER_BLOB_LIMIT_BYTES: u64 = 1024 * 1024;

/// Origin result type.
pub type Result<T> = std::result::Result<T, OriginError>;

/// Origin error taxonomy.
#[derive(Debug, Error)]
pub enum OriginError {
    /// Durable storage failed.
    #[error(transparent)]
    Durable(#[from] DurableError),
    /// Filesystem operation failed.
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON encoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Git command failed.
    #[error("git command failed: {program} {args:?}: {stderr}")]
    Git {
        /// Program name.
        program: String,
        /// Arguments.
        args: Vec<String>,
        /// Standard error.
        stderr: String,
    },
    /// A path inside a bare repository was not safe to persist.
    #[error("unsafe git repository path: {0}")]
    UnsafePath(String),
    /// The repository root changed concurrently.
    #[error("repository root changed concurrently")]
    Conflict,
    /// A root publication returned an ambiguous outcome.
    #[error("repository root publication outcome is unknown")]
    OutcomeUnknown,
    /// HTTP request could not be served.
    #[error("http error: {0}")]
    Http(String),
}

/// Repository identity mapped to a durable storage scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryScope {
    tenant: String,
    repository: String,
    encryption_domain: String,
}

impl RepositoryScope {
    /// Creates a repository scope.
    pub fn new(
        tenant: impl Into<String>,
        repository: impl Into<String>,
        encryption_domain: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            repository: repository.into(),
            encryption_domain: encryption_domain.into(),
        }
    }

    /// Converts the Origin repository identity to a durable scope.
    pub fn storage_scope(&self) -> StorageScope {
        StorageScope::new(
            TenantId::new(&self.tenant),
            DatasetId::new(&self.repository),
            EncryptionDomainId::new(&self.encryption_domain),
        )
    }
}

/// Durable-backed repository handle.
#[derive(Clone)]
pub struct OriginRepository<B> {
    storage: Arc<ScopedStorage<B>>,
    root_name: RootName,
    metrics: Arc<OriginRepositoryMetrics>,
    #[cfg(test)]
    force_next_root_outcome_unknown: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Default)]
struct OriginRepositoryMetrics {
    durable_bytes_written: AtomicU64,
    durable_bytes_read: AtomicU64,
    local_materializations: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cas_conflicts: AtomicU64,
    cas_retries: AtomicU64,
}

/// Point-in-time repository engine counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginRepositoryMetricsSnapshot {
    pub durable_bytes_written: u64,
    pub durable_bytes_read: u64,
    pub local_materializations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cas_conflicts: u64,
    pub cas_retries: u64,
}

/// Local bare repository materialized from one exact durable root revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedRepository {
    expected: ExpectedRevision,
    wal_digest: Option<String>,
}

impl MaterializedRepository {
    /// Returns the WAL index digest this local cache was materialized from, if any.
    pub fn wal_digest(&self) -> Option<&str> {
        self.wal_digest.as_deref()
    }
}

impl<B> OriginRepository<B>
where
    B: RawBackend + 'static,
{
    /// Creates a repository handle over a scope-bound durable store.
    pub fn new(storage: Arc<ScopedStorage<B>>) -> Self {
        Self {
            storage,
            root_name: RootName::new("origin.git.wal.v1"),
            metrics: Arc::new(OriginRepositoryMetrics::default()),
            #[cfg(test)]
            force_next_root_outcome_unknown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Returns the underlying durable storage handle.
    pub fn storage(&self) -> &Arc<ScopedStorage<B>> {
        &self.storage
    }

    /// Returns a point-in-time copy of repository engine metrics.
    pub fn metrics(&self) -> OriginRepositoryMetricsSnapshot {
        OriginRepositoryMetricsSnapshot {
            durable_bytes_written: self.metrics.durable_bytes_written.load(Ordering::Relaxed),
            durable_bytes_read: self.metrics.durable_bytes_read.load(Ordering::Relaxed),
            local_materializations: self.metrics.local_materializations.load(Ordering::Relaxed),
            cache_hits: self.metrics.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.metrics.cache_misses.load(Ordering::Relaxed),
            cas_conflicts: self.metrics.cas_conflicts.load(Ordering::Relaxed),
            cas_retries: self.metrics.cas_retries.load(Ordering::Relaxed),
        }
    }

    /// Test hook that makes the next successful WAL root CAS look ambiguous.
    #[cfg(test)]
    pub fn force_next_wal_root_outcome_unknown(&self) {
        self.force_next_root_outcome_unknown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Captures a bare Git repository and publishes it against the latest root.
    ///
    /// Prefer `publish_materialized_bare_repository` when publishing a repo
    /// cache produced by `materialize_bare_repository`; it preserves the base
    /// revision and rejects stale writers.
    pub async fn publish_bare_repository(&self, bare_repo: impl AsRef<Path>) -> Result<()> {
        let expected = RootRegister::read(self.storage.as_ref(), &self.root_name)
            .await?
            .map_or(ExpectedRevision::Missing, |root| {
                ExpectedRevision::Exact(root.revision())
            });
        self.publish_bare_repository_with_expected(bare_repo.as_ref(), expected)
            .await
            .map(|_| ())
    }

    /// Publishes a materialized bare repository against its captured base.
    pub async fn publish_materialized_bare_repository(
        &self,
        bare_repo: impl AsRef<Path>,
        materialized: MaterializedRepository,
    ) -> Result<MaterializedRepository> {
        self.publish_bare_repository_with_expected(bare_repo.as_ref(), materialized.expected)
            .await
    }

    async fn publish_bare_repository_with_expected(
        &self,
        bare_repo: &Path,
        expected: ExpectedRevision,
    ) -> Result<MaterializedRepository> {
        info!(bare_repo = %bare_repo.display(), "publishing repository");
        git(bare_repo, ["fsck", "--no-dangling"])?;

        let current = self.read_wal_head().await?;
        self.ensure_expected_matches_head(expected, current.as_ref())?;
        let previous = if let Some((_, _, index)) = current.as_ref() {
            self.replay_wal_index(index).await?
        } else {
            None
        };
        let mut publication = self
            .capture_bare_repository(bare_repo, previous.as_ref())
            .await?;
        let sequence = current
            .as_ref()
            .map(|(_, _, index)| index.entries.len() as u64 + 1)
            .unwrap_or(1);
        let new_pack_indexes =
            previous.as_ref().map(|base| base.packs.len()).unwrap_or(0)..publication.packs.len();
        let event = GitWalEvent::Push(GitPushWalEntry {
            version: 1,
            sequence,
            base_digest: previous
                .as_ref()
                .map(|publication| publication.digest.clone()),
            next_digest: publication.digest.clone(),
            refs: publication.refs.clone(),
            objects: publication.objects.clone(),
            packs: publication.packs.clone(),
            new_pack_indexes: new_pack_indexes.collect(),
        });
        let entry = self.put_wal_event(&event).await?;
        let entry_digest = git_wal_event_digest(&event)?;
        let next_index = self.next_wal_index(current.as_ref(), entry, &entry_digest, &event);
        let index_ref = self.put_wal_index(&next_index).await?;
        let root_value = serde_json::to_vec(&GitWalRootPointer {
            version: 1,
            entries: next_index.entries.len() as u64,
            digest: next_index.digest.clone(),
            index: index_ref,
        })?;

        #[cfg(test)]
        let mut outcome = self
            .storage
            .compare_exchange(&self.root_name, expected, root_value)
            .await?;

        #[cfg(not(test))]
        let outcome = self
            .storage
            .compare_exchange(&self.root_name, expected, root_value)
            .await?;
        #[cfg(test)]
        if matches!(outcome, PublishOutcome::Applied(_))
            && self
                .force_next_root_outcome_unknown
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            outcome = PublishOutcome::OutcomeUnknown;
        }

        match outcome {
            PublishOutcome::Applied(root) => {
                publication.wal_entries = next_index.entries.clone();
                write_cache_marker(bare_repo, &next_index.digest)?;
                info!(
                    bare_repo = %bare_repo.display(),
                    digest = %publication.digest,
                    wal_digest = %next_index.digest,
                    wal_entries = next_index.entries.len(),
                    refs = publication.refs.len(),
                    objects = publication.objects.len(),
                    packs = publication.packs.len(),
                    revision = ?root.revision(),
                    "published repository WAL entry"
                );
                Ok(MaterializedRepository {
                    expected: ExpectedRevision::Exact(root.revision()),
                    wal_digest: Some(next_index.digest),
                })
            }
            PublishOutcome::Conflict { .. } => {
                self.metrics.cas_conflicts.fetch_add(1, Ordering::Relaxed);
                warn!(bare_repo = %bare_repo.display(), "repository WAL publish conflict");
                Err(OriginError::Conflict)
            }
            PublishOutcome::OutcomeUnknown => match self.resolve_wal_entry(&entry_digest).await? {
                PublicationResolution::Applied(index_entry) => {
                    let Some((root, _, index)) = self.read_wal_head().await? else {
                        return Err(OriginError::OutcomeUnknown);
                    };
                    if !index.entries.contains(&index_entry) {
                        return Err(OriginError::OutcomeUnknown);
                    }
                    write_cache_marker(bare_repo, &index.digest)?;
                    warn!(
                        bare_repo = %bare_repo.display(),
                        entry_digest = %entry_digest,
                        "repository WAL CAS outcome resolved as applied"
                    );
                    Ok(MaterializedRepository {
                        expected: ExpectedRevision::Exact(root.revision()),
                        wal_digest: Some(index.digest),
                    })
                }
                PublicationResolution::NotApplied => Err(OriginError::Conflict),
                PublicationResolution::RepositoryMissing => Err(OriginError::OutcomeUnknown),
            },
        }
    }

    /// Materializes the current durable repository state to a local bare repo.
    pub async fn materialize_bare_repository(
        &self,
        bare_repo: impl AsRef<Path>,
    ) -> Result<MaterializedRepository> {
        let bare_repo = bare_repo.as_ref();
        if let Some((root, pointer, index)) = self.read_wal_head().await? {
            info!(
                bare_repo = %bare_repo.display(),
                revision = ?root.revision(),
                wal_digest = %pointer.digest,
                "materializing repository"
            );
            let publication = self
                .replay_wal_index(&index)
                .await?
                .ok_or_else(|| OriginError::Http("repository WAL is empty".into()))?;
            self.write_publication_to_bare_repository(bare_repo, &publication)
                .await?;
            write_cache_marker(bare_repo, &pointer.digest)?;
            self.metrics
                .local_materializations
                .fetch_add(1, Ordering::Relaxed);
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
                wal_digest: Some(pointer.digest),
            })
        } else {
            info!(
                bare_repo = %bare_repo.display(),
                "initializing empty repository"
            );
            materialize_empty_bare_repo(bare_repo)?;
            self.metrics
                .local_materializations
                .fetch_add(1, Ordering::Relaxed);
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Missing,
                wal_digest: None,
            })
        }
    }

    /// Materializes the current durable state, reusing a verified local cache when it matches.
    pub async fn materialize_bare_repository_cached(
        &self,
        bare_repo: impl AsRef<Path>,
    ) -> Result<MaterializedRepository> {
        let bare_repo = bare_repo.as_ref();
        if let Some((root, pointer, index)) = self.read_wal_head().await? {
            let publication = self
                .replay_wal_index(&index)
                .await?
                .ok_or_else(|| OriginError::Http("repository WAL is empty".into()))?;
            if cache_matches_publication(bare_repo, &pointer.digest, &publication) {
                debug!(
                    bare_repo = %bare_repo.display(),
                    wal_digest = %pointer.digest,
                    "using repository cache"
                );
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(MaterializedRepository {
                    expected: ExpectedRevision::Exact(root.revision()),
                    wal_digest: Some(pointer.digest),
                });
            }
            info!(
                bare_repo = %bare_repo.display(),
                wal_digest = %pointer.digest,
                "refreshing repository cache"
            );
            self.write_publication_to_bare_repository(bare_repo, &publication)
                .await?;
            write_cache_marker(bare_repo, &pointer.digest)?;
            self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .local_materializations
                .fetch_add(1, Ordering::Relaxed);
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
                wal_digest: Some(pointer.digest),
            })
        } else {
            info!(
                bare_repo = %bare_repo.display(),
                "initializing empty repository cache"
            );
            materialize_empty_bare_repo(bare_repo)?;
            self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .local_materializations
                .fetch_add(1, Ordering::Relaxed);
            Ok(MaterializedRepository {
                expected: ExpectedRevision::Missing,
                wal_digest: None,
            })
        }
    }

    /// Reads the current repository state by replaying the authoritative WAL.
    pub async fn current_publication(&self) -> Result<Option<GitPublication>> {
        let Some((_, _, index)) = self.read_wal_head().await? else {
            return Ok(None);
        };
        self.replay_wal_index(&index).await
    }

    /// Reads the current authoritative WAL index.
    pub async fn current_wal_index(&self) -> Result<Option<GitWalIndex>> {
        let Some((_, _, index)) = self.read_wal_head().await? else {
            return Ok(None);
        };
        Ok(Some(index))
    }

    /// Reads the exact packed bytes for a cataloged Git object from durable storage.
    pub async fn read_git_object_pack_range(&self, oid: &str) -> Result<Vec<u8>> {
        let publication = self
            .current_publication()
            .await?
            .ok_or_else(|| OriginError::Http("repository not found".into()))?;
        let object = publication
            .objects
            .iter()
            .find(|object| object.oid == oid)
            .cloned()
            .ok_or_else(|| OriginError::Http(format!("git object not found: {oid}")))?;
        let pack = publication
            .packs
            .iter()
            .find(|pack| pack.name == object.location.pack_name)
            .ok_or_else(|| OriginError::Http(format!("git object has invalid pack: {oid}")))?;
        let start = object.location.pack_offset;
        let end = start + object.location.packed_size;
        let bytes = self.storage.read_range(&pack.pack, start..end).await?;
        self.metrics
            .durable_bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    async fn capture_bare_repository(
        &self,
        bare_repo: &Path,
        previous: Option<&GitPublication>,
    ) -> Result<GitPublication> {
        let refs = capture_git_refs(bare_repo)?;
        let object_metadata = capture_git_objects(bare_repo)?;
        let previous_objects = previous
            .map(|publication| {
                publication
                    .objects
                    .iter()
                    .map(|object| object.oid.as_str())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let missing_oids = object_metadata
            .iter()
            .filter(|object| !previous_objects.contains(object.oid.as_str()))
            .map(|object| object.oid.clone())
            .collect::<Vec<_>>();

        let mut packs = previous
            .map(|publication| publication.packs.clone())
            .unwrap_or_default();
        let (new_pack_index, new_pack_name, new_locations) = if missing_oids.is_empty() {
            (None, None, BTreeMap::new())
        } else {
            let (pack, locations) = self.pack_git_objects(bare_repo, &missing_oids).await?;
            let pack_name = pack.name.clone();
            packs.push(pack);
            (Some(packs.len() - 1), Some(pack_name), locations)
        };

        let previous_locations = previous
            .map(|publication| {
                publication
                    .objects
                    .iter()
                    .map(|object| (object.oid.as_str(), object.location.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let objects = object_metadata
            .into_iter()
            .map(|mut object| {
                object.location = previous_locations
                    .get(object.oid.as_str())
                    .cloned()
                    .or_else(|| {
                        new_pack_index.and_then(|pack_index| {
                            new_locations.get(object.oid.as_str()).map(
                                |(pack_offset, packed_size)| GitObjectLocation {
                                    pack_index,
                                    pack_name: new_pack_name.clone().unwrap_or_default(),
                                    pack_offset: *pack_offset,
                                    packed_size: *packed_size,
                                },
                            )
                        })
                    })
                    .unwrap_or_default();
                object
            })
            .collect::<Vec<_>>();

        let digest = publication_digest(&refs, &objects, &packs);
        debug!(
            bare_repo = %bare_repo.display(),
            digest = %digest,
            refs = refs.len(),
            objects = objects.len(),
            packs = packs.len(),
            "captured repository publication"
        );
        Ok(GitPublication {
            version: 1,
            digest,
            refs,
            objects,
            packs,
            wal_entries: Vec::new(),
        })
    }

    async fn pack_git_objects(
        &self,
        bare_repo: &Path,
        oids: &[String],
    ) -> Result<(GitPackEntry, BTreeMap<String, (u64, u64)>)> {
        let temp = tempfile::tempdir()?;
        let pack_path = temp.path().join("origin-incremental.pack");
        let mut input = oids.join("\n");
        input.push('\n');
        let pack_bytes = command_output_with_input(
            "git",
            [
                "-C".to_string(),
                path_str(bare_repo)?.to_string(),
                "pack-objects".to_string(),
                "--compression=0".to_string(),
                "--stdout".to_string(),
            ],
            input.as_bytes(),
        )?;
        fs::write(&pack_path, &pack_bytes)?;
        let name = String::from_utf8_lossy(&command_output(
            "git",
            ["index-pack", path_str(&pack_path)?],
        )?)
        .trim()
        .to_string();
        let idx_path = pack_path.with_extension("idx");
        let locations = pack_index_locations(&idx_path)?;
        let idx_bytes = fs::read(idx_path)?;
        let pack = self
            .put_custom_object(
                ObjectFormat::Custom("origin.git.pack.v1".into()),
                &pack_bytes,
            )
            .await?;
        let index = self
            .put_custom_object(
                ObjectFormat::Custom("origin.git.pack-index.v1".into()),
                &idx_bytes,
            )
            .await?;
        Ok((
            GitPackEntry {
                pack,
                index,
                name,
                objects: locations.len(),
            },
            locations,
        ))
    }

    async fn write_publication_to_bare_repository(
        &self,
        bare_repo: &Path,
        publication: &GitPublication,
    ) -> Result<()> {
        materialize_empty_bare_repo(bare_repo)?;
        let pack_dir = bare_repo.join("objects").join("pack");
        fs::create_dir_all(&pack_dir)?;
        for pack in &publication.packs {
            let pack_path = pack_dir.join(format!("pack-{}.pack", pack.name));
            let idx_path = pack_dir.join(format!("pack-{}.idx", pack.name));
            let pack_bytes = ImmutableObjects::read(self.storage.as_ref(), &pack.pack).await?;
            self.metrics
                .durable_bytes_read
                .fetch_add(pack_bytes.len() as u64, Ordering::Relaxed);
            fs::write(pack_path, pack_bytes)?;
            let index_bytes = ImmutableObjects::read(self.storage.as_ref(), &pack.index).await?;
            self.metrics
                .durable_bytes_read
                .fetch_add(index_bytes.len() as u64, Ordering::Relaxed);
            fs::write(idx_path, index_bytes)?;
        }
        write_loose_refs(bare_repo, &publication.refs)?;
        git(bare_repo, ["fsck", "--no-dangling"])?;
        Ok(())
    }

    /// Appends a compaction event that can seed future WAL replay.
    pub async fn compact_wal(&self) -> Result<Option<MaterializedRepository>> {
        let Some((root, _, index)) = self.read_wal_head().await? else {
            return Ok(None);
        };
        let Some(publication) = self.replay_wal_index(&index).await? else {
            return Ok(None);
        };
        let sequence = index.entries.len() as u64 + 1;
        let event = GitWalEvent::Compaction(GitCompactionWalEntry {
            version: 1,
            sequence,
            compacted_through: index.entries.len() as u64,
            state_digest: publication.digest,
            refs: publication.refs,
            objects: publication.objects,
            packs: publication.packs,
        });
        let entry = self.put_wal_event(&event).await?;
        let entry_digest = git_wal_event_digest(&event)?;
        let mut entries = index.entries.clone();
        entries.push(GitWalIndexEntry {
            sequence: event.sequence(),
            kind: event.kind(),
            object: entry,
            entry_digest: entry_digest.clone(),
            state_digest: event.state_digest().to_string(),
        });
        let next_index = GitWalIndex {
            version: 1,
            digest: git_wal_index_digest(&entries),
            entries,
        };
        let index_ref = self.put_wal_index(&next_index).await?;
        let root_value = serde_json::to_vec(&GitWalRootPointer {
            version: 1,
            entries: next_index.entries.len() as u64,
            digest: next_index.digest.clone(),
            index: index_ref,
        })?;
        match self
            .storage
            .compare_exchange(
                &self.root_name,
                ExpectedRevision::Exact(root.revision()),
                root_value,
            )
            .await?
        {
            PublishOutcome::Applied(root) => Ok(Some(MaterializedRepository {
                expected: ExpectedRevision::Exact(root.revision()),
                wal_digest: Some(next_index.digest),
            })),
            PublishOutcome::Conflict { .. } => {
                self.metrics.cas_conflicts.fetch_add(1, Ordering::Relaxed);
                Err(OriginError::Conflict)
            }
            PublishOutcome::OutcomeUnknown => match self.resolve_wal_entry(&entry_digest).await? {
                PublicationResolution::Applied(_) => {
                    let Some((root, pointer, _)) = self.read_wal_head().await? else {
                        return Err(OriginError::OutcomeUnknown);
                    };
                    Ok(Some(MaterializedRepository {
                        expected: ExpectedRevision::Exact(root.revision()),
                        wal_digest: Some(pointer.digest),
                    }))
                }
                PublicationResolution::NotApplied => Err(OriginError::Conflict),
                PublicationResolution::RepositoryMissing => Err(OriginError::OutcomeUnknown),
            },
        }
    }

    async fn read_wal_head(&self) -> Result<Option<(RootState, GitWalRootPointer, GitWalIndex)>> {
        let Some(root) = RootRegister::read(self.storage.as_ref(), &self.root_name).await? else {
            return Ok(None);
        };
        let pointer: GitWalRootPointer = serde_json::from_slice(root.value())?;
        let bytes = ImmutableObjects::read(self.storage.as_ref(), &pointer.index).await?;
        self.metrics
            .durable_bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let index: GitWalIndex = serde_json::from_slice(&bytes)?;
        if index.digest != pointer.digest || index.entries.len() as u64 != pointer.entries {
            return Err(OriginError::Http(
                "WAL root pointer does not match index".into(),
            ));
        }
        Ok(Some((root, pointer, index)))
    }

    fn ensure_expected_matches_head(
        &self,
        expected: ExpectedRevision,
        current: Option<&(RootState, GitWalRootPointer, GitWalIndex)>,
    ) -> Result<()> {
        match (expected, current) {
            (ExpectedRevision::Missing, None) => Ok(()),
            (ExpectedRevision::Exact(revision), Some((root, _, _)))
                if root.revision() == revision =>
            {
                Ok(())
            }
            _ => Err(OriginError::Conflict),
        }
    }

    async fn replay_wal_index(&self, index: &GitWalIndex) -> Result<Option<GitPublication>> {
        let mut publication = None;
        for entry in &index.entries {
            let event = self.read_wal_event(&entry.object).await?;
            match event {
                GitWalEvent::Push(push) => {
                    if push.sequence != entry.sequence || push.next_digest != entry.state_digest {
                        return Err(OriginError::Http(
                            "WAL push entry did not match index".into(),
                        ));
                    }
                    publication = Some(GitPublication {
                        version: 1,
                        digest: push.next_digest,
                        refs: push.refs,
                        objects: push.objects,
                        packs: push.packs,
                        wal_entries: Vec::new(),
                    });
                }
                GitWalEvent::Compaction(compaction) => {
                    if compaction.sequence != entry.sequence
                        || compaction.state_digest != entry.state_digest
                    {
                        return Err(OriginError::Http(
                            "WAL compaction entry did not match index".into(),
                        ));
                    }
                    publication = Some(GitPublication {
                        version: 1,
                        digest: compaction.state_digest,
                        refs: compaction.refs,
                        objects: compaction.objects,
                        packs: compaction.packs,
                        wal_entries: Vec::new(),
                    });
                }
            }
        }
        if let Some(publication) = publication.as_mut() {
            publication.wal_entries = index.entries.clone();
        }
        Ok(publication)
    }

    fn next_wal_index(
        &self,
        current: Option<&(RootState, GitWalRootPointer, GitWalIndex)>,
        object: DurableObjectRef,
        entry_digest: &str,
        event: &GitWalEvent,
    ) -> GitWalIndex {
        let mut entries = current
            .map(|(_, _, index)| index.entries.clone())
            .unwrap_or_default();
        entries.push(GitWalIndexEntry {
            sequence: event.sequence(),
            kind: event.kind(),
            object,
            entry_digest: entry_digest.to_string(),
            state_digest: event.state_digest().to_string(),
        });
        let digest = git_wal_index_digest(&entries);
        GitWalIndex {
            version: 1,
            digest,
            entries,
        }
    }

    async fn put_wal_event(&self, event: &GitWalEvent) -> Result<DurableObjectRef> {
        let bytes = serde_json::to_vec(event)?;
        self.put_custom_object(
            ObjectFormat::Custom("origin.git.wal-entry.v1".into()),
            &bytes,
        )
        .await
    }

    async fn read_wal_event(&self, reference: &DurableObjectRef) -> Result<GitWalEvent> {
        let bytes = ImmutableObjects::read(self.storage.as_ref(), reference).await?;
        self.metrics
            .durable_bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let event = serde_json::from_slice(&bytes)?;
        let digest = git_wal_event_digest(&event)?;
        let expected = compute_object_id(
            &ObjectFormat::Custom("origin.git.wal-entry.v1".into()),
            &bytes,
        );
        if reference.object_id() != expected {
            return Err(OriginError::Http(format!(
                "WAL entry digest mismatch: {digest}"
            )));
        }
        Ok(event)
    }

    async fn put_wal_index(&self, index: &GitWalIndex) -> Result<DurableObjectRef> {
        let bytes = serde_json::to_vec(index)?;
        self.put_custom_object(
            ObjectFormat::Custom("origin.git.wal-index.v1".into()),
            &bytes,
        )
        .await
    }

    /// Resolves whether an uncertain WAL entry became authoritative.
    pub async fn resolve_wal_entry(&self, entry_digest: &str) -> Result<PublicationResolution> {
        let Some((_, _, index)) = self.read_wal_head().await? else {
            return Ok(PublicationResolution::RepositoryMissing);
        };
        Ok(index
            .entries
            .into_iter()
            .find(|entry| entry.entry_digest == entry_digest)
            .map(PublicationResolution::Applied)
            .unwrap_or(PublicationResolution::NotApplied))
    }

    async fn put_custom_object(
        &self,
        format: ObjectFormat,
        bytes: &[u8],
    ) -> Result<DurableObjectRef> {
        let id = compute_object_id(&format, bytes);
        let reference = self
            .storage
            .put(id, format, bytes, Durability::BackendDefault)
            .await?;
        self.metrics
            .durable_bytes_written
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(reference)
    }
}

fn cache_matches_publication(
    bare_repo: &Path,
    wal_digest: &str,
    publication: &GitPublication,
) -> bool {
    cache_marker_matches(bare_repo, wal_digest)
        && git(bare_repo, ["fsck", "--no-dangling"]).is_ok()
        && capture_git_refs(bare_repo)
            .map(|refs| refs == publication.refs)
            .unwrap_or(false)
}

/// Builds a local RustFS-backed Origin repository.
pub async fn local_rustfs_repository(
    scope: RepositoryScope,
) -> Result<OriginRepository<s3::S3Backend>> {
    rustfs_repository(scope, local_rustfs_config()).await
}

/// Builds a local RustFS-backed Origin repository catalog.
pub async fn local_rustfs_catalog() -> Result<OriginCatalog<s3::S3Backend>> {
    rustfs_catalog(local_rustfs_config()).await
}

/// Builds a RustFS/S3-backed Origin repository from an explicit config.
pub async fn rustfs_repository(
    scope: RepositoryScope,
    config: s3::S3BackendConfig,
) -> Result<OriginRepository<s3::S3Backend>> {
    let backend = Arc::new(s3::S3Backend::new(config).await?);
    let storage = Arc::new(ScopedStorage::new(scope.storage_scope(), backend));
    Ok(OriginRepository::new(storage))
}

/// Builds a RustFS/S3-backed Origin repository catalog from an explicit config.
pub async fn rustfs_catalog(config: s3::S3BackendConfig) -> Result<OriginCatalog<s3::S3Backend>> {
    let backend = Arc::new(s3::S3Backend::new(config).await?);
    let scope = StorageScope::new(
        TenantId::new(CATALOG_TENANT),
        DatasetId::new(CATALOG_DATASET),
        EncryptionDomainId::new(CATALOG_ENCRYPTION_DOMAIN),
    );
    let storage = Arc::new(ScopedStorage::new(scope, backend));
    Ok(OriginCatalog::new(storage))
}

/// Returns local RustFS config, compatible with durable's integration env vars.
pub fn local_rustfs_config() -> s3::S3BackendConfig {
    s3::S3BackendConfig {
        endpoint: std::env::var("DURABLE_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9100".into()),
        region: "us-east-1".into(),
        bucket: std::env::var("DURABLE_RUSTFS_BUCKET").unwrap_or_else(|_| "origin-dev".into()),
        access_key: std::env::var("DURABLE_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "origin".into()),
        secret_key: std::env::var("DURABLE_RUSTFS_SECRET_KEY")
            .unwrap_or_else(|_| "origin-secret".into()),
        create_bucket_if_missing: true,
    }
}

/// Runs a minimal smart-HTTP Git server on the supplied listener.
pub async fn serve_http(listener: TcpListener, config: s3::S3BackendConfig) -> Result<()> {
    let cache_temp = tempfile::tempdir()?;
    serve_http_with_cache_dir_and_guard(
        listener,
        config,
        cache_temp.path().to_path_buf(),
        Some(cache_temp),
    )
    .await
}

/// Runs a minimal smart-HTTP Git server using a persistent local repository cache.
pub async fn serve_http_with_cache_dir(
    listener: TcpListener,
    config: s3::S3BackendConfig,
    cache_root: impl Into<PathBuf>,
) -> Result<()> {
    serve_http_with_cache_dir_and_guard(listener, config, cache_root.into(), None).await
}

/// Runs the read-only repository browser API on the supplied listener.
pub async fn serve_browser_api(listener: TcpListener, config: s3::S3BackendConfig) -> Result<()> {
    let cache_temp = tempfile::tempdir()?;
    serve_browser_api_with_cache_dir_and_guard(
        listener,
        config,
        cache_temp.path().to_path_buf(),
        Some(cache_temp),
    )
    .await
}

/// Runs the read-only repository browser API using a persistent local cache.
pub async fn serve_browser_api_with_cache_dir(
    listener: TcpListener,
    config: s3::S3BackendConfig,
    cache_root: impl Into<PathBuf>,
) -> Result<()> {
    serve_browser_api_with_cache_dir_and_guard(listener, config, cache_root.into(), None).await
}

async fn serve_http_with_cache_dir_and_guard(
    listener: TcpListener,
    config: s3::S3BackendConfig,
    cache_root: PathBuf,
    cache_temp: Option<tempfile::TempDir>,
) -> Result<()> {
    fs::create_dir_all(&cache_root)?;
    info!(
        address = %listener.local_addr().map_err(|error| OriginError::Http(error.to_string()))?,
        cache_root = %cache_root.display(),
        "serving origin http"
    );
    let state = Arc::new(HttpState {
        cache_root,
        _cache_temp: cache_temp,
        config,
        repositories: Mutex::new(HashMap::new()),
    });
    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route(
            "/{tenant}/{repo}/{*git_path}",
            axum::routing::get(git_http).post(git_http),
        )
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .layer(axum::middleware::from_fn(log_request))
        .with_state(state);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|error| OriginError::Http(error.to_string()))
}

async fn serve_browser_api_with_cache_dir_and_guard(
    listener: TcpListener,
    config: s3::S3BackendConfig,
    cache_root: PathBuf,
    cache_temp: Option<tempfile::TempDir>,
) -> Result<()> {
    fs::create_dir_all(&cache_root)?;
    info!(
        address = %listener.local_addr().map_err(|error| OriginError::Http(error.to_string()))?,
        cache_root = %cache_root.display(),
        "serving origin browser api"
    );
    let state = Arc::new(HttpState {
        cache_root,
        _cache_temp: cache_temp,
        config,
        repositories: Mutex::new(HashMap::new()),
    });
    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/api/repos", axum::routing::get(api_repositories))
        .route(
            "/api/repos/{tenant}/{repo}/refs",
            axum::routing::get(api_refs),
        )
        .route(
            "/api/repos/{tenant}/{repo}/tree",
            axum::routing::get(api_tree),
        )
        .route(
            "/api/repos/{tenant}/{repo}/blob",
            axum::routing::get(api_blob),
        )
        .layer(axum::middleware::from_fn(log_request))
        .with_state(state);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|error| OriginError::Http(error.to_string()))
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::NO_CONTENT
}

async fn log_request(request: axum::extract::Request, next: Next) -> axum::response::Response {
    let method = request.method().clone();
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status();
    let elapsed_ms = started.elapsed().as_millis();
    if status.is_server_error() {
        error!(%method, %path, %status, elapsed_ms, "http request failed");
    } else if status.is_client_error() {
        warn!(%method, %path, %status, elapsed_ms, "http request rejected");
    } else {
        info!(%method, %path, %status, elapsed_ms, "http request");
    }
    response
}

#[derive(Debug, Serialize)]
struct ApiRepositories {
    repositories: Vec<RepositoryCatalogEntry>,
}

#[derive(Debug, Serialize)]
struct ApiRefs {
    refs: Vec<GitRefEntry>,
}

#[derive(Debug, Deserialize)]
struct TreeQuery {
    #[serde(rename = "ref")]
    reference: Option<String>,
    path: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiTree {
    reference: String,
    path: String,
    entries: Vec<ApiTreeEntry>,
}

#[derive(Debug, Serialize)]
struct ApiTreeEntry {
    name: String,
    path: String,
    kind: String,
    oid: String,
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BlobQuery {
    #[serde(rename = "ref")]
    reference: Option<String>,
    path: String,
}

#[derive(Debug, Serialize)]
struct ApiBlob {
    reference: String,
    path: String,
    content: String,
}

async fn api_repositories(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
) -> axum::response::Response {
    api_result(async move {
        let catalog = rustfs_catalog(state.config.clone()).await?;
        let catalog = catalog.read().await?;
        info!(
            repositories = catalog.repositories.len(),
            "api listed repositories"
        );
        Ok(Json(ApiRepositories {
            repositories: catalog.repositories,
        }))
    })
    .await
}

async fn api_refs(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo)): axum::extract::Path<(String, String)>,
) -> axum::response::Response {
    api_result(async move {
        validate_repository_name(&repo)?;
        let repository = rustfs_repository(
            RepositoryScope::new(tenant.clone(), repo.clone(), "edek"),
            state.config.clone(),
        )
        .await?;
        let publication = repository
            .current_publication()
            .await?
            .ok_or_else(|| OriginError::Http("repository not found".into()))?;
        info!(
            tenant = %tenant,
            repo = %repo,
            refs = publication.refs.len(),
            "api listed refs"
        );
        Ok(Json(ApiRefs {
            refs: publication.refs,
        }))
    })
    .await
}

async fn api_tree(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo)): axum::extract::Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<TreeQuery>,
) -> axum::response::Response {
    api_result(async move {
        let reference = normalize_browser_ref(query.reference.as_deref());
        let path = normalize_browser_path(query.path.as_deref())?;
        let bare_repo = materialize_browser_repository(&state, &tenant, &repo).await?;
        let entries = list_git_tree(&bare_repo, &reference, &path)?;
        info!(
            tenant = %tenant,
            repo = %repo,
            reference = %reference,
            path = %path,
            entries = entries.len(),
            "api listed tree"
        );
        Ok(Json(ApiTree {
            reference,
            path,
            entries,
        }))
    })
    .await
}

async fn api_blob(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo)): axum::extract::Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<BlobQuery>,
) -> axum::response::Response {
    api_result(async move {
        let reference = normalize_browser_ref(query.reference.as_deref());
        let path = normalize_browser_path(Some(&query.path))?;
        let bare_repo = materialize_browser_repository(&state, &tenant, &repo).await?;
        let content = read_git_blob(&bare_repo, &reference, &path)?;
        info!(
            tenant = %tenant,
            repo = %repo,
            reference = %reference,
            path = %path,
            bytes = content.len(),
            "api read blob"
        );
        Ok(Json(ApiBlob {
            reference,
            path,
            content,
        }))
    })
    .await
}

async fn api_result<F, T>(future: F) -> axum::response::Response
where
    F: std::future::Future<Output = Result<Json<T>>>,
    T: Serialize,
{
    match future.await {
        Ok(response) => response.into_response(),
        Err(error) => {
            let status = match error {
                OriginError::UnsafePath(_) => axum::http::StatusCode::NOT_FOUND,
                OriginError::Conflict => axum::http::StatusCode::CONFLICT,
                OriginError::Http(_) => axum::http::StatusCode::BAD_REQUEST,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, error.to_string()).into_response()
        }
    }
}

struct HttpState {
    cache_root: PathBuf,
    _cache_temp: Option<tempfile::TempDir>,
    config: s3::S3BackendConfig,
    repositories: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl HttpState {
    fn repository_lock(&self, tenant: &str, repo: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!("{tenant}/{repo}");
        let mut repositories = self
            .repositories
            .lock()
            .expect("repository lock map mutex poisoned");
        repositories
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

async fn git_http(
    axum::extract::State(state): axum::extract::State<Arc<HttpState>>,
    axum::extract::Path((tenant, repo_segment, git_path)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    match git_http_inner(GitHttpRequest {
        state,
        tenant,
        repo_segment,
        git_path,
        query: raw_query.unwrap_or_default(),
        method,
        headers,
        body,
    })
    .await
    {
        Ok(response) => response,
        Err(error) => {
            let status = match error {
                OriginError::UnsafePath(_) => axum::http::StatusCode::NOT_FOUND,
                OriginError::Conflict => axum::http::StatusCode::CONFLICT,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, error.to_string()).into_response()
        }
    }
}

struct GitHttpRequest {
    state: Arc<HttpState>,
    tenant: String,
    repo_segment: String,
    git_path: String,
    query: String,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
}

async fn git_http_inner(request: GitHttpRequest) -> Result<axum::response::Response> {
    let repo_name = request
        .repo_segment
        .strip_suffix(".git")
        .ok_or_else(|| OriginError::UnsafePath(request.repo_segment.clone()))?;
    validate_path_segment(&request.tenant)?;
    validate_repository_name(repo_name)?;
    let repo_lock = request.state.repository_lock(&request.tenant, repo_name);
    let _guard = repo_lock.lock().await;
    let scope = RepositoryScope::new(request.tenant.clone(), repo_name, "edek");
    let repository = rustfs_repository(scope, request.state.config.clone()).await?;
    rustfs_catalog(request.state.config.clone())
        .await?
        .register(&request.tenant, repo_name)
        .await?;
    let tenant_root = safe_join(&request.state.cache_root, &request.tenant)?;
    fs::create_dir_all(&tenant_root)?;
    let bare_repo = safe_join(&tenant_root, &request.repo_segment)?;
    let materialized = repository
        .materialize_bare_repository_cached(&bare_repo)
        .await?;

    let path_info = format!("/{}/{}", request.repo_segment, request.git_path);
    let operation = git_operation(&path_info, &request.query);
    info!(
        tenant = %request.tenant,
        repo = %repo_name,
        operation,
        path_info = %path_info,
        method = %request.method,
        bytes = request.body.len(),
        "git http operation started"
    );
    let output = run_git_http_backend(
        &tenant_root,
        &path_info,
        &request.query,
        request.method,
        request.headers,
        request.body,
    )?;
    if path_info.ends_with("/git-receive-pack") {
        repository
            .publish_materialized_bare_repository(&bare_repo, materialized)
            .await?;
        info!(
            tenant = %request.tenant,
            repo = %repo_name,
            "git receive-pack published"
        );
    }
    let response = cgi_to_response(&output)?;
    info!(
        tenant = %request.tenant,
        repo = %repo_name,
        operation,
        status = %response.status(),
        "git http operation completed"
    );
    Ok(response)
}

fn run_git_http_backend(
    project_root: &Path,
    path_info: &str,
    query: &str,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Vec<u8>> {
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", path_str(project_root)?)
        .env("GIT_HTTP_EXPORT_ALL", "")
        .env("REQUEST_METHOD", method.as_str())
        .env("PATH_INFO", path_info)
        .env("QUERY_STRING", query)
        .env("CONTENT_LENGTH", body.len().to_string());
    if let Some(content_type) = headers.get(axum::http::header::CONTENT_TYPE) {
        command.env(
            "CONTENT_TYPE",
            content_type
                .to_str()
                .map_err(|error| OriginError::Http(error.to_string()))?,
        );
    }
    let output = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(&body)?;
            }
            child.wait_with_output()
        })?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(OriginError::Git {
            program: "git".into(),
            args: vec!["http-backend".into()],
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn cgi_to_response(bytes: &[u8]) -> Result<axum::response::Response> {
    let (header_bytes, body) = split_cgi_response(bytes)
        .ok_or_else(|| OriginError::Http("git http-backend response had no headers".into()))?;
    let headers = String::from_utf8_lossy(header_bytes);
    let mut status = axum::http::StatusCode::OK;
    let mut response_headers = BTreeMap::new();
    for line in headers.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("Status") {
            let code = value
                .trim()
                .split(' ')
                .next()
                .and_then(|code| code.parse::<u16>().ok())
                .ok_or_else(|| OriginError::Http(format!("invalid CGI status: {value}")))?;
            status = axum::http::StatusCode::from_u16(code)
                .map_err(|error| OriginError::Http(error.to_string()))?;
        } else {
            response_headers.insert(name.trim().to_string(), value.trim().to_string());
        }
    }
    let mut builder = axum::http::Response::builder().status(status);
    for (name, value) in response_headers {
        builder = builder.header(name, value);
    }
    builder
        .body(axum::body::Body::from(body.to_vec()))
        .map_err(|error| OriginError::Http(error.to_string()))
}

fn split_cgi_response(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (&bytes[..index], &bytes[index + 4..]))
        .or_else(|| {
            bytes
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|index| (&bytes[..index], &bytes[index + 2..]))
        })
}

fn git_operation(path_info: &str, query: &str) -> &'static str {
    if path_info.ends_with("/git-receive-pack") || query.contains("service=git-receive-pack") {
        "receive-pack"
    } else if path_info.ends_with("/git-upload-pack") || query.contains("service=git-upload-pack") {
        "upload-pack"
    } else {
        "metadata"
    }
}

fn normalize_browser_ref(value: Option<&str>) -> String {
    value
        .filter(|reference| !reference.trim().is_empty())
        .unwrap_or("refs/heads/main")
        .to_string()
}

fn validate_browser_ref(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('-')
        || value.contains("..")
        || value.contains(' ')
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(OriginError::UnsafePath(value.into()));
    }
    Ok(())
}

fn normalize_browser_path(value: Option<&str>) -> Result<String> {
    let Some(value) = value.filter(|path| !path.is_empty()) else {
        return Ok(String::new());
    };
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(OriginError::UnsafePath(value.into()));
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| OriginError::UnsafePath(value.into()))?,
            ),
            _ => return Err(OriginError::UnsafePath(value.into())),
        }
    }
    Ok(parts.join("/"))
}

async fn materialize_browser_repository(
    state: &HttpState,
    tenant: &str,
    repo: &str,
) -> Result<PathBuf> {
    validate_path_segment(tenant)?;
    validate_repository_name(repo)?;
    let repo_lock = state.repository_lock(tenant, repo);
    let _guard = repo_lock.lock().await;
    let repository = rustfs_repository(
        RepositoryScope::new(tenant.to_string(), repo.to_string(), "edek"),
        state.config.clone(),
    )
    .await?;
    if repository.current_publication().await?.is_none() {
        return Err(OriginError::Http("repository not found".into()));
    }
    let tenant_root = safe_join(&state.cache_root, tenant)?;
    fs::create_dir_all(&tenant_root)?;
    let bare_repo = safe_join(&tenant_root, &format!("{repo}.git"))?;
    repository
        .materialize_bare_repository_cached(&bare_repo)
        .await?;
    Ok(bare_repo)
}

fn list_git_tree(repo: &Path, reference: &str, path: &str) -> Result<Vec<ApiTreeEntry>> {
    validate_browser_ref(reference)?;
    let treeish = git_treeish(reference, path);
    let output = git_bytes_output(repo, ["ls-tree", "-z", "-l", &treeish])?;
    let mut entries = Vec::new();
    for raw in output.split(|byte| *byte == 0) {
        if raw.is_empty() {
            continue;
        }
        let line = String::from_utf8_lossy(raw);
        let (metadata, name) = line
            .split_once('\t')
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let mut fields = metadata.split_whitespace();
        let _mode = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let kind = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let oid = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        let size = fields
            .next()
            .ok_or_else(|| OriginError::Http(format!("invalid git tree line: {line}")))?;
        if fields.next().is_some() || !is_git_oid(oid) || !is_git_tree_kind(kind) {
            return Err(OriginError::Http(format!("invalid git tree line: {line}")));
        }
        let entry_path = if path.is_empty() {
            name.to_string()
        } else {
            format!("{path}/{name}")
        };
        entries.push(ApiTreeEntry {
            name: name.to_string(),
            path: entry_path,
            kind: kind.to_string(),
            oid: oid.to_string(),
            size: if size == "-" {
                None
            } else {
                Some(size.parse().map_err(|error| {
                    OriginError::Http(format!("invalid git tree size: {error}"))
                })?)
            },
        });
    }
    entries.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .reverse()
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(entries)
}

fn read_git_blob(repo: &Path, reference: &str, path: &str) -> Result<String> {
    validate_browser_ref(reference)?;
    if path.is_empty() {
        return Err(OriginError::UnsafePath(path.into()));
    }
    let treeish = git_treeish(reference, path);
    let kind = git_output(repo, ["cat-file", "-t", &treeish])?;
    if kind.trim() != "blob" {
        return Err(OriginError::Http(format!("path is not a blob: {path}")));
    }
    let size = git_output(repo, ["cat-file", "-s", &treeish])?
        .trim()
        .parse::<u64>()
        .map_err(|error| OriginError::Http(format!("invalid git blob size: {error}")))?;
    if size > BROWSER_BLOB_LIMIT_BYTES {
        return Err(OriginError::Http(format!(
            "blob exceeds browser API limit of {BROWSER_BLOB_LIMIT_BYTES} bytes"
        )));
    }
    let output = git_bytes_output(repo, ["show", &treeish])?;
    if output.contains(&0) {
        return Err(OriginError::Http("binary blob cannot be displayed".into()));
    }
    String::from_utf8(output).map_err(|error| OriginError::Http(error.to_string()))
}

fn git_treeish(reference: &str, path: &str) -> String {
    if path.is_empty() {
        reference.to_string()
    } else {
        format!("{reference}:{path}")
    }
}

fn is_git_tree_kind(value: &str) -> bool {
    matches!(value, "blob" | "commit" | "tag" | "tree")
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrate::InMemoryBackend;

    #[tokio::test]
    async fn empty_repository_cache_does_not_create_authoritative_wal() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = Arc::new(ScopedStorage::new(test_scope("repo"), backend));
        let repo = OriginRepository::new(storage);
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.git");
        let second = temp.path().join("second.git");

        repo.materialize_bare_repository(&first).await.unwrap();
        repo.materialize_bare_repository(&second).await.unwrap();

        assert!(second.join("HEAD").exists());
        assert!(repo.current_publication().await.unwrap().is_none());
        assert!(repo.current_wal_index().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn push_persists_a_wal_entry_before_it_is_acknowledged() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        git(&client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(client.join("README.md"), "hello from wal\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "initial"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();

        repo.publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();
        let index = repo.current_wal_index().await.unwrap().unwrap();
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.entries[0].sequence, 1);
        assert_eq!(index.entries[0].kind, GitWalEventKind::Push);
        let entry_bytes = ImmutableObjects::read(repo.storage().as_ref(), &index.entries[0].object)
            .await
            .unwrap();
        let event: GitWalEvent = serde_json::from_slice(&entry_bytes).unwrap();
        assert_eq!(event.state_digest(), index.entries[0].state_digest);

        let publication = repo.current_publication().await.unwrap().unwrap();

        let main = publication
            .refs
            .iter()
            .find(|git_ref| git_ref.name == "refs/heads/main")
            .expect("main ref should be recorded");
        assert!(is_git_oid(&main.target));
        assert!(publication
            .objects
            .iter()
            .any(|object| object.kind == "commit"));
        assert!(publication
            .objects
            .iter()
            .any(|object| object.kind == "tree"));
        assert!(publication
            .objects
            .iter()
            .any(|object| object.kind == "blob"));
        assert!(!publication.packs.is_empty());
        assert!(publication
            .objects
            .iter()
            .all(|object| object.location.pack_index < publication.packs.len()));
        assert!(publication.objects.iter().all(|object| {
            object.location.pack_offset < publication.packs[object.location.pack_index].pack.size()
                && !object.location.pack_name.is_empty()
                && object.location.packed_size > 0
        }));
        let blob = publication
            .objects
            .iter()
            .find(|object| object.kind == "blob")
            .expect("blob should be cataloged");
        let packed_bytes = repo.read_git_object_pack_range(&blob.oid).await.unwrap();
        assert_eq!(packed_bytes.len(), blob.location.packed_size as usize);
    }

    #[tokio::test]
    async fn second_push_appends_wal_entry_for_new_objects_and_materializes() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let restored = temp.path().join("restored.git");
        let clone = temp.path().join("clone");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        git(&client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(client.join("README.md"), "one\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "one"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();

        let materialized = repo
            .publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();
        let first = repo.current_publication().await.unwrap().unwrap();
        assert_eq!(first.packs.len(), 1);
        assert_eq!(first.wal_entries.len(), 1);
        assert_eq!(first.wal_entries[0].sequence, 1);
        let first_pack = first.packs[0].pack.object_id();

        fs::write(client.join("README.md"), "two\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "two"]).unwrap();
        git(&client, ["push", "origin", "main"]).unwrap();
        repo.publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();

        let second = repo.current_publication().await.unwrap().unwrap();
        assert_eq!(second.packs.len(), 2);
        assert_eq!(second.wal_entries.len(), 2);
        assert_eq!(second.wal_entries[1].sequence, 2);
        assert_eq!(second.wal_entries[1].state_digest, second.digest);
        assert!(matches!(
            repo.resolve_wal_entry(&second.wal_entries[1].entry_digest)
                .await
                .unwrap(),
            PublicationResolution::Applied(entry) if entry.state_digest == second.digest
        ));
        assert_eq!(
            repo.resolve_wal_entry("missing-entry").await.unwrap(),
            PublicationResolution::NotApplied
        );
        assert_eq!(second.packs[0].pack.object_id(), first_pack);
        assert!(second.objects.len() > first.objects.len());

        repo.materialize_bare_repository(&restored).await.unwrap();
        command(
            "git",
            [
                "clone",
                path_str(&restored).unwrap(),
                path_str(&clone).unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(clone.join("README.md")).unwrap(),
            "two\n"
        );
    }

    #[tokio::test]
    async fn unknown_wal_root_cas_is_resolved_by_reading_the_index() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        git(&client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(client.join("README.md"), "lost ack\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "initial"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();

        repo.force_next_wal_root_outcome_unknown();
        let materialized = repo
            .publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();
        assert!(matches!(materialized.expected, ExpectedRevision::Exact(_)));
        assert_eq!(
            repo.current_wal_index()
                .await
                .unwrap()
                .unwrap()
                .entries
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn compaction_is_a_replayable_wal_event() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let restored = temp.path().join("restored.git");
        let clone = temp.path().join("clone");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        git(&client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(client.join("README.md"), "compact me\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "initial"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();
        repo.publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();

        repo.compact_wal().await.unwrap().unwrap();
        let index = repo.current_wal_index().await.unwrap().unwrap();
        assert_eq!(index.entries.len(), 2);
        assert_eq!(index.entries[1].kind, GitWalEventKind::Compaction);

        repo.materialize_bare_repository(&restored).await.unwrap();
        command(
            "git",
            [
                "clone",
                path_str(&restored).unwrap(),
                path_str(&clone).unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(clone.join("README.md")).unwrap(),
            "compact me\n"
        );
    }

    #[tokio::test]
    async fn corrupt_local_cache_is_repaired_from_wal() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let clone = temp.path().join("clone");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        git(&client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(client.join("README.md"), "repair me\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "initial"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();
        repo.publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();

        fs::remove_dir_all(bare.join("objects")).unwrap();
        repo.materialize_bare_repository_cached(&bare)
            .await
            .unwrap();
        command(
            "git",
            ["clone", path_str(&bare).unwrap(), path_str(&clone).unwrap()],
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(clone.join("README.md")).unwrap(),
            "repair me\n"
        );
    }

    #[tokio::test]
    async fn local_cache_with_stale_refs_is_repaired_from_wal() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let bare = temp.path().join("origin.git");
        let clone = temp.path().join("clone");
        let client = temp.path().join("client");

        let materialized = repo.materialize_bare_repository(&bare).await.unwrap();
        command("git", ["init", path_str(&client).unwrap()]).unwrap();
        git(&client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&client, ["config", "user.name", "Agent"]).unwrap();
        git(&client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(client.join("README.md"), "restore my refs\n").unwrap();
        git(&client, ["add", "README.md"]).unwrap();
        git(&client, ["commit", "-m", "initial"]).unwrap();
        git(&client, ["branch", "-M", "main"]).unwrap();
        git(
            &client,
            ["remote", "add", "origin", path_str(&bare).unwrap()],
        )
        .unwrap();
        git(&client, ["push", "-u", "origin", "main"]).unwrap();
        repo.publish_materialized_bare_repository(&bare, materialized)
            .await
            .unwrap();

        fs::remove_file(bare.join("refs").join("heads").join("main")).unwrap();
        repo.materialize_bare_repository_cached(&bare)
            .await
            .unwrap();
        command(
            "git",
            ["clone", path_str(&bare).unwrap(), path_str(&clone).unwrap()],
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(clone.join("README.md")).unwrap(),
            "restore my refs\n"
        );
    }

    #[tokio::test]
    async fn concurrent_conflicting_pushes_linearize_one_winner() {
        let backend = Arc::new(InMemoryBackend::new());
        let first_repo = OriginRepository::new(Arc::new(ScopedStorage::new(
            test_scope("repo"),
            backend.clone(),
        )));
        let second_repo =
            OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let first_bare = temp.path().join("first.git");
        let second_bare = temp.path().join("second.git");
        let first_client = temp.path().join("first-client");
        let second_client = temp.path().join("second-client");

        let first_materialized = first_repo
            .materialize_bare_repository(&first_bare)
            .await
            .unwrap();
        let second_materialized = second_repo
            .materialize_bare_repository(&second_bare)
            .await
            .unwrap();
        command("git", ["init", path_str(&first_client).unwrap()]).unwrap();
        git(&first_client, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&first_client, ["config", "user.name", "Agent"]).unwrap();
        git(&first_client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(first_client.join("README.md"), "first\n").unwrap();
        git(&first_client, ["add", "README.md"]).unwrap();
        git(&first_client, ["commit", "-m", "first"]).unwrap();
        git(&first_client, ["branch", "-M", "main"]).unwrap();
        git(
            &first_client,
            ["remote", "add", "origin", path_str(&first_bare).unwrap()],
        )
        .unwrap();
        git(&first_client, ["push", "-u", "origin", "main"]).unwrap();

        command("git", ["init", path_str(&second_client).unwrap()]).unwrap();
        git(
            &second_client,
            ["config", "user.email", "agent@example.com"],
        )
        .unwrap();
        git(&second_client, ["config", "user.name", "Agent"]).unwrap();
        git(&second_client, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(second_client.join("README.md"), "second\n").unwrap();
        git(&second_client, ["add", "README.md"]).unwrap();
        git(&second_client, ["commit", "-m", "second"]).unwrap();
        git(&second_client, ["branch", "-M", "main"]).unwrap();
        git(
            &second_client,
            ["remote", "add", "origin", path_str(&second_bare).unwrap()],
        )
        .unwrap();
        git(&second_client, ["push", "-u", "origin", "main"]).unwrap();

        let (first_result, second_result) = tokio::join!(
            first_repo.publish_materialized_bare_repository(&first_bare, first_materialized),
            second_repo.publish_materialized_bare_repository(&second_bare, second_materialized)
        );
        assert_ne!(first_result.is_ok(), second_result.is_ok());
        let index = first_repo.current_wal_index().await.unwrap().unwrap();
        assert_eq!(index.entries.len(), 1);
    }

    #[tokio::test]
    async fn repository_scopes_are_isolated() {
        let backend = Arc::new(InMemoryBackend::new());
        let first = OriginRepository::new(Arc::new(ScopedStorage::new(
            test_scope("first"),
            backend.clone(),
        )));
        let second =
            OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("second"), backend)));
        let temp = tempfile::tempdir().unwrap();

        let materialized = first
            .materialize_bare_repository(temp.path().join("first.git"))
            .await
            .unwrap();
        first
            .publish_materialized_bare_repository(temp.path().join("first.git"), materialized)
            .await
            .unwrap();

        assert!(first.current_publication().await.unwrap().is_some());
        assert!(second.current_publication().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn catalog_root_stays_small_when_many_repositories_are_registered() {
        let backend = Arc::new(InMemoryBackend::new());
        let storage = Arc::new(ScopedStorage::new(
            StorageScope::new(
                TenantId::new(CATALOG_TENANT),
                DatasetId::new(CATALOG_DATASET),
                EncryptionDomainId::new(CATALOG_ENCRYPTION_DOMAIN),
            ),
            backend,
        ));
        let catalog = OriginCatalog::new(storage.clone());

        for index in 0..150 {
            catalog
                .register("tenant", &format!("repo-{index:04}"))
                .await
                .unwrap();
        }

        let read = catalog.read().await.unwrap();
        assert_eq!(read.repositories.len(), 150);
        let root = RootRegister::read(
            storage.as_ref(),
            &RootName::new("origin.repository.catalog.v1"),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            root.value().len() < 1024,
            "catalog root should contain a compact durable object ref"
        );
        assert!(serde_json::from_slice::<DurableObjectRef>(root.value()).is_ok());
    }

    #[tokio::test]
    async fn stale_materialized_repository_publish_is_rejected() {
        let backend = Arc::new(InMemoryBackend::new());
        let repo = OriginRepository::new(Arc::new(ScopedStorage::new(test_scope("repo"), backend)));
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.git");
        let second = temp.path().join("second.git");

        let first_materialized = repo.materialize_bare_repository(&first).await.unwrap();
        let second_materialized = repo.materialize_bare_repository(&second).await.unwrap();

        let first_result = repo
            .publish_materialized_bare_repository(&first, first_materialized)
            .await;
        assert!(first_result.is_ok());

        let second_result = repo
            .publish_materialized_bare_repository(&second, second_materialized)
            .await;
        assert!(matches!(second_result, Err(OriginError::Conflict)));
        assert_eq!(
            repo.current_wal_index()
                .await
                .unwrap()
                .unwrap()
                .entries
                .len(),
            1
        );
    }

    #[test]
    fn browser_blob_reader_rejects_binary_content() {
        let (temp, repo) = browser_test_repository("binary.bin", b"text\0binary");
        let error = read_git_blob(&repo, "HEAD", "binary.bin").unwrap_err();

        assert!(
            matches!(error, OriginError::Http(ref message) if message.contains("binary blob")),
            "unexpected error: {error}"
        );
        drop(temp);
    }

    #[test]
    fn browser_blob_reader_rejects_large_content() {
        let content = vec![b'x'; BROWSER_BLOB_LIMIT_BYTES as usize + 1];
        let (temp, repo) = browser_test_repository("large.txt", &content);
        let error = read_git_blob(&repo, "HEAD", "large.txt").unwrap_err();

        assert!(
            matches!(error, OriginError::Http(ref message) if message.contains("browser API limit")),
            "unexpected error: {error}"
        );
        drop(temp);
    }

    fn browser_test_repository(file_name: &str, content: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        command("git", ["init", path_str(&repo).unwrap()]).unwrap();
        git(&repo, ["config", "user.email", "agent@example.com"]).unwrap();
        git(&repo, ["config", "user.name", "Agent"]).unwrap();
        git(&repo, ["config", "commit.gpgSign", "false"]).unwrap();
        fs::write(repo.join(file_name), content).unwrap();
        git(&repo, ["add", file_name]).unwrap();
        git(&repo, ["commit", "-m", "test"]).unwrap();
        (temp, repo)
    }

    fn test_scope(name: &str) -> StorageScope {
        RepositoryScope::new("tenant", name, "edek").storage_scope()
    }
}
