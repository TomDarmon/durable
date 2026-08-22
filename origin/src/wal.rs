use crate::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use substrate::DurableObjectRef;

/// Repository state obtained by replaying the durable WAL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPublication {
    /// Stable format version.
    pub version: u32,
    /// Deterministic digest of captured refs, Git objects, and durable pack IDs.
    pub digest: String,
    /// Published refs captured from the bare repository.
    #[serde(default)]
    pub refs: Vec<GitRefEntry>,
    /// Git object catalog captured from the bare repository object database.
    #[serde(default)]
    pub objects: Vec<GitObjectEntry>,
    /// Immutable pack layout required to reconstruct and serve the repository.
    #[serde(default)]
    pub packs: Vec<GitPackEntry>,
    /// WAL entries replayed to produce this state.
    #[serde(default)]
    pub wal_entries: Vec<GitWalIndexEntry>,
}

/// Small root object referenced directly by the repository root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GitWalRootPointer {
    pub version: u32,
    pub entries: u64,
    pub digest: String,
    pub index: DurableObjectRef,
}

/// Authoritative ordered WAL index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitWalIndex {
    pub version: u32,
    pub digest: String,
    pub entries: Vec<GitWalIndexEntry>,
}

/// One immutable WAL entry named by the current WAL index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitWalIndexEntry {
    pub sequence: u64,
    pub kind: GitWalEventKind,
    pub object: DurableObjectRef,
    pub entry_digest: String,
    pub state_digest: String,
}

/// WAL event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitWalEventKind {
    Push,
    Compaction,
}

/// One immutable WAL event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "event")]
pub enum GitWalEvent {
    Push(GitPushWalEntry),
    Compaction(GitCompactionWalEntry),
}

/// WAL event produced by one accepted Git push.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPushWalEntry {
    pub version: u32,
    pub sequence: u64,
    pub base_digest: Option<String>,
    pub next_digest: String,
    pub refs: Vec<GitRefEntry>,
    pub objects: Vec<GitObjectEntry>,
    pub packs: Vec<GitPackEntry>,
    pub new_pack_indexes: Vec<usize>,
}

/// WAL event that rewrites replay state from a compacted durable snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCompactionWalEntry {
    pub version: u32,
    pub sequence: u64,
    pub compacted_through: u64,
    pub state_digest: String,
    pub refs: Vec<GitRefEntry>,
    pub objects: Vec<GitObjectEntry>,
    pub packs: Vec<GitPackEntry>,
}

/// One Git ref published through the WAL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRefEntry {
    /// Full ref name, for example `refs/heads/main`.
    pub name: String,
    /// Git object ID targeted by the ref.
    pub target: String,
}

/// One Git object published through the WAL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitObjectEntry {
    /// Git object ID.
    pub oid: String,
    /// Git object type, for example `commit`, `tree`, `blob`, or `tag`.
    pub kind: String,
    /// Uncompressed object size reported by Git.
    pub size: u64,
    /// Published location of this object in the replayed immutable pack layout.
    #[serde(default)]
    pub location: GitObjectLocation,
}

/// Location metadata for one Git object in the current immutable pack layout.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitObjectLocation {
    /// Index into `GitPublication::packs`.
    pub pack_index: usize,
    /// Stable pack name containing this object.
    #[serde(default)]
    pub pack_name: String,
    /// Byte offset of this object inside the pack.
    #[serde(default)]
    pub pack_offset: u64,
    /// Compressed object byte size reported by the pack index.
    #[serde(default)]
    pub packed_size: u64,
}

/// One immutable Git pack plus its index in durable storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPackEntry {
    /// Durable bytes of the `.pack` file.
    pub pack: DurableObjectRef,
    /// Durable bytes of the `.idx` file.
    pub index: DurableObjectRef,
    /// Git pack checksum/name reported by `git index-pack`.
    pub name: String,
    /// Number of cataloged objects assigned to this pack.
    pub objects: usize,
}

/// Result of resolving an uncertain WAL append by reading durable state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationResolution {
    /// The WAL entry is present in the authoritative index.
    Applied(GitWalIndexEntry),
    /// The repository root advanced, but not to an index containing this entry.
    NotApplied,
    /// The repository is still absent.
    RepositoryMissing,
}

impl GitWalEvent {
    pub(crate) fn sequence(&self) -> u64 {
        match self {
            Self::Push(push) => push.sequence,
            Self::Compaction(compaction) => compaction.sequence,
        }
    }

    pub(crate) fn kind(&self) -> GitWalEventKind {
        match self {
            Self::Push(_) => GitWalEventKind::Push,
            Self::Compaction(_) => GitWalEventKind::Compaction,
        }
    }

    pub(crate) fn state_digest(&self) -> &str {
        match self {
            Self::Push(push) => &push.next_digest,
            Self::Compaction(compaction) => &compaction.state_digest,
        }
    }
}

pub(crate) fn publication_digest(
    refs: &[GitRefEntry],
    objects: &[GitObjectEntry],
    packs: &[GitPackEntry],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"origin.git.publication.digest.v1\0");
    for git_ref in refs {
        hasher.update(b"ref\0");
        hasher.update(git_ref.name.as_bytes());
        hasher.update([0]);
        hasher.update(git_ref.target.as_bytes());
        hasher.update([0]);
    }
    for object in objects {
        hasher.update(b"object\0");
        hasher.update(object.oid.as_bytes());
        hasher.update([0]);
        hasher.update(object.kind.as_bytes());
        hasher.update([0]);
        hasher.update(object.size.to_string().as_bytes());
        hasher.update([0]);
        hasher.update(object.location.pack_index.to_string().as_bytes());
        hasher.update([0]);
    }
    for pack in packs {
        hasher.update(b"pack\0");
        hasher.update(pack.name.as_bytes());
        hasher.update([0]);
        hasher.update(pack.pack.object_id().to_hex().as_bytes());
        hasher.update([0]);
        hasher.update(pack.index.object_id().to_hex().as_bytes());
        hasher.update([0]);
        hasher.update(pack.objects.to_string().as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

pub(crate) fn git_wal_event_digest(event: &GitWalEvent) -> Result<String> {
    let bytes = serde_json::to_vec(event)?;
    let mut hasher = Sha256::new();
    hasher.update(b"origin.git.wal-entry.digest.v1\0");
    hasher.update(bytes);
    Ok(hex(&hasher.finalize()))
}

pub(crate) fn git_wal_index_digest(entries: &[GitWalIndexEntry]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"origin.git.wal-index.digest.v1\0");
    for entry in entries {
        hasher.update(entry.sequence.to_string().as_bytes());
        hasher.update([0]);
        hasher.update(format!("{:?}", entry.kind).as_bytes());
        hasher.update([0]);
        hasher.update(entry.object.object_id().to_hex().as_bytes());
        hasher.update([0]);
        hasher.update(entry.entry_digest.as_bytes());
        hasher.update([0]);
        hasher.update(entry.state_digest.as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
