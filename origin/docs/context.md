# Origin Context

Origin is a future application layer, not part of the durable substrate. Its job
is to provide Git/agent workflows using the storage guarantees already supplied
by `../durable`.

## Durable Capabilities To Rely On

- Verified immutable objects with domain-separated SHA-256 IDs.
- Scope-bound durable references.
- Opaque logical root revisions.
- Linearizable root compare-and-swap over RustFS/S3.
- Explicit ambiguous publication outcomes.
- Immutable journal pages with CAS head and group commit.
- Memory and disk object-cache tiers.
- At-least-once queue and worker runtime.
- Reader retention epochs with physical deletion disabled.
- Deterministic fault injection for model and failure tests.

## Origin V1 Boundaries

- `RepositoryScope`: maps organization/user + repository + encryption domain to
  durable `StorageScope`.
- `wal`: defines immutable push/compaction events, the WAL index, and stable
  digests.
- `git_cache`: owns local bare-repository creation, validation, Git CLI
  helpers, pack inspection, and cache markers.
- `OriginRepository`: appends durable WAL events, advances the WAL index root
  with CAS, resolves unknown CAS outcomes by rereading the root/index, and
  replays WAL state into disposable bare repos.
- `FetchView`: materializes or verifies the local cache against the current WAL
  index before serving `git-upload-pack`.
- `Maintenance`: records compaction as WAL events. The queue is not
  authoritative.

## First Tests To Write

- Create repository scope without publishing an empty authority record.
- Push one commit object and update one ref.
- Reject stale ref update.
- Treat lost root ACK as explicitly ambiguous and resolve by reading the root.
- Fetch after client/cache restart.
- Cross-repository object/ref access is rejected.
- Cache deletion does not lose repository data.
- Corrupt local cache state is repaired from the WAL before reads.
- A compaction WAL event can be replayed by a fresh cache.

## Important Constraints

- Do not expose S3 ETags as application revisions.
- Do not model Git OIDs inside the durable `substrate` crate.
- Do not add unsafe GC until durable publication permits and sweep proofs exist.
- Do not use queue delivery as the source of truth for refs or objects.
