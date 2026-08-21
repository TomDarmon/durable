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

## Origin V1 Candidate Boundaries

- `RepositoryScope`: maps organization/user + repository + encryption domain to
  durable `StorageScope`.
- `GitObjectStore`: validates Git object bytes, then writes them as durable
  immutable objects with an Origin-specific `ObjectFormat::Custom`.
- `RefStore`: stores refs as a small root value and publishes with durable CAS.
- `PushTransaction`: validates expected refs, writes missing objects, then
  publishes the new refs root. It must surface conflicts and outcome-unknown
  states explicitly.
- `FetchView`: reads a strong refs root, enters a retention epoch, and serves
  referenced immutable objects through durable cache.
- `MaintenanceQueue`: schedules pack/index/cache jobs through durable queue and
  worker runtime. The queue is never authoritative.

## First Tests To Write

- Create repository scope and publish initial empty refs.
- Push one commit object and update one ref.
- Reject stale ref update.
- Treat lost root ACK as explicitly ambiguous and resolve by reading the root.
- Fetch after RustFS restart.
- Cross-repository object/ref access is rejected.
- Cache deletion does not lose repository data.

## Important Constraints

- Do not expose S3 ETags as application revisions.
- Do not model Git OIDs inside `durable-core`.
- Do not add unsafe GC until durable publication permits and sweep proofs exist.
- Do not use queue delivery as the source of truth for refs or objects.
