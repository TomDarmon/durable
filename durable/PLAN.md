Implement iteration 1 of a new Rust project called `durable`.

The architecture has already been researched and approved. Do not restart a general brainstorming phase. Read the RFC first, inspect the reference implementation, produce a concrete implementation plan, and then execute it end-to-end.

## Locations

Create the new project here:

/Users/tom.darmon/dev/durable

## Project status and engineering policy

This project is fully experimental. There are no compatibility commitments: APIs,
on-disk formats, configuration, crate boundaries, and internal architecture may be
changed or removed at any time. Prefer large refactors when they make the code more
readable or correct. Remove dead code rather than preserving it for hypothetical
future use.

The project must optimize for fast Rust iteration while retaining clear,
human-readable code. Use conventional, explicit designs; do not introduce broad
frameworks, speculative abstractions, generated boilerplate, or AI-style filler.

Testing policy:

- Prefer one focused, fast, real end-to-end test for each happy-path behavior over
  many overlapping unit tests.
- Use unit tests only for meaningful edge cases, failure classification, or logic
  that cannot be exercised clearly through an end-to-end test.
- End-to-end tests must exercise real component boundaries and real local services
  where applicable, including RustFS for storage integration.
- Use mocks only when the real dependency is impractical, nondeterministic, or the
  behavior under test is a narrowly scoped fault condition.
- Keep end-to-end tests fast and deterministic; use controlled barriers and fault
  injection instead of sleep-based timing assumptions.

Maintain these rules in `AGENT.md` at the repository root so future agents apply
them before making changes.

The approved RFC is here:

/tmp/origin/rfc.html

The existing vector implementation is available for reference here:

/Users/tom.darmon/dev/turbo-vector

Do not modify `turbo-vector`. Study it for its S3/GCS adapters, CAS queue, broker, Docker Compose setup, tests, caches, and failure modes.

Copy the RFC into:

/Users/tom.darmon/dev/durable/docs/RFC-0001.html

## Goal

Implement the reusable storage substrate only.

Do not implement Origin (cursor github alternative for agents (https://cursor.com/blog/git-at-any-scale) or a vector database in this iteration.

The result must be a compilable, tested Rust workspace with a fully local RustFS stack. It must validate the foundational claims of the RFC:

- Verified immutable objects.
- Scope isolation.
- Linearizable conditional root publication.
- Explicit ambiguous outcomes.
- A paged journal with group commit and bounded idempotency.
- Memory and disk object-cache tiers with replaceable policies.
- A reusable at-least-once queue and worker runtime.
- Deterministic fault injection.
- Backend conformance tests.
- Local integration tests against RustFS.

Prefer a small correct implementation over broad but shallow scaffolding.

## Required workspace

Use this structure unless concrete implementation constraints justify a small change:

durable/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── Makefile
├── docker-compose.yml
├── docs/
│   ├── RFC-0001.html
│   ├── implementation-notes.md
│   └── limitations.md
├── crates/
│   ├── substrate/
│   ├── s3/
│   ├── cache/
│   ├── journal/
│   ├── queue/
│   ├── worker/
│   ├── retention/
│   ├── testkit/
│   └── conformance/
└── tests/

Do not create empty crates just to match this tree. If a crate is not meaningfully implemented, omit it and explain why.

## Phase 1: inspect and validate RustFS

Before relying on RustFS, create a minimal conformance probe and verify:

- `If-None-Match: *` supports atomic create-if-absent.
- `If-Match` rejects a stale version.
- Two concurrent CAS operations from the same expected revision cannot both succeed.
- Rewriting identical logical bytes still produces a fresh logical `Revision`.
- Exact-key reads are immediately able to observe a successful write.
- Range reads return the correct bytes.
- Data survives a RustFS container restart with a persistent volume.
- A missing object, a precondition failure, and an unavailable backend are distinguishable.

Use a root envelope containing a random logical revision/nonce. Do not expose the S3 ETag as the public `Revision`.

If RustFS does not satisfy a required contract, do not fake the guarantee. Document the failure, keep the in-memory backend working, and implement the strongest honest RustFS adapter possible.

Use a pinned RustFS image version if a stable explicit version can be identified. Do not silently rely on `latest` without documenting it.

## Phase 2: substrate

Implement the smallest public core from the RFC.

Required concepts:

- `StorageScope`
- `TenantId`
- `DatasetId`
- `EncryptionDomainId`
- Fixed 32-byte substrate `ObjectId`
- Separate application-level IDs such as Git OIDs must not be modeled in core
- `ObjectFormat`
- `Durability`
- `DurabilityDomainId`
- Non-forgeable `DurableObjectRef`
- `ImmutableObjects`
- `RawRanges`
- `RootName`
- Opaque logical `Revision`
- `RootState`
- `ExpectedRevision`
- `PublishOutcome::{Applied, Conflict, OutcomeUnknown}`
- `RootRegister`

Use canonical encoding and domain-separated hashing:

H(domain_separator(format) || canonical_bytes)

`put` must verify the supplied ID while streaming.

A successful object write must return a non-forgeable, scope-bound `DurableObjectRef`.

All public storage handles and references must be scope-safe. It must be impossible to accidentally append or read a durable reference from another tenant or encryption domain.

Do not add:

- Generic transactions.
- Generic materializers.
- Universal manifests.
- Domain-specific Git or vector types.
- Cross-root atomic transactions.
- Cross-region failover claims.

## Phase 3: deterministic model and fault injection

Implement a synchronous deterministic reference model in `testkit`.

Do not implement the model by wrapping the production async code.

Support deterministic fault points including:

- Before object persistence.
- After object persistence before ACK.
- Before root condition evaluation.
- After root commit before ACK.
- After journal page persistence before head CAS.
- After journal head CAS before ACK.
- During cache fill.
- During queue ACK.
- During worker heartbeat.

Support fault actions including:

- Return unavailable.
- Return `OutcomeUnknown`.
- Drop response.
- Pause and resume at a deterministic barrier.
- Simulate process crash.
- Corrupt a cached read.

Every randomized test must report a reproducible seed and command trace.

## Phase 4: cache

Implement a cache stack bound to one immutable `StorageScope`.

Separate these responsibilities:

- `CacheStore`
- `LoadPolicy`
- `AdmissionPolicy`
- `EvictionPolicy`
- `CachedObjects`

Implement:

- Process-local memory tier.
- Node-local filesystem tier.
- Read-through loading from `ImmutableObjects`.
- Full-object loading.
- Raw range loading.
- Prefetch.
- Bypass.
- Local checksums for disk entries.
- Corrupt-entry eviction and authoritative reload.
- Size limits.
- At least one simple eviction policy such as LRU.
- At least one frequency-aware or size-aware policy if it can be implemented clearly.
- A policy interface allowing Origin or the vector engine to provide access hints later.

Cache keys must include:

- Private scope identity.
- Object ID.
- Representation.
- Extent or chunk/range.
- Required integrity level.

Verification provenance is generated by the loader, not supplied by callers.

Mutable roots must not use the immutable object cache.

A strong root read must always call `RootRegister`.

Do not implement distributed cache coherence.

## Phase 5: journal

Implement the journal as immutable pages plus a small CAS head.

Required types and behavior:

- `StreamId`
- `ClientRequestKey`
- Service-issued, non-forgeable `RequestToken`
- Bounded retry window
- `AppendRequest`
- `AppendReceipt`
- `AppendOutcome`
- `AppendResolution`
- Comparable but non-allocatable `JournalPosition`
- `JournalSnapshot`
- `CheckpointRef`
- `Journal`
- `JournalMaintenance`

A page header must include:

- Format version.
- Scope and stream identity.
- Previous page ID.
- First and last positions.
- Record count.
- Record framing.
- Records checksum.

Journal guarantees:

- Total order only inside one stream.
- Atomic batch visibility.
- No visible gaps.
- Durable records before head publication.
- Records are non-forgeable `DurableObjectRef` values.
- Same request token returns the same receipt during the retry window.
- Reusing a client key with a different digest is rejected.
- Expired request tokens are rejected and never execute again.
- Lost append ACK can be resolved or remains explicitly unknown.
- Notifications are not part of journal correctness.
- Scan before the retained checkpoint returns `SnapshotExpired`.

Implement group commit with an in-process broker:

- Bounded channel.
- Bounded batch size.
- Bounded batch delay.
- One immutable journal page per committed group.
- One root CAS per committed group.
- CAS retry against the latest head.
- Losing pages are harmless orphans.

Do not use one ever-growing JSON object as the journal.

## Phase 6: queue and worker runtime

Implement a reusable at-least-once queue and worker runtime as optional crates.

Required queue behavior:

- Enqueue.
- Claim.
- Claim token with generation.
- Heartbeat.
- ACK.
- Lease expiration.
- Retry with backoff.
- Maximum attempts.
- Permanent failure or dead-letter state.
- Stale workers cannot heartbeat or ACK after reassignment.
- Queue delivery is not an authoritative write log.

Required worker behavior:

- Configurable concurrency.
- Automatic heartbeat.
- Cancellation token.
- Resource budget passed to handlers.
- Retry policy.
- `JobDisposition::{Completed, Superseded, Retry, Reconcile, PermanentFailure}`.
- Metrics hooks or structured tracing.
- Explicit at-least-once handler contract.

The worker claim does not grant publication authority.

Do not implement `Materializer` or `UniversalCompactor`.

## Phase 7: retention

Implement the safe subset needed by future Origin and vector consumers.

Required behavior:

- Reader enters an epoch before reading a root.
- Epoch can be renewed and released.
- Expired epochs stop protecting data.
- A local cache pin is not a durable retention pin.
- Retention APIs are scope-bound.

If the complete GC publication-permit protocol is too large for this iteration, implement safe retention and a no-delete default. Do not add an unsafe mark-and-sweep implementation.

Physical deletion must remain disabled unless all of the following exist and are tested:

- Publication permits bound to scope, root, expected revision, new-value digest, object references, and GC epoch.
- `begin_sweep`.
- `close_sweep`.
- Waiting for outstanding publication permits.
- A scope-bound `ClosedSweep` proof.
- Deletion requiring that proof.
- `complete_sweep`.

It is acceptable for iteration 1 to retain orphaned objects and document GC as incomplete. It is not acceptable to implement unsafe reclamation.

## Phase 8: local stack

Provide a Docker Compose stack with:

- RustFS.
- Bucket initialization.
- Persistent RustFS volume.
- Optional Toxiproxy if useful.
- A conformance test runner if useful.

Provide simple commands:

make check
make test
make integration-up
make integration-test
make integration-down
make clean-local

`make check` must run formatting, clippy with warnings denied, and unit tests.

`make integration-test` must exercise RustFS rather than only an in-memory backend.

## Required tests

Implement focused tests for at least these scenarios:

- Identical object put is idempotent.
- Different bytes under the same ID are rejected.
- Cross-scope object access is rejected.
- Object hash mismatch is rejected.
- Range reads return exact expected bytes.
- Two root publishers race and exactly one wins.
- Same logical root bytes still receive a fresh revision after publication.
- Root commit succeeds but response is lost.
- Root conflict returns the actual current root.
- RustFS restart preserves objects and roots.
- Journal group commit batches concurrent appends.
- Journal never exposes a partial batch.
- Journal retry returns the original receipt.
- Client key reused with another digest is rejected.
- Expired request token is rejected.
- Journal scan detects corruption or a broken page chain.
- Lost queue notification does not affect journal state.
- Queue lease expires and another worker reclaims the job.
- Old worker heartbeat and ACK are rejected.
- Worker ACK loss causes safe re-execution.
- Memory cache hit avoids an authoritative object read.
- Disk cache survives process-local memory loss.
- Corrupt disk cache entry is evicted and reloaded.
- Cache scope isolation is enforced.
- Removing the entire cache does not lose durable data.

Use deterministic concurrency where possible. Avoid sleep-based race tests.

## Code quality

- Prefer small, explicit types over generic frameworks.
- Treat the workspace as experimental: make breaking simplifications and large
  refactors whenever they improve readability or correctness.
- Remove dead code promptly; do not retain compatibility shims or speculative
  extension points.
- Keep implementation code human-readable and avoid generated-looking boilerplate
  or generic AI-style abstractions.
- Keep distributed semantics visible in method names and result types.
- Do not use a generic `StorageError` for every failure.
- Distinguish missing, corrupt, unavailable, conflict, expired, fenced, and ambiguous outcomes.
- Use strict clippy settings.
- Add rustdoc for every public invariant-bearing type and method.
- Avoid unsafe Rust unless there is a compelling documented reason.
- Keep dependencies conservative.
- Do not optimize before tests establish correctness.
- Prefer a single fast, real end-to-end happy-path test per behavior. Add unit
  tests only for edge cases or behavior that cannot be tested meaningfully end to
  end.
- Use mocks only when they are justified by an impractical real dependency,
  nondeterminism, or a narrowly targeted fault condition.
- Do not copy large sections from `turbo-vector`; reuse ideas and rewrite cleanly.
- Do not modify `/Users/tom.darmon/dev/turbo-vector`.
- Do not implement Origin or vector-domain code yet.
- Do not commit or push unless explicitly requested.

## Documentation

Write:

README.md

It must explain setup, local commands, crate responsibilities, and current maturity.

docs/implementation-notes.md

It must map RFC decisions to concrete implementation choices and record any deviations.

docs/limitations.md

It must explicitly cover:

- RustFS is a local validation backend, not proof of production S3 semantics.
- No cross-region linearizability.
- No production failover authority.
- No automatic safe GC unless fully implemented.
- No exactly-once external side effects.
- No Git or vector semantics in the core.
- Cache is discardable.
- Queue is not a durable application log.

## Execution rules

First inspect the RFC and relevant parts of `turbo-vector`.

Then write a short implementation plan into:

/Users/tom.darmon/dev/durable/docs/iteration-1-plan.md

After writing the plan, execute it. Do not stop after planning.

Work autonomously through ordinary implementation problems. Ask only if a decision would materially change the approved architecture.

Run tests continuously.

Before finishing, run every required quality gate and integration test.

If Docker or RustFS is unavailable, complete all in-memory and fault-model work, record the exact blocker, and provide the exact command needed to resume integration testing.

## Completion report

Return:

- The implemented crate tree.
- Important API decisions.
- Deviations from RFC and why.
- Exact commands run.
- Unit and integration test results.
- RustFS CAS conformance results.
- Known limitations.
- What iteration 2 can now rely on.
- Any core abstraction that should be removed or changed before implementing Origin.

The iteration is complete only when the workspace builds, tests pass, the local stack is documented, and RustFS conformance has either passed or has a precise reproducible blocker.
