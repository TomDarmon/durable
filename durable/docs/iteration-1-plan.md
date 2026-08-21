# Iteration 1 Implementation Plan

1. Preserve RFC-0001 and project policy documentation in the repository.
2. Build only meaningful crates: core, S3/RustFS, cache, journal, queue, worker,
   retention, testkit, and conformance.
3. Keep `durable-core` small: verified immutable objects, scope-bound durable
   references, raw ranges, and linearizable root CAS with opaque logical
   revisions.
4. Implement `durable-s3` as an honest RustFS/S3 adapter: conditional object
   creation for immutable blobs, private ETag CAS for root envelopes, logical
   UUID revisions, range reads, and distinguishable missing/precondition/backend
   errors.
5. Implement deterministic fault injection in `durable-testkit` for the failure
   points required by the plan, including outcome-unknown after commit.
6. Implement cache, journal, queue, worker, and retention as small protocols over
   the core contracts, with no Git/vector semantics and no unsafe GC.
7. Add one focused test per required behavior, using in-memory backends for fast
   correctness and RustFS for backend conformance.
8. Provide local Docker Compose and Make targets for `check`, unit tests, RustFS
   startup, integration tests, shutdown, and cleanup.
