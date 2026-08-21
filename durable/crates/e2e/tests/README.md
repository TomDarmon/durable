# RustFS E2E Tests

These tests are ignored by the normal unit-test suite and run through
`make integration-e2e` or `make integration-test`.

Backend conformance lives in
`durable/crates/conformance/tests/rustfs_integration.rs`. Those tests answer
"does RustFS satisfy the storage contract the library needs?"

This crate answers "do the durable library layers compose correctly when the
backing store is real RustFS?"

## Test Files

- `object_storage_cache.rs`: immutable object writes, verified full reads,
  range reads, memory cache refill, disk cache refill, cache deletion safety,
  missing root reads, and object recovery after a RustFS restart.
- `worker_journal_root.rs`: queue delivery, worker execution, journal append,
  job acknowledgement, and root publication over RustFS.
- `full_stack_restart.rs`: retention no-delete behavior, object storage, cache,
  journal scan, root publication, and object recovery after a RustFS restart.
- `support/`: shared assertions and scenario plumbing. Test intent should stay
  in the top-level files; only reusable setup belongs here.
