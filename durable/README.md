# durable

`durable` is an experimental Rust workspace for object-store-native systems. It
implements the reusable substrate from RFC-0001: verified immutable objects,
scope-bound references, compare-and-swap root publication, a paged journal,
discardable object caches, an at-least-once queue, a worker runtime, safe
retention epochs, deterministic faults, and RustFS conformance tests.

The library intentionally contains no Git, Origin, or vector-database semantics.

## Local Commands

- `make check`: format check, clippy with warnings denied, and unit tests.
- `make test`: all default tests.
- `make integration-up`: start local RustFS and initialize the bucket.
- `make integration-conformance`: run the RustFS/S3 backend contract tests.
- `make integration-e2e`: run the RustFS-backed library-composition tests.
- `make integration-test`: run both backend conformance and e2e tests.
- `make integration-rustfs-persistence`: optional local Docker volume smoke test.
- `make integration-down`: stop the local stack.
- `make clean-local`: stop the stack and remove local build/cache artifacts.

RustFS listens on `http://127.0.0.1:9000` with access key `durable` and secret
key `durable-secret`. The bucket is `durable-dev`; the compose image is pinned
by digest for reproducible local validation.

## Crates

- `substrate`: scope-safe immutable objects and CAS root registers.
- `s3`: S3/RustFS adapter for the substrate storage contracts.
- `cache`: memory and filesystem read-through object cache.
- `journal`: immutable journal pages plus CAS head and group commit.
- `queue`: at-least-once queue with fenced claim tokens.
- `worker`: small worker loop over the queue contract.
- `retention`: scope-bound reader epochs with no-delete default.
- `testkit`: deterministic model and fault injection helpers.
- `conformance`: reusable backend conformance checks.
- `e2e`: RustFS full-stack tests that compose the library layers.

`conformance` checks that RustFS provides the object-store contract required by
the library. `e2e` checks the durable library itself across real component
boundaries: substrate, cache, journal, queue, worker, retention, and root
publication.

The normal integration suites model S3 behavior with fresh clients and
authoritative reads. They do not restart the backing bucket. The optional
RustFS persistence target restarts the local container only to check the Docker
volume setup.

## Maturity

This is iteration 1. APIs, file formats, crate boundaries, and internal
architecture are expected to change. RustFS validates the local S3 contract but
does not prove production object-store semantics.
