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
- `make integration-test`: run RustFS-backed backend conformance and full-stack
  e2e tests.
- `make integration-down`: stop the local stack.
- `make clean-local`: stop the stack and remove local build/cache artifacts.

RustFS listens on `http://127.0.0.1:9000` with access key `durable` and secret
key `durable-secret`. The bucket is `durable-dev`; the compose image is pinned
by digest for reproducible local validation.

## Crates

- `durable-core`: scope-safe immutable objects and CAS root registers.
- `durable-s3`: S3/RustFS adapter for the core storage contracts.
- `durable-cache`: memory and filesystem read-through object cache.
- `durable-journal`: immutable journal pages plus CAS head and group commit.
- `durable-queue`: at-least-once queue with fenced claim tokens.
- `durable-worker`: small worker loop over the queue contract.
- `durable-retention`: scope-bound reader epochs with no-delete default.
- `durable-testkit`: deterministic model and fault injection helpers.
- `durable-conformance`: reusable backend conformance checks.
- `durable-e2e`: RustFS full-stack tests that compose the library layers.

## Maturity

This is iteration 1. APIs, file formats, crate boundaries, and internal
architecture are expected to change. RustFS validates the local S3 contract but
does not prove production object-store semantics.
