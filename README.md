# durable monorepo

This repository is organized for two layers:

- `durable/`: reusable Rust durable-storage substrate.
- `origin/`: future Origin-like application layer built on top of `durable`.

The durable library is implemented and tested first. Origin is intentionally only
context and planning material right now; implementation should start in a new
goal.

## Commands

From the repository root:

- `make check`: run durable formatting, clippy, and normal tests.
- `make test`: run durable normal tests.
- `make integration-up`: start local RustFS for durable integration tests.
- `make integration-conformance`: run durable RustFS backend conformance tests.
- `make integration-e2e`: run durable full-stack e2e tests over RustFS.
- `make integration-test`: run both integration suites.
- `make integration-rustfs-persistence`: optional local Docker volume smoke test.
- `make integration-down`: stop local RustFS.
- `make origin-check`: run Origin formatting, clippy, and tests.
- `make origin-e2e-up`: start Origin's local RustFS stack.
- `make origin-e2e-test`: run Origin real-git e2e tests.
- `make origin-e2e-down`: stop Origin's local RustFS stack.
- `make clean-local`: remove local RustFS volumes and durable build artifacts.

Normal tests do not require RustFS. RustFS-backed tests are ignored in the normal
suite and run through `make integration-test` or `make origin-e2e-test`.
