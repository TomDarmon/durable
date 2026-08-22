# durable monorepo

This repository is organized for two layers:

- `durable/`: reusable Rust durable-storage substrate.
- `origin/`: Origin Git engine built on top of `durable`.
- `gateway/`: Python API gateway in front of Origin.

Origin owns Git smart HTTP, WAL publication, cache materialization, and durable
storage correctness. The Python gateway owns externally-facing API concerns:
auth, tenant/repository policy, rate limits, validation, audit logs, metrics,
and control-plane error shape.

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
- `make gateway-install`: install the Python gateway and development tools.
- `make gateway-format-check`: check Python formatting.
- `make gateway-lint`: run Ruff on the gateway.
- `make gateway-typecheck`: run mypy on the gateway.
- `make gateway-test`: run gateway unit and fake-backend integration tests.
- `make gateway-e2e`: run real Git CLI e2e tests through the gateway against
  Origin/RustFS.
- `make gateway-dev`: run the gateway development server.
- `make clean-local`: remove local RustFS volumes and durable build artifacts.

Normal tests do not require RustFS. RustFS-backed tests are ignored in the normal
suite and run through `make integration-test` or `make origin-e2e-test`.
