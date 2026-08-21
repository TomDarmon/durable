# Origin

`origin/` is the application layer for an Origin-like Git hosting service for
agents. It is built on top of the reusable substrate in `../durable`.

The current v1 stores the authoritative state of a bare Git serving repository
as immutable durable objects plus a small durable root. Local bare repositories
are rebuildable serving caches: real `git push` can write to one, Origin
publishes the resulting files to durable storage, and a fresh server cache can
be materialized later for `git clone`/fetch.

## Commands

- `make check`: format check, clippy with warnings denied, and tests.
- `make test`: normal tests.
- `make e2e-up`: start the local RustFS stack.
- `make e2e-test`: run ignored real-git e2e tests over RustFS.
- `make e2e-down`: stop the local stack.

## Intended V1 Shape

- Store materialized Git server files as verified immutable durable objects.
- Publish repository state through durable root compare-and-swap.
- Use durable scopes to isolate tenants, repositories, and encryption domains.
- Treat local bare repositories as discardable serving caches.
- Use durable queue and worker runtime for asynchronous maintenance jobs.
- Use durable journal only where Origin needs an ordered admission log.
- Keep safe retention/no-delete behavior until a correct GC protocol exists.

## Non-Goals For The First Origin Build

- No production multi-region failover.
- No unsafe garbage collection.
- No custom object-store adapter inside Origin.
- No vector-database semantics.
- No universal materializer framework.

Start by reading `docs/context.md`.
