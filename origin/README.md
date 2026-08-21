# Origin

`origin/` is the application layer for a future Origin-like Git hosting service
for agents. It should be built on top of the reusable substrate in `../durable`.

No Origin implementation exists yet. This folder only records context and
boundaries so the next build goal can start cleanly.

## Intended V1 Shape

- Store Git objects as verified immutable durable objects.
- Publish repository refs through durable root compare-and-swap.
- Use durable scopes to isolate tenants, repositories, and encryption domains.
- Use durable cache for discardable object/pack reads.
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
