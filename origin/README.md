# Origin

`origin/` is the application layer for an Origin-like Git hosting service for
agents. It is built on top of the reusable substrate in `../durable`.

The current v1 stores the authoritative state of a bare Git serving repository
as an append-only WAL in S3-compatible durable storage. Each accepted push is
captured as an immutable WAL entry before Origin acknowledges it, and the small
durable root is advanced with compare-and-swap to point at the new WAL index.
Local bare repositories are rebuildable serving caches: real Git operations run
against normal bare repositories on disk, but fetch/clone first confirm that the
cache has caught up to the current WAL index. A missing or corrupt cache is
materialized again by replaying the WAL.

## Commands

- `make check`: format check, clippy with warnings denied, and tests.
- `make test`: normal tests.
- `make e2e-up`: start the local RustFS stack.
- `make e2e-test`: run ignored real-git e2e tests over RustFS.
- `make e2e-down`: stop the local stack.

The compose stack exposes Git HTTP engine instances at `http://127.0.0.1:9200`
and `http://127.0.0.1:9202`, the Python gateway at
`http://127.0.0.1:9400`, a read-only Origin metadata API at
`http://127.0.0.1:9210`, and a Next.js/tRPC webapp at
`http://127.0.0.1:9300`. The Git engine services share the same RustFS bucket
and keep discardable local bare-repository caches under `/var/lib/origin/cache`.
The metadata API has its own discardable cache under
`/var/lib/origin-ui-api/cache`. Each service exposes `GET /healthz`, returning
`204 No Content` when ready.

The webapp is intentionally a raw GitHub-like shell rather than a product suite:
it lists repositories, shows clone URLs that route through the gateway, browses
branches/refs/files, and displays gateway/webapp/Origin status. Push and clone
still go through the gateway; Origin still owns Git smart HTTP, WAL, and cache
correctness.

## Logs

Origin logs request timing and repository events with `tracing`. Git traffic and
UI browsing are split across services so their logs can be followed separately.
In the compose stack, follow the primary Git server with:

```sh
docker compose -f origin/docker-compose.yml logs -f origin
```

Use the alternate service name to watch the second server:

```sh
docker compose -f origin/docker-compose.yml logs -f origin-alt
```

Follow browser read-model calls with:

```sh
docker compose -f origin/docker-compose.yml logs -f origin-ui-api
```

Follow the Next.js/tRPC app with:

```sh
docker compose -f origin/docker-compose.yml logs -f origin-ui
```

Follow the Python gateway with:

```sh
docker compose -f origin/docker-compose.yml logs -f origin-gateway
```

The default Git server filter is `origin=info,origin_server=info`; the default
browser API filter is `origin=info,origin_ui_api=info`. For cache-hit and
CAS-retry detail, restart the stack with:

```sh
ORIGIN_RUST_LOG=origin=debug ORIGIN_UI_API_RUST_LOG=origin=debug,origin_ui_api=info make e2e-up
```

The e2e tests are split by layer:

- `tests/durable_repository_e2e.rs`: durable WAL replay/materialization without HTTP.
- `tests/smart_http_e2e.rs`: in-process Smart HTTP behavior with real Git clients, plus browser API reads and cross-service WAL catch-up from data published through Smart HTTP.
- `tests/docker_compose_e2e.rs`: Docker/RustFS service behavior, browser/UI API smoke, restarts, cache, and multi-service conflicts.

## Current V1 Shape

- Store immutable WAL entries and Git pack objects in durable storage.
- Publish repository visibility through a durable WAL index root compare-and-swap.
- Resolve ambiguous root-CAS acknowledgements by rereading the durable WAL index.
- Use durable scopes to isolate tenants, repositories, and encryption domains.
- Treat local bare repositories as discardable serving caches.
- Model compaction as a durable WAL event replayable by a fresh cache.
- Keep safe retention/no-delete behavior until a correct GC protocol exists.

## Non-Goals For The First Origin Build

- No production multi-region failover.
- No unsafe garbage collection.
- No custom object-store adapter inside Origin.
- No vector-database semantics.
- No universal materializer framework.

Start by reading `docs/context.md`.
