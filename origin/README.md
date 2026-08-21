# Origin

`origin/` is the application layer for an Origin-like Git hosting service for
agents. It is built on top of the reusable substrate in `../durable`.

The current v1 stores the authoritative state of a bare Git serving repository
as immutable durable objects plus a small durable root. Local bare repositories
are rebuildable serving caches: real `git push` can write to one, Origin
publishes the resulting files to durable storage, and a fresh server cache can
be materialized later for `git clone`/fetch.
Each published root points at an Origin manifest containing the captured refs,
Git object catalog metadata, and the durable file objects needed to rebuild the
serving cache.

## Commands

- `make check`: format check, clippy with warnings denied, and tests.
- `make test`: normal tests.
- `make e2e-up`: start the local RustFS stack.
- `make e2e-test`: run ignored real-git e2e tests over RustFS.
- `make e2e-down`: stop the local stack.

The compose stack exposes Git HTTP servers at `http://127.0.0.1:9200` and
`http://127.0.0.1:9202`, a read-only browser API at
`http://127.0.0.1:9210`, and a Next.js/tRPC repository browser at
`http://127.0.0.1:9300`. The Git services share the same RustFS bucket and keep
discardable local bare-repository caches under `/var/lib/origin/cache`. The
browser API has its own discardable cache under `/var/lib/origin-ui-api/cache`.
Each service exposes `GET /healthz`, returning `204 No Content` when ready.

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

The default Git server filter is `origin=info,origin_server=info`; the default
browser API filter is `origin=info,origin_ui_api=info`. For cache-hit and
CAS-retry detail, restart the stack with:

```sh
ORIGIN_RUST_LOG=origin=debug ORIGIN_UI_API_RUST_LOG=origin=debug,origin_ui_api=info make e2e-up
```

The e2e tests are split by layer:

- `tests/durable_repository_e2e.rs`: durable publication/materialization without HTTP.
- `tests/smart_http_e2e.rs`: in-process Smart HTTP behavior with real Git clients, plus browser API reads from data published through Smart HTTP.
- `tests/docker_compose_e2e.rs`: Docker/RustFS service behavior, browser/UI API smoke, restarts, cache, and multi-service conflicts.

## Intended V1 Shape

- Store materialized Git server files as verified immutable durable objects.
- Publish refs and Git object metadata in an Origin-owned manifest.
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
