# Origin Gateway

`origin-gateway` is the externally-facing Python API gateway for Origin. It keeps
product and API concerns outside the Rust Origin engine:

- The gateway owns authentication, authorization, tenant/repository policy,
  request validation, rate limits, API error shape, audit logs, metrics, and
  backend protection.
- The Rust Origin services continue to own Git smart HTTP execution, WAL
  publication, cache materialization, read catch-up, and durable storage
  correctness.
- Git traffic is proxied to Origin over HTTP. The gateway does not implement the
  Git smart HTTP protocol.

## API Shape

- `GET /healthz`: process liveness, returns `204`.
- `GET /readyz`: checks all configured Origin Git backends.
- `GET /metrics`: Prometheus-compatible metrics.
- `GET|POST /{tenant}/{repo}.git/{git_path}`: authenticated Git smart HTTP proxy.
- `GET /admin/repos`: authenticated admin repository listing, proxied to the
  configured Origin browser/control API.
- `GET /admin/repos/{tenant}/{repo}/refs`: authenticated admin refs metadata.
- `GET /admin/repos/{tenant}/{repo}/metadata`: alias for refs metadata.
- `POST /admin/repos/{tenant}/{repo}/compact`: returns `501` until Origin exposes
  a safe HTTP compaction trigger.

OpenAPI docs are available at `/docs` and `/openapi.json` for non-Git control
plane APIs.

## Configuration

Settings are centralized in `origin_gateway.config.GatewaySettings` and use the
`ORIGIN_GATEWAY_` environment prefix.

Required for a useful deployment:

- `ORIGIN_GATEWAY_ORIGIN_BACKENDS`: JSON array of Origin Git backend URLs.
- `ORIGIN_GATEWAY_AUTH_TOKENS`: JSON array of token principals, or
  `ORIGIN_GATEWAY_DEV_AUTH_TOKEN` for local development only.
- `ORIGIN_GATEWAY_CONTROL_BACKEND_URL`: Origin browser/control API URL for admin
  metadata endpoints.

Common settings:

- `ORIGIN_GATEWAY_ALLOWED_TENANTS`: optional JSON array tenant allow-list.
- `ORIGIN_GATEWAY_REQUEST_BODY_LIMIT_BYTES`: Git request body limit.
- `ORIGIN_GATEWAY_ADMIN_BODY_LIMIT_BYTES`: admin request body limit.
- `ORIGIN_GATEWAY_BACKEND_MAX_RETRIES`: conservative retry count for safe GETs.
- `ORIGIN_GATEWAY_CIRCUIT_BREAKER_FAILURES`: failures before a backend is
  temporarily avoided.
- `ORIGIN_GATEWAY_CIRCUIT_BREAKER_RESET_SECONDS`: backend cool-down duration.
- `ORIGIN_GATEWAY_RATE_LIMITS`: JSON object keyed by `git_receive_pack`,
  `git_upload_pack`, `git_metadata`, and `admin`.

Example token config:

```json
[
  {
    "token": "replace-with-a-long-random-secret",
    "actor": "ci",
    "tenants": ["tenant"],
    "repositories": ["*"],
    "scopes": ["git:read", "git:write", "metadata:read", "admin"]
  }
]
```

Local-only dev auth:

```sh
export ORIGIN_GATEWAY_DEV_AUTH_TOKEN=local-dev-token-123456
export ORIGIN_GATEWAY_DEV_AUTH_TENANTS=tenant
```

Dev auth is explicit and is not enabled unless the token variable is set.

## Local Run

From the repository root:

```sh
make gateway-install
make origin-e2e-up
ORIGIN_GATEWAY_ORIGIN_BACKENDS='["http://127.0.0.1:9200","http://127.0.0.1:9202"]' \
ORIGIN_GATEWAY_CONTROL_BACKEND_URL=http://127.0.0.1:9210 \
ORIGIN_GATEWAY_DEV_AUTH_TOKEN=local-dev-token-123456 \
ORIGIN_GATEWAY_DEV_AUTH_TENANTS='["tenant"]' \
make gateway-dev
```

Git against the gateway:

```sh
git clone http://local-dev:local-dev-token-123456@127.0.0.1:9400/tenant/example.git
```

Admin API example:

```sh
curl -H 'Authorization: Bearer local-dev-token-123456' \
  http://127.0.0.1:9400/admin/repos
```

## Quality Gates

```sh
make gateway-format-check
make gateway-lint
make gateway-typecheck
make gateway-test
make gateway-e2e
```

`make gateway-e2e` starts the Origin/RustFS compose stack and runs real `git`
CLI tests through the Python gateway.

## Security Notes And Threat Model

The gateway assumes it is the public ingress point and Origin backends are
private services. It validates tenant, repository, and Git path segments before
proxying; rejects path traversal; requires explicit operation scopes; rate-limits
by tenant, actor, repository, IP, and operation class; avoids logging secrets or
Git payloads; and emits audit events for auth decisions, allowed Git requests,
rejections, rate-limit decisions, and admin operations.

Primary threats covered here are credential guessing, unauthorized tenant/repo
access, accidental public exposure of unsafe Origin paths, large request body
abuse, excessive push/fetch/admin traffic, and backend failure amplification.
Remaining production work includes replacing static tokens with an external
identity provider, moving rate-limit state to a shared store for multi-gateway
deployments, terminating TLS at ingress, and adding a real Origin compaction
control endpoint before enabling compaction through the gateway.
