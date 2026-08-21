RUSTFS_ENDPOINT ?= http://127.0.0.1:9000
RUSTFS_BUCKET ?= durable-dev
RUSTFS_ACCESS_KEY ?= durable
RUSTFS_SECRET_KEY ?= durable-secret

.PHONY: check test integration-up integration-conformance integration-e2e integration-test integration-rustfs-persistence integration-down clean-local

check:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace

test:
	cargo test --workspace

integration-up:
	docker compose up -d rustfs rustfs-init

integration-conformance:
	DURABLE_RUSTFS_ENDPOINT=$(RUSTFS_ENDPOINT) \
	DURABLE_RUSTFS_BUCKET=$(RUSTFS_BUCKET) \
	DURABLE_RUSTFS_ACCESS_KEY=$(RUSTFS_ACCESS_KEY) \
	DURABLE_RUSTFS_SECRET_KEY=$(RUSTFS_SECRET_KEY) \
	cargo test -p conformance --test rustfs_integration -- --ignored --nocapture

integration-e2e:
	DURABLE_RUSTFS_ENDPOINT=$(RUSTFS_ENDPOINT) \
	DURABLE_RUSTFS_BUCKET=$(RUSTFS_BUCKET) \
	DURABLE_RUSTFS_ACCESS_KEY=$(RUSTFS_ACCESS_KEY) \
	DURABLE_RUSTFS_SECRET_KEY=$(RUSTFS_SECRET_KEY) \
	cargo test -p e2e --tests -- --ignored --nocapture

integration-test: integration-conformance integration-e2e

integration-rustfs-persistence:
	DURABLE_RUSTFS_ENDPOINT=$(RUSTFS_ENDPOINT) \
	DURABLE_RUSTFS_BUCKET=$(RUSTFS_BUCKET) \
	DURABLE_RUSTFS_ACCESS_KEY=$(RUSTFS_ACCESS_KEY) \
	DURABLE_RUSTFS_SECRET_KEY=$(RUSTFS_SECRET_KEY) \
	cargo test -p conformance --test rustfs_local_persistence -- --ignored --nocapture

integration-down:
	docker compose down

clean-local:
	docker compose down -v --remove-orphans
	cargo clean
