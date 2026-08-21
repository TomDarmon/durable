RUSTFS_ENDPOINT ?= http://127.0.0.1:9100
RUSTFS_BUCKET ?= origin-dev
RUSTFS_ACCESS_KEY ?= origin
RUSTFS_SECRET_KEY ?= origin-secret

.PHONY: check test e2e-up e2e-test e2e-down clean-local

check:
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	cargo test

test:
	cargo test

e2e-up:
	docker compose up --build -d rustfs rustfs-init origin origin-alt

e2e-test:
	DURABLE_RUSTFS_ENDPOINT=$(RUSTFS_ENDPOINT) \
	DURABLE_RUSTFS_BUCKET=$(RUSTFS_BUCKET) \
	DURABLE_RUSTFS_ACCESS_KEY=$(RUSTFS_ACCESS_KEY) \
	DURABLE_RUSTFS_SECRET_KEY=$(RUSTFS_SECRET_KEY) \
	cargo test --test git_cli_e2e -- --ignored --nocapture

e2e-down:
	docker compose down

clean-local:
	docker compose down -v --remove-orphans
	cargo clean
