.PHONY: check test integration-up integration-conformance integration-e2e integration-test integration-rustfs-persistence integration-down origin-check origin-test origin-e2e-up origin-e2e-test origin-e2e-down gateway-install gateway-format gateway-format-check gateway-lint gateway-typecheck gateway-test gateway-e2e gateway-run gateway-dev clean-local

check:
	$(MAKE) -C durable check
	$(MAKE) -C origin check
	$(MAKE) -C gateway format-check lint typecheck test

test:
	$(MAKE) -C durable test
	$(MAKE) -C origin test
	$(MAKE) -C gateway test

integration-up:
	$(MAKE) -C durable integration-up

integration-conformance:
	$(MAKE) -C durable integration-conformance

integration-e2e:
	$(MAKE) -C durable integration-e2e

integration-test:
	$(MAKE) -C durable integration-test

integration-rustfs-persistence:
	$(MAKE) -C durable integration-rustfs-persistence

integration-down:
	$(MAKE) -C durable integration-down

origin-check:
	$(MAKE) -C origin check

origin-test:
	$(MAKE) -C origin test

origin-e2e-up:
	$(MAKE) -C origin e2e-up

origin-e2e-test:
	$(MAKE) -C origin e2e-test

origin-e2e-down:
	$(MAKE) -C origin e2e-down

gateway-install:
	$(MAKE) -C gateway install

gateway-format:
	$(MAKE) -C gateway format

gateway-format-check:
	$(MAKE) -C gateway format-check

gateway-lint:
	$(MAKE) -C gateway lint

gateway-typecheck:
	$(MAKE) -C gateway typecheck

gateway-test:
	$(MAKE) -C gateway test

gateway-e2e:
	$(MAKE) -C origin e2e-up
	$(MAKE) -C gateway e2e

gateway-run:
	$(MAKE) -C gateway run

gateway-dev:
	$(MAKE) -C gateway dev

clean-local:
	$(MAKE) -C durable clean-local
	$(MAKE) -C origin clean-local
