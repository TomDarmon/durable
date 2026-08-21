.PHONY: check test integration-up integration-conformance integration-e2e integration-test integration-rustfs-persistence integration-down origin-check origin-test origin-e2e-up origin-e2e-test origin-e2e-down clean-local

check:
	$(MAKE) -C durable check
	$(MAKE) -C origin check

test:
	$(MAKE) -C durable test
	$(MAKE) -C origin test

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

clean-local:
	$(MAKE) -C durable clean-local
	$(MAKE) -C origin clean-local
