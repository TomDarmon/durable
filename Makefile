.PHONY: check test integration-up integration-conformance integration-e2e integration-test integration-down clean-local

check:
	$(MAKE) -C durable check

test:
	$(MAKE) -C durable test

integration-up:
	$(MAKE) -C durable integration-up

integration-conformance:
	$(MAKE) -C durable integration-conformance

integration-e2e:
	$(MAKE) -C durable integration-e2e

integration-test:
	$(MAKE) -C durable integration-test

integration-down:
	$(MAKE) -C durable integration-down

clean-local:
	$(MAKE) -C durable clean-local
