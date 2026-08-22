.PHONY: install format format-check lint typecheck test e2e run dev

PYTHON_BOOTSTRAP ?= python3
PYTHON ?= .venv/bin/python
HOST ?= 127.0.0.1
PORT ?= 9400

install:
	$(PYTHON_BOOTSTRAP) -m venv .venv
	$(PYTHON) -m pip install -e .[dev]

format:
	$(PYTHON) -m black origin_gateway tests e2e
	$(PYTHON) -m ruff check --fix origin_gateway tests e2e

format-check:
	$(PYTHON) -m black --check origin_gateway tests e2e

lint:
	$(PYTHON) -m ruff check origin_gateway tests e2e

typecheck:
	$(PYTHON) -m mypy origin_gateway

test:
	$(PYTHON) -m pytest tests

e2e:
	GATEWAY_E2E=1 $(PYTHON) -m pytest e2e -m e2e

run:
	$(PYTHON) -m uvicorn origin_gateway.main:app --host $(HOST) --port $(PORT)

dev:
	$(PYTHON) -m uvicorn origin_gateway.main:app --host $(HOST) --port $(PORT) --reload
