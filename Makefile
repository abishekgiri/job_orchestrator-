SHELL := /bin/bash
PYTHONPATH := .
COMPOSE := docker-compose -f docker/docker-compose.yml

.PHONY: up down reset migrate verify verify-ci bench logs

# --- Infrastructure ---
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down --remove-orphans

reset:
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) up -d --build

migrate:
	./scripts/run_alembic.py upgrade head

# --- Verification ---
# Full state-of-the-art verification suite
verify: reset migrate
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_e2e.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_lease_expiry.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_no_double_claim.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_retry_dlq.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_idempotency.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_execution_timeout.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_fairness.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_priority_aging.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_scheduling.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_outbox.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_observability.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/benchmark.py

# Faster verification for CI/CD environments
verify-ci: reset migrate
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_e2e.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_no_double_claim.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_retry_dlq.py
	PYTHONPATH=$(PYTHONPATH) ./scripts/verify_idempotency.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_outbox.py
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/verify_observability.py

# --- Utilities ---
bench:
	PYTHONPATH=$(PYTHONPATH) python3 ./scripts/benchmark.py

logs:
	$(COMPOSE) logs -f --tail=200
