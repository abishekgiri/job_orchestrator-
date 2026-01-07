.PHONY: up down migrate verify test-e2e test-expiry test-concurrency test-retry test-idempotency test-timeout benchmark demo

# --- Core Infrastructure ---
up:
	docker-compose -f docker/docker-compose.yml up -d --build

down:
	docker-compose -f docker/docker-compose.yml down

reset:
	docker-compose -f docker/docker-compose.yml down -v --remove-orphans

logs:
	docker-compose -f docker/docker-compose.yml logs -f --tail=100

migrate:
	./scripts/run_alembic.py upgrade head

# --- Verification Suite ---
# Run all verifications
verify: migrate verify-e2e test-expiry test-concurrency test-retry test-idempotency test-timeout

verify-e2e:
	PYTHONPATH=. ./scripts/verify_e2e.py

test-expiry:
	PYTHONPATH=. ./scripts/verify_lease_expiry.py

test-concurrency:
	PYTHONPATH=. ./scripts/verify_no_double_claim.py

test-retry:
	PYTHONPATH=. ./scripts/verify_retry_dlq.py

test-idempotency:
	PYTHONPATH=. ./scripts/verify_idempotency.py

test-timeout:
	PYTHONPATH=. python3 ./scripts/verify_execution_timeout.py

# --- Performance ---
benchmark:
	PYTHONPATH=. python3 ./scripts/benchmark.py

# --- The Perfect Demo ---
demo: reset up verify benchmark
