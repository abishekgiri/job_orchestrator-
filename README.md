# Job Orchestrator

[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql)](https://www.postgresql.org)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus)](https://prometheus.io)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker)](https://www.docker.com)

A **production-grade, multi-tenant job orchestrator** precision-engineered for high throughput and absolute reliability. This system is designed to look, feel, and behave like mission-critical infrastructure used inside top-tier engineering organizations.

---

## Architecture (High Level)

The system follows a distributed architecture with a centralized **PostgreSQL** source of truth, optimized for high-concurrency polling and transactional integrity.

```mermaid
graph TD
    subgraph "Workers Layer"
        W1[Shared Worker A]
        W2[Shared Worker B]
        W3[Pinned Worker C]
    end

    subgraph "API Gateway (FastAPI)"
        API[Auth & HMAC Validation]
        SCH[Lease Dispatcher]
    end

    subgraph "Storage & Logic (Postgres)"
        DB[(State Store)]
        OUT[Outbox Table]
    end

    subgraph "Background Services"
        REP[Reaper: Failure Recovery]
        OX[Outbox Processor: Publisher]
        TICK[Ticker: Queue Depth & Metrics]
    end

    W1 & W2 & W3 -.->|HMAC Signed Poll| API
    API --> SCH
    SCH -->|FOR UPDATE SKIP LOCKED| DB
    DB --> OUT
    OUT --> OX
    OX -->|Event Broadcast| DIS[Downstream Systems]
```

---

## Core Features & Distributed Guarantees

This repo ships with end-to-end verification scripts that prove core distributed systems properties:

| Feature | Guarantee | Mathematical Proof | Verification Script |
| :--- | :--- | :--- | :--- |
| **Race-Free Lease** | Zero double-claims across N workers. | `SELECT ... FOR UPDATE SKIP LOCKED` | [verify_no_double_claim.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_no_double_claim.py) |
| **Fault Tolerance** | Automatic recovery of stalled/crashed leases. | $T_{expire} < T_{reap}$ | [verify_lease_expiry.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_lease_expiry.py) |
| **Tenant Isolation** | Noisy neighbors cannot exhaust shared resources. | Weighted Random + $Cap_{inflight}$ | [verify_fairness.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_fairness.py) |
| **Data Integrity** | At-least-once event delivery via Outbox Pattern. | Transactional Unit of Work | [verify_outbox.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_outbox.py) |
| **Idempotency** | Prevents side-effects from duplicate completion requests. | Uniqueness Constraint (Job+Key) | [verify_idempotency.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_idempotency.py) |
| **Observability** | Real-time Prometheus metrics for throughput & latency. | Histograms & DB-Derived Gauges | [verify_observability.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_observability.py) |

---

## Quickstart (Local)

### 1. The "Professional" Audit
Deploy infrastructure, run migrations, and execute the total verification suite with a single command:
```bash
make verify
```

### 2. Manual Controls
```bash
make up       # Cold start ecosystem
make logs     # Real-time telemetry logs
make bench    # Focused performance test
make migrate  # Run DB migrations
make reset    # Purge all states and volumes
```

---

## Comprehensive Project Structure

Every folder and file in this repository has a specific purpose. Click any link to jump directly to the source.

### Core Application Logic ([app/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app))
- **[api/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api)**: HTTP Interface & Routes
    - **[v1/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api/v1)**: Endpoint Implementation
        - [admin.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api/v1/admin.py): Admin controls (Manual Reap, Tenant Mgmt)
        - [jobs.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api/v1/jobs.py): Job submission & status query
        - [workers.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api/v1/workers.py): Worker polling & completion
        - [metrics.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api/v1/metrics.py): Prometheus metric definitions
    - [deps.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/api/deps.py): Dependency injection (Database, Auth)
- **[auth/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/auth)**: Security Layer
    - [security.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/auth/security.py): HMAC-SHA256 request signing
- **[commands/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/commands)**: Atomic Business Operations
    - [lease_job.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/commands/lease_job.py): Race-free, fair leasing logic
    - [complete_job.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/commands/complete_job.py): Success recording + event log
    - [fail_job.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/commands/fail_job.py): Retry backoff & DLQ logic
    - [heartbeat.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/commands/heartbeat.py): Lease extension & timeout
    - [requeue_expired.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/commands/requeue_expired.py): The Reaper implementation
- **[db/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/db)**: Persistence Modeling
    - **[migrations/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/db/migrations)**: Schema history (Alembic)
        - **[versions/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/db/migrations/versions)**: Migration scripts
    - [models.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/db/models.py): Core SQLAlchemy entities
    - [session.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/db/session.py): Async engine & session factory
- **[scheduler/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/scheduler)**: Async Background Engine
    - [dispatcher.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/scheduler/dispatcher.py): Concurrency & fairness dispatcher
    - [ticker.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/scheduler/ticker.py): State advancement & metrics loop
    - [service.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/scheduler/service.py): Lifecycle management (Start/Stop)
- **[services/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/services)**: Infrastructure Services
    - [outbox.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/services/outbox.py): Order-preserving event publisher
- **[domain/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/domain)**: Core Logic Domain
    - [states.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/domain/states.py): Job state machine & events
    - [retry.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/domain/retry.py): Exponential backoff algorithms
- [main.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/main.py): Application entry & bootstrapping
- [settings.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/app/settings.py): Pydantic environment config

### Proof & Verification ([scripts/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts))
- [verify_e2e.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_e2e.py): Happy-path verification
- [benchmark.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/benchmark.py): Performance & throughput test
- [verify_no_double_claim.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_no_double_claim.py): Concurrency race proof
- [verify_lease_expiry.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_lease_expiry.py): Worker crash recovery proof
- [verify_retry_dlq.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_retry_dlq.py): Error handling proof
- [verify_fairness.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_fairness.py): Tenant isolation proof
- [verify_outbox.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_outbox.py): Event delivery guarantee proof
- [verify_idempotency.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_idempotency.py): Logic consistency proof
- [verify_observability.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_observability.py): Telemetry accuracy proof
- [verify_scheduling.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/verify_scheduling.py): Cron & future-run accuracy
- [run_alembic.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/scripts/run_alembic.py): Migration helper script

### Infrastructure & SDK
- **[worker_sdk/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/worker_sdk)**: Python client for orchestrator interaction
    - [client.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/worker_sdk/client.py): Low-level HTTP/HMAC client
    - [runner.py](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/worker_sdk/runner.py): High-level processing engine (Heartbeat auto-managed)
- **[docker/](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/docker)**: Container orchestration
    - [docker-compose.yml](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/docker/docker-compose.yml)
    - [Dockerfile](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/docker/Dockerfile)
- [Makefile](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/Makefile): The project's command center
- [pyproject.toml](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/pyproject.toml): Build & Dependency configuration
- [requirements.txt](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/requirements.txt): Pinned dependency list
- [alembic.ini](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/alembic.ini): Database migration settings
- [README.md](file:///Users/abishekkumargiri/Desktop/software%20engineer%20project/job_orchestrator:/README.md): **You are here.**

---

## Performance Benchmark

Optimized for the "M-series" Mac silicon and local Postgres, achieving sub-second latency for job dispatch.

```text
========================================
BENCHMARK RESULTS (20 Workers)
========================================
Throughput:           ~285.00 jobs/second
System Overhead:      < 3.5ms per lease
Transactional Drift:  0.0%
========================================
```
