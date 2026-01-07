# Job Orchestrator (Temporal-lite)

A fault-tolerant, lease-based job orchestration backend that guarantees correct execution under crashes, retries, concurrency, and timeouts — validated by an automated verification and benchmarking suite.

This project demonstrates how real production systems (Temporal, SQS, Kubernetes Jobs, Celery) are built from first principles.

## Resume-Ready Description

Built a fault-tolerant job orchestration backend with lease-based execution, crash recovery, concurrency-safe single-claim scheduling, retry/DLQ handling, idempotent completion, and execution timeouts; validated via an automated verification suite and benchmarked at ~360 jobs/sec with 20 concurrent workers.

## Key Features

### Core Job Execution
- Job creation, leasing, execution, completion
- Lease-based execution model (visibility timeout semantics)
- Worker polling API with lease tokens
- Graceful crash recovery via lease expiration

### Correctness Guarantees
- **At-least-once execution**
- **Exactly-once effects** via idempotency keys
- **Atomic single-claim leasing** (no double execution under concurrency)
- **Retry with exponential backoff**
- **Dead-Letter Queue (DLQ)** for poison jobs
- **Execution timeouts + heartbeat enforcement**

### Operational Safety
- Lease reaper for expired jobs
- Execution timeout enforcement for hung workers
- Deterministic recovery after crashes
- Clean restart from empty database

### Performance
- Scales with concurrent workers
- Benchmarked at ~360 jobs/sec with 20 workers

## Why This Project Matters

This is not a CRUD app. It demonstrates:

- Distributed systems thinking
- Concurrency control and race-condition safety
- Failure mode design (crashes, retries, duplicates, timeouts)
- Exactly-once semantics (the hardest problem in backend systems)
- Production-grade testing and verification

These are the same concerns handled by:
- Temporal / Cadence
- AWS SQS / Step Functions
- Kubernetes controllers
- Stripe / Uber / DoorDash backends

## Architecture Overview

### Components
- **API Server**: FastAPI control plane
- **Postgres**: Job state, leases, attempts, idempotency
- **Workers**: Poll, lease, heartbeat, complete jobs
- **Reaper**: Requeues expired leases
- **Verification Suite**: Proves correctness guarantees
- **Benchmark Harness**: Measures throughput

### Job State Machine
```
pending -> leased -> succeeded
        \
         pending (retry)
           \
            dlq
```

### Leasing Model
- Jobs are claimed atomically using DB transactions
- Each lease has a unique token and expiration
- Only the lease holder can heartbeat or complete
- Expired leases are requeued automatically

## Tech Stack
- Python 3.11
- FastAPI
- PostgreSQL
- SQLAlchemy (async)
- Alembic
- Docker + Docker Compose
- Makefile automation

## Project Structure

```bash
job_orchestrator/
├── Makefile
├── README.md
├── alembic.ini
├── docker/
│   └── docker-compose.yml
├── requirements.txt
├── scripts/
│   ├── benchmark.py
│   ├── run_alembic.py
│   ├── verify_e2e.py
│   ├── verify_execution_timeout.py
│   ├── verify_idempotency.py
│   ├── verify_lease_expiry.py
│   ├── verify_no_double_claim.py
│   └── verify_retry_dlq.py
├── tests/
├── worker_sdk/
└── app/
    ├── main.py
    ├── settings.py
    ├── api/
    │   ├── deps.py
    │   └── v1/
    │       ├── jobs.py
    │       ├── metrics.py
    │       └── workers.py
    ├── commands/
    │   ├── complete_job.py
    │   ├── fail_job.py
    │   ├── heartbeat.py
    │   └── lease_job.py
    ├── db/
    │   ├── migrations/
    │   ├── models.py
    │   └── session.py
    ├── domain/
    │   ├── errors.py
    │   └── states.py
    └── scheduler/
        └── dispatcher.py
```

## Quickstart (One-Command Demo)

```bash
make demo
```

This single command:
1. Resets the database
2. Builds and starts services
3. Runs migrations
4. Runs full verification suite
5. Runs performance benchmark

## Verification Suite (What Is Proven)

Run manually:
```bash
make verify
```

| Test | What It Proves |
|---|---|
| `verify_e2e.py` | Happy-path execution |
| `verify_lease_expiry.py` | Crash recovery via lease expiry |
| `verify_no_double_claim.py` | Atomic single-claim under concurrency |
| `verify_retry_dlq.py` | Retry logic + DLQ routing |
| `verify_idempotency.py` | Exactly-once completion effects |
| `verify_execution_timeout.py` | Hung worker detection |

**All tests pass from a fresh database.**

## Benchmark Results

Run:
```bash
make benchmark
```

**Sample Results (Local Machine):**
```
Concurrent Workers:   20
Total Jobs Processed: 2000
Processing Time:      ~5.5 seconds
Throughput:           ~360 jobs/second
```

This validates both correctness and scalability.

## Exactly-Once Semantics (Important)

The system guarantees:
1. **At-least-once execution**
2. **Exactly-once effects**

This is achieved using:
- Idempotency keys on completion
- A dedicated `job_completions` table
- First write wins, replays return the stored result

This is the same pattern used by Stripe and payment systems.

## Makefile Commands

```bash
make up         # build + start services
make down       # stop services
make reset      # stop + delete volumes
make verify     # run full verification suite
make benchmark  # performance benchmark
make demo       # reset -> up -> verify -> benchmark
make logs       # follow logs
```

## What Makes This "Senior-Level"
- Correctness under failure (crash, retry, duplicate requests)
- Concurrency-safe leasing
- Idempotent side-effects
- Execution timeout enforcement
- Deterministic verification suite
- Benchmark-driven performance analysis

This is **infrastructure engineering**, not app development.

## Author

Abishek Giri
