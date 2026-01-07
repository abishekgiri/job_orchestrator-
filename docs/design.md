# System Design

## Architecture
The Job Orchestrator is a distributed system built on PostgreSQL as the state store and coordination mechanism. It uses a "leasing" model where workers acquire a time-limited lock on a job to process it.

## State Machine
Jobs transition through the following states:
1. **PENDING**: Job is created and waiting to be picked up.
2. **LEASED**: Worker has claimed the job. `available_at` is set to `now + lease_duration`.
3. **RUNNING**: (Implicit in LEASED, but `started_at` is tracked).
4. **SUCCEEDED**: Worker completed the job successfully.
5. **FAILED_RETRYABLE**: Worker reported failure, but `attempts < max_attempts`. Job moves back to `PENDING` with a backoff delay.
6. **DLQ** (Dead Letter Queue): Job failed more than `max_attempts` times.
7. **CANCELED**: User canceled the job.

## Leasing Algorithm
We use PostgreSQL's `FOR UPDATE SKIP LOCKED` to implement a high-performance, non-blocking queue.
- **Claim:** Worker selects a PENDING job where `available_at <= now()`.
- **Lock:** The row is locked. Other workers skip it (`SKIP LOCKED`).
- **Update:** The status changes to `LEASED`, `expires_at` is set, and a token is generated.
- **Return:** The job and lease token are returned to the worker.

This guarantees:
- **Atomicity:** Only one worker can claim a job.
- **Fairness:** Orders by `priority` DESC and `created_at` ASC.
- **Efficiency:** No spin-locking or "thundering herd" on a single row.

## Retry Policy
We use **Exponential Backoff with Jitter**.
- `delay = base_delay * (2 ^ attempts)`
- `jitter = random(0, 0.1 * delay)`
- `available_at = now() + delay`

This prevents synchronized retries from overwhelming the system.

## Exactly-Once vs At-Least-Once
The system guarantees **At-Least-Once** execution.
- If a worker crashes, the lease expires.
- The "Reaper" (or next poll loop) resets the job to `PENDING`.
- The job runs again.

**Idempotency** is handled at the API level (using `idempotency_key` on creation) and should be handled by workers for their side effects.

## Reaper Responsibility
A background process (or cron job) periodically scans for "zombie" leases where `expires_at < now()`.
1. It identifies jobs that have been LEASED for too long without completion or heartbeat.
2. It assumes the worker has crashed or network partitioned.
3. It resets the job status to `PENDING` (incrementing `attempts`).
4. This ensures that no job is lost due to worker failure (**At-Least-Once** guarantee).

## Fairness
- **Ordering:** The promptest jobs (`available_at` ASC) with highest priority (`priority` DESC) are served first.
- **Concurrency:** `SKIP LOCKED` ensures different workers get different jobs without contention.
- **Multi-tenancy (Planned):** Future improvements will add per-tenant rate limiting and round-robin scheduling to prevent one tenant from starving others.

## Dead Letter Queue (DLQ)
When a job exceeds its `max_attempts`:
1. It is marked with status `DLQ`.
2. It remains in the system for manual inspection.
3. It does not block other jobs.
4. Admin endpoints can be used to re-drive these jobs (reset to `PENDING`).
