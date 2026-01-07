from enum import StrEnum, auto

class JobStatus(StrEnum):
    PENDING = auto()          # Created, waiting for lease
    LEASED = auto()           # Picked up by a worker
    RUNNING = auto()          # Worker has started execution (optional ack)
    SUCCEEDED = auto()        # Completed successfully
    FAILED_RETRYABLE = auto() # Failed, will be retried
    FAILED_FINAL = auto()     # Failed, no more retries
    CANCELED = auto()         # User canceled
    DLQ = auto()              # Dead letter queue (poison pill)

class JobEvent(StrEnum):
    CREATED = auto()
    LEASED = auto()
    LEASE_RENEWED = auto()
    STARTED = auto()
    COMPLETED = auto()
    FAILED = auto()
    RETRIED = auto()
    CANCELED = auto()
    DLQ_ROUTED = auto()
