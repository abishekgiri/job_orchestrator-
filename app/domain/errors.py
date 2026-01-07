class JobError(Exception):
    """Base exception for job orchestrator errors."""
    pass

class TenantError(JobError):
    pass

class ConfigurationError(JobError):
    pass

class JobNotFoundError(JobError):
    def __init__(self, job_id):
        super().__init__(f"Job {job_id} not found")

class InvalidJobStateError(JobError):
    def __init__(self, current_status, target_status):
        super().__init__(f"Cannot transition from {current_status} to {target_status}")

class LeaseError(JobError):
    pass

class LeaseExpiredError(LeaseError):
    pass

class LeaseNotFoundError(LeaseError):
    pass
