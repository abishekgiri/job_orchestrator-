import random
from datetime import datetime, timedelta

def calculate_next_run(
    attempts: int,
    base_delay_seconds: int = 10,
    max_delay_seconds: int = 3600,
    jitter: bool = True
) -> datetime:
    """
    Calculates the next run time using exponential backoff with optional jitter.
    
    Formula:
        delay = min(base * (2 ^ attempts), max_delay)
        if jitter:
            delay = delay + random_uniform(0, 0.1 * delay)
            
    Args:
        attempts: Number of attempts so far (starting from 0 or 1).
                  Usually if attempts=0, we want immediate execution, 
                  but this function calculates delay for the *next* retry after a failure.
                  So attempts=1 means "we failed once, when should we try again?"
                  
                  If attempts <= 0, we assume it's the first retry (delay = base).
                  
    Returns:
        datetime: The calculated future timestamp.
    """
    if attempts < 0:
        attempts = 0
        
    # 2^0 = 1, 2^1 = 2, 2^2 = 4...
    # If attempts is huge, pow might overflow or be huge. Cap attempts.
    # 2^20 is ~1 million seconds (~11 days), which likely hits max_delay.
    safe_attempts = min(attempts, 20)
    
    delay = base_delay_seconds * (2 ** safe_attempts)
    
    # Cap at max delay
    if delay > max_delay_seconds:
        delay = max_delay_seconds
        
    if jitter:
        # Add up to 10% jitter to avoid thundering herd
        jitter_amount = delay * 0.1
        delay += random.uniform(0, jitter_amount)
        
    return datetime.now() + timedelta(seconds=delay)
