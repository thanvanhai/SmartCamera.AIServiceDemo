# shared/decorators/retry.py
import time
import functools
from typing import Callable, Any

def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    """
    Decorator to retry a function on failure.

    Args:
        max_retries (int): Number of retry attempts.
        delay (float): Delay between retries in seconds.
        exceptions (tuple): Exception types to catch for retry.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_retries:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator
