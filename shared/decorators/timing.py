import time
import functools
import logging


def time_execution(func):
    """
    Decorator to measure execution time of a function.
    Logs the execution time with the function's qualified name.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed_ms = (time.time() - start_time) * 1000
            logger = logging.getLogger(func.__module__)
            logger.debug(f"⏱️ {func.__qualname__} executed in {elapsed_ms:.2f} ms")

    return wrapper
