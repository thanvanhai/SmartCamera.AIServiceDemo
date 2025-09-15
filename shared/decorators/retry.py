# shared/decorators/retry.py
import asyncio
import logging
import functools
from typing import Callable, Any, Optional, List
from datetime import datetime
import random

logger = logging.getLogger(__name__)

def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    exceptions: Optional[List[type]] = None
):
    """
    Retry decorator với exponential backoff
    
    Args:
        max_attempts: Số lần thử tối đa
        delay: Thời gian delay ban đầu (giây)
        backoff: Hệ số nhân cho delay
        max_delay: Thời gian delay tối đa
        jitter: Thêm random vào delay để tránh thundering herd
        exceptions: Chỉ retry với các exception này (mặc định: tất cả)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry this exception
                    if exceptions and type(e) not in exceptions:
                        logger.debug(f"🚫 Not retrying exception type: {type(e).__name__}")
                        raise e
                    
                    # Don't sleep on last attempt
                    if attempt == max_attempts - 1:
                        break
                    
                    # Calculate delay
                    current_delay = min(delay * (backoff ** attempt), max_delay)
                    
                    # Add jitter
                    if jitter:
                        current_delay *= (0.5 + random.random() * 0.5)
                    
                    logger.warning(f"🔄 Retry {attempt + 1}/{max_attempts} for {func.__name__} "
                                 f"after {current_delay:.2f}s - Error: {e}")
                    
                    await asyncio.sleep(current_delay)
            
            # All attempts failed
            logger.error(f"❌ All {max_attempts} retry attempts failed for {func.__name__}")
            raise last_exception
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            import time
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if exceptions and type(e) not in exceptions:
                        logger.debug(f"🚫 Not retrying exception type: {type(e).__name__}")
                        raise e
                    
                    if attempt == max_attempts - 1:
                        break
                    
                    current_delay = min(delay * (backoff ** attempt), max_delay)
                    if jitter:
                        current_delay *= (0.5 + random.random() * 0.5)
                    
                    logger.warning(f"🔄 Retry {attempt + 1}/{max_attempts} for {func.__name__} "
                                 f"after {current_delay:.2f}s - Error: {e}")
                    
                    time.sleep(current_delay)
            
            logger.error(f"❌ All {max_attempts} retry attempts failed for {func.__name__}")
            raise last_exception
        
        # Return appropriate wrapper
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator