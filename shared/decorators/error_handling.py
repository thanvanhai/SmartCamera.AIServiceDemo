# shared/decorators/error_handling.py
import functools
import logging
import traceback
from typing import Any, Callable, Optional, Union, Dict, List
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

def handle_errors(
    default_return: Any = None,
    log_errors: bool = True,
    reraise: bool = False,
    ignored_exceptions: Optional[List[type]] = None,
    custom_handler: Optional[Callable] = None
):
    """
    Decorator để xử lý lỗi tự động
    
    Args:
        default_return: Giá trị trả về mặc định khi có lỗi
        log_errors: Có ghi log lỗi không
        reraise: Có raise lại exception không
        ignored_exceptions: Danh sách exception bỏ qua
        custom_handler: Handler tùy chỉnh cho lỗi
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                return _handle_exception(
                    e, func.__name__, args, kwargs,
                    default_return, log_errors, reraise,
                    ignored_exceptions, custom_handler
                )
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return _handle_exception(
                    e, func.__name__, args, kwargs,
                    default_return, log_errors, reraise,
                    ignored_exceptions, custom_handler
                )
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

def _handle_exception(
    exception: Exception,
    func_name: str,
    args: tuple,
    kwargs: dict,
    default_return: Any,
    log_errors: bool,
    reraise: bool,
    ignored_exceptions: Optional[List[type]],
    custom_handler: Optional[Callable]
) -> Any:
    """Internal exception handling logic"""
    
    # Check if exception should be ignored
    if ignored_exceptions and type(exception) in ignored_exceptions:
        if log_errors:
            logger.debug(f"🔇 Ignored exception in {func_name}: {exception}")
        return default_return
    
    # Use custom handler if provided
    if custom_handler:
        try:
            return custom_handler(exception, func_name, args, kwargs)
        except Exception as handler_error:
            logger.error(f"💥 Custom error handler failed: {handler_error}")
    
    # Log the error
    if log_errors:
        error_info = {
            'function': func_name,
            'exception_type': type(exception).__name__,
            'exception_message': str(exception),
            'args_count': len(args),
            'kwargs_keys': list(kwargs.keys()),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        logger.error(f"❌ Error in {func_name}: {exception}")
        logger.debug(f"📋 Error details: {error_info}")
        logger.debug(f"🔍 Traceback: {traceback.format_exc()}")
    
    # Reraise if requested
    if reraise:
        raise exception
    
    return default_return

# Specialized error handlers for common scenarios

def handle_network_errors(default_return: Any = False, timeout: float = 30.0):
    """Decorator cho network/HTTP errors"""
    import httpx
    
    network_exceptions = [
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.NetworkError,
        ConnectionError,
        TimeoutError
    ]
    
    def custom_handler(exception, func_name, args, kwargs):
        if isinstance(exception, httpx.TimeoutException):
            logger.warning(f"⏰ Network timeout in {func_name} (>{timeout}s)")
        elif isinstance(exception, httpx.ConnectError):
            logger.warning(f"🔌 Connection error in {func_name}")
        elif isinstance(exception, httpx.NetworkError):
            logger.warning(f"🌐 Network error in {func_name}: {exception}")
        else:
            logger.warning(f"📡 Network issue in {func_name}: {exception}")
        
        return default_return
    
    return handle_errors(
        default_return=default_return,
        log_errors=True,
        reraise=False,
        ignored_exceptions=[],
        custom_handler=custom_handler
    )

def handle_kafka_errors(default_return: Any = None):
    """Decorator cho Kafka errors"""
    from kafka.errors import KafkaError, KafkaTimeoutError
    
    kafka_exceptions = [KafkaError, KafkaTimeoutError]
    
    def custom_handler(exception, func_name, args, kwargs):
        if isinstance(exception, KafkaTimeoutError):
            logger.warning(f"⏰ Kafka timeout in {func_name}")
        elif isinstance(exception, KafkaError):
            logger.warning(f"📨 Kafka error in {func_name}: {exception}")
        
        return default_return
    
    return handle_errors(
        default_return=default_return,
        log_errors=True,
        reraise=False,
        custom_handler=custom_handler
    )

def handle_ai_model_errors(default_return: Any = None):
    """Decorator cho AI model errors"""
    
    def custom_handler(exception, func_name, args, kwargs):
        error_msg = str(exception).lower()
        
        if 'cuda' in error_msg or 'gpu' in error_msg:
            logger.error(f"🎮 GPU/CUDA error in {func_name}: {exception}")
        elif 'memory' in error_msg or 'oom' in error_msg:
            logger.error(f"💾 Memory error in {func_name}: {exception}")
        elif 'model' in error_msg:
            logger.error(f"🤖 Model error in {func_name}: {exception}")
        else:
            logger.error(f"🔬 AI processing error in {func_name}: {exception}")
        
        return default_return
    
    return handle_errors(
        default_return=default_return,
        log_errors=True,
        reraise=False,
        custom_handler=custom_handler
    )

def safe_async(default_return: Any = None):
    """Simple async error handler"""
    return handle_errors(
        default_return=default_return,
        log_errors=True,
        reraise=False
    )

def critical_operation(reraise: bool = True):
    """Cho các operation quan trọng - log nhưng vẫn raise"""
    return handle_errors(
        default_return=None,
        log_errors=True,
        reraise=reraise
    )

# Context manager cho error handling
class ErrorContext:
    """Context manager cho error handling trong block code"""
    
    def __init__(
        self, 
        operation_name: str,
        default_return: Any = None,
        log_errors: bool = True,
        reraise: bool = False
    ):
        self.operation_name = operation_name
        self.default_return = default_return
        self.log_errors = log_errors
        self.reraise = reraise
        self.start_time = None
        self.exception_occurred = None
    
    def __enter__(self):
        self.start_time = datetime.utcnow()
        if self.log_errors:
            logger.debug(f"🔄 Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        
        if exc_type is None:
            # Success
            if self.log_errors:
                logger.debug(f"✅ Operation completed: {self.operation_name} ({duration:.2f}s)")
            return True
        
        # Error occurred
        self.exception_occurred = exc_val
        
        if self.log_errors:
            logger.error(f"❌ Operation failed: {self.operation_name} ({duration:.2f}s) - {exc_val}")
            logger.debug(f"🔍 Traceback: {traceback.format_exc()}")
        
        # Return True to suppress exception, False to reraise
        return not self.reraise
    
    def get_result(self, success_value: Any = None):
        """Get result after context exit"""
        if self.exception_occurred:
            return self.default_return
        return success_value

# Usage examples:
"""
# Basic usage
@handle_errors(default_return=False)
async def risky_operation():
    # Code that might fail
    pass

# Network operations
@handle_network_errors(default_return=None)
async def api_call():
    # HTTP request code
    pass

# Critical operations
@critical_operation(reraise=True)
def important_task():
    # Must succeed or fail loudly
    pass

# Context manager
with ErrorContext("database_operation", default_return=[]) as ctx:
    data = fetch_from_database()
    result = ctx.get_result(data)
"""