# ================================================================================================
# shared/config/logging_config.py - Structured Logging Configuration
# ================================================================================================

import logging
import logging.handlers
import structlog
from pathlib import Path
from typing import Dict, Any

def configure_logging(
    log_level: str = "INFO",
    log_format: str = "json",  # json, console
    log_file_path: Path = Path("./data/logs/smartcamera-ai.log"),
    log_max_size: str = "100MB",
    log_backup_count: int = 5
) -> None:
    """Configure structured logging with structlog"""
    
    # Create log directory
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert string size to bytes
    max_bytes = _parse_size(log_max_size)
    
    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=[
            logging.handlers.RotatingFileHandler(
                log_file_path,
                maxBytes=max_bytes,
                backupCount=log_backup_count,
                encoding='utf-8'
            ),
            logging.StreamHandler()
        ]
    )
    
    # Configure structlog
    if log_format == "json":
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ]
    else:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.dev.ConsoleRenderer()
        ]
    
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

def _parse_size(size_str: str) -> int:
    """Parse size string like '100MB' to bytes"""
    size_str = size_str.upper()
    if size_str.endswith('KB'):
        return int(size_str[:-2]) * 1024
    elif size_str.endswith('MB'):
        return int(size_str[:-2]) * 1024 * 1024
    elif size_str.endswith('GB'):
        return int(size_str[:-2]) * 1024 * 1024 * 1024
    else:
        return int(size_str)

# Logger instance for the application
logger = structlog.get_logger("smartcamera.ai")