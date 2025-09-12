"""
Status types enumeration for the Smart Camera AI Service.
"""

from enum import Enum, IntEnum


class ProcessingStatus(str, Enum):
    """Processing status for jobs/tasks"""
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class WorkerStatus(str, Enum):
    """AI Worker status"""
    STARTING = "starting"
    HEALTHY = "healthy"
    BUSY = "busy"
    OVERLOADED = "overloaded"
    ERROR = "error"
    STOPPING = "stopping"
    STOPPED = "stopped"


class Priority(IntEnum):
    """Task priority levels"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3
    EMERGENCY = 4


class DeviceType(str, Enum):
    """Hardware device types"""
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    NPU = "npu"


class LogLevel(str, Enum):
    """Logging levels"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"