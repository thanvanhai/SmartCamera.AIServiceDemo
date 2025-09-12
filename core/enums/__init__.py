"""
Core enumerations package for the Smart Camera AI Service.
"""

from .model_types import ModelType, DetectionModelType
from .detection_types import DetectionType, AlertLevel, DetectionRegion
from .status_types import ProcessingStatus, WorkerStatus, Priority, DeviceType, LogLevel

__all__ = [
    # Model types
    'ModelType',
    'DetectionModelType',
    
    # Detection types
    'DetectionType',
    'AlertLevel', 
    'DetectionRegion',
    
    # Status types
    'ProcessingStatus',
    'WorkerStatus',
    'Priority',
    'DeviceType',
    'LogLevel'
]