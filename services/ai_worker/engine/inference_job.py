"""
InferenceJob data structure for AI Worker inference tasks
"""

import time
import numpy as np
from dataclasses import dataclass
from typing import Dict, Any

from core.enums import ModelType


@dataclass
class InferenceJob:
    """
    Represents a single inference job with all necessary data
    """
    job_id: str
    camera_id: str
    frame: np.ndarray
    timestamp: float
    model_type: ModelType
    config: Dict[str, Any]
    priority: int = 0

    def __post_init__(self):
        """Validate job data after initialization"""
        if not self.job_id:
            self.job_id = f"{self.camera_id}_{time.time()}"
        
        if self.timestamp <= 0:
            self.timestamp = time.time()
            
        if not isinstance(self.frame, np.ndarray):
            raise ValueError("Frame must be a numpy array")
            
        if self.frame.size == 0:
            raise ValueError("Frame cannot be empty")

    @property
    def age_seconds(self) -> float:
        """Get the age of this job in seconds"""
        return time.time() - self.timestamp

    @property
    def is_expired(self, max_age_seconds: float = 30.0) -> bool:
        """Check if job has expired based on age"""
        return self.age_seconds > max_age_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary (excluding frame data)"""
        return {
            'job_id': self.job_id,
            'camera_id': self.camera_id,
            'timestamp': self.timestamp,
            'model_type': self.model_type.value if hasattr(self.model_type, 'value') else str(self.model_type),
            'config': self.config,
            'priority': self.priority,
            'frame_shape': self.frame.shape,
            'age_seconds': self.age_seconds
        }