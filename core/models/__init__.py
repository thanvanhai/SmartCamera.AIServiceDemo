# Core Models Package
"""
Core data models for the Smart Camera AI Service.
Contains detection models, results, and related data structures.
"""

from .detection_models import (
    DetectionResult,
    Detection,
    BoundingBox,
    BatchDetectionResult,
    InferenceStats,
    DetectionStatus
)

__all__ = [
    'DetectionResult',
    'Detection', 
    'BoundingBox',
    'BatchDetectionResult',
    'InferenceStats',
    'DetectionStatus'
]