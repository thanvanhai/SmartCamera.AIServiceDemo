"""
AI Worker Inference Engine Package

This package contains the refactored inference engine components:
- InferenceEngine: Main orchestrator
- JobProcessor: Job processing logic
- KafkaHandler: Kafka operations
- PerformanceMonitor: System monitoring
- MetricsCollector: Metrics handling
- InferenceJob: Job data structure
"""

from .inference_job import InferenceJob
from .metrics_collector import MetricsCollector, MockMetrics
from .performance_monitor import PerformanceMonitor
from .kafka_handler import KafkaHandler
from .job_processor import JobProcessor
from .inference_engine import InferenceEngine

__version__ = "1.0.0"

__all__ = [
    "InferenceEngine",
    "JobProcessor", 
    "KafkaHandler",
    "PerformanceMonitor",
    "MetricsCollector",
    "MockMetrics",
    "InferenceJob"
]