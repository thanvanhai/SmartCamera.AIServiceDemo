"""
Metrics collection and handling for AI Worker inference engine
"""

import logging
from typing import Dict, Any, Optional
from infrastructure.monitoring.metrics import ModelMetrics


class MetricsCollector:
    """
    Handles metrics collection and reporting for inference engine
    """
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.logger = logging.getLogger(f"metrics_collector_{worker_id}")
        
        # Try to initialize real metrics, fallback to mock
        try:
            self.metrics = ModelMetrics(worker_id)
            self.logger.info("✅ Real metrics collector initialized")
        except Exception as e:
            self.logger.warning(f"⚠️ Real metrics unavailable, using mock: {e}")
            self.metrics = MockMetrics()

    def increment(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric"""
        try:
            if hasattr(self.metrics, 'increment'):
                if tags:
                    self.metrics.increment(name, value, tags=tags)
                else:
                    self.metrics.increment(name, value)
            else:
                # Fallback for simple increment
                self.metrics.increment(name, value)
        except Exception as e:
            self.logger.debug(f"Metrics increment error: {e}")

    def record(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a gauge/histogram metric"""
        try:
            if hasattr(self.metrics, 'record'):
                if tags:
                    self.metrics.record(name, value, tags=tags)
                else:
                    self.metrics.record(name, value)
            else:
                # Fallback for simple record
                self.metrics.record(name, value)
        except Exception as e:
            self.logger.debug(f"Metrics record error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics statistics"""
        try:
            if hasattr(self.metrics, 'get_stats'):
                return self.metrics.get_stats()
            return {}
        except Exception as e:
            self.logger.debug(f"Error getting metrics stats: {e}")
            return {}


class MockMetrics:
    """
    Mock metrics class for testing when ModelMetrics is not available
    """
    
    def __init__(self):
        self.counters = {}
        self.gauges = {}
    
    def increment(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric"""
        key = f"{name}_{tags}" if tags else name
        self.counters[key] = self.counters.get(key, 0) + value
    
    def record(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """Record a gauge metric"""
        key = f"{name}_{tags}" if tags else name
        self.gauges[key] = value
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics stats"""
        return {
            'counters': self.counters.copy(),
            'gauges': self.gauges.copy()
        }