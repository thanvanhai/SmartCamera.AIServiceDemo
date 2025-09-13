# /infrastructure/monitoring/metrics.py
import time
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict, deque
from dataclasses import dataclass
import threading

# Prometheus
from prometheus_client import start_http_server, Gauge


@dataclass
class ModelLoadMetric:
    """Metric cho việc load model"""
    model_name: str
    load_time_seconds: float
    timestamp: float
    success: bool = True
    error_message: Optional[str] = None


@dataclass
class ModelUsageMetric:
    """Metric cho việc sử dụng model"""
    model_name: str
    usage_count: int = 0
    total_inference_time: float = 0.0
    avg_inference_time: float = 0.0
    last_used: Optional[float] = None


@dataclass
class InferenceMetric:
    """Metric cho một lần inference"""
    model_name: str
    inference_time_ms: float
    input_size: tuple
    detection_count: int
    timestamp: float
    success: bool = True
    error_message: Optional[str] = None


class ModelMetrics:
    """Quản lý metrics cho AI models"""
    
    def __init__(self, max_history: int = 1000):
        self.logger = logging.getLogger(__name__)
        self.max_history = max_history
        self.lock = threading.Lock()
        
        # Storage for metrics
        self.load_metrics: Dict[str, ModelLoadMetric] = {}
        self.usage_metrics: Dict[str, ModelUsageMetric] = defaultdict(ModelUsageMetric)
        self.inference_history: deque = deque(maxlen=max_history)
        
        # Performance counters
        self.total_inferences = 0
        self.successful_inferences = 0
        self.failed_inferences = 0
        self.start_time = time.time()
    
    def record_model_load_time(self, model_name: str, load_time: float, success: bool = True, error: str = None):
        """Ghi lại thời gian load model"""
        with self.lock:
            metric = ModelLoadMetric(
                model_name=model_name,
                load_time_seconds=load_time,
                timestamp=time.time(),
                success=success,
                error_message=error
            )
            
            self.load_metrics[model_name] = metric
            
            if success:
                self.logger.info(f"📊 Model {model_name} loaded in {load_time:.3f}s")
            else:
                self.logger.error(f"📊 Model {model_name} load failed: {error}")
    
    def increment_model_usage(self, model_name: str):
        """Tăng counter sử dụng model"""
        with self.lock:
            if model_name not in self.usage_metrics:
                self.usage_metrics[model_name] = ModelUsageMetric(model_name=model_name)
            
            self.usage_metrics[model_name].usage_count += 1
            self.usage_metrics[model_name].last_used = time.time()
    
    def record_inference(self, model_name: str, inference_time_ms: float, 
                        input_size: tuple, detection_count: int, 
                        success: bool = True, error: str = None):
        """Ghi lại metrics cho một lần inference"""
        with self.lock:
            # Update counters
            self.total_inferences += 1
            if success:
                self.successful_inferences += 1
            else:
                self.failed_inferences += 1
            
            # Create inference metric
            metric = InferenceMetric(
                model_name=model_name,
                inference_time_ms=inference_time_ms,
                input_size=input_size,
                detection_count=detection_count,
                timestamp=time.time(),
                success=success,
                error_message=error
            )
            
            # Add to history
            self.inference_history.append(metric)
            
            # Update usage metrics
            if model_name not in self.usage_metrics:
                self.usage_metrics[model_name] = ModelUsageMetric(model_name=model_name)
            
            usage = self.usage_metrics[model_name]
            usage.total_inference_time += inference_time_ms
            usage.usage_count += 1
            usage.avg_inference_time = usage.total_inference_time / usage.usage_count
            usage.last_used = time.time()
    
    def get_model_stats(self, model_name: str) -> Dict[str, Any]:
        """Lấy statistics cho một model"""
        with self.lock:
            load_metric = self.load_metrics.get(model_name)
            usage_metric = self.usage_metrics.get(model_name)
            
            # Get inference history for this model
            model_inferences = [
                m for m in self.inference_history 
                if m.model_name == model_name
            ]
            
            recent_inferences = model_inferences[-10:] if model_inferences else []
            
            return {
                'model_name': model_name,
                'load_time': load_metric.load_time_seconds if load_metric else None,
                'load_success': load_metric.success if load_metric else None,
                'usage_count': usage_metric.usage_count if usage_metric else 0,
                'avg_inference_time_ms': usage_metric.avg_inference_time if usage_metric else 0,
                'total_inference_time_ms': usage_metric.total_inference_time if usage_metric else 0,
                'last_used': usage_metric.last_used if usage_metric else None,
                'recent_inferences': len(recent_inferences),
                'success_rate': self._calculate_success_rate(model_inferences)
            }
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """Lấy tổng quan statistics"""
        with self.lock:
            uptime = time.time() - self.start_time
            
            return {
                'uptime_seconds': uptime,
                'total_models_loaded': len(self.load_metrics),
                'total_inferences': self.total_inferences,
                'successful_inferences': self.successful_inferences,
                'failed_inferences': self.failed_inferences,
                'success_rate': self.successful_inferences / max(self.total_inferences, 1),
                'avg_inferences_per_second': self.total_inferences / max(uptime, 1),
                'models': list(self.usage_metrics.keys())
            }
    
    def get_top_models(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Lấy top models được sử dụng nhiều nhất"""
        with self.lock:
            sorted_models = sorted(
                self.usage_metrics.items(),
                key=lambda x: x[1].usage_count,
                reverse=True
            )
            
            return [
                {
                    'model_name': name,
                    'usage_count': metric.usage_count,
                    'avg_inference_time_ms': metric.avg_inference_time
                }
                for name, metric in sorted_models[:limit]
            ]
    
    def get_recent_inferences(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Lấy các inference gần đây nhất"""
        with self.lock:
            recent = list(self.inference_history)[-limit:] if self.inference_history else []
            
            return [
                {
                    'model_name': metric.model_name,
                    'inference_time_ms': metric.inference_time_ms,
                    'detection_count': metric.detection_count,
                    'success': metric.success,
                    'timestamp': metric.timestamp
                }
                for metric in reversed(recent)
            ]
    
    def _calculate_success_rate(self, inferences: List[InferenceMetric]) -> float:
        """Tính success rate cho một list inferences"""
        if not inferences:
            return 1.0
        
        successful = sum(1 for i in inferences if i.success)
        return successful / len(inferences)
    
    def reset_metrics(self):
        """Reset tất cả metrics"""
        with self.lock:
            self.load_metrics.clear()
            self.usage_metrics.clear()
            self.inference_history.clear()
            
            self.total_inferences = 0
            self.successful_inferences = 0
            self.failed_inferences = 0
            self.start_time = time.time()
            
            self.logger.info("📊 All metrics reset")
    
    def export_metrics(self) -> Dict[str, Any]:
        """Export tất cả metrics để lưu trữ"""
        with self.lock:
            return {
                'load_metrics': {
                    name: {
                        'model_name': metric.model_name,
                        'load_time_seconds': metric.load_time_seconds,
                        'timestamp': metric.timestamp,
                        'success': metric.success,
                        'error_message': metric.error_message
                    }
                    for name, metric in self.load_metrics.items()
                },
                'usage_metrics': {
                    name: {
                        'model_name': metric.model_name,
                        'usage_count': metric.usage_count,
                        'total_inference_time': metric.total_inference_time,
                        'avg_inference_time': metric.avg_inference_time,
                        'last_used': metric.last_used
                    }
                    for name, metric in self.usage_metrics.items()
                },
                'overall_stats': self.get_overall_stats(),
                'export_timestamp': time.time()
            }


# ================= Prometheus Exporter ================= #

_prometheus_initialized = False
GAUGE_TOTAL_INFERENCES = None
GAUGE_SUCCESSFUL_INFERENCES = None
GAUGE_FAILED_INFERENCES = None

def setup_prometheus_metrics(worker_name: str, port: int = 9090):
    """
    Khởi động Prometheus metrics exporter cho AI Worker.
    Dùng Prometheus scrape tại http://localhost:9090/metrics (port tuỳ chỉnh).
    """
    global _prometheus_initialized
    global GAUGE_TOTAL_INFERENCES, GAUGE_SUCCESSFUL_INFERENCES, GAUGE_FAILED_INFERENCES

    if _prometheus_initialized:
        return

    start_http_server(port)
    logging.getLogger(__name__).info(
        f"📊 Prometheus metrics server started for {worker_name} on port {port}"
    )
    _prometheus_initialized = True

    GAUGE_TOTAL_INFERENCES = Gauge(
        "ai_worker_total_inferences",
        "Tổng số inference đã chạy",
        ["worker"]
    )
    GAUGE_SUCCESSFUL_INFERENCES = Gauge(
        "ai_worker_successful_inferences",
        "Số inference thành công",
        ["worker"]
    )
    GAUGE_FAILED_INFERENCES = Gauge(
        "ai_worker_failed_inferences",
        "Số inference thất bại",
        ["worker"]
    )

    # Init values
    GAUGE_TOTAL_INFERENCES.labels(worker=worker_name).set(0)
    GAUGE_SUCCESSFUL_INFERENCES.labels(worker=worker_name).set(0)
    GAUGE_FAILED_INFERENCES.labels(worker=worker_name).set(0)


def update_prometheus_metrics(worker_name: str, metrics: "ModelMetrics"):
    """
    Cập nhật metrics từ ModelMetrics sang Prometheus Gauge
    """
    if not _prometheus_initialized:
        return

    GAUGE_TOTAL_INFERENCES.labels(worker=worker_name).set(metrics.total_inferences)
    GAUGE_SUCCESSFUL_INFERENCES.labels(worker=worker_name).set(metrics.successful_inferences)
    GAUGE_FAILED_INFERENCES.labels(worker=worker_name).set(metrics.failed_inferences)
