"""
Performance monitoring for AI Worker inference engine
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional

# Import một lần ở đầu file để kiểm tra sự tồn tại
try:
    import psutil
except ImportError:
    psutil = None

try:
    import pynvml
except ImportError:
    pynvml = None


class PerformanceMonitor:
    """
    Monitors system performance and collects metrics for the inference engine
    """
    
    def __init__(self, worker_id: str, metrics_collector, redis_client=None):
        self.worker_id = worker_id
        self.metrics = metrics_collector
        self.redis_client = redis_client
        self.logger = logging.getLogger(f"performance_monitor_{worker_id}")
        
        # State tracking
        self.start_time = time.time()
        self.processed_frames = 0
        self.last_log_time = 0
        self.nvml_initialized = False
        self.is_running = False
        
        # Initialize GPU monitoring
        self._initialize_nvml()

    def start(self):
        """Start the performance monitor"""
        self.is_running = True
        self.start_time = time.time()
        self.logger.info("📊 Performance monitor started")

    def stop(self):
        """Stop the performance monitor"""
        self.is_running = False
        self._shutdown_nvml()
        self.logger.info("📊 Performance monitor stopped")

    async def monitor_loop(self, job_queue, result_queue, thread_pool):
        """Main monitoring loop"""
        self.logger.info("📊 Starting performance monitoring loop...")
        
        while self.is_running:
            try:
                await self._collect_and_update_metrics(job_queue, result_queue, thread_pool)
                await asyncio.sleep(5)  # Monitor every 5 seconds
            except Exception as e:
                self.logger.error(f"Performance monitor error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _collect_and_update_metrics(self, job_queue, result_queue, thread_pool):
        """Collect performance metrics and update storage"""
        now = time.time()
        uptime = now - self.start_time
        fps = self.processed_frames / uptime if uptime > 0 else 0
        
        # Collect performance data
        perf_data = {
            'worker_id': self.worker_id,
            'status': 'healthy',
            'uptime_seconds': uptime,
            'processed_frames': self.processed_frames,
            'current_fps': fps,
            'job_queue_size': job_queue.qsize(),
            'result_queue_size': result_queue.qsize(),
            'memory_usage_mb': self._get_memory_usage(),
            'gpu_utilization': self._get_gpu_utilization(),
            'thread_pool_active': len(thread_pool._threads) if hasattr(thread_pool, '_threads') else 0,
            'timestamp': now
        }
        
        # Update Redis if available (for caching and dashboard)
        if self.redis_client:
            try:
                await self.redis_client.set(
                    f"ai_worker_performance:{self.worker_id}",
                    perf_data,
                    ttl=60
                )
            except Exception as e:
                self.logger.warning(f"Failed to update Redis metrics: {e}")
        
        # Update local metrics
        self.metrics.record('current_fps', fps)
        self.metrics.record('queue_size', job_queue.qsize())
        self.metrics.record('memory_usage_mb', perf_data['memory_usage_mb'])
        self.metrics.record('gpu_utilization', perf_data['gpu_utilization'])

        # Periodic logging (every 30 seconds)
        if now - self.last_log_time > 30:
            self.logger.info(
                f"📊 Performance: {fps:.1f} FPS | "
                f"Queues(Job/Result): {perf_data['job_queue_size']}/{perf_data['result_queue_size']} | "
                f"Memory: {perf_data['memory_usage_mb']:.1f}MB | "
                f"GPU: {perf_data['gpu_utilization']:.1f}% | "
                f"Threads: {perf_data['thread_pool_active']}"
            )
            self.last_log_time = now

    def increment_processed_frames(self):
        """Increment processed frames counter"""
        self.processed_frames += 1

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        uptime = time.time() - self.start_time
        fps = self.processed_frames / uptime if uptime > 0 else 0
        
        return {
            'worker_id': self.worker_id,
            'uptime_seconds': uptime,
            'processed_frames': self.processed_frames,
            'current_fps': fps,
            'memory_usage_mb': self._get_memory_usage(),
            'gpu_utilization': self._get_gpu_utilization(),
            'is_running': self.is_running
        }

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        if not psutil:
            return 0.0
        try:
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except Exception as e:
            self.logger.debug(f"Could not get memory usage: {e}")
            return 0.0

    def _initialize_nvml(self):
        """Initialize NVIDIA Management Library for GPU monitoring"""
        if pynvml and not self.nvml_initialized:
            try:
                pynvml.nvmlInit()
                self.nvml_initialized = True
                self.logger.info("✅ NVML initialized for GPU monitoring")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not initialize NVML: {e}")
                self.nvml_initialized = False

    def _shutdown_nvml(self):
        """Shutdown NVIDIA Management Library"""
        if pynvml and self.nvml_initialized:
            try:
                pynvml.nvmlShutdown()
                self.logger.info("✅ NVML shutdown")
            except Exception:
                pass  # Ignore shutdown errors
            finally:
                self.nvml_initialized = False

    def _get_gpu_utilization(self) -> float:
        """Get GPU utilization percentage"""
        if not self.nvml_initialized:
            return 0.0
        
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            info = pynvml.nvmlDeviceGetUtilizationRates(handle)
            return float(info.gpu)
        except Exception as e:
            # GPU errors are common, log as debug only
            self.logger.debug(f"Could not get GPU utilization: {e}")
            return 0.0