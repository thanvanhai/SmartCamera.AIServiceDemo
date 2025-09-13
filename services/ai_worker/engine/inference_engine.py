"""
Main Inference Engine orchestrator for AI Worker
Refactored into smaller, focused components
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional

from infrastructure.storage.redis.client import RedisClient
from .inference_job import InferenceJob
from .metrics_collector import MetricsCollector
from .performance_monitor import PerformanceMonitor
from .kafka_handler import KafkaHandler
from .job_processor import JobProcessor


class InferenceEngine:
    """
    Main orchestrator for the AI inference engine
    Coordinates all components and manages the overall inference workflow
    """
    
    def __init__(self, worker_id: str, config: Dict[str, Any]):
        self.worker_id = worker_id
        self.config = config
        self.logger = logging.getLogger(f"inference_engine_{worker_id}")

        # Initialize components
        self.metrics = MetricsCollector(worker_id)
        self.redis_client = RedisClient()
        self.kafka_handler = KafkaHandler(worker_id, config, self.metrics)
        self.job_processor = JobProcessor(worker_id, config, self.metrics)
        self.performance_monitor = PerformanceMonitor(
            worker_id, self.metrics, self.redis_client
        )

        # Queues for job processing
        self.job_queue = asyncio.PriorityQueue(maxsize=config.get('queue_size', 200))
        self.result_queue = asyncio.Queue(maxsize=100)

        # State management
        self.is_running = False
        self.test_mode = config.get('test_mode', True)
        
        self.logger.info(f"🧪 Inference Engine initialized in test mode: {self.test_mode}")

    async def start(self):
        """Start the inference engine and all its components"""
        self.logger.info(f"🚀 Starting Inference Engine {self.worker_id}")
        self.is_running = True

        # Initialize components
        await self._initialize_components()

        # Start processing tasks
        tasks = [
            asyncio.create_task(self._frame_consumer(), name="frame_consumer"),
            asyncio.create_task(self._inference_processor(), name="inference_processor"),
            asyncio.create_task(self._result_publisher(), name="result_publisher"),
            asyncio.create_task(
                self.performance_monitor.monitor_loop(
                    self.job_queue, self.result_queue, self.job_processor.thread_pool
                ),
                name="performance_monitor"
            )
        ]
        
        # Add batch processor task if enabled
        if self.job_processor.enable_batch_processing:
            tasks.append(
                asyncio.create_task(self._batch_processor(), name="batch_processor")
            )

        self.logger.info(f"✅ Inference Engine {self.worker_id} started with {len(tasks)} tasks")
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"❌ Inference Engine error: {e}", exc_info=True)
            raise

    async def stop(self):
        """Stop the inference engine and all its components"""
        self.logger.info(f"🛑 Stopping Inference Engine {self.worker_id}")
        self.is_running = False
        
        # Stop components
        self.performance_monitor.stop()
        await self.job_processor.shutdown()
        await self.kafka_handler.disconnect()
        
        if self.redis_client:
            try:
                await self.redis_client.disconnect()
                self.logger.info("✅ Redis disconnected")
            except Exception as e:
                self.logger.warning(f"⚠️ Error disconnecting Redis: {e}")

        self.logger.info(f"✅ Inference Engine {self.worker_id} stopped")

    async def _initialize_components(self):
        """Initialize all engine components"""
        # Connect to Kafka
        await self.kafka_handler.connect()
        
        # Connect to Redis (optional)
        try:
            await self.redis_client.connect()
            self.logger.info("✅ Redis client connected")
        except Exception as e:
            self.logger.warning(f"⚠️ Redis connection failed, continuing without Redis: {e}")
            self.redis_client = None

        # Initialize job processor
        await self.job_processor.initialize()
        
        # Start performance monitoring
        self.performance_monitor.start()

    async def _frame_consumer(self):
        """Consume video frames from Kafka and queue them for processing"""
        self.logger.info("📥 Starting frame consumer...")
        
        try:
            async for job in self.kafka_handler.consume_frames():
                if not self.is_running:
                    break
                    
                if job is None:
                    continue
                    
                try:
                    # Add job to processing queue with priority and timestamp
                    await self.job_queue.put((job.priority, time.time(), job))
                    
                except asyncio.QueueFull:
                    self.logger.warning("Job queue full, dropping frame")
                    self.metrics.increment('frames_dropped')
                    
        except Exception as e:
            self.logger.error(f"Frame consumer fatal error: {e}", exc_info=True)

    async def _inference_processor(self):
        """Process inference jobs from the queue"""
        self.logger.info("🧠 Starting inference processor...")
        
        while self.is_running:
            try:
                # Get job from queue with timeout
                _, queued_time, job = await asyncio.wait_for(
                    self.job_queue.get(), 
                    timeout=1.0
                )
                
                # Track queue wait time
                queue_wait_time = (time.time() - queued_time) * 1000
                self.metrics.record('queue_wait_time_ms', queue_wait_time)
                
                # Process job
                result = await self.job_processor.process_job(job)
                
                # Queue result for publishing if we got one
                if result:
                    await self.result_queue.put(result)
                    self.performance_monitor.increment_processed_frames()
                
                self.job_queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout is normal, continue processing
                continue
            except Exception as e:
                self.logger.error(f"Error processing inference: {e}", exc_info=True)
                self.metrics.increment('errors_inference')

    async def _result_publisher(self):
        """Publish inference results to Kafka"""
        self.logger.info("📤 Starting result publisher...")
        
        while self.is_running:
            try:
                # Get result from queue with timeout
                result = await asyncio.wait_for(
                    self.result_queue.get(), 
                    timeout=1.0
                )
                
                # Publish result
                success = await self.kafka_handler.publish_result(result)
                
                if success and self.test_mode:
                    self.logger.debug(
                        f"Published result: Camera={result.camera_id}, "
                        f"Detections={len(result.detections)}, "
                        f"ProcessingTime={result.processing_time_ms:.1f}ms"
                    )
                
                self.result_queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout is normal, continue processing
                continue
            except Exception as e:
                self.logger.error(f"Error in result publisher: {e}", exc_info=True)

    async def _batch_processor(self):
        """Handle batch processing results"""
        self.logger.info("🔄 Starting batch processor handler...")
        
        while self.is_running:
            try:
                # Get batch results
                results = await self.job_processor.get_batch_results()
                
                # Queue results for publishing
                for result in results:
                    await self.result_queue.put(result)
                    self.performance_monitor.increment_processed_frames()
                
                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"Batch processor handler error: {e}", exc_info=True)

    # Test and monitoring methods
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics (for testing)"""
        if not self.test_mode:
            return {}
        
        stats = self.performance_monitor.get_performance_stats()
        stats.update({
            'job_queue_size': self.job_queue.qsize(),
            'result_queue_size': self.result_queue.qsize(),
        })
        
        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Health check endpoint (for testing and monitoring)"""
        try:
            # Check component health
            kafka_healthy = await self.kafka_handler.health_check()
            redis_healthy = not self.redis_client or await self.redis_client.health_check()
            processor_healthy = self.job_processor.is_healthy()
            
            return {
                'status': 'healthy' if kafka_healthy and redis_healthy and processor_healthy else 'unhealthy',
                'worker_id': self.worker_id,
                'uptime': time.time() - self.performance_monitor.start_time,
                'components': {
                    'kafka': kafka_healthy,
                    'redis': redis_healthy,
                    'processor': processor_healthy
                },
                'performance': self.get_performance_stats() if self.test_mode else {}
            }
            
        except Exception as e:
            self.logger.error(f"Health check error: {e}", exc_info=True)
            return {
                'status': 'unhealthy',
                'error': str(e),
                'worker_id': self.worker_id
            }