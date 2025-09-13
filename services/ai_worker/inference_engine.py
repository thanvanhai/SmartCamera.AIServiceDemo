import asyncio
import logging
import time
import numpy as np
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor
import cv2
from dataclasses import dataclass

from core.models import DetectionResult
from core.enums import ModelType
from infrastructure.messaging.kafka.producer import KafkaProducer
from infrastructure.messaging.kafka.consumer import KafkaConsumer  
from infrastructure.storage.redis.client import RedisClient
from services.ai_worker.model_loader import ModelLoader
from services.ai_worker.batch_processor import BatchProcessor
from infrastructure.monitoring.metrics import ModelMetrics
from shared.decorators.timing import time_execution

# Import một lần ở đầu file để kiểm tra sự tồn tại
try:
    import psutil
except ImportError:
    psutil = None

try:
    import pynvml
except ImportError:
    pynvml = None


@dataclass
class InferenceJob:
    job_id: str
    camera_id: str
    frame: np.ndarray
    timestamp: float
    model_type: ModelType
    config: Dict[str, Any]
    priority: int = 0


class InferenceEngine:
    def __init__(self, worker_id: str, config: Dict[str, Any]):
        self.worker_id = worker_id
        self.config = config
        self.logger = logging.getLogger(f"inference_engine_{worker_id}")

        # Components - Tạm thời chỉ giữ lại những thành phần cần thiết cho test
        self.kafka_client = KafkaClient()
        self.redis_client = RedisClient()  # Giữ lại Redis cho caching và metrics
        self.model_loader = ModelLoader(config)
        self.batch_processor = BatchProcessor(config)
        #self.metrics = AIMetrics(worker_id)

        # Queues
        self.job_queue = asyncio.PriorityQueue(maxsize=config.get('queue_size', 200))
        self.result_queue = asyncio.Queue(maxsize=100)

        # Thread pool cho các tác vụ nặng về CPU
        self.thread_pool = ThreadPoolExecutor(
            max_workers=config.get('max_threads', 4),
            thread_name_prefix=f"inference_{worker_id}"
        )

        # State
        self.is_running = False
        self.processed_frames = 0
        self.start_time = None
        self.nvml_initialized = False
        self.last_log_time = 0

        # Performance Settings
        self.enable_batch_processing = config.get('enable_batch_processing', True)
        self.max_batch_size = config.get('max_batch_size', 8)
        self.batch_timeout_ms = config.get('batch_timeout_ms', 50)

        # Test mode - không lưu vào persistent storage
        self.test_mode = config.get('test_mode', True)
        self.logger.info(f"🧪 Inference Engine running in test mode: {self.test_mode}")

    async def start(self):
        """Start the inference engine"""
        self.logger.info(f"🚀 Starting Inference Engine {self.worker_id}")
        self.start_time = time.time()
        self.is_running = True

        # Initialize hardware monitoring
        self._initialize_nvml()

        # Connect to required services
        try:
            await self.kafka_client.connect()
            self.logger.info("✅ Kafka client connected")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise

        try:
            await self.redis_client.connect()
            self.logger.info("✅ Redis client connected")
        except Exception as e:
            self.logger.warning(f"⚠️ Redis connection failed, continuing without Redis: {e}")
            self.redis_client = None

        try:
            await self.model_loader.initialize()
            self.logger.info("✅ Model loader initialized")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize models: {e}")
            raise

        # Start processing tasks
        tasks = [
            asyncio.create_task(self._frame_consumer(), name="frame_consumer"),
            asyncio.create_task(self._inference_processor(), name="inference_processor"),
            asyncio.create_task(self._result_publisher(), name="result_publisher"),
            asyncio.create_task(self._performance_monitor(), name="performance_monitor")
        ]
        
        if self.enable_batch_processing:
            tasks.append(asyncio.create_task(self._batch_processor(), name="batch_processor"))

        self.logger.info(f"✅ Inference Engine {self.worker_id} started with {len(tasks)} tasks")
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"❌ Inference Engine error: {e}", exc_info=True)
            raise

    async def stop(self):
        """Stop the inference engine"""
        self.logger.info(f"🛑 Stopping Inference Engine {self.worker_id}")
        self.is_running = False
        
        self._shutdown_nvml()
        
        # Disconnect from services
        try:
            await self.kafka_client.disconnect()
            self.logger.info("✅ Kafka disconnected")
        except Exception as e:
            self.logger.warning(f"⚠️ Error disconnecting Kafka: {e}")

        if self.redis_client:
            try:
                await self.redis_client.disconnect()
                self.logger.info("✅ Redis disconnected")
            except Exception as e:
                self.logger.warning(f"⚠️ Error disconnecting Redis: {e}")

        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True)
        self.logger.info(f"✅ Inference Engine {self.worker_id} stopped")

    async def _frame_consumer(self):
        """Consume video frames from Kafka"""
        self.logger.info("📥 Starting frame consumer...")
        
        try:
            async for message in self.kafka_client.consume(topic='video_frames'):
                if not self.is_running:
                    break
                    
                try:
                    # Process frame data in thread pool to avoid blocking
                    job = await asyncio.get_event_loop().run_in_executor(
                        self.thread_pool,
                        self._create_inference_job_sync,
                        message.value
                    )
                    
                    if job:
                        # Add job to queue with priority and timestamp
                        await self.job_queue.put((job.priority, time.time(), job))
                        self.metrics.increment('frames_received')
                    else:
                        self.metrics.increment('frames_invalid')
                        
                except asyncio.QueueFull:
                    self.logger.warning("Job queue full, dropping frame")
                    self.metrics.increment('frames_dropped')
                except Exception as e:
                    self.logger.error(f"Error consuming frame: {e}", exc_info=True)
                    self.metrics.increment('errors_consume')
                    
        except Exception as e:
            self.logger.error(f"Frame consumer fatal error: {e}", exc_info=True)
            # In production, this might trigger a restart

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
                
                # Process job based on batch processing setting
                if self.enable_batch_processing:
                    await self.batch_processor.add_job(job)
                else:
                    result = await self._process_single_job(job)
                    if result:
                        await self.result_queue.put(result)
                
                self.job_queue.task_done()
                
            except asyncio.TimeoutError:
                # Timeout is normal, continue processing
                continue
            except Exception as e:
                self.logger.error(f"Error processing inference: {e}", exc_info=True)
                self.metrics.increment('errors_inference')

    async def _process_single_job(self, job: InferenceJob) -> Optional[DetectionResult]:
        """Process a single inference job"""
        try:
            # Run inference in thread pool to avoid blocking
            result = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, 
                self._run_inference_sync, 
                job
            )
            
            if result:
                self.processed_frames += 1
                self.metrics.increment('frames_processed')
                
            return result
            
        except Exception as e:
            self.logger.error(f"Single job processing error: {e}", exc_info=True)
            self.metrics.increment('errors_single_inference')
            return None

    @time_execution
    def _run_inference_sync(self, job: InferenceJob) -> Optional[DetectionResult]:
        """Run synchronous inference (called from thread pool)"""
        try:
            start_time = time.time()
            
            # Get appropriate detector
            detector = self.model_loader.get_detector(job.model_type)
            if not detector:
                self.logger.error(f"Detector {job.model_type} not available for job {job.job_id}")
                return None
            
            # Pre-process frame
            processed_frame = detector.preprocess(job.frame)
            
            # Run detection
            detections = detector.detect(processed_frame, job.config)
            
            # Post-process detections
            filtered_detections = self._post_process_detections(detections, job.config)
            
            # Calculate processing time
            processing_time = (time.time() - start_time) * 1000
            
            # Create result object
            result = DetectionResult(
                job_id=job.job_id,
                camera_id=job.camera_id,
                timestamp=job.timestamp,
                detections=filtered_detections,
                processing_time_ms=processing_time,
                model_type=job.model_type.value,
                worker_id=self.worker_id,
                confidence_threshold=job.config.get('confidence_threshold', 0.5)
            )
            
            # Record metrics
            self.metrics.record('inference_time_ms', processing_time)
            self.metrics.record('detections_count', len(filtered_detections))
            
            return result
            
        except Exception as e:
            self.logger.error(f"Sync inference error for job {job.job_id}: {e}", exc_info=True)
            return None

    def _post_process_detections(self, detections: List[Dict], config: Dict) -> List[Dict]:
        """Post-process detection results"""
        if not detections:
            return []
        
        # Filter by confidence threshold
        confidence_threshold = config.get('confidence_threshold', 0.5)
        confident_detections = [
            d for d in detections 
            if d.get('confidence', 0) >= confidence_threshold
        ]
        
        # Limit number of detections
        max_detections = config.get('max_detections', 50)
        if len(confident_detections) > max_detections:
            # Sort by confidence and take top K
            confident_detections.sort(
                key=lambda x: x.get('confidence', 0), 
                reverse=True
            )
            top_detections = confident_detections[:max_detections]
        else:
            top_detections = confident_detections
            
        # Add metadata
        processed_time = time.time()
        for det in top_detections:
            det['worker_id'] = self.worker_id
            det['processed_at'] = processed_time
            
        return top_detections

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
                
                # Publish to Kafka
                await self.kafka_client.produce(
                    topic='ai_results',
                    message=result.to_dict(),
                    key=result.camera_id
                )
                
                self.metrics.increment('results_published')
                self.result_queue.task_done()
                
                # In test mode, also log results for debugging
                if self.test_mode:
                    self.logger.debug(
                        f"Published result: Camera={result.camera_id}, "
                        f"Detections={len(result.detections)}, "
                        f"ProcessingTime={result.processing_time_ms:.1f}ms"
                    )
                
            except asyncio.TimeoutError:
                # Timeout is normal, continue processing
                continue
            except Exception as e:
                self.logger.error(f"Error publishing result: {e}", exc_info=True)
                self.metrics.increment('errors_publish')

    async def _batch_processor(self):
        """
        Process batched inference jobs
        
        TODO: Improve this to be event-driven instead of polling:
        1. Pass asyncio.Queue to BatchProcessor
        2. BatchProcessor puts results into queue when ready
        3. This method awaits queue.get() instead of sleeping
        """
        self.logger.info("🔄 Starting batch processor (polling mode)...")
        
        while self.is_running:
            try:
                # Get processed results from batch processor
                results = await self.batch_processor.get_results()
                
                # Queue results for publishing
                for result in results:
                    await self.result_queue.put(result)
                
                # Small sleep to prevent busy waiting
                # TODO: Replace with event-driven approach
                await asyncio.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"Batch processor error: {e}", exc_info=True)
                self.metrics.increment('errors_batch')

    async def _performance_monitor(self):
        """Monitor system performance and update metrics"""
        self.logger.info("📊 Starting performance monitor...")
        
        while self.is_running:
            try:
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
                    'job_queue_size': self.job_queue.qsize(),
                    'result_queue_size': self.result_queue.qsize(),
                    'memory_usage_mb': self._get_memory_usage(),
                    'gpu_utilization': self._get_gpu_utilization(),
                    'thread_pool_active': len(self.thread_pool._threads),
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
                self.metrics.record('queue_size', self.job_queue.qsize())

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

            except Exception as e:
                self.logger.error(f"Performance monitor error: {e}", exc_info=True)
            
            # Monitor every 5 seconds
            await asyncio.sleep(5)

    def _create_inference_job_sync(self, frame_data: Dict) -> Optional[InferenceJob]:
        """Create inference job from frame data (synchronous)"""
        try:
            # Extract frame bytes
            frame_bytes = frame_data.get('frame')
            if not frame_bytes:
                return None
            
            # Decode frame
            nparr = np.frombuffer(bytes(frame_bytes), np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if frame is None:
                return None
            
            # Create job
            priority = frame_data.get('priority', 1)
            
            return InferenceJob(
                job_id=frame_data.get('job_id', f"{frame_data['camera_id']}_{time.time()}"),
                camera_id=frame_data['camera_id'],
                frame=frame,
                timestamp=frame_data.get('timestamp', time.time()),
                model_type=ModelType(frame_data.get('model_type', 'yolo_v8')),
                config=frame_data.get('config', {}),
                priority=priority
            )
            
        except Exception as e:
            self.logger.error(f"Error creating inference job: {e}", exc_info=True)
            return None

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

    # Test helper methods (only for test mode)
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics (for testing)"""
        if not self.test_mode:
            return {}
        
        uptime = time.time() - self.start_time if self.start_time else 0
        fps = self.processed_frames / uptime if uptime > 0 else 0
        
        return {
            'worker_id': self.worker_id,
            'uptime_seconds': uptime,
            'processed_frames': self.processed_frames,
            'current_fps': fps,
            'job_queue_size': self.job_queue.qsize(),
            'result_queue_size': self.result_queue.qsize(),
            'memory_usage_mb': self._get_memory_usage(),
            'gpu_utilization': self._get_gpu_utilization(),
            'is_running': self.is_running
        }

    async def health_check(self) -> Dict[str, Any]:
        """Health check endpoint (for testing and monitoring)"""
        try:
            # Check if all components are healthy
            kafka_healthy = (self.kafka_producer and await self.kafka_producer.health_check() and 
                           self.kafka_consumer and await self.kafka_consumer.health_check())
            redis_healthy = not self.redis_client or await self.redis_client.health_check()
            model_healthy = self.model_loader and self.model_loader.is_healthy()
            
            return {
                'status': 'healthy' if kafka_healthy and redis_healthy and model_healthy else 'unhealthy',
                'worker_id': self.worker_id,
                'uptime': time.time() - self.start_time if self.start_time else 0,
                'components': {
                    'kafka': kafka_healthy,
                    'redis': redis_healthy,
                    'models': model_healthy
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