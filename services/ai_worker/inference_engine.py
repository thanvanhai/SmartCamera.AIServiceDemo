# services/ai_worker/inference_engine.py
import asyncio
import logging
import time
import numpy as np
from typing import List, Dict, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import cv2
from dataclasses import dataclass

from core.models import DetectionResult, ProcessingFrame
from core.enums import ModelType, DetectionType
from infrastructure.messaging.kafka_client import KafkaClient
from infrastructure.storage.redis_client import RedisClient
from services.ai_worker.model_loader import ModelLoader
from services.ai_worker.batch_processor import BatchProcessor
from shared.monitoring.metrics import AIMetrics
from shared.utils.image_utils import preprocess_frame
from shared.decorators.timing import time_execution


@dataclass
class InferenceJob:
    """Inference job data structure"""
    job_id: str
    camera_id: str
    frame: np.ndarray
    timestamp: float
    model_type: ModelType
    config: Dict[str, Any]
    priority: int = 0


class InferenceEngine:
    """Main Inference Engine - Xử lý AI inference cho frames"""
    
    def __init__(self, worker_id: str, config: Dict[str, Any]):
        self.worker_id = worker_id
        self.config = config
        self.logger = logging.getLogger(f"inference_engine_{worker_id}")
        
        # Initialize components
        self.kafka_client = KafkaClient()
        self.redis_client = RedisClient()
        self.model_loader = ModelLoader(config)
        self.batch_processor = BatchProcessor(config)
        self.metrics = AIMetrics(worker_id)
        
        # Processing queues
        self.job_queue = asyncio.PriorityQueue(maxsize=config.get('queue_size', 200))
        self.result_queue = asyncio.Queue(maxsize=100)
        
        # Thread pool cho CPU-bound tasks
        self.thread_pool = ThreadPoolExecutor(
            max_workers=config.get('max_threads', 4),
            thread_name_prefix=f"inference_{worker_id}"
        )
        
        # Engine state
        self.is_running = False
        self.processed_frames = 0
        self.start_time = None
        
        # Performance settings
        self.enable_batch_processing = config.get('enable_batch_processing', True)
        self.max_batch_size = config.get('max_batch_size', 8)
        self.batch_timeout_ms = config.get('batch_timeout_ms', 50)
    
    async def start(self):
        """Start inference engine"""
        self.logger.info(f"🚀 Starting Inference Engine {self.worker_id}")
        
        self.start_time = time.time()
        self.is_running = True
        
        # Initialize components
        await self.kafka_client.connect()
        await self.redis_client.connect()
        await self.model_loader.initialize()
        
        # Start processing tasks
        tasks = [
            asyncio.create_task(self._frame_consumer(), name="frame_consumer"),
            asyncio.create_task(self._inference_processor(), name="inference_processor"),
            asyncio.create_task(self._result_publisher(), name="result_publisher"),
            asyncio.create_task(self._performance_monitor(), name="performance_monitor")
        ]
        
        # Nếu enable batch processing
        if self.enable_batch_processing:
            tasks.append(asyncio.create_task(self._batch_processor(), name="batch_processor"))
        
        self.logger.info(f"✅ Inference Engine {self.worker_id} started with {len(tasks)} tasks")
        
        # Run all tasks
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"❌ Inference Engine error: {e}")
            raise
    
    async def stop(self):
        """Stop inference engine"""
        self.logger.info(f"🛑 Stopping Inference Engine {self.worker_id}")
        
        self.is_running = False
        
        # Cleanup
        await self.kafka_client.disconnect()
        await self.redis_client.disconnect()
        self.thread_pool.shutdown(wait=True)
        
        self.logger.info(f"✅ Inference Engine {self.worker_id} stopped")
    
    async def _frame_consumer(self):
        """Consumer frames từ Kafka topic"""
        self.logger.info("📥 Starting frame consumer")
        
        async for message in self.kafka_client.consume('video_frames'):
            if not self.is_running:
                break
            
            try:
                # Parse frame message
                frame_data = message.value
                job = await self._create_inference_job(frame_data)
                
                if job:
                    # Add to priority queue
                    await self.job_queue.put((job.priority, time.time(), job))
                    self.metrics.increment('frames_received')
                else:
                    self.metrics.increment('frames_invalid')
                    
            except asyncio.QueueFull:
                self.logger.warning("Job queue full, dropping frame")
                self.metrics.increment('frames_dropped')
            except Exception as e:
                self.logger.error(f"Error consuming frame: {e}")
                self.metrics.increment('errors_consume')
    
    async def _inference_processor(self):
        """Process inference jobs"""
        self.logger.info("🧠 Starting inference processor")
        
        while self.is_running:
            try:
                # Get job from queue với timeout
                _, queued_time, job = await asyncio.wait_for(
                    self.job_queue.get(), 
                    timeout=1.0
                )
                
                # Track queue wait time
                wait_time = time.time() - queued_time
                self.metrics.record('queue_wait_time_ms', wait_time * 1000)
                
                # Process job
                if self.enable_batch_processing:
                    await self.batch_processor.add_job(job)
                else:
                    result = await self._process_single_job(job)
                    if result:
                        await self.result_queue.put(result)
                
                self.job_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error processing inference: {e}")
                self.metrics.increment('errors_inference')
    
    async def _process_single_job(self, job: InferenceJob) -> Optional[DetectionResult]:
        """Process single inference job"""
        try:
            # Run inference in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.thread_pool,
                self._run_inference_sync,
                job
            )
            
            if result:
                self.processed_frames += 1
                self.metrics.increment('frames_processed')
            
            return result
            
        except Exception as e:
            self.logger.error(f"Single job processing error: {e}")
            self.metrics.increment('errors_single_inference')
            return None
    
    @time_execution
    def _run_inference_sync(self, job: InferenceJob) -> Optional[DetectionResult]:
        """Synchronous inference execution"""
        try:
            start_time = time.time()
            
            # Get model
            detector = self.model_loader.get_detector(job.model_type)
            if not detector:
                self.logger.error(f"Detector {job.model_type} not available")
                return None
            
            # Preprocess frame
            processed_frame = detector.preprocess(job.frame)
            
            # Run detection
            detections = detector.detect(processed_frame, job.config)
            
            # Post-process detections
            filtered_detections = self._post_process_detections(detections, job.config)
            
            processing_time = time.time() - start_time
            
            # Create result
            result = DetectionResult(
                job_id=job.job_id,
                camera_id=job.camera_id,
                timestamp=job.timestamp,
                detections=filtered_detections,
                processing_time_ms=processing_time * 1000,
                model_type=job.model_type.value,
                worker_id=self.worker_id,
                confidence_threshold=job.config.get('confidence_threshold', 0.5)
            )
            
            # Record metrics
            self.metrics.record('inference_time_ms', processing_time * 1000)
            self.metrics.record('detections_count', len(filtered_detections))
            
            return result
            
        except Exception as e:
            self.logger.error(f"Sync inference error: {e}")
            return None
    
    def _post_process_detections(self, detections: List[Dict], config: Dict) -> List[Dict]:
        """Post-process detection results"""
        if not detections:
            return []
        
        # Filter by confidence
        confidence_threshold = config.get('confidence_threshold', 0.5)
        filtered_detections = [
            det for det in detections 
            if det.get('confidence', 0) >= confidence_threshold
        ]
        
        # Limit number of detections
        max_detections = config.get('max_detections', 50)
        if len(filtered_detections) > max_detections:
            # Sort by confidence và giữ lại top detections
            filtered_detections.sort(key=lambda x: x.get('confidence', 0), reverse=True)
            filtered_detections = filtered_detections[:max_detections]
        
        # Add metadata
        for detection in filtered_detections:
            detection['worker_id'] = self.worker_id
            detection['processed_at'] = time.time()
        
        return filtered_detections
    
    async def _result_publisher(self):
        """Publish results to Kafka"""
        self.logger.info("📤 Starting result publisher")
        
        while self.is_running:
            try:
                # Get result từ queue
                result = await asyncio.wait_for(
                    self.result_queue.get(),
                    timeout=1.0
                )
                
                # Publish to Kafka
                await self.kafka_client.produce(
                    'ai_results',
                    result.to_dict(),
                    key=result.camera_id
                )
                
                self.metrics.increment('results_published')
                self.result_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error publishing result: {e}")
                self.metrics.increment('errors_publish')
    
    async def _batch_processor(self):
        """Handle batch processing"""
        self.logger.info("🔄 Starting batch processor")
        
        while self.is_running:
            try:
                # Process any completed batches
                results = await self.batch_processor.get_results()
                
                for result in results:
                    await self.result_queue.put(result)
                
                await asyncio.sleep(0.01)  # Small delay
                
            except Exception as e:
                self.logger.error(f"Batch processor error: {e}")
                self.metrics.increment('errors_batch')
    
    async def _performance_monitor(self):
        """Monitor và report performance metrics"""
        self.logger.info("📊 Starting performance monitor")
        
        while self.is_running:
            try:
                current_time = time.time()
                uptime = current_time - self.start_time
                fps = self.processed_frames / uptime if uptime > 0 else 0
                
                # Performance data
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
                    'thread_pool_active': self.thread_pool._threads,
                    'timestamp': current_time
                }
                
                # Store in Redis với TTL
                await self.redis_client.set(
                    f"ai_worker_performance:{self.worker_id}",
                    perf_data,
                    ttl=60  # 1 minute TTL
                )
                
                # Log performance periodically
                if int(current_time) % 30 == 0:  # Every 30 seconds
                    self.logger.info(
                        f"📊 Performance: {fps:.1f} FPS, "
                        f"Queue: {self.job_queue.qsize()}, "
                        f"Memory: {perf_data['memory_usage_mb']:.1f}MB"
                    )
                
                self.metrics.record('current_fps', fps)
                self.metrics.record('queue_size', self.job_queue.qsize())
                
            except Exception as e:
                self.logger.error(f"Performance monitor error: {e}")
            
            await asyncio.sleep(5)  # Update every 5 seconds
    
    async def _create_inference_job(self, frame_data: Dict) -> Optional[InferenceJob]:
        """Create inference job từ frame data"""
        try:
            # Decode frame
            frame_bytes = frame_data.get('frame')
            if not frame_bytes:
                return None
            
            # Decode image
            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if frame is None:
                return None
            
            # Determine priority (urgent cameras có priority cao hơn)
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
            self.logger.error(f"Error creating inference job: {e}")
            return None
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0
    
    def _get_gpu_utilization(self) -> float:
        """Get GPU utilization if available"""
        try:
            import pynvml
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            info = pynvml.nvmlDeviceGetUtilizationRates(handle)
            return info.gpu
        except:
            return 0.0
