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
from infrastructure.messaging.kafka_client import KafkaClient
from infrastructure.storage.redis_client import RedisClient
from services.ai_worker.model_loader import ModelLoader
from services.ai_worker.batch_processor import BatchProcessor
from shared.monitoring.metrics import AIMetrics
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

        # Components
        self.kafka_client = KafkaClient()
        self.redis_client = RedisClient()
        self.model_loader = ModelLoader(config)
        self.batch_processor = BatchProcessor(config)
        self.metrics = AIMetrics(worker_id)

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

    async def start(self):
        self.logger.info(f"🚀 Starting Inference Engine {self.worker_id}")
        self.start_time = time.time()
        self.is_running = True

        self._initialize_nvml()

        await self.kafka_client.connect()
        await self.redis_client.connect()
        await self.model_loader.initialize()

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
        self.logger.info(f"🛑 Stopping Inference Engine {self.worker_id}")
        self.is_running = False
        self._shutdown_nvml()
        await self.kafka_client.disconnect()
        await self.redis_client.disconnect()
        self.thread_pool.shutdown(wait=True)
        self.logger.info(f"✅ Inference Engine {self.worker_id} stopped")

    async def _frame_consumer(self):
        self.logger.info("📥 Starting frame consumer...")
        async for message in self.kafka_client.consume(topic='video_frames'):
            if not self.is_running: break
            try:
                job = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    self._create_inference_job_sync,
                    message.value
                )
                if job:
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

    async def _inference_processor(self):
        self.logger.info("🧠 Starting inference processor...")
        while self.is_running:
            try:
                _, queued_time, job = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)
                self.metrics.record('queue_wait_time_ms', (time.time() - queued_time) * 1000)
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
                self.logger.error(f"Error processing inference: {e}", exc_info=True)
                self.metrics.increment('errors_inference')

    async def _process_single_job(self, job: InferenceJob) -> Optional[DetectionResult]:
        try:
            result = await asyncio.get_event_loop().run_in_executor(self.thread_pool, self._run_inference_sync, job)
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
        try:
            start_time = time.time()
            detector = self.model_loader.get_detector(job.model_type)
            if not detector:
                self.logger.error(f"Detector {job.model_type} not available for job {job.job_id}")
                return None
            
            processed_frame = detector.preprocess(job.frame)
            detections = detector.detect(processed_frame, job.config)
            filtered_detections = self._post_process_detections(detections, job.config)
            
            processing_time = (time.time() - start_time) * 1000
            
            result = DetectionResult(
                job_id=job.job_id, camera_id=job.camera_id, timestamp=job.timestamp,
                detections=filtered_detections, processing_time_ms=processing_time,
                model_type=job.model_type.value, worker_id=self.worker_id,
                confidence_threshold=job.config.get('confidence_threshold', 0.5)
            )
            
            self.metrics.record('inference_time_ms', processing_time)
            self.metrics.record('detections_count', len(filtered_detections))
            return result
        except Exception as e:
            self.logger.error(f"Sync inference error for job {job.job_id}: {e}", exc_info=True)
            return None

    def _post_process_detections(self, detections: List[Dict], config: Dict) -> List[Dict]:
        if not detections:
            return []
        
        confidence_threshold = config.get('confidence_threshold', 0.5)
        confident_detections = [d for d in detections if d.get('confidence', 0) >= confidence_threshold]
        
        max_detections = config.get('max_detections', 50)
        if len(confident_detections) > max_detections:
            # Sắp xếp và lấy top K detections
            confident_detections.sort(key=lambda x: x.get('confidence', 0), reverse=True)
            top_detections = confident_detections[:max_detections]
        else:
            top_detections = confident_detections
            
        processed_time = time.time()
        for det in top_detections:
            det['worker_id'] = self.worker_id
            det['processed_at'] = processed_time
        return top_detections

    async def _result_publisher(self):
        self.logger.info("📤 Starting result publisher...")
        while self.is_running:
            try:
                result = await asyncio.wait_for(self.result_queue.get(), timeout=1.0)
                await self.kafka_client.produce(topic='ai_results', message=result.to_dict(), key=result.camera_id)
                self.metrics.increment('results_published')
                self.result_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error publishing result: {e}", exc_info=True)
                self.metrics.increment('errors_publish')

    async def _batch_processor(self):
        """
        GHI CHÚ QUAN TRỌNG:
        Vòng lặp này đang sử dụng polling (sleep) - không hiệu quả về CPU.
        Một kiến trúc tốt hơn là Event-Driven:
        1. Truyền một asyncio.Queue() vào BatchProcessor.
        2. Khi BatchProcessor xử lý xong, nó sẽ .put(results) vào queue đó.
        3. Vòng lặp này sẽ await queue.get() và sẽ "ngủ" hiệu quả cho đến khi có dữ liệu.
        """
        self.logger.info("🔄 Starting batch processor (polling mode)...")
        while self.is_running:
            try:
                results = await self.batch_processor.get_results()
                for r in results:
                    await self.result_queue.put(r)
                await asyncio.sleep(0.01)
            except Exception as e:
                self.logger.error(f"Batch processor error: {e}", exc_info=True)
                self.metrics.increment('errors_batch')

    async def _performance_monitor(self):
        self.logger.info("📊 Starting performance monitor...")
        while self.is_running:
            try:
                now = time.time()
                uptime = now - self.start_time
                fps = self.processed_frames / uptime if uptime > 0 else 0
                
                perf_data = {
                    'worker_id': self.worker_id, 'status': 'healthy', 'uptime_seconds': uptime,
                    'processed_frames': self.processed_frames, 'current_fps': fps,
                    'job_queue_size': self.job_queue.qsize(), 'result_queue_size': self.result_queue.qsize(),
                    'memory_usage_mb': self._get_memory_usage(), 'gpu_utilization': self._get_gpu_utilization(),
                    'thread_pool_active': len(self.thread_pool._threads), 'timestamp': now
                }
                
                await self.redis_client.set(f"ai_worker_performance:{self.worker_id}", perf_data, ttl=60)
                
                self.metrics.record('current_fps', fps)
                self.metrics.record('queue_size', self.job_queue.qsize())

                # Cải thiện: Log định kỳ 30 giây một lần
                if now - self.last_log_time > 30:
                    self.logger.info(
                        f"Perf: {fps:.1f} FPS, Queues(Job/Res): {perf_data['job_queue_size']}/{perf_data['result_queue_size']}, "
                        f"Mem: {perf_data['memory_usage_mb']:.1f}MB, GPU: {perf_data['gpu_utilization']}%"
                    )
                    self.last_log_time = now

            except Exception as e:
                self.logger.error(f"Performance monitor error: {e}", exc_info=True)
            
            await asyncio.sleep(5)

    def _create_inference_job_sync(self, frame_data: Dict) -> Optional[InferenceJob]:
        try:
            frame_bytes = frame_data.get('frame')
            if not frame_bytes: return None
            
            nparr = np.frombuffer(bytes(frame_bytes), np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if frame is None: return None
            
            priority = frame_data.get('priority', 1)
            
            return InferenceJob(
                job_id=frame_data.get('job_id', f"{frame_data['camera_id']}_{time.time()}"),
                camera_id=frame_data['camera_id'], frame=frame,
                timestamp=frame_data.get('timestamp', time.time()),
                model_type=ModelType(frame_data.get('model_type', 'yolo_v8')),
                config=frame_data.get('config', {}), priority=priority
            )
        except Exception as e:
            self.logger.error(f"Error creating inference job: {e}", exc_info=True)
            return None

    def _get_memory_usage(self) -> float:
        if not psutil: return 0.0
        try:
            return psutil.Process().memory_info().rss / (1024 * 1024)
        except Exception as e:
            # Cải thiện: Bắt lỗi cụ thể và ghi log
            self.logger.warning(f"Could not get memory usage: {e}")
            return 0.0

    def _initialize_nvml(self):
        if pynvml and not self.nvml_initialized:
            try:
                pynvml.nvmlInit()
                self.nvml_initialized = True
                self.logger.info("pynvml initialized.")
            except pynvml.NVMLError as e:
                self.logger.warning(f"Could not initialize pynvml: {e}")
                self.nvml_initialized = False

    def _shutdown_nvml(self):
        if pynvml and self.nvml_initialized:
            try:
                pynvml.nvmlShutdown()
            except pynvml.NVMLError:
                pass # Bỏ qua lỗi khi shutdown
            self.nvml_initialized = False

    def _get_gpu_utilization(self) -> float:
        if not self.nvml_initialized: return 0.0
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            info = pynvml.nvmlDeviceGetUtilizationRates(handle)
            return float(info.gpu)
        except pynvml.NVMLError:
            # Có thể xảy ra nếu driver không phản hồi, không cần log thành lỗi
            return 0.0
        except Exception as e:
            # Cải thiện: Bắt lỗi cụ thể và ghi log
            self.logger.warning(f"Could not get GPU utilization: {e}")
            return 0.0

