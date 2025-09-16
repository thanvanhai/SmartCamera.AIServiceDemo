# services/ai_worker/batch_processor.py
import asyncio
import time
import logging
import numpy as np
from typing import List, Dict, Any, Optional, Callable
from collections import defaultdict, deque
from dataclasses import dataclass

from core.models import DetectionResult
from core.enums import ModelType
from infrastructure.monitoring.metrics import ModelMetrics


@dataclass
class BatchJob:
    """Batch job container"""
    jobs: List[Any]  # List of InferenceJob
    model_type: ModelType
    created_at: float
    batch_id: str


class BatchProcessor:
    """Batch Processor - Xử lý inference theo batch để optimize performance"""

    def __init__(self, config: Dict[str, Any], detector_factory: Optional[Callable] = None):
        self.config = config
        self.metrics = ModelMetrics()  # dùng ModelMetrics
        self.detector_factory = detector_factory

        # Batch settings
        self.max_batch_size = config.get("max_batch_size", 8)
        self.batch_timeout_ms = config.get("batch_timeout_ms", 50)
        self.enable_dynamic_batching = config.get("enable_dynamic_batching", True)

        # Job queues theo model type
        self.job_queues: Dict[ModelType, deque] = defaultdict(deque)
        self.pending_batches: List[BatchJob] = []
        self.completed_results: asyncio.Queue = asyncio.Queue()

        # Processing state
        self.batch_counter = 0
        self.processing_lock = asyncio.Lock()
        self.is_running = False

        # Statistics
        self.total_batches_processed = 0
        self.total_jobs_processed = 0

        # Background tasks
        self.batch_processor_task: Optional[asyncio.Task] = None

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.batch_processor_task = asyncio.create_task(self._batch_processor_loop())
        logging.info("BatchProcessor started")

    async def stop(self):
        self.is_running = False
        if self.batch_processor_task:
            self.batch_processor_task.cancel()
            try:
                await self.batch_processor_task
            except asyncio.CancelledError:
                pass
        await self._process_all_pending_batches()
        logging.info("BatchProcessor stopped")

    async def add_job(self, job):
        if not self.is_running:
            await self.start()
        async with self.processing_lock:
            model_type = job.model_type
            self.job_queues[model_type].append(job)
            if len(self.job_queues[model_type]) >= self.max_batch_size:
                await self._create_batch(model_type)

    async def get_results(self) -> List[DetectionResult]:
        results = []
        while True:
            try:
                result = self.completed_results.get_nowait()
                results.append(result)
            except asyncio.QueueEmpty:
                break
        return results

    async def _batch_processor_loop(self):
        while self.is_running:
            try:
                await self._check_and_create_timeout_batches()
                await self._process_ready_batches()
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Batch processor loop error: {e}")
                await asyncio.sleep(0.1)

    async def _check_and_create_timeout_batches(self):
        async with self.processing_lock:
            current_time = time.time()
            for model_type, queue in self.job_queues.items():
                if not queue:
                    continue
                oldest_job = queue[0]
                age_ms = (current_time - oldest_job.timestamp) * 1000
                if age_ms >= self.batch_timeout_ms:
                    await self._create_batch(model_type)

    async def _create_batch(self, model_type: ModelType):
        if not self.job_queues[model_type]:
            return

        jobs = []
        queue = self.job_queues[model_type]
        for _ in range(min(self.max_batch_size, len(queue))):
            if queue:
                jobs.append(queue.popleft())

        if jobs:
            batch_id = f"batch_{self.batch_counter}_{model_type.value}_{int(time.time())}"
            self.batch_counter += 1

            batch = BatchJob(
                jobs=jobs,
                model_type=model_type,
                created_at=time.time(),
                batch_id=batch_id,
            )

            self.pending_batches.append(batch)
            # Ghi metrics batch bằng record_inference (batch được tính là 1 inference)
            self.metrics.record_inference(
                model_name=model_type.value,
                inference_time_ms=0,
                input_size=(0, 0),
                detection_count=0,
                success=True,
            )

    async def _process_ready_batches(self):
        batches_to_process = self.pending_batches.copy()
        self.pending_batches.clear()
        if batches_to_process:
            tasks = [self._process_batch(batch) for batch in batches_to_process]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_all_pending_batches(self):
        async with self.processing_lock:
            for model_type in list(self.job_queues.keys()):
                if self.job_queues[model_type]:
                    await self._create_batch(model_type)
        await self._process_ready_batches()

    async def _process_batch(self, batch: BatchJob):
        try:
            start_time = time.time()
            logging.debug(f"Processing batch {batch.batch_id} with {len(batch.jobs)} jobs")

            if self.detector_factory and hasattr(self.detector_factory, "supports_batch"):
                results = await self._process_batch_optimized(batch)
            else:
                results = await self._process_batch_individual(batch)

            for result in results:
                await self.completed_results.put(result)

            processing_time_ms = (time.time() - start_time) * 1000
            # Ghi metrics batch + inference
            self.metrics.record_inference(
                model_name=batch.model_type.value,
                inference_time_ms=processing_time_ms,
                input_size=(
                    batch.jobs[0].frame.shape[1],
                    batch.jobs[0].frame.shape[0],
                )
                if batch.jobs[0].frame is not None
                else (0, 0),
                detection_count=sum(len(r.detections) for r in results),
                success=True,
            )

            self.total_batches_processed += 1
            self.total_jobs_processed += len(batch.jobs)

            logging.debug(f"Batch {batch.batch_id} completed in {processing_time_ms:.3f}ms")

        except Exception as e:
            logging.error(f"Batch processing error for {batch.batch_id}: {e}")
            self.metrics.record_inference(
                model_name=batch.model_type.value,
                inference_time_ms=0,
                input_size=(0, 0),
                detection_count=0,
                success=False,
                error=str(e),
            )

    async def _process_batch_optimized(self, batch: BatchJob) -> List[DetectionResult]:
        try:
            detector = self.detector_factory(batch.model_type)
            frames = [job.frame for job in batch.jobs]
            batch_frames = np.array(frames)
            batch_results = await detector.detect_batch(batch_frames)

            results = []
            for job, detection_result in zip(batch.jobs, batch_results):
                processing_time_ms = detection_result.processing_time if detection_result else 0

                result = DetectionResult(
                    job_id=job.job_id,
                    camera_id=job.camera_id,
                    timestamp=job.timestamp,
                    model_type=batch.model_type,
                    detections=detection_result.detections if detection_result else [],
                    confidence_threshold=job.config.get("confidence_threshold", 0.5),
                    processing_time_ms=processing_time_ms,
                    worker_id=getattr(self, "worker_id", "batch_worker"),
                    frame_size=(job.frame.shape[1], job.frame.shape[0])
                    if job.frame is not None
                    else (0, 0),
                )
                results.append(result)
            return results
        except Exception as e:
            logging.error(f"Batch optimized processing error: {e}")
            return await self._process_batch_individual(batch)

    async def _process_batch_individual(self, batch: BatchJob) -> List[DetectionResult]:
        results = []
        for job in batch.jobs:
            try:
                processing_time_ms = 0
                detections = []

                if self.detector_factory:
                    start_time = time.time()
                    detector = self.detector_factory(batch.model_type)
                    detection_result = await detector.detect(job.frame)
                    processing_time_ms = (time.time() - start_time) * 1000
                    detections = detection_result.detections if detection_result else []

                result = DetectionResult(
                    job_id=job.job_id,
                    camera_id=job.camera_id,
                    timestamp=job.timestamp,
                    model_type=batch.model_type,
                    detections=detections,
                    confidence_threshold=job.config.get("confidence_threshold", 0.5),
                    processing_time_ms=processing_time_ms,
                    worker_id=getattr(self, "worker_id", "batch_worker"),
                    frame_size=(job.frame.shape[1], job.frame.shape[0])
                    if job.frame is not None
                    else (0, 0),
                )
                results.append(result)
            except Exception as e:
                logging.error(f"Individual job processing error for {job.job_id}: {e}")
                results.append(
                    DetectionResult(
                        job_id=job.job_id,
                        camera_id=job.camera_id,
                        timestamp=job.timestamp,
                        model_type=batch.model_type,
                        detections=[],
                        confidence_threshold=job.config.get("confidence_threshold", 0.5),
                        processing_time_ms=0,
                        worker_id=getattr(self, "worker_id", "batch_worker"),
                        frame_size=(0, 0),
                        error=str(e),
                    )
                )
        return results

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "total_batches_processed": self.total_batches_processed,
            "total_jobs_processed": self.total_jobs_processed,
            "pending_batches": len(self.pending_batches),
            "queued_jobs": {mt.value: len(q) for mt, q in self.job_queues.items()},
            "average_batch_size": (
                self.total_jobs_processed / self.total_batches_processed
                if self.total_batches_processed > 0
                else 0
            ),
            "is_running": self.is_running,
        }

    async def flush_results(self) -> List[DetectionResult]:
        await self._process_all_pending_batches()
        return await self.get_results()

    def configure_dynamic_batching(
        self, enable: bool, max_size: int = None, timeout_ms: int = None
    ):
        self.enable_dynamic_batching = enable
        if max_size is not None:
            self.max_batch_size = max_size
        if timeout_ms is not None:
            self.batch_timeout_ms = timeout_ms
        logging.info(
            f"Dynamic batching configured: enable={enable}, "
            f"max_size={self.max_batch_size}, timeout_ms={self.batch_timeout_ms}"
        )
