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
        self.metrics = BatchMetrics()
        self.detector_factory = detector_factory
        
        # Batch settings
        self.max_batch_size = config.get('max_batch_size', 8)
        self.batch_timeout_ms = config.get('batch_timeout_ms', 50)
        self.enable_dynamic_batching = config.get('enable_dynamic_batching', True)
        
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
        """Start batch processor background tasks"""
        if self.is_running:
            return
            
        self.is_running = True
        self.batch_processor_task = asyncio.create_task(self._batch_processor_loop())
        logging.info("BatchProcessor started")
    
    async def stop(self):
        """Stop batch processor"""
        self.is_running = False
        
        if self.batch_processor_task:
            self.batch_processor_task.cancel()
            try:
                await self.batch_processor_task
            except asyncio.CancelledError:
                pass
        
        # Process any remaining batches
        await self._process_all_pending_batches()
        logging.info("BatchProcessor stopped")
    
    async def add_job(self, job):
        """Add job to appropriate batch queue"""
        if not self.is_running:
            await self.start()
            
        async with self.processing_lock:
            model_type = job.model_type
            self.job_queues[model_type].append(job)
            
            # Check if we can form a batch immediately
            if len(self.job_queues[model_type]) >= self.max_batch_size:
                await self._create_batch(model_type)
    
    async def get_results(self) -> List[DetectionResult]:
        """Get completed inference results"""
        results = []
        
        # Get all available results without blocking
        while True:
            try:
                result = self.completed_results.get_nowait()
                results.append(result)
            except asyncio.QueueEmpty:
                break
        
        return results
    
    async def _batch_processor_loop(self):
        """Background loop to process batches"""
        while self.is_running:
            try:
                await self._check_and_create_timeout_batches()
                await self._process_ready_batches()
                
                # Sleep for a short time to avoid busy waiting
                await asyncio.sleep(0.01)  # 10ms
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Batch processor loop error: {e}")
                await asyncio.sleep(0.1)
    
    async def _check_and_create_timeout_batches(self):
        """Create batches from queues that have timed out"""
        async with self.processing_lock:
            current_time = time.time()
            
            for model_type, queue in self.job_queues.items():
                if not queue:
                    continue
                
                # Check if oldest job has timed out
                oldest_job = queue[0]
                age_ms = (current_time - oldest_job.timestamp) * 1000
                
                if age_ms >= self.batch_timeout_ms:
                    await self._create_batch(model_type)
    
    async def _create_batch(self, model_type: ModelType):
        """Create batch from queued jobs"""
        if not self.job_queues[model_type]:
            return
        
        # Get jobs for batch
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
                batch_id=batch_id
            )
            
            self.pending_batches.append(batch)
            self.metrics.increment_batches_created()
    
    async def _process_ready_batches(self):
        """Process all pending batches"""
        batches_to_process = self.pending_batches.copy()
        self.pending_batches.clear()
        
        # Process batches concurrently
        if batches_to_process:
            tasks = [self._process_batch(batch) for batch in batches_to_process]
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_all_pending_batches(self):
        """Process all remaining batches and queued jobs"""
        # Create batches from remaining queued jobs
        async with self.processing_lock:
            for model_type in list(self.job_queues.keys()):
                if self.job_queues[model_type]:
                    await self._create_batch(model_type)
        
        # Process all pending batches
        await self._process_ready_batches()
    
    async def _process_batch(self, batch: BatchJob):
        """Process a single batch"""
        try:
            start_time = time.time()
            
            logging.debug(f"Processing batch {batch.batch_id} with {len(batch.jobs)} jobs")
            
            # Process batch based on capabilities
            if self.detector_factory and hasattr(self.detector_factory, 'supports_batch'):
                results = await self._process_batch_optimized(batch)
            else:
                results = await self._process_batch_individual(batch)
            
            # Add results to output queue
            for result in results:
                await self.completed_results.put(result)
            
            # Record metrics
            processing_time = time.time() - start_time
            self.metrics.record_batch_processing_time(processing_time)
            self.metrics.increment_batches_processed()
            
            self.total_batches_processed += 1
            self.total_jobs_processed += len(batch.jobs)
            
            logging.debug(f"Batch {batch.batch_id} completed in {processing_time:.3f}s")
            
        except Exception as e:
            logging.error(f"Batch processing error for {batch.batch_id}: {e}")
            self.metrics.increment_batch_errors()
    
    async def _process_batch_optimized(self, batch: BatchJob) -> List[DetectionResult]:
        """Process batch with optimized batch inference"""
        try:
            # Get detector instance
            detector = self.detector_factory(batch.model_type)
            
            # Prepare batch data
            frames = []
            job_metadata = []
            
            for job in batch.jobs:
                frames.append(job.frame)
                job_metadata.append({
                    'job_id': job.job_id,
                    'camera_id': job.camera_id,
                    'timestamp': job.timestamp,
                    'config': job.config
                })
            
            # Convert to numpy array for batch processing
            batch_frames = np.array(frames)
            
            # Run batch inference
            batch_results = await detector.detect_batch(batch_frames)
            
            # Create DetectionResult objects
            results = []
            for i, (job, detection_result) in enumerate(zip(batch.jobs, batch_results)):
                result = DetectionResult(
                    job_id=job.job_id,
                    camera_id=job.camera_id,
                    timestamp=job.timestamp,
                    model_type=batch.model_type,
                    detections=detection_result.detections if detection_result else [],
                    confidence_threshold=job.config.get('confidence_threshold', 0.5),
                    processing_time=detection_result.processing_time if detection_result else 0,
                    frame_size=(job.frame.shape[1], job.frame.shape[0]) if job.frame is not None else (0, 0)
                )
                results.append(result)
            
            return results
            
        except Exception as e:
            logging.error(f"Batch optimized processing error: {e}")
            # Fallback to individual processing
            return await self._process_batch_individual(batch)
    
    async def _process_batch_individual(self, batch: BatchJob) -> List[DetectionResult]:
        """Process batch jobs individually (fallback)"""
        results = []
        
        # Process each job individually
        for job in batch.jobs:
            try:
                # Get detector instance
                if self.detector_factory:
                    detector = self.detector_factory(batch.model_type)
                    
                    # Run detection
                    detection_result = await detector.detect(job.frame)
                    
                    # Create result object
                    result = DetectionResult(
                        job_id=job.job_id,
                        camera_id=job.camera_id,
                        timestamp=job.timestamp,
                        model_type=batch.model_type,
                        detections=detection_result.detections if detection_result else [],
                        confidence_threshold=job.config.get('confidence_threshold', 0.5),
                        processing_time=detection_result.processing_time if detection_result else 0,
                        frame_size=(job.frame.shape[1], job.frame.shape[0]) if job.frame is not None else (0, 0)
                    )
                else:
                    # No detector available - create empty result
                    result = DetectionResult(
                        job_id=job.job_id,
                        camera_id=job.camera_id,
                        timestamp=job.timestamp,
                        model_type=batch.model_type,
                        detections=[],
                        confidence_threshold=job.config.get('confidence_threshold', 0.5),
                        processing_time=0,
                        frame_size=(0, 0)
                    )
                
                results.append(result)
                
            except Exception as e:
                logging.error(f"Individual job processing error for {job.job_id}: {e}")
                # Create error result
                error_result = DetectionResult(
                    job_id=job.job_id,
                    camera_id=job.camera_id,
                    timestamp=job.timestamp,
                    model_type=batch.model_type,
                    detections=[],
                    confidence_threshold=job.config.get('confidence_threshold', 0.5),
                    processing_time=0,
                    frame_size=(0, 0),
                    error=str(e)
                )
                results.append(error_result)
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return {
            'total_batches_processed': self.total_batches_processed,
            'total_jobs_processed': self.total_jobs_processed,
            'pending_batches': len(self.pending_batches),
            'queued_jobs': {
                model_type.value: len(queue) 
                for model_type, queue in self.job_queues.items()
            },
            'average_batch_size': (
                self.total_jobs_processed / self.total_batches_processed 
                if self.total_batches_processed > 0 else 0
            ),
            'is_running': self.is_running
        }
    
    async def flush_results(self) -> List[DetectionResult]:
        """Flush all results and wait for completion"""
        # Process any remaining jobs
        await self._process_all_pending_batches()
        
        # Return all results
        return await self.get_results()
    
    def configure_dynamic_batching(self, enable: bool, max_size: int = None, timeout_ms: int = None):
        """Configure dynamic batching settings"""
        self.enable_dynamic_batching = enable
        
        if max_size is not None:
            self.max_batch_size = max_size
            
        if timeout_ms is not None:
            self.batch_timeout_ms = timeout_ms
        
        logging.info(f"Dynamic batching configured: enable={enable}, max_size={self.max_batch_size}, timeout_ms={self.batch_timeout_ms}")