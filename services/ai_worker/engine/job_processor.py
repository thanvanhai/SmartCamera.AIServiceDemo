"""
Job processing logic for AI Worker inference engine
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

from core.models import DetectionResult
from services.ai_worker.model_loader import ModelLoader
from services.ai_worker.batch_processor import BatchProcessor
from shared.decorators.timing import time_execution
from .inference_job import InferenceJob


class JobProcessor:
    """
    Handles the processing of inference jobs using AI models
    """
    
    def __init__(self, worker_id: str, config: Dict[str, Any], metrics_collector):
        self.worker_id = worker_id
        self.config = config
        self.metrics = metrics_collector
        self.logger = logging.getLogger(f"job_processor_{worker_id}")
        
        # AI Components
        self.model_loader = ModelLoader(config)
        self.batch_processor = BatchProcessor(config)
        
        # Threading
        self.thread_pool = ThreadPoolExecutor(
            max_workers=config.get('max_threads', 4),
            thread_name_prefix=f"inference_{worker_id}"
        )
        
        # Processing settings
        self.enable_batch_processing = config.get('enable_batch_processing', True)
        self.max_batch_size = config.get('max_batch_size', 8)
        self.batch_timeout_ms = config.get('batch_timeout_ms', 50)

    async def initialize(self):
        """Initialize the job processor"""
        try:
            await self.model_loader.initialize()
            self.logger.info("✅ Job processor initialized")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize job processor: {e}")
            raise

    async def shutdown(self):
        """Shutdown the job processor"""
        self.thread_pool.shutdown(wait=True)
        self.logger.info("✅ Job processor shutdown")

    async def process_job(self, job: InferenceJob) -> Optional[DetectionResult]:
        """
        Process a single inference job
        
        Args:
            job: InferenceJob to process
            
        Returns:
            DetectionResult or None if processing failed
        """
        if self.enable_batch_processing:
            # Add to batch processor
            await self.batch_processor.add_job(job)
            return None  # Results will be handled by batch processor
        else:
            # Process single job immediately
            return await self._process_single_job(job)

    async def _process_single_job(self, job: InferenceJob) -> Optional[DetectionResult]:
        """Process a single inference job synchronously"""
        try:
            # Run inference in thread pool to avoid blocking
            result = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, 
                self._run_inference_sync, 
                job
            )
            
            if result:
                self.metrics.increment('frames_processed')
                
            return result
            
        except Exception as e:
            self.logger.error(f"Single job processing error: {e}", exc_info=True)
            self.metrics.increment('errors_single_inference')
            return None

    async def get_batch_results(self) -> List[DetectionResult]:
        """
        Get processed results from batch processor
        
        Returns:
            List of DetectionResult objects
        """
        if not self.enable_batch_processing:
            return []
        
        try:
            results = await self.batch_processor.get_results()
            for result in results:
                self.metrics.increment('frames_processed')
            return results
        except Exception as e:
            self.logger.error(f"Batch processor error: {e}", exc_info=True)
            self.metrics.increment('errors_batch')
            return []

    @time_execution
    def _run_inference_sync(self, job: InferenceJob) -> Optional[DetectionResult]:
        """
        Run synchronous inference (called from thread pool)
        
        Args:
            job: InferenceJob to process
            
        Returns:
            DetectionResult or None if processing failed
        """
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
        """
        Post-process detection results
        
        Args:
            detections: List of raw detection dictionaries
            config: Processing configuration
            
        Returns:
            List of filtered and processed detection dictionaries
        """
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

    def is_healthy(self) -> bool:
        """
        Check if job processor is healthy
        
        Returns:
            bool: True if healthy, False otherwise
        """
        try:
            return (
                self.model_loader and self.model_loader.is_healthy() and
                not self.thread_pool._shutdown
            )
        except Exception:
            return False