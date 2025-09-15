"""
Kafka operations handler for AI Worker inference engine
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, AsyncGenerator

from infrastructure.messaging.kafka.producer import KafkaProducer
from infrastructure.messaging.kafka.consumer import KafkaConsumer
from core.models import DetectionResult
from .inference_job import InferenceJob


class KafkaHandler:
    """
    Handles all Kafka operations for the inference engine
    """
    
    def __init__(self, worker_id: str, config: Dict[str, Any], metrics_collector):
        self.worker_id = worker_id
        self.kafka_config = config.get('kafka', {})
        
        # Add worker-specific group_id
        self.kafka_config['group_id'] = f"ai_worker_{worker_id}"
        
        self.metrics = metrics_collector
        self.logger = logging.getLogger(f"kafka_handler_{worker_id}")
        
        # Kafka clients with configuration
        self.producer = KafkaProducer(self.kafka_config)
        self.consumer = KafkaConsumer(self.kafka_config)
        
        # State
        self.is_connected = False

    async def connect(self):
        """Connect to Kafka services"""
        try:
            await self.producer.connect()
            await self.consumer.connect()
            self.is_connected = True
            self.logger.info("Kafka clients connected")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise

    async def disconnect(self):
        """Disconnect from Kafka services"""
        try:
            await self.producer.disconnect()
            await self.consumer.disconnect()
            self.is_connected = False
            self.logger.info("Kafka clients disconnected")
        except Exception as e:
            self.logger.warning(f"Error disconnecting from Kafka: {e}")

    async def consume_frames(self) -> AsyncGenerator[Optional[InferenceJob], None]:
        """
        Consume video frames from Kafka and yield InferenceJob objects
        """
        self.logger.info("Starting frame consumption from 'smartcamera' topic...")
        
        try:
            async for message in self.consumer.consume(topic='smartcamera'):
                try:
                    # Create inference job from message
                    job = self._create_inference_job(message.value)
                    
                    if job:
                        self.metrics.increment('frames_received')
                        yield job
                    else:
                        self.metrics.increment('frames_invalid')
                        yield None
                        
                except Exception as e:
                    self.logger.error(f"Error processing frame message: {e}", exc_info=True)
                    self.metrics.increment('errors_consume')
                    yield None
                    
        except Exception as e:
            self.logger.error(f"Frame consumer fatal error: {e}", exc_info=True)
            raise

    async def publish_result(self, result: DetectionResult) -> bool:
        """
        Publish inference result to Kafka
        
        Returns:
            bool: True if successfully published, False otherwise
        """
        try:
            success = await self.producer.produce(
                topic='ai_results',
                message=result.to_dict(),
                key=result.camera_id
            )
            
            if success:
                self.metrics.increment('results_published')
            else:
                self.metrics.increment('errors_publish')
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error publishing result: {e}", exc_info=True)
            self.metrics.increment('errors_publish')
            return False

    async def health_check(self) -> bool:
        """
        Check Kafka connection health
        
        Returns:
            bool: True if healthy, False otherwise
        """
        try:
            producer_healthy = await self.producer.health_check()
            consumer_healthy = await self.consumer.health_check()
            return producer_healthy and consumer_healthy
        except Exception as e:
            self.logger.debug(f"Kafka health check failed: {e}")
            return False

    def _create_inference_job(self, frame_data: Dict) -> Optional[InferenceJob]:
        """
        Create InferenceJob from Kafka message data
        
        Args:
            frame_data: Dict containing frame data from Kafka message
            
        Returns:
            InferenceJob or None if creation failed
        """
        try:
            import cv2
            import numpy as np
            from pathlib import Path
            from core.enums import ModelType
            
            # Handle both frame bytes and frame path (Claim-Check pattern)
            frame = None
            
            if 'frame' in frame_data:
                # Direct frame bytes
                frame_bytes = frame_data.get('frame')
                if frame_bytes:
                    nparr = np.frombuffer(bytes(frame_bytes), np.uint8)
                    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            elif 'frame_path' in frame_data:
                # Claim-Check pattern - load from file
                frame_path = Path("./frames") / frame_data.get('frame_path')
                if frame_path.exists():
                    frame = cv2.imread(str(frame_path))
                else:
                    self.logger.warning(f"Frame file not found: {frame_path}")
                    return None
            
            if frame is None:
                self.logger.warning("No valid frame data in message")
                return None
            
            # Extract other job parameters
            camera_id = frame_data.get('camera_id')
            if not camera_id:
                self.logger.warning("No camera_id in message")
                return None
            
            # Create and return job
            return InferenceJob(
                job_id=frame_data.get('job_id', f"{camera_id}_{time.time()}"),
                camera_id=camera_id,
                frame=frame,
                timestamp=frame_data.get('timestamp', time.time()),
                model_type=ModelType(frame_data.get('model_type', 'yolo_v8')),
                config=frame_data.get('config', {}),
                priority=frame_data.get('priority', 1)
            )
            
        except Exception as e:
            self.logger.error(f"Error creating inference job: {e}", exc_info=True)
            return None