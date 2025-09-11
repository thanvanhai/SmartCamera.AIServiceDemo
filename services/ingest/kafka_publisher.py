import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
import cv2
import numpy as np

logger = logging.getLogger(__name__)

class KafkaPublisher:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.admin_client = None
        self.created_topics = set()  # Track created topics

    async def connect(self):
        """Connect to Kafka cluster"""
        try:
            # Initialize producer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip',
                max_batch_size=16384,
                linger_ms=10
            )
            await self.producer.start()
            
            # Initialize admin client for topic management
            self.admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            
            logger.info("✅ Connected to Kafka at %s", self.bootstrap_servers)
            
        except Exception as e:
            logger.error("❌ Failed to connect to Kafka: %s", str(e))
            raise

    async def ensure_topic_exists(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """Ensure topic exists, create if it doesn't"""
        if topic_name in self.created_topics:
            return  # Already ensured this topic exists
            
        try:
            # Check if topic exists by trying to create it
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            await self.admin_client.create_topics([topic])
            logger.info("✅ Created Kafka topic: %s", topic_name)
            
        except TopicAlreadyExistsError:
            logger.info("ℹ️ Topic already exists: %s", topic_name)
        except Exception as e:
            logger.warning("⚠️ Could not ensure topic exists %s: %s", topic_name, str(e))
            # Continue anyway - topic might exist or auto-creation is enabled
        
        self.created_topics.add(topic_name)

    async def publish_frame(self, camera_id: str, frame: np.ndarray):
        """Publish a video frame to Kafka"""
        if self.producer is None:
            logger.error("❌ Kafka producer not initialized")
            return False

        try:
            topic_name = f"frames.{camera_id}"
            
            # Ensure topic exists
            await self.ensure_topic_exists(topic_name)
            
            # Encode frame to JPEG
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            frame_bytes = buffer.tobytes()
            
            # Create message
            message = {
                "camera_id": camera_id,
                "timestamp": asyncio.get_event_loop().time(),
                "frame_size": len(frame_bytes),
                "format": "jpeg"
            }
            
            # Send metadata to Kafka
            await self.producer.send_and_wait(
                topic_name,
                value=message,
                key=camera_id.encode('utf-8')
            )
            
            logger.debug("📤 Published frame for camera %s (size: %d bytes)", 
                        camera_id, len(frame_bytes))
            return True
            
        except Exception as e:
            logger.error("❌ Failed to publish frame for camera %s: %s", camera_id, str(e))
            return False

    async def close(self):
        """Close Kafka connections"""
        if self.producer:
            await self.producer.stop()
            logger.info("🔌 Kafka producer closed")
            
        if self.admin_client:
            await self.admin_client.close()
            logger.info("🔌 Kafka admin client closed")