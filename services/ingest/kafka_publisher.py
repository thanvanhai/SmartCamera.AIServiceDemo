import json
import base64
import logging
from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

class KafkaPublisher:
    def __init__(self, brokers: str):
        self.brokers = brokers
        self.producer = None

    async def connect(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.brokers)
        await self.producer.start()
        logger.info("Connected to Kafka at %s", self.brokers)

    async def publish_frame(self, camera_id: int, frame):
        import cv2
        _, buffer = cv2.imencode(".jpg", frame)
        b64 = base64.b64encode(buffer).decode("utf-8")

        topic = f"frames.{camera_id}"
        payload = json.dumps({"camera_id": camera_id, "frame": b64}).encode()

        await self.producer.send_and_wait(topic, payload)
        logger.debug("Published frame to %s", topic)

    async def close(self):
        if self.producer:
            await self.producer.stop()
