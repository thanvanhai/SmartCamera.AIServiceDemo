import json
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from datetime import datetime
from app.settings import settings
import asyncio

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Async Kafka Producer wrapper using AIOKafka"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.bootstrap_servers = (
            self.config.get("bootstrap_servers")
            or getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        )

        self.producer: Optional[AIOKafkaProducer] = None
        self.stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "last_send_time": None,
            "start_time": None,
        }

    async def connect(self):
        """Connect to Kafka (alias for start for compatibility)"""
        await self.start()

    async def start(self):
        """Start the producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                key_serializer=lambda x: x.encode("utf-8") if x else None,
                acks="all",
                linger_ms=10,
                max_request_size=1048576,  # 1MB
            )

            await self.producer.start()
            self.stats["start_time"] = datetime.utcnow()
            logger.info("✅ Kafka Producer started")

        except Exception as e:
            logger.error(f"❌ Failed to start Kafka Producer: {e}")
            raise

    async def produce(
        self,
        topic: str,
        message: Dict[str, Any],
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: int = 3,
        retry_delay: float = 1.0,
    ) -> bool:
        """
        Produce message to Kafka with retry

        Args:
            topic: Kafka topic
            message: Message data (will be JSON serialized)
            key: Message key
            partition: Specific partition (optional)
            headers: Message headers
            retries: Retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            True if sent successfully
        """
        if not self.producer:
            logger.error("❌ Producer not started")
            return False

        attempt = 0
        while attempt <= retries:
            try:
                # Add metadata
                enhanced_message = {
                    **message,
                    "sent_at": datetime.utcnow().isoformat(),
                    "producer_id": id(self.producer),
                }

                # Convert headers to bytes
                kafka_headers = []
                if headers:
                    for k, v in headers.items():
                        kafka_headers.append((k, str(v).encode("utf-8")))

                # Send message
                await self.producer.send(
                    topic=topic,
                    value=enhanced_message,
                    key=key,
                    partition=partition,
                    headers=kafka_headers or None,
                )

                self.stats["messages_sent"] += 1
                self.stats["last_send_time"] = datetime.utcnow()

                logger.debug(f"📤 Message sent to {topic}")
                return True

            except KafkaError as e:
                attempt += 1
                logger.error(f"⚠️ Kafka error sending message to {topic} (attempt {attempt}/{retries}): {e}")
                self.stats["messages_failed"] += 1
                if attempt <= retries:
                    await asyncio.sleep(retry_delay)
            except Exception as e:
                logger.error(f"⚠️ Unexpected error sending message to {topic}: {e}")
                self.stats["messages_failed"] += 1
                return False

        return False

    async def send(
        self, topic: str, message: Dict[str, Any], key: Optional[str] = None, **kwargs
    ) -> bool:
        """Alias for produce method"""
        return await self.produce(topic, message, key, **kwargs)

    async def disconnect(self):
        """Disconnect from Kafka (alias for stop for compatibility)"""
        await self.stop()

    async def stop(self):
        """Stop the producer"""
        if self.producer:
            await self.producer.stop()
            self.producer = None

        logger.info("🛑 Kafka Producer stopped")

    async def health_check(self) -> bool:
        """Check producer health"""
        return self.producer is not None

    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        success_rate = 0
        total_attempts = self.stats["messages_sent"] + self.stats["messages_failed"]
        if total_attempts > 0:
            success_rate = (self.stats["messages_sent"] / total_attempts) * 100

        uptime = None
        if self.stats["start_time"]:
            uptime = (datetime.utcnow() - self.stats["start_time"]).total_seconds()

        return {
            "messages_sent": self.stats["messages_sent"],
            "messages_failed": self.stats["messages_failed"],
            "success_rate_percent": round(success_rate, 2),
            "last_send_time": self.stats["last_send_time"].isoformat()
            if self.stats["last_send_time"]
            else None,
            "uptime_seconds": uptime,
            "is_running": self.producer is not None,
        }
