import json
import logging
from typing import List, Dict, Any, Optional, AsyncGenerator
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from datetime import datetime
from app.settings import settings

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Async Kafka Consumer wrapper using AIOKafka"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.bootstrap_servers = (
            self.config.get('bootstrap_servers') or 
            getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        )
        
        # Consumer configuration
        self.group_id = self.config.get('group_id', 'ai_worker_group')
        self.auto_offset_reset = self.config.get('auto_offset_reset', 'latest')
        
        self.consumer = None
        self.stats = {
            'messages_consumed': 0,
            'messages_failed': 0,
            'last_consume_time': None,
            'start_time': None
        }
    
    async def connect(self):
        """Connect to Kafka (alias for start for compatibility)"""
        await self.start()
    
    async def start(self):
        """Start the consumer"""
        try:
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                enable_auto_commit=True,
                max_poll_records=10
            )
            
            await self.consumer.start()
            self.stats['start_time'] = datetime.utcnow()
            logger.info("Kafka Consumer started")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka Consumer: {e}")
            raise
    
    async def subscribe(self, topics: list):
        """Subscribe to topics"""
        if not self.consumer:
            logger.error("Consumer not started")
            return False
        
        try:
            self.consumer.subscribe(topics)
            logger.info(f"Subscribed to topics: {topics}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe to topics {topics}: {e}")
            return False
    
    async def consume(self, topic: str = None) -> AsyncGenerator[Any, None]:
        """
        Consume messages from Kafka
        
        Args:
            topic: Single topic to consume from (will subscribe if not already subscribed)
            
        Yields:
            Message objects with .value, .key, .topic, .partition, .offset attributes
        """
        if not self.consumer:
            logger.error("Consumer not started")
            return
        
        try:
            # Subscribe to topic if provided
            if topic:
                await self.subscribe([topic])
            
            async for message in self.consumer:
                try:
                    self.stats['messages_consumed'] += 1
                    self.stats['last_consume_time'] = datetime.utcnow()
                    
                    logger.debug(f"Consumed message from {message.topic} partition {message.partition}")
                    yield message
                    
                except Exception as e:
                    logger.error(f"Error processing consumed message: {e}")
                    self.stats['messages_failed'] += 1
                    
        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")
            raise
        except Exception as e:
            logger.error(f"Consumer fatal error: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Kafka (alias for stop for compatibility)"""
        await self.stop()
    
    async def stop(self):
        """Stop the consumer"""
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None
        
        logger.info("Kafka Consumer stopped")
    
    async def health_check(self) -> bool:
        """Check consumer health"""
        return self.consumer is not None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        error_rate = 0
        total_messages = self.stats['messages_consumed'] + self.stats['messages_failed']
        if total_messages > 0:
            error_rate = (self.stats['messages_failed'] / total_messages) * 100
        
        uptime = None
        if self.stats['start_time']:
            uptime = (datetime.utcnow() - self.stats['start_time']).total_seconds()
        
        return {
            'messages_consumed': self.stats['messages_consumed'],
            'messages_failed': self.stats['messages_failed'],
            'error_rate_percent': round(error_rate, 2),
            'last_consume_time': self.stats['last_consume_time'].isoformat() if self.stats['last_consume_time'] else None,
            'uptime_seconds': uptime,
            'is_running': self.consumer is not None
        }