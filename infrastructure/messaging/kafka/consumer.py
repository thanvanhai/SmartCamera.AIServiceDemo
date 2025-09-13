import asyncio
import json
import logging
from typing import Dict, Any, Optional, List, AsyncGenerator
import time
from dataclasses import dataclass

try:
    from aiokafka import AIOKafkaConsumer, ConsumerRecord
    from aiokafka.errors import KafkaError, CommitFailedError, IllegalStateError
    from aiokafka.structs import TopicPartition
except ImportError:
    # Mock for development without kafka installed
    AIOKafkaConsumer = None
    ConsumerRecord = None
    KafkaError = Exception
    CommitFailedError = Exception
    IllegalStateError = Exception
    TopicPartition = None


@dataclass
class MessageMetadata:
    """Message metadata wrapper"""
    topic: str
    partition: int
    offset: int
    timestamp: float
    key: Optional[str]
    headers: Dict[str, str]


@dataclass
class ConsumedMessage:
    """Consumed message wrapper"""
    value: Dict[str, Any]
    metadata: MessageMetadata
    raw_record: Any  # Original ConsumerRecord


class KafkaConsumer:
    """Asynchronous Kafka Consumer for AI Service"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Connection settings
        self.bootstrap_servers = self.config.get(
            'bootstrap_servers',
            ['localhost:9092']
        )
        self.group_id = self.config.get('group_id', 'ai_service_consumer')
        self.consumer = None
        self.is_connected = False
        
        # Consumer settings
        self.auto_offset_reset = self.config.get('auto_offset_reset', 'latest')
        self.enable_auto_commit = self.config.get('enable_auto_commit', True)
        self.auto_commit_interval_ms = self.config.get('auto_commit_interval_ms', 1000)
        self.max_poll_records = self.config.get('max_poll_records', 500)
        self.session_timeout_ms = self.config.get('session_timeout_ms', 30000)
        self.heartbeat_interval_ms = self.config.get('heartbeat_interval_ms', 3000)
        
        # Performance settings
        self.fetch_max_wait_ms = self.config.get('fetch_max_wait_ms', 500)
        self.fetch_min_bytes = self.config.get('fetch_min_bytes', 1024)
        self.fetch_max_bytes = self.config.get('fetch_max_bytes', 52428800)  # 50MB
        
        # Monitoring
        self.messages_consumed = 0
        self.bytes_consumed = 0
        self.errors_count = 0
        self.last_error_time = None
        self.start_time = None
        self.subscribed_topics = set()
        
        # Processing state
        self.is_consuming = False
        self.consumer_task = None
        
    async def connect(self):
        """Connect to Kafka cluster"""
        if self.is_connected:
            self.logger.warning("Consumer already connected")
            return
            
        if not AIOKafkaConsumer:
            self.logger.error("aiokafka not installed. Run: pip install aiokafka")
            raise ImportError("aiokafka package required")
        
        try:
            self.logger.info(f"🔗 Connecting Kafka Consumer: {self.bootstrap_servers}")
            
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=self._deserialize_message,
                key_deserializer=self._deserialize_key,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=self.enable_auto_commit,
                auto_commit_interval_ms=self.auto_commit_interval_ms,
                max_poll_records=self.max_poll_records,
                session_timeout_ms=self.session_timeout_ms,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                fetch_max_wait_ms=self.fetch_max_wait_ms,
                fetch_min_bytes=self.fetch_min_bytes,
                fetch_max_bytes=self.fetch_max_bytes,
                # Security settings (if needed)
                security_protocol=self.config.get('security_protocol', 'PLAINTEXT'),
                # Consumer specific settings
                isolation_level=self.config.get('isolation_level', 'read_uncommitted')
            )
            
            await self.consumer.start()
            self.is_connected = True
            self.start_time = time.time()
            
            self.logger.info(f"✅ Kafka Consumer connected (group: {self.group_id})")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect Kafka Consumer: {e}")
            self.is_connected = False
            raise
    
    async def disconnect(self):
        """Disconnect from Kafka cluster"""
        if not self.is_connected or not self.consumer:
            return
            
        try:
            self.logger.info("🔌 Disconnecting Kafka Consumer...")
            
            # Stop consuming if running
            if self.is_consuming:
                self.is_consuming = False
                if self.consumer_task:
                    self.consumer_task.cancel()
                    try:
                        await self.consumer_task
                    except asyncio.CancelledError:
                        pass
            
            # Commit final offsets
            if self.enable_auto_commit:
                await self.consumer.commit()
            
            await self.consumer.stop()
            
            self.is_connected = False
            self.consumer = None
            self.subscribed_topics.clear()
            
            self.logger.info("✅ Kafka Consumer disconnected")
            
        except Exception as e:
            self.logger.error(f"❌ Error disconnecting Kafka Consumer: {e}")
    
    async def subscribe(self, topics: List[str]):
        """Subscribe to Kafka topics"""
        if not self.is_connected or not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")
        
        try:
            self.consumer.subscribe(topics)
            self.subscribed_topics.update(topics)
            
            self.logger.info(f"📋 Subscribed to topics: {topics}")
            
        except Exception as e:
            self.logger.error(f"❌ Error subscribing to topics {topics}: {e}")
            raise
    
    async def unsubscribe(self):
        """Unsubscribe from all topics"""
        if not self.is_connected or not self.consumer:
            return
            
        try:
            self.consumer.unsubscribe()
            self.subscribed_topics.clear()
            
            self.logger.info("📋 Unsubscribed from all topics")
            
        except Exception as e:
            self.logger.error(f"❌ Error unsubscribing: {e}")
    
    async def consume(self, topic: str) -> AsyncGenerator[ConsumedMessage, None]:
        """
        Consume messages from a specific topic
        
        Args:
            topic: Topic name to consume from
            
        Yields:
            ConsumedMessage: Message with metadata
        """
        if not self.is_connected or not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")
        
        # Subscribe to topic if not already subscribed
        if topic not in self.subscribed_topics:
            await self.subscribe([topic])
        
        self.is_consuming = True
        
        try:
            self.logger.info(f"📥 Starting consumption from topic: {topic}")
            
            async for record in self.consumer:
                if not self.is_consuming:
                    break
                
                # Filter by topic (in case we're subscribed to multiple)
                if record.topic != topic:
                    continue
                
                try:
                    # Create message wrapper
                    metadata = MessageMetadata(
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        timestamp=record.timestamp / 1000.0 if record.timestamp else time.time(),
                        key=record.key,
                        headers={k: v.decode('utf-8') if isinstance(v, bytes) else str(v) 
                                for k, v in (record.headers or {}).items()}
                    )
                    
                    message = ConsumedMessage(
                        value=record.value,
                        metadata=metadata,
                        raw_record=record
                    )
                    
                    # Update metrics
                    self.messages_consumed += 1
                    if record.serialized_value_size:
                        self.bytes_consumed += record.serialized_value_size
                    
                    yield message
                    
                except Exception as e:
                    self.errors_count += 1
                    self.last_error_time = time.time()
                    self.logger.error(f"❌ Error processing message from {topic}: {e}")
                    continue
                    
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Fatal error consuming from {topic}: {e}")
            raise
        finally:
            self.is_consuming = False
    
    async def consume_batch(self, topic: str, max_messages: int = 100, timeout_ms: int = 5000) -> List[ConsumedMessage]:
        """
        Consume a batch of messages from topic
        
        Args:
            topic: Topic to consume from
            max_messages: Maximum number of messages to consume
            timeout_ms: Timeout for batch consumption
            
        Returns:
            List of consumed messages
        """
        if not self.is_connected or not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")
        
        if topic not in self.subscribed_topics:
            await self.subscribe([topic])
        
        messages = []
        start_time = time.time()
        timeout_seconds = timeout_ms / 1000.0
        
        try:
            async for message in self.consume(topic):
                messages.append(message)
                
                # Check limits
                if len(messages) >= max_messages:
                    break
                
                if time.time() - start_time > timeout_seconds:
                    break
            
            self.logger.info(f"📦 Consumed batch: {len(messages)} messages from {topic}")
            return messages
            
        except Exception as e:
            self.logger.error(f"❌ Error consuming batch from {topic}: {e}")
            raise
    
    async def commit_offsets(self, offsets: Optional[Dict[TopicPartition, int]] = None):
        """Manually commit offsets"""
        if not self.is_connected or not self.consumer:
            raise RuntimeError("Consumer not connected")
        
        try:
            if offsets:
                # Commit specific offsets
                await self.consumer.commit(offsets)
            else:
                # Commit current offsets
                await self.consumer.commit()
                
            self.logger.debug("✅ Offsets committed")
            
        except CommitFailedError as e:
            self.logger.error(f"❌ Commit failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"❌ Error committing offsets: {e}")
            raise
    
    async def seek_to_beginning(self, topic: str, partition: int = 0):
        """Seek to beginning of topic partition"""
        if not self.is_connected or not self.consumer:
            raise RuntimeError("Consumer not connected")
        
        try:
            tp = TopicPartition(topic, partition)
            self.consumer.seek_to_beginning(tp)
            
            self.logger.info(f"⏪ Seeked to beginning: {topic}[{partition}]")
            
        except Exception as e:
            self.logger.error(f"❌ Error seeking to beginning: {e}")
            raise
    
    async def seek_to_end(self, topic: str, partition: int = 0):
        """Seek to end of topic partition"""
        if not self.is_connected or not self.consumer:
            raise RuntimeError("Consumer not connected")
        
        try:
            tp = TopicPartition(topic, partition)
            self.consumer.seek_to_end(tp)
            
            self.logger.info(f"⏩ Seeked to end: {topic}[{partition}]")
            
        except Exception as e:
            self.logger.error(f"❌ Error seeking to end: {e}")
            raise
    
    async def pause_consumption(self, topic: str, partition: int = 0):
        """Pause consumption from topic partition"""
        if not self.is_connected or not self.consumer:
            return
        
        try:
            tp = TopicPartition(topic, partition)
            self.consumer.pause(tp)
            
            self.logger.info(f"⏸️ Paused consumption: {topic}[{partition}]")
            
        except Exception as e:
            self.logger.error(f"❌ Error pausing consumption: {e}")
    
    async def resume_consumption(self, topic: str, partition: int = 0):
        """Resume consumption from topic partition"""
        if not self.is_connected or not self.consumer:
            return
        
        try:
            tp = TopicPartition(topic, partition)
            self.consumer.resume(tp)
            
            self.logger.info(f"▶️ Resumed consumption: {topic}[{partition}]")
            
        except Exception as e:
            self.logger.error(f"❌ Error resuming consumption: {e}")
    
    async def health_check(self) -> bool:
        """Check if consumer is healthy"""
        try:
            if not self.is_connected or not self.consumer:
                return False
            
            # Check if consumer is part of group
            try:
                assignment = self.consumer.assignment()
                if assignment is not None:
                    return True
            except IllegalStateError:
                # Consumer might not be in a group yet
                pass
            
            # Basic connection check
            return self.is_connected and self.consumer is not None
            
        except Exception as e:
            self.logger.debug(f"Health check failed: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer performance metrics"""
        uptime = time.time() - self.start_time if self.start_time else 0
        
        return {
            'is_connected': self.is_connected,
            'is_consuming': self.is_consuming,
            'group_id': self.group_id,
            'subscribed_topics': list(self.subscribed_topics),
            'messages_consumed': self.messages_consumed,
            'bytes_consumed': self.bytes_consumed,
            'errors_count': self.errors_count,
            'last_error_time': self.last_error_time,
            'uptime_seconds': uptime,
            'messages_per_second': self.messages_consumed / uptime if uptime > 0 else 0,
            'bytes_per_second': self.bytes_consumed / uptime if uptime > 0 else 0,
            'config': {
                'bootstrap_servers': self.bootstrap_servers,
                'auto_offset_reset': self.auto_offset_reset,
                'enable_auto_commit': self.enable_auto_commit,
                'max_poll_records': self.max_poll_records
            }
        }
    
    @staticmethod
    def _deserialize_message(raw_data: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """Deserialize message from JSON bytes"""
        if raw_data is None:
            return None
            
        try:
            return json.loads(raw_data.decode('utf-8'))
        except Exception as e:
            logging.getLogger('KafkaConsumer').error(f"Message deserialization error: {e}")
            # Return raw data as fallback
            return {'raw_data': raw_data.decode('utf-8', errors='ignore')}
    
    @staticmethod
    def _deserialize_key(raw_key: Optional[bytes]) -> Optional[str]:
        """Deserialize key from bytes"""
        if raw_key is None:
            return None
        return raw_key.decode('utf-8')
    
    def __repr__(self):
        return f"KafkaConsumer(connected={self.is_connected}, group={self.group_id}, topics={self.subscribed_topics})"


# Example usage and testing
async def test_consumer():
    """Test function for development"""
    import os
    
    config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
        'group_id': 'test_ai_consumer',
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'max_poll_records': 100
    }
    
    consumer = KafkaConsumer(config)
    
    try:
        await consumer.connect()
        
        print("✅ Consumer connected, waiting for messages...")
        
        # Test consuming messages
        message_count = 0
        async for message in consumer.consume('ai_results'):
            message_count += 1
            print(f"📥 Received message {message_count}: {message.value}")
            
            # Stop after 10 messages for testing
            if message_count >= 10:
                break
        
        # Test batch consumption
        print("\n🔄 Testing batch consumption...")
        batch_messages = await consumer.consume_batch('video_frames', max_messages=5, timeout_ms=3000)
        print(f"📦 Received {len(batch_messages)} messages in batch")
        
        # Print metrics
        metrics = consumer.get_metrics()
        print(f"📊 Consumer metrics: {metrics}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        
    finally:
        await consumer.disconnect()


if __name__ == "__main__":
    # Run test
    asyncio.run(test_consumer())