import asyncio
import json
import logging
from typing import Dict, Any, Optional
import time
from dataclasses import asdict

try:
    from aiokafka import AIOKafkaProducer
    from aiokafka.errors import KafkaError, KafkaTimeoutError
except ImportError:
    # Mock for development without kafka installed
    AIOKafkaProducer = None
    KafkaError = Exception
    KafkaTimeoutError = Exception


class KafkaProducer:
    """Asynchronous Kafka Producer for AI Service"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Connection settings
        self.bootstrap_servers = self.config.get(
            'bootstrap_servers', 
            ['localhost:9092']
        )
        self.producer = None
        self.is_connected = False
        
        # Performance settings
        self.batch_size = self.config.get('batch_size', 16384)
        self.linger_ms = self.config.get('linger_ms', 10)
        self.compression_type = self.config.get('compression_type', 'gzip')
        self.acks = self.config.get('acks', 'all')
        self.retries = self.config.get('retries', 3)
        self.retry_backoff_ms = self.config.get('retry_backoff_ms', 100)
        
        # Monitoring
        self.messages_sent = 0
        self.bytes_sent = 0
        self.errors_count = 0
        self.last_error_time = None
        self.start_time = None
        
    async def connect(self):
        """Connect to Kafka cluster"""
        if self.is_connected:
            self.logger.warning("Producer already connected")
            return
            
        if not AIOKafkaProducer:
            self.logger.error("aiokafka not installed. Run: pip install aiokafka")
            raise ImportError("aiokafka package required")
        
        try:
            self.logger.info(f"🔗 Connecting to Kafka: {self.bootstrap_servers}")
            
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=self._serialize_message,
                key_serializer=self._serialize_key,
                batch_size=self.batch_size,
                linger_ms=self.linger_ms,
                compression_type=self.compression_type,
                acks=self.acks,
                retries=self.retries,
                retry_backoff_ms=self.retry_backoff_ms,
                # Security settings (if needed)
                security_protocol=self.config.get('security_protocol', 'PLAINTEXT'),
                # Performance settings
                max_request_size=self.config.get('max_request_size', 1048576),  # 1MB
                request_timeout_ms=self.config.get('request_timeout_ms', 30000),
                # Error handling
                enable_idempotence=self.config.get('enable_idempotence', True)
            )
            
            await self.producer.start()
            self.is_connected = True
            self.start_time = time.time()
            
            self.logger.info("✅ Kafka Producer connected successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect Kafka Producer: {e}")
            self.is_connected = False
            raise
    
    async def disconnect(self):
        """Disconnect from Kafka cluster"""
        if not self.is_connected or not self.producer:
            return
            
        try:
            self.logger.info("🔌 Disconnecting Kafka Producer...")
            
            # Flush remaining messages
            await self.producer.flush()
            await self.producer.stop()
            
            self.is_connected = False
            self.producer = None
            
            self.logger.info("✅ Kafka Producer disconnected")
            
        except Exception as e:
            self.logger.error(f"❌ Error disconnecting Kafka Producer: {e}")
    
    async def produce(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """
        Produce message to Kafka topic
        
        Args:
            topic: Kafka topic name
            message: Message data (dict)
            key: Optional partition key
        """
        if not self.is_connected or not self.producer:
            raise RuntimeError("Producer not connected. Call connect() first.")
        
        try:
            # Add metadata to message
            enriched_message = {
                **message,
                'producer_timestamp': time.time(),
                'producer_id': id(self.producer)
            }
            
            # Send message
            record_metadata = await self.producer.send_and_wait(
                topic=topic,
                value=enriched_message,
                key=key
            )
            
            # Update metrics
            self.messages_sent += 1
            message_size = len(json.dumps(enriched_message).encode('utf-8'))
            self.bytes_sent += message_size
            
            self.logger.debug(
                f"📤 Message sent to {topic}[{record_metadata.partition}] "
                f"offset={record_metadata.offset}, size={message_size}bytes"
            )
            
            return record_metadata
            
        except KafkaTimeoutError as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"⏱️ Kafka timeout sending to {topic}: {e}")
            raise
            
        except KafkaError as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Kafka error sending to {topic}: {e}")
            raise
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"💥 Unexpected error sending to {topic}: {e}")
            raise
    
    async def produce_batch(self, topic: str, messages: list, keys: Optional[list] = None):
        """
        Produce multiple messages to Kafka topic
        
        Args:
            topic: Kafka topic name  
            messages: List of message dicts
            keys: Optional list of partition keys (same length as messages)
        """
        if not self.is_connected or not self.producer:
            raise RuntimeError("Producer not connected. Call connect() first.")
        
        if keys and len(keys) != len(messages):
            raise ValueError("Keys list must have same length as messages list")
        
        try:
            # Send all messages
            futures = []
            for i, message in enumerate(messages):
                key = keys[i] if keys else None
                
                enriched_message = {
                    **message,
                    'producer_timestamp': time.time(),
                    'producer_id': id(self.producer),
                    'batch_index': i
                }
                
                future = self.producer.send(
                    topic=topic,
                    value=enriched_message,
                    key=key
                )
                futures.append(future)
            
            # Wait for all messages to be sent
            results = await asyncio.gather(*futures, return_exceptions=True)
            
            # Count successes and failures
            success_count = 0
            error_count = 0
            
            for result in results:
                if isinstance(result, Exception):
                    error_count += 1
                    self.logger.error(f"❌ Batch message failed: {result}")
                else:
                    success_count += 1
            
            # Update metrics
            self.messages_sent += success_count
            self.errors_count += error_count
            
            if error_count > 0:
                self.last_error_time = time.time()
            
            self.logger.info(
                f"📤 Batch sent to {topic}: {success_count} success, {error_count} failed"
            )
            
            return {
                'success_count': success_count,
                'error_count': error_count,
                'results': results
            }
            
        except Exception as e:
            self.logger.error(f"💥 Batch send error to {topic}: {e}")
            raise
    
    async def flush(self):
        """Flush any pending messages"""
        if self.is_connected and self.producer:
            try:
                await self.producer.flush()
                self.logger.debug("🚽 Producer flushed")
            except Exception as e:
                self.logger.error(f"❌ Error flushing producer: {e}")
                raise
    
    async def health_check(self) -> bool:
        """Check if producer is healthy"""
        try:
            if not self.is_connected or not self.producer:
                return False
            
            # Try to get cluster metadata as health check
            cluster = self.producer.client.cluster
            if cluster and len(cluster.brokers()) > 0:
                return True
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Health check failed: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer performance metrics"""
        uptime = time.time() - self.start_time if self.start_time else 0
        
        return {
            'is_connected': self.is_connected,
            'messages_sent': self.messages_sent,
            'bytes_sent': self.bytes_sent,
            'errors_count': self.errors_count,
            'last_error_time': self.last_error_time,
            'uptime_seconds': uptime,
            'messages_per_second': self.messages_sent / uptime if uptime > 0 else 0,
            'bytes_per_second': self.bytes_sent / uptime if uptime > 0 else 0,
            'config': {
                'bootstrap_servers': self.bootstrap_servers,
                'batch_size': self.batch_size,
                'linger_ms': self.linger_ms,
                'compression_type': self.compression_type
            }
        }
    
    @staticmethod
    def _serialize_message(message: Dict[str, Any]) -> bytes:
        """Serialize message to JSON bytes"""
        try:
            # Handle dataclass objects
            if hasattr(message, '__dataclass_fields__'):
                message = asdict(message)
            
            return json.dumps(message, ensure_ascii=False, default=str).encode('utf-8')
            
        except Exception as e:
            logging.getLogger('KafkaProducer').error(f"Message serialization error: {e}")
            raise
    
    @staticmethod  
    def _serialize_key(key: Optional[str]) -> Optional[bytes]:
        """Serialize key to bytes"""
        if key is None:
            return None
        return str(key).encode('utf-8')
    
    def __repr__(self):
        return f"KafkaProducer(connected={self.is_connected}, servers={self.bootstrap_servers})"


# Example usage and testing
async def test_producer():
    """Test function for development"""
    import os
    
    config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
        'batch_size': 16384,
        'linger_ms': 10,
        'compression_type': 'gzip'
    }
    
    producer = KafkaProducer(config)
    
    try:
        await producer.connect()
        
        # Test single message
        test_message = {
            'camera_id': 'test_camera_001',
            'detections': [
                {'class': 'person', 'confidence': 0.95, 'bbox': [100, 100, 200, 300]},
                {'class': 'car', 'confidence': 0.88, 'bbox': [300, 150, 500, 400]}
            ],
            'timestamp': time.time(),
            'processing_time_ms': 45.2
        }
        
        await producer.produce(
            topic='ai_results',
            message=test_message,
            key='test_camera_001'
        )
        
        print("✅ Test message sent successfully")
        
        # Test batch messages
        batch_messages = [
            {'camera_id': f'camera_{i}', 'data': f'test_data_{i}', 'timestamp': time.time()}
            for i in range(5)
        ]
        
        await producer.produce_batch(
            topic='test_batch',
            messages=batch_messages,
            keys=[f'camera_{i}' for i in range(5)]
        )
        
        print("✅ Batch messages sent successfully")
        
        # Print metrics
        metrics = producer.get_metrics()
        print(f"📊 Metrics: {metrics}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        
    finally:
        await producer.disconnect()


if __name__ == "__main__":
    # Run test
    asyncio.run(test_producer())