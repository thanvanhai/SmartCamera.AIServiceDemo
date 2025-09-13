# /home/haicoi/SmartCamera.AIServiceDemo/infrastructure/messaging/kafka/consumer.py

import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, List, AsyncGenerator
from dataclasses import dataclass

# Sử dụng try-except để không bị lỗi nếu chưa cài aiokafka
try:
    from aiokafka import AIOKafkaConsumer, ConsumerRecord
    from aiokafka.errors import KafkaError, CommitFailedError, IllegalStateError
    from aiokafka.structs import TopicPartition
except ImportError:
    AIOKafkaConsumer = None
    # ... (giữ nguyên phần mock của bạn)

# --- Các lớp dữ liệu (Data Classes) ---
# Giữ nguyên các dataclass vì chúng giúp cấu trúc code rõ ràng
@dataclass
class MessageMetadata:
    """Đóng gói metadata của message"""
    topic: str
    partition: int
    offset: int
    timestamp: float
    key: Optional[str]

@dataclass
class ConsumedMessage:
    """Đóng gói message đã được consume"""
    value: Dict[str, Any]
    metadata: MessageMetadata
    raw_record: Any  # Giữ lại record gốc nếu cần


class KafkaConsumer:
    """
    Lớp Kafka Consumer bất đồng bộ, được tái cấu trúc để có cách khởi tạo tường minh.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        # Các tham số cấu hình khác với giá trị mặc định hợp lý
        auto_offset_reset: str = 'latest',
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 1000,
        session_timeout_ms: int = 30000,
        heartbeat_interval_ms: int = 3000,
        fetch_max_bytes: int = 52428800  # 50MB
    ):
        """
        Khởi tạo Consumer với các tham số rõ ràng thay vì một config dictionary.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # --- THAY ĐỔI CHÍNH NẰM Ở ĐÂY ---
        # Gán trực tiếp các tham số thay vì dùng .get() từ dictionary
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.session_timeout_ms = session_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.fetch_max_bytes = fetch_max_bytes
        
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.is_connected = False
        
        # Các thông số giám sát (monitoring)
        self.messages_consumed = 0
        self.start_time = None
        self.subscribed_topics = set()
        self.is_consuming = False

    async def connect(self):
        """Kết nối tới Kafka cluster"""
        if self.is_connected:
            self.logger.warning("Consumer đã được kết nối.")
            return
            
        self.logger.info(f"🔗 Đang kết nối Kafka Consumer tới {self.bootstrap_servers}...")
        try:
            self.consumer = AIOKafkaConsumer(
                # Topics sẽ được truyền vào khi gọi subscribe() hoặc consume()
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=self._deserialize_message,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=self.enable_auto_commit,
                auto_commit_interval_ms=self.auto_commit_interval_ms,
                session_timeout_ms=self.session_timeout_ms,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                fetch_max_bytes=self.fetch_max_bytes
            )
            await self.consumer.start()
            self.is_connected = True
            self.start_time = time.time()
            self.logger.info(f"✅ Kết nối thành công (group: {self.group_id})")
        except Exception as e:
            self.logger.error(f"❌ Kết nối thất bại: {e}", exc_info=True)
            self.is_connected = False
            raise

    async def subscribe(self, topics: List[str]):
        """Đăng ký (subscribe) vào các topic"""
        if not self.is_connected:
            raise RuntimeError("Consumer chưa kết nối. Hãy gọi connect() trước.")
        
        self.logger.info(f"📋 Đăng ký vào các topic: {topics}")
        self.consumer.subscribe(topics)
        self.subscribed_topics.update(topics)

    async def consume(self, topic: str) -> AsyncGenerator[ConsumedMessage, None]:
        """
        Nhận message liên tục từ một topic cụ thể dưới dạng async generator.
        """
        if not self.is_connected:
            raise RuntimeError("Consumer chưa kết nối. Hãy gọi connect() trước.")
        
        if topic not in self.subscribed_topics:
            await self.subscribe([topic])
        
        self.is_consuming = True
        self.logger.info(f"📥 Bắt đầu nhận message từ topic: {topic}")
        try:
            async for record in self.consumer:
                if not self.is_consuming: break
                if record.topic != topic: continue
                
                self.messages_consumed += 1
                yield ConsumedMessage(
                    value=record.value,
                    metadata=MessageMetadata(
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        timestamp=record.timestamp / 1000.0,
                        key=record.key
                    ),
                    raw_record=record
                )
        except Exception as e:
            self.logger.error(f"❌ Lỗi nghiêm trọng khi consume từ {topic}: {e}", exc_info=True)
            raise
        finally:
            self.is_consuming = False
            self.logger.info(f"🛑 Đã dừng nhận message từ topic: {topic}")

    async def consume_batch(self, topic: str, max_messages: int = 100, timeout_ms: int = 5000) -> List[ConsumedMessage]:
        """Nhận một lô (batch) message từ topic."""
        # Logic của hàm này không thay đổi, nó vẫn hoạt động tốt
        batch = []
        try:
            async for message in self.consume(topic):
                batch.append(message)
                if len(batch) >= max_messages:
                    break
            # Lưu ý: aiokafka tự quản lý timeout, nên không cần check time thủ công phức tạp
        except Exception as e:
            self.logger.error(f"❌ Lỗi khi consume batch từ {topic}: {e}")
        return batch

    async def close(self):
        """Ngắt kết nối an toàn"""
        if not self.is_connected: return
        
        self.logger.info("🔌 Đang ngắt kết nối Kafka Consumer...")
        self.is_consuming = False
        if self.consumer:
            await self.consumer.stop()
        self.is_connected = False
        self.logger.info("✅ Đã ngắt kết nối.")
    
    @staticmethod
    def _deserialize_message(raw_data: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """Giải mã message từ JSON bytes"""
        if not raw_data: return None
        try:
            return json.loads(raw_data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.getLogger('KafkaConsumer').error(f"Lỗi giải mã message: {e}")
            return {"error": "deserialization_failed", "raw_data": raw_data.decode('utf-8', errors='ignore')}

    def get_metrics(self) -> Dict[str, Any]:
        """Lấy các chỉ số hoạt động của consumer"""
        uptime = time.time() - self.start_time if self.start_time else 0
        return {
            'is_connected': self.is_connected,
            'is_consuming': self.is_consuming,
            'group_id': self.group_id,
            'subscribed_topics': list(self.subscribed_topics),
            'messages_consumed': self.messages_consumed,
            'uptime_seconds': uptime,
            'messages_per_second': self.messages_consumed / uptime if uptime > 0 else 0,
        }

    def __repr__(self):
        return f"KafkaConsumer(connected={self.is_connected}, group={self.group_id}, topics={list(self.subscribed_topics)})"