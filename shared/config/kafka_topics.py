# ================================================================================================
# shared/config/kafka_topics.py - Kafka Topic Definitions
# ================================================================================================

from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class KafkaTopicConfig:
    """Kafka topic configuration"""
    name: str
    partitions: int = 1
    replication_factor: int = 1
    config: Optional[Dict[str, str]] = None

class KafkaTopics:
    """Kafka topic definitions"""
    
    # =============================================================================
    # Raw Frame Topics
    # =============================================================================
    
    RAW_FRAMES = KafkaTopicConfig(
        name="smartcamera.raw_frames",
        partitions=4,  # Parallel processing
        replication_factor=1,
        config={
            "retention.ms": "300000",  # 5 minutes
            "max.message.bytes": "10485760",  # 10MB for images
            "compression.type": "snappy"
        }
    )
    
    # =============================================================================
    # AI Results Topics
    # =============================================================================
    
    AI_RESULTS = KafkaTopicConfig(
        name="smartcamera.ai_results",
        partitions=2,
        replication_factor=1,
        config={
            "retention.ms": "86400000",  # 24 hours
            "max.message.bytes": "1048576",  # 1MB
            "compression.type": "gzip"
        }
    )
    
    ALERTS = KafkaTopicConfig(
        name="smartcamera.alerts",
        partitions=1,
        replication_factor=1,
        config={
            "retention.ms": "604800000",  # 7 days
            "max.message.bytes": "1048576"  # 1MB
        }
    )
    
    # =============================================================================
    # Camera Specific Topics (Dynamic)
    # =============================================================================
    
    @staticmethod
    def camera_frames_topic(camera_id: str) -> KafkaTopicConfig:
        """Generate camera-specific frame topic"""
        return KafkaTopicConfig(
            name=f"smartcamera.camera_{camera_id}.frames",
            partitions=1,
            config={
                "retention.ms": "300000",  # 5 minutes
                "max.message.bytes": "10485760"  # 10MB
            }
        )
    
    @staticmethod
    def camera_results_topic(camera_id: str) -> KafkaTopicConfig:
        """Generate camera-specific results topic"""
        return KafkaTopicConfig(
            name=f"smartcamera.camera_{camera_id}.results",
            partitions=1,
            config={
                "retention.ms": "86400000",  # 24 hours
                "max.message.bytes": "1048576"  # 1MB
            }
        )
    
    @classmethod
    def get_all_static_topics(cls) -> Dict[str, KafkaTopicConfig]:
        """Get all static topic configurations"""
        return {
            "raw_frames": cls.RAW_FRAMES,
            "ai_results": cls.AI_RESULTS,
            "alerts": cls.ALERTS
        }