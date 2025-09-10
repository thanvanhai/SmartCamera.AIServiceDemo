# ================================================================================================
# scripts/setup/setup_kafka_topics.py - Kafka Topics Setup
# ================================================================================================

import asyncio
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError
import structlog
from typing import List, Dict

from shared.config.kafka_topics import KafkaTopics, KafkaTopicConfig
from app.settings import settings

logger = structlog.get_logger(__name__)

class KafkaTopicManager:
    """Manage Kafka topics creation and configuration"""
    
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="smartcamera_setup"
        )
    
    async def create_topics(self, topic_configs: Dict[str, KafkaTopicConfig]) -> None:
        """Create Kafka topics from configurations"""
        
        new_topics = []
        for name, config in topic_configs.items():
            topic = NewTopic(
                name=config.name,
                num_partitions=config.partitions,
                replication_factor=config.replication_factor,
                topic_configs=config.config or {}
            )
            new_topics.append(topic)
            
            logger.info(
                "Creating Kafka topic",
                topic_name=config.name,
                partitions=config.partitions,
                replication_factor=config.replication_factor
            )
        
        try:
            # Create topics
            fs = self.admin_client.create_topics(new_topics, validate_only=False)
            
            # Wait for creation to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info("Topic created successfully", topic=topic)
                except TopicAlreadyExistsError:
                    logger.warning("Topic already exists", topic=topic)
                except Exception as e:
                    logger.error("Failed to create topic", topic=topic, error=str(e))
                    
        except Exception as e:
            logger.error("Failed to create topics", error=str(e))
            raise
    
    async def list_topics(self) -> List[str]:
        """List existing topics"""
        try:
            metadata = self.admin_client.describe_topics()
            topics = list(metadata.keys())
            logger.info("Listed Kafka topics", topics=topics, count=len(topics))
            return topics
        except Exception as e:
            logger.error("Failed to list topics", error=str(e))
            return []
    
    async def delete_topics(self, topic_names: List[str]) -> None:
        """Delete Kafka topics"""
        try:
            fs = self.admin_client.delete_topics(topic_names)
            
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info("Topic deleted successfully", topic=topic)
                except Exception as e:
                    logger.error("Failed to delete topic", topic=topic, error=str(e))
                    
        except Exception as e:
            logger.error("Failed to delete topics", error=str(e))
            raise
    
    def close(self) -> None:
        """Close admin client"""
        self.admin_client.close()

async def setup_kafka_topics():
    """Main function to setup Kafka topics"""
    logger.info("Setting up Kafka topics", servers=settings.kafka_servers_list)
    
    topic_manager = KafkaTopicManager(settings.kafka_servers_list)
    
    try:
        # Get all static topics
        static_topics = KafkaTopics.get_all_static_topics()
        
        # Create topics
        await topic_manager.create_topics(static_topics)
        
        # List created topics
        topics = await topic_manager.list_topics()
        logger.info("Kafka setup completed", created_topics=topics)
        
    except Exception as e:
        logger.error("Kafka setup failed", error=str(e))
        raise
    finally:
        topic_manager.close()

if __name__ == "__main__":
    asyncio.run(setup_kafka_topics())