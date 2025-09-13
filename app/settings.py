# app/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import validator, Field
from typing import List, Optional
from pathlib import Path

# =============================================================================
# AI Worker Config (nested)
# =============================================================================
class AIWorkerConfig(BaseSettings):
    """Configuration for AI Worker"""

    # Model Settings
    ai_models_path: Path = Path("./models")
    ai_batch_size: int = 4
    ai_confidence_threshold: float = 0.5
    ai_nms_threshold: float = 0.4
    ai_max_workers: int = 4

    # GPU Settings
    cuda_visible_devices: str = "0"
    ai_device: str = "cuda"  # cuda, cpu, mps

    # Detection Types
    enable_person_detection: bool = True
    enable_face_recognition: bool = True
    enable_vehicle_detection: bool = True
    enable_custom_detection: bool = False

    # Performance
    max_frame_queue_size: int = 1000
    frame_skip_interval: int = 1
    detection_cooldown: int = 5

    # Validators
    @validator('ai_models_path')
    def validate_models_path(cls, v):
        Path(v).mkdir(parents=True, exist_ok=True)
        return v

    @validator('ai_confidence_threshold', 'ai_nms_threshold')
    def validate_threshold(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError("Threshold must be between 0.0 and 1.0")
        return v

    @validator('ai_device')
    def validate_device(cls, v):
        valid_devices = ['cuda', 'cpu', 'mps']
        if v not in valid_devices:
            raise ValueError(f"Device must be one of: {valid_devices}")
        return v

    # -------------------------------------------------------------------------
    # Pydantic Settings Config
    # -------------------------------------------------------------------------
    model_config = SettingsConfigDict(
        env_prefix="AI_WORKER__",   # map .env variables like AI_WORKER__AI_BATCH_SIZE
        extra="ignore"
    )


# =============================================================================
# Main Application Settings
# =============================================================================
class Settings(BaseSettings):
    """Application settings with validation"""

    # -------------------------------------------------------------------------
    # Application Settings
    # -------------------------------------------------------------------------
    app_name: str = "SmartCamera.AIServiceDemo"
    app_version: str = "1.0.0"
    debug: bool = False
    log_level: str = "INFO"

    # Service Ports
    fastapi_port: int = 8000
    ingest_port: int = 8001
    worker_port: int = 8002
    results_port: int = 8003

    # -------------------------------------------------------------------------
    # External Services
    # -------------------------------------------------------------------------
    webapi_base_url: str = "http://localhost:5000"
    webapi_api_key: str = ""
    webapi_timeout: int = 30

    # -------------------------------------------------------------------------
    # Message Brokers
    # -------------------------------------------------------------------------
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_auto_offset_reset: str = "latest"
    kafka_group_id: str = "smartcamera-ai"
    kafka_max_poll_records: int = 100

    # Kafka Topics
    kafka_topic_raw_frames: str = "smartcamera.raw_frames"
    kafka_topic_ai_results: str = "smartcamera.ai_results"
    kafka_topic_alerts: str = "smartcamera.alerts"

    # RabbitMQ
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_vhost: str = "/"

    # RabbitMQ Topics
    rabbitmq_exchange_control: str = "smartcamera.control"
    rabbitmq_queue_camera_commands: str = "camera.commands"
    rabbitmq_queue_ai_status: str = "ai.status"

    # -------------------------------------------------------------------------
    # Storage & Database
    # -------------------------------------------------------------------------
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_database: str = "smartcamera"
    clickhouse_username: str = "default"
    clickhouse_password: str = ""

    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket_snapshots: str = "smartcamera-snapshots"
    minio_bucket_models: str = "smartcamera-models"
    minio_secure: bool = False

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None

    # -------------------------------------------------------------------------
    # Monitoring & Performance
    # -------------------------------------------------------------------------
    prometheus_port: int = 9090
    enable_metrics: bool = True
    metrics_update_interval: int = 10

    health_check_interval: int = 30
    max_unhealthy_checks: int = 3

    log_format: str = "json"  # json, console
    log_file_path: Path = Path("./data/logs/smartcamera-ai.log")
    log_max_size: str = "100MB"
    log_backup_count: int = 5

    # -------------------------------------------------------------------------
    # Nested AI Worker Config
    # -------------------------------------------------------------------------
    ai_worker: AIWorkerConfig = AIWorkerConfig()

    # -------------------------------------------------------------------------
    # Computed Properties
    # -------------------------------------------------------------------------
    @property
    def kafka_servers_list(self) -> List[str]:
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",")]

    @property
    def rabbitmq_url(self) -> str:
        return f"amqp://{self.rabbitmq_username}:{self.rabbitmq_password}@{self.rabbitmq_host}:{self.rabbitmq_port}{self.rabbitmq_vhost}"

    @property
    def clickhouse_url(self) -> str:
        auth = f"{self.clickhouse_username}:{self.clickhouse_password}@" if self.clickhouse_password else f"{self.clickhouse_username}@"
        return f"clickhouse://{auth}{self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_database}"

    @property
    def redis_url(self) -> str:
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # -------------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------------
    @validator('log_file_path')
    def validate_log_path(cls, v):
        Path(v).parent.mkdir(parents=True, exist_ok=True)
        return v

    # -------------------------------------------------------------------------
    # Pydantic Settings Config
    # -------------------------------------------------------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


# Global settings instance
settings = Settings()
