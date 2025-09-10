# ================================  
# core/models/camera_models.py
# ================================
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

class CameraStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    ERROR = "error"
    PROCESSING = "processing"
    MAINTENANCE = "maintenance"

class StreamProtocol(str, Enum):
    RTSP = "rtsp"
    HTTP = "http"
    HTTPS = "https"

class CameraConfig(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    stream_url: str
    protocol: StreamProtocol
    location: Optional[str] = None
    
    # AI Configuration
    ai_enabled: bool = True
    detection_models: List[str] = Field(default_factory=lambda: ["YOLO"])
    confidence_threshold: float = 0.7
    processing_fps: int = 10
    
    # Technical specs
    resolution_width: int = 1920
    resolution_height: int = 1080
    fps: int = 30
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    status: CameraStatus = CameraStatus.OFFLINE
    
    # Additional settings
    settings: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True