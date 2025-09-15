# core/models/result_models.py

from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime


@dataclass
class BoundingBox:
    x: int
    y: int
    width: int
    height: int


@dataclass
class Detection:
    id: Optional[str] = None
    class_name: str = ""
    confidence: float = 0.0
    bbox: BoundingBox = field(default_factory=lambda: BoundingBox(0, 0, 0, 0))


@dataclass
class AIResult:
    camera_id: str
    frame_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    detections: List[Detection] = field(default_factory=list)
    processing_time_ms: Optional[float] = None
    worker_id: Optional[str] = None
    detector_version: Optional[str] = None
    model_name: Optional[str] = None
    fps: Optional[float] = None
    avg_confidence: Optional[float] = None
