# core/models/detection_models.py
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple
import time
from enum import Enum


class DetectionStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PROCESSING = "processing"
    TIMEOUT = "timeout"


@dataclass
class BoundingBox:
    """Bounding box cho object detection"""
    x: float  # Left coordinate (normalized 0-1 or pixel)
    y: float  # Top coordinate (normalized 0-1 or pixel)
    width: float  # Width (normalized 0-1 or pixel)
    height: float  # Height (normalized 0-1 or pixel)
    
    def to_dict(self) -> Dict[str, float]:
        return {
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height
        }
    
    def to_xyxy(self) -> tuple:
        """Convert to (x1, y1, x2, y2) format"""
        return (self.x, self.y, self.x + self.width, self.y + self.height)


@dataclass
class Detection:
    """Một detection/object được phát hiện"""
    class_id: int
    class_name: str
    confidence: float
    bbox: BoundingBox
    worker_id: Optional[str] = None
    processed_at: Optional[float] = None
    additional_info: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.processed_at is None:
            self.processed_at = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'class_id': self.class_id,
            'class_name': self.class_name,
            'confidence': self.confidence,
            'bbox': self.bbox.to_dict(),
            'worker_id': self.worker_id,
            'processed_at': self.processed_at,
            'additional_info': self.additional_info or {}
        }


@dataclass
class DetectionResult:
    """Kết quả inference từ AI Worker"""
    job_id: str
    camera_id: str
    timestamp: float
    detections: List[Dict[str, Any]]  # Raw detections from inference_engine
    processing_time_ms: float
    model_type: str
    worker_id: str
    confidence_threshold: float
    
    # THÊM CÁC FIELD THIẾU để fix lỗi constructor
    processing_time: Optional[float] = None  # Processing time in seconds
    frame_size: Optional[Tuple[int, int]] = None  # Frame dimensions (width, height)
    error: Optional[str] = None  # Error message if any
    
    # Các field cũ giữ nguyên
    status: DetectionStatus = DetectionStatus.SUCCESS
    error_message: Optional[str] = None
    frame_width: Optional[int] = None
    frame_height: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        
        # Sync frame_size with frame_width/frame_height
        if self.frame_size and not self.frame_width:
            self.frame_width, self.frame_height = self.frame_size
        elif self.frame_width and self.frame_height and not self.frame_size:
            self.frame_size = (self.frame_width, self.frame_height)
        
        # Sync error with error_message
        if self.error and not self.error_message:
            self.error_message = self.error
        elif self.error_message and not self.error:
            self.error = self.error_message
            
        # Convert processing_time_ms to processing_time if not set
        if self.processing_time is None and self.processing_time_ms is not None:
            self.processing_time = self.processing_time_ms / 1000.0
    
    @property
    def detection_count(self) -> int:
        """Số lượng detections"""
        return len(self.detections)
    
    @property
    def avg_confidence(self) -> float:
        """Confidence trung bình của các detections"""
        if not self.detections:
            return 0.0
        total_confidence = sum(d.get('confidence', 0.0) for d in self.detections)
        return total_confidence / len(self.detections)
    
    def get_detections_by_class(self, class_name: str) -> List[Dict[str, Any]]:
        """Lấy tất cả detections theo class name"""
        return [d for d in self.detections if d.get('class_name') == class_name]
    
    def get_high_confidence_detections(self, threshold: Optional[float] = None) -> List[Dict[str, Any]]:
        """Lấy detections có confidence cao hơn threshold"""
        if threshold is None:
            threshold = self.confidence_threshold
        return [d for d in self.detections if d.get('confidence', 0.0) >= threshold]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'job_id': self.job_id,
            'camera_id': self.camera_id,
            'timestamp': self.timestamp,
            'detections': self.detections,
            'processing_time_ms': self.processing_time_ms,
            'processing_time': self.processing_time,
            'model_type': self.model_type,
            'worker_id': self.worker_id,
            'confidence_threshold': self.confidence_threshold,
            'status': self.status.value if isinstance(self.status, DetectionStatus) else self.status,
            'error_message': self.error_message,
            'error': self.error,
            'frame_width': self.frame_width,
            'frame_height': self.frame_height,
            'frame_size': self.frame_size,
            'detection_count': self.detection_count,
            'avg_confidence': self.avg_confidence,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DetectionResult':
        """Create DetectionResult from dictionary"""
        # Convert status back to enum if it's a string
        status = data.get('status', DetectionStatus.SUCCESS)
        if isinstance(status, str):
            try:
                status = DetectionStatus(status)
            except ValueError:
                status = DetectionStatus.SUCCESS
        
        return cls(
            job_id=data['job_id'],
            camera_id=data['camera_id'],
            timestamp=data['timestamp'],
            detections=data['detections'],
            processing_time_ms=data['processing_time_ms'],
            model_type=data['model_type'],
            worker_id=data['worker_id'],
            confidence_threshold=data['confidence_threshold'],
            processing_time=data.get('processing_time'),
            frame_size=data.get('frame_size'),
            error=data.get('error'),
            status=status,
            error_message=data.get('error_message'),
            frame_width=data.get('frame_width'),
            frame_height=data.get('frame_height'),
            metadata=data.get('metadata', {})
        )


@dataclass
class BatchDetectionResult:
    """Kết quả xử lý batch từ BatchProcessor"""
    batch_id: str
    job_ids: List[str]
    results: List[DetectionResult]
    batch_size: int
    total_processing_time_ms: float
    avg_processing_time_ms: float
    worker_id: str
    timestamp: float
    
    def __post_init__(self):
        if not self.batch_id:
            self.batch_id = f"batch_{int(self.timestamp * 1000)}"
        
        if not self.job_ids:
            self.job_ids = [r.job_id for r in self.results]
        
        if self.batch_size == 0:
            self.batch_size = len(self.results)
    
    @property
    def total_detections(self) -> int:
        """Tổng số detections trong batch"""
        return sum(r.detection_count for r in self.results)
    
    @property
    def success_rate(self) -> float:
        """Tỷ lệ thành công trong batch"""
        if not self.results:
            return 0.0
        successful = sum(1 for r in self.results if r.status == DetectionStatus.SUCCESS)
        return successful / len(self.results)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'batch_id': self.batch_id,
            'job_ids': self.job_ids,
            'results': [r.to_dict() for r in self.results],
            'batch_size': self.batch_size,
            'total_processing_time_ms': self.total_processing_time_ms,
            'avg_processing_time_ms': self.avg_processing_time_ms,
            'worker_id': self.worker_id,
            'timestamp': self.timestamp,
            'total_detections': self.total_detections,
            'success_rate': self.success_rate
        }


@dataclass
class InferenceStats:
    """Thống kê inference performance"""
    worker_id: str
    total_frames: int
    successful_frames: int
    failed_frames: int
    avg_processing_time_ms: float
    min_processing_time_ms: float
    max_processing_time_ms: float
    avg_fps: float
    total_detections: int
    avg_detections_per_frame: float
    uptime_seconds: float
    timestamp: float
    
    @property
    def success_rate(self) -> float:
        if self.total_frames == 0:
            return 0.0
        return self.successful_frames / self.total_frames
    
    @property
    def error_rate(self) -> float:
        return 1.0 - self.success_rate
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)