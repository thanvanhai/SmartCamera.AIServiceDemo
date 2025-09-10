# services/ai_worker/detectors/base_detector.py
import time
import asyncio
import logging
import numpy as np
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass

from core.models import Detection, BoundingBox
from core.enums import ModelType


@dataclass
class DetectionOutput:
    """Output from detector inference"""
    detections: List[Detection]
    processing_time: float
    model_confidence: float
    inference_metadata: Dict[str, Any]


@dataclass
class ModelConfig:
    """Model configuration"""
    model_path: str
    confidence_threshold: float = 0.5
    nms_threshold: float = 0.4
    input_size: Tuple[int, int] = (640, 640)
    device: str = 'cuda'
    max_batch_size: int = 8
    class_names: List[str] = None
    preprocessing_params: Dict[str, Any] = None


class BaseDetector(ABC):
    """Base class cho tất cả detectors"""
    
    def __init__(self, model_config: ModelConfig):
        self.config = model_config
        self.model = None
        self.is_loaded = False
        self.model_type = ModelType.UNKNOWN
        self.supports_batch = False
        
        # Performance tracking
        self.total_inferences = 0
        self.total_processing_time = 0.0
        self.last_inference_time = 0.0
        
        # Preprocessing cache
        self._preprocessing_cache = {}
    
    @abstractmethod
    async def load_model(self) -> bool:
        """Load model vào memory"""
        pass
    
    @abstractmethod
    async def detect(self, frame: np.ndarray, **kwargs) -> DetectionOutput:
        """Detect objects trong single frame"""
        pass
    
    async def detect_batch(self, frames: np.ndarray, **kwargs) -> List[DetectionOutput]:
        """Detect objects trong batch frames - override if supports batch"""
        if not self.supports_batch:
            # Fallback to individual processing
            results = []
            for frame in frames:
                result = await self.detect(frame, **kwargs)
                results.append(result)
            return results
        
        # Implement trong subclass nếu support batch
        raise NotImplementedError("Batch detection not implemented")
    
    @abstractmethod
    def preprocess_frame(self, frame: np.ndarray) -> np.ndarray:
        """Preprocess frame trước khi inference"""
        pass
    
    @abstractmethod
    def postprocess_output(self, raw_output: Any, frame_shape: Tuple[int, int]) -> List[Detection]:
        """Postprocess raw model output thành Detection objects"""
        pass
    
    def unload_model(self):
        """Unload model khỏi memory"""
        if self.model is not None:
            del self.model
            self.model = None
            self.is_loaded = False
            logging.info(f"{self.__class__.__name__} model unloaded")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get detector statistics"""
        avg_processing_time = (
            self.total_processing_time / self.total_inferences 
            if self.total_inferences > 0 else 0
        )
        
        return {
            'model_type': self.model_type.value,
            'is_loaded': self.is_loaded,
            'supports_batch': self.supports_batch,
            'total_inferences': self.total_inferences,
            'average_processing_time': avg_processing_time,
            'last_inference_time': self.last_inference_time,
            'config': {
                'confidence_threshold': self.config.confidence_threshold,
                'nms_threshold': self.config.nms_threshold,
                'input_size': self.config.input_size,
                'device': self.config.device,
                'max_batch_size': self.config.max_batch_size
            }
        }
    
    def _update_statistics(self, processing_time: float):
        """Update processing statistics"""
        self.total_inferences += 1
        self.total_processing_time += processing_time
        self.last_inference_time = processing_time
    
    def _create_detection(self, 
                         class_id: int, 
                         confidence: float, 
                         bbox: List[float],
                         frame_shape: Tuple[int, int]) -> Detection:
        """Helper method để tạo Detection object"""
        # Normalize bbox coordinates
        x1, y1, x2, y2 = bbox
        h, w = frame_shape
        
        # Clamp coordinates
        x1 = max(0, min(w, x1))
        y1 = max(0, min(h, y1))
        x2 = max(0, min(w, x2))
        y2 = max(0, min(h, y2))
        
        # Create bounding box
        bounding_box = BoundingBox(
            x=int(x1),
            y=int(y1),
            width=int(x2 - x1),
            height=int(y2 - y1)
        )
        
        # Get class name
        class_name = (
            self.config.class_names[class_id] 
            if self.config.class_names and class_id < len(self.config.class_names)
            else f"class_{class_id}"
        )
        
        return Detection(
            class_id=class_id,
            class_name=class_name,
            confidence=confidence,
            bounding_box=bounding_box
        )
    
    def _resize_frame(self, frame: np.ndarray, target_size: Tuple[int, int]) -> Tuple[np.ndarray, float]:
        """Resize frame maintaining aspect ratio"""
        h, w = frame.shape[:2]
        target_h, target_w = target_size
        
        # Calculate scale factor
        scale = min(target_w / w, target_h / h)
        
        # Calculate new dimensions
        new_w = int(w * scale)
        new_h = int(h * scale)
        
        # Resize frame
        import cv2
        resized = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
        
        # Create padded frame
        padded = np.full((target_h, target_w, 3), 114, dtype=np.uint8)
        
        # Calculate padding offsets
        y_offset = (target_h - new_h) // 2
        x_offset = (target_w - new_w) // 2
        
        # Place resized frame in center
        padded[y_offset:y_offset + new_h, x_offset:x_offset + new_w] = resized
        
        return padded, scale
    
    def _normalize_frame(self, frame: np.ndarray) -> np.ndarray:
        """Normalize frame values to [0, 1]"""
        return frame.astype(np.float32) / 255.0
    
    def _to_tensor_format(self, frame: np.ndarray) -> np.ndarray:
        """Convert frame to tensor format (CHW)"""
        # HWC -> CHW
        if len(frame.shape) == 3:
            frame = np.transpose(frame, (2, 0, 1))
        
        # Add batch dimension if needed
        if len(frame.shape) == 3:
            frame = np.expand_dims(frame, axis=0)
        
        return frame
    
    async def warmup(self, num_iterations: int = 5):
        """Warmup model với dummy inference"""
        if not self.is_loaded:
            await self.load_model()
        
        dummy_frame = np.random.randint(
            0, 255, 
            (*self.config.input_size, 3), 
            dtype=np.uint8
        )
        
        logging.info(f"Warming up {self.__class__.__name__} with {num_iterations} iterations")
        
        for i in range(num_iterations):
            try:
                await self.detect(dummy_frame)
            except Exception as e:
                logging.warning(f"Warmup iteration {i+1} failed: {e}")
        
        logging.info(f"{self.__class__.__name__} warmup completed")
    
    def validate_frame(self, frame: np.ndarray) -> bool:
        """Validate input frame"""
        if frame is None:
            return False
        
        if not isinstance(frame, np.ndarray):
            return False
        
        if len(frame.shape) != 3:
            return False
        
        if frame.shape[2] != 3:
            return False
        
        return True
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        try:
            # Check if model is loaded
            if not self.is_loaded:
                return {
                    'status': 'unhealthy',
                    'reason': 'model_not_loaded',
                    'details': 'Model is not loaded in memory'
                }
            
            # Try dummy inference
            dummy_frame = np.random.randint(
                0, 255, 
                (*self.config.input_size, 3), 
                dtype=np.uint8
            )
            
            start_time = time.time()
            result = await self.detect(dummy_frame)
            inference_time = time.time() - start_time
            
            return {
                'status': 'healthy',
                'inference_time': inference_time,
                'detections_count': len(result.detections),
                'model_confidence': result.model_confidence,
                'memory_usage': self._get_memory_usage()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'reason': 'inference_failed',
                'details': str(e)
            }
    
    def _get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage information"""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                'rss': memory_info.rss,
                'vms': memory_info.vms,
                'percent': process.memory_percent()
            }
        except ImportError:
            return {'error': 'psutil not available'}
        except Exception as e:
            return {'error': str(e)}


class DetectorFactory:
    """Factory class để tạo detector instances"""
    
    _detector_registry = {}
    
    @classmethod
    def register_detector(cls, model_type: ModelType, detector_class):
        """Register detector class cho model type"""
        cls._detector_registry[model_type] = detector_class
    
    @classmethod
    def create_detector(cls, model_type: ModelType, model_config: ModelConfig) -> BaseDetector:
        """Create detector instance"""
        if model_type not in cls._detector_registry:
            raise ValueError(f"No detector registered for model type: {model_type}")
        
        detector_class = cls._detector_registry[model_type]
        return detector_class(model_config)
    
    @classmethod
    def get_supported_models(cls) -> List[ModelType]:
        """Get list of supported model types"""
        return list(cls._detector_registry.keys())