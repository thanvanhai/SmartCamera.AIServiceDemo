# services/ai_worker/model_loader.py
import os
import logging
import asyncio
from typing import Dict, Optional, List
from pathlib import Path

from core.enums.model_types import ModelType
from services.ai_worker.detectors.base_detector import BaseDetector
from services.ai_worker.detectors.yolo_detector import YOLODetector 
from services.ai_worker.detectors.face_detector import FaceDetector
from services.ai_worker.detectors.person_detector import PersonDetector
from services.ai_worker.detectors.vehicle_detector import VehicleDetector
from infrastructure.monitoring.metrics import ModelMetrics

class ModelLoader:
    """Model Loader - Quản lý loading và switching AI models"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.detectors: Dict[ModelType, BaseDetector] = {}
        self.metrics = ModelMetrics()
        
        # Model configurations
        self.model_configs = config.get('models', {})
        self.models_dir = Path(config.get('models_dir', 'models'))
        
        # Device configuration
        self.device = self._select_device()
        
        # Loading state
        self.loading_lock = asyncio.Lock()
        self.loaded_models: List[ModelType] = []
    
    async def initialize(self):
        """Initialize và load tất cả configured models"""
        self.logger.info("🔧 Initializing Model Loader")
        
        async with self.loading_lock:
            for model_type_str, model_config in self.model_configs.items():
                try:
                    model_type = ModelType(model_type_str)
                    success = await self._load_model(model_type, model_config)
                    
                    if success:
                        self.loaded_models.append(model_type)
                        self.logger.info(f"✅ Loaded model: {model_type.value}")
                    else:
                        self.logger.error(f"❌ Failed to load model: {model_type.value}")
                        
                except ValueError as e:
                    self.logger.error(f"Invalid model type {model_type_str}: {e}")
                except Exception as e:
                    self.logger.error(f"Error loading {model_type_str}: {e}")
        
        self.logger.info(f"🚀 Model Loader initialized with {len(self.loaded_models)} models")
    
    async def _load_model(self, model_type: ModelType, config: Dict) -> bool:
        """Load specific model"""
        try:
            model_path = self.models_dir / config['path']
            
            if not model_path.exists():
                self.logger.error(f"Model file not found: {model_path}")
                return False
            
            # Create detector instance dựa vào model type
            detector = self._create_detector(model_type, str(model_path), config)
            
            if not detector:
                return False
            
            # Load model
            start_time = asyncio.get_event_loop().time()
            await detector.load()
            load_time = asyncio.get_event_loop().time() - start_time
            
            # Store detector
            self.detectors[model_type] = detector
            
            # Record metrics
            self.metrics.record_model_load_time(model_type.value, load_time)
            
            self.logger.info(f"✅ Loaded {model_type.value} in {load_time:.2f}s")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load {model_type.value}: {e}")
            return False
    
    def _create_detector(self, model_type: ModelType, model_path: str, config: Dict) -> Optional[BaseDetector]:
        """Create detector instance based on model type"""
        detector_map = {
            # YOLO variants
            ModelType.YOLO_V5: YOLODetector,
            ModelType.YOLO_V8: YOLODetector,
            ModelType.YOLO_V10: YOLODetector,
            ModelType.YOLO_NAS: YOLODetector,
            
            # Face detection
            ModelType.MTCNN: FaceDetector,
            ModelType.FACE_RECOGNITION: FaceDetector,
            
            # Other detectors
            ModelType.SSD: PersonDetector,  # Can be configured for person detection
            ModelType.EFFICIENTDET: PersonDetector,
            
            # Custom mapping - these would need to be defined in your enum
            # For now, map common detection tasks to appropriate detectors
        }
        
        # Try to map model type to appropriate detector
        detector_class = detector_map.get(model_type)
        
        # If no direct mapping, infer from model name
        if not detector_class:
            model_name_lower = model_type.value.lower()
            if 'yolo' in model_name_lower:
                detector_class = YOLODetector
            elif 'face' in model_name_lower or 'mtcnn' in model_name_lower:
                detector_class = FaceDetector
            elif 'person' in model_name_lower:
                detector_class = PersonDetector
            elif 'vehicle' in model_name_lower or 'car' in model_name_lower:
                detector_class = VehicleDetector
            else:
                # Default to YOLO for unknown types
                detector_class = YOLODetector
                self.logger.warning(f"Unknown model type {model_type}, defaulting to YOLODetector")
        
        if not detector_class:
            self.logger.error(f"Unsupported model type: {model_type}")
            return None
        
        try:
            return detector_class(model_path, config, self.device)
        except Exception as e:
            self.logger.error(f"Failed to create {model_type.value} detector: {e}")
            return None
    
    def get_detector(self, model_type: ModelType) -> Optional[BaseDetector]:
        """Get loaded detector instance"""
        detector = self.detectors.get(model_type)
        
        if detector:
            self.metrics.increment_model_usage(model_type.value)
        else:
            self.logger.warning(f"Detector {model_type.value} not loaded")
            
        return detector
    
    def get_available_models(self) -> List[ModelType]:
        """Get list of loaded models"""
        return self.loaded_models.copy()
    
    async def reload_model(self, model_type: ModelType) -> bool:
        """Reload specific model"""
        self.logger.info(f"🔄 Reloading model: {model_type.value}")
        
        async with self.loading_lock:
            # Unload existing
            if model_type in self.detectors:
                await self.detectors[model_type].unload()
                del self.detectors[model_type]
            
            # Load again
            model_config = self.model_configs.get(model_type.value)
            if model_config:
                return await self._load_model(model_type, model_config)
            
            return False
    
    def get_model_info(self) -> Dict:
        """Get information about loaded models"""
        return {
            'loaded_models': [model.value for model in self.loaded_models],
            'device': self.device,
            'models_dir': str(self.models_dir),
            'total_models': len(self.detectors)
        }
    
    def _select_device(self) -> str:
        """Select appropriate device (GPU/CPU)"""
        try:
            import torch
            if torch.cuda.is_available():
                gpu_id = self.config.get('gpu_id', 0)
                device = f"cuda:{gpu_id}"
                self.logger.info(f"🎮 Using GPU: {device}")
                return device
        except ImportError:
            pass
        
        self.logger.info("💻 Using CPU")
        return "cpu"
    
    async def cleanup(self):
        """Cleanup all loaded models"""
        self.logger.info("🧹 Cleaning up Model Loader")
        
        async with self.loading_lock:
            for model_type, detector in self.detectors.items():
                try:
                    await detector.unload()
                    self.logger.info(f"✅ Unloaded {model_type.value}")
                except Exception as e:
                    self.logger.error(f"Failed to unload {model_type.value}: {e}")
            
            self.detectors.clear()
            self.loaded_models.clear()
        
        self.logger.info("✅ Model Loader cleanup completed")