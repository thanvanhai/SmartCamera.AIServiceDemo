# ================================================================================================
# shared/config/model_configs.py - AI Model Configurations
# ================================================================================================

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pathlib import Path

@dataclass
class ModelConfig:
    """Configuration for an AI model"""
    name: str
    path: Path
    model_type: str  # yolo, tensorflow, pytorch, onnx
    input_size: tuple  # (width, height)
    confidence_threshold: float
    nms_threshold: float
    classes: List[str]
    preprocessing: Dict[str, Any]
    postprocessing: Dict[str, Any]
    device: str = "cuda"
    batch_size: int = 1
    enabled: bool = True

class ModelConfigs:
    """Centralized model configurations"""
    
    # =============================================================================
    # YOLO Models
    # =============================================================================
    
    YOLO_V8_NANO = ModelConfig(
        name="yolov8n",
        path=Path("models/yolo/yolov8n.pt"),
        model_type="yolo",
        input_size=(640, 640),
        confidence_threshold=0.5,
        nms_threshold=0.4,
        classes=[
            "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train",
            "truck", "boat", "traffic light", "fire hydrant", "stop sign",
            "parking meter", "bench", "bird", "cat", "dog", "horse", "sheep",
            "cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella",
            "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard",
            "sports ball", "kite", "baseball bat", "baseball glove", "skateboard",
            "surfboard", "tennis racket", "bottle", "wine glass", "cup", "fork",
            "knife", "spoon", "bowl", "banana", "apple", "sandwich", "orange",
            "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
            "couch", "potted plant", "bed", "dining table", "toilet", "tv",
            "laptop", "mouse", "remote", "keyboard", "cell phone", "microwave",
            "oven", "toaster", "sink", "refrigerator", "book", "clock", "vase",
            "scissors", "teddy bear", "hair drier", "toothbrush"
        ],
        preprocessing={
            "normalize": True,
            "mean": [0.485, 0.456, 0.406],
            "std": [0.229, 0.224, 0.225]
        },
        postprocessing={
            "apply_nms": True,
            "merge_overlapping": True
        },
        batch_size=4
    )
    
    YOLO_V8_SMALL = ModelConfig(
        name="yolov8s",
        path=Path("models/yolo/yolov8s.pt"),
        model_type="yolo",
        input_size=(640, 640),
        confidence_threshold=0.5,
        nms_threshold=0.4,
        classes=YOLO_V8_NANO.classes,  # Same as nano
        preprocessing=YOLO_V8_NANO.preprocessing,
        postprocessing=YOLO_V8_NANO.postprocessing,
        batch_size=2  # Smaller batch for larger model
    )
    
    # =============================================================================
    # Person Detection Models
    # =============================================================================
    
    PERSON_DETECTOR = ModelConfig(
        name="person_detector",
        path=Path("models/person_detection/person_detector.tflite"),
        model_type="tensorflow",
        input_size=(320, 320),
        confidence_threshold=0.6,
        nms_threshold=0.5,
        classes=["person"],
        preprocessing={
            "normalize": True,
            "scale": 1.0/255.0,
            "format": "RGB"
        },
        postprocessing={
            "apply_nms": True,
            "tracking": True
        }
    )
    
    # =============================================================================
    # Face Recognition Models
    # =============================================================================
    
    FACE_RECOGNITION = ModelConfig(
        name="facenet",
        path=Path("models/face_recognition/facenet_model.pb"),
        model_type="tensorflow",
        input_size=(160, 160),
        confidence_threshold=0.7,
        nms_threshold=0.3,
        classes=["face"],
        preprocessing={
            "normalize": True,
            "mean": [127.5, 127.5, 127.5],
            "scale": 1.0/128.0,
            "format": "RGB"
        },
        postprocessing={
            "embedding_size": 512,
            "similarity_threshold": 0.8
        }
    )
    
    # =============================================================================
    # Vehicle Detection Models  
    # =============================================================================
    
    VEHICLE_DETECTOR = ModelConfig(
        name="vehicle_detector",
        path=Path("models/vehicle/vehicle_detector.onnx"),
        model_type="onnx", 
        input_size=(416, 416),
        confidence_threshold=0.5,
        nms_threshold=0.4,
        classes=[
            "car", "truck", "bus", "motorcycle", "bicycle", 
            "van", "suv", "taxi", "ambulance", "police_car"
        ],
        preprocessing={
            "normalize": True,
            "mean": [0.485, 0.456, 0.406],
            "std": [0.229, 0.224, 0.225],
            "format": "RGB"
        },
        postprocessing={
            "apply_nms": True,
            "tracking": True,
            "speed_estimation": True
        }
    )
    
    # =============================================================================
    # Custom Models
    # =============================================================================
    
    CUSTOM_MODEL = ModelConfig(
        name="custom_detector",
        path=Path("models/custom/custom_model.onnx"),
        model_type="onnx",
        input_size=(640, 640),
        confidence_threshold=0.5,
        nms_threshold=0.4,
        classes=["custom_class_1", "custom_class_2"],
        preprocessing={
            "normalize": True,
            "format": "RGB"
        },
        postprocessing={
            "apply_nms": True
        },
        enabled=False  # Disabled by default
    )
    
    @classmethod
    def get_all_models(cls) -> Dict[str, ModelConfig]:
        """Get all model configurations"""
        return {
            "yolov8n": cls.YOLO_V8_NANO,
            "yolov8s": cls.YOLO_V8_SMALL,
            "person_detector": cls.PERSON_DETECTOR,
            "face_recognition": cls.FACE_RECOGNITION,
            "vehicle_detector": cls.VEHICLE_DETECTOR,
            "custom_model": cls.CUSTOM_MODEL
        }
    
    @classmethod
    def get_enabled_models(cls) -> Dict[str, ModelConfig]:
        """Get only enabled models"""
        return {name: config for name, config in cls.get_all_models().items() if config.enabled}
    
    @classmethod
    def get_models_by_type(cls, model_type: str) -> Dict[str, ModelConfig]:
        """Get models by type (yolo, tensorflow, onnx, etc.)"""
        return {name: config for name, config in cls.get_all_models().items() 
                if config.model_type == model_type and config.enabled}
