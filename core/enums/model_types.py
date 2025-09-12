"""
AI Model types enumeration for the Smart Camera AI Service.
"""

from enum import Enum


class ModelType(str, Enum):
    """AI Model types supported by the system"""
    YOLO_V5 = "yolo_v5"
    YOLO_V8 = "yolo_v8"
    YOLO_V10 = "yolo_v10"
    YOLO_NAS = "yolo_nas"
    DETECTRON2 = "detectron2"
    FASTER_RCNN = "faster_rcnn"
    SSD = "ssd"
    EFFICIENTDET = "efficientdet"
    CENTERNET = "centernet"
    RETINANET = "retinanet"
    
    # Face detection models
    MTCNN = "mtcnn"
    FACE_RECOGNITION = "face_recognition"
    
    # Custom models
    CUSTOM = "custom"


# Backward compatibility alias
DetectionModelType = ModelType