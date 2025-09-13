# services/ai_worker/detectors/person_detector.py
import cv2
import numpy as np
import logging
from typing import List, Dict, Any, Optional
import asyncio
from pathlib import Path

from services.ai_worker.detectors.base_detector import BaseDetector
from core.models.detection_models import Detection, BoundingBox


class PersonDetector(BaseDetector):
    """Person Detection using YOLO, SSD, or HOG+SVM"""
    
    def __init__(self, model_path: str, config: Dict, device: str = "cpu"):
        super().__init__(model_path, config, device)
        self.logger = logging.getLogger(__name__)
        
        # Model configuration
        self.confidence_threshold = config.get('confidence_threshold', 0.5)
        self.nms_threshold = config.get('nms_threshold', 0.4)
        self.input_size = config.get('input_size', (416, 416))
        
        # Model type (yolo, ssd, hog)
        self.model_type = config.get('type', 'yolo')
        
        # Model components
        self.net = None
        self.hog = None
        
        # Class mapping (for COCO dataset person class = 0)
        self.person_class_id = 0
        self.class_names = ['person']
    
    async def load(self) -> bool:
        """Load person detection model"""
        try:
            self.logger.info(f"🔧 Loading Person Detector: {self.model_type}")
            
            if self.model_type == 'yolo':
                return await self._load_yolo()
            elif self.model_type == 'ssd':
                return await self._load_ssd()
            elif self.model_type == 'hog':
                return await self._load_hog()
            else:
                self.logger.error(f"Unsupported person detection type: {self.model_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load person detector: {e}")
            return False
    
    async def _load_yolo(self) -> bool:
        """Load YOLO person detection model"""
        try:
            model_path = Path(self.model_path)
            
            # For YOLO, expect .weights, .cfg files
            if model_path.suffix == '.weights':
                weights_path = model_path
                config_path = model_path.with_suffix('.cfg')
            else:
                weights_path = model_path.parent / f"{model_path.stem}.weights"
                config_path = model_path.parent / f"{model_path.stem}.cfg"
            
            if not weights_path.exists() or not config_path.exists():
                self.logger.error(f"YOLO files not found: {weights_path}, {config_path}")
                return False
            
            # Load YOLO
            self.net = cv2.dnn.readNet(str(weights_path), str(config_path))
            
            # Set backend/target
            if self.device == "cuda" and cv2.cuda.getCudaEnabledDeviceCount() > 0:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_CUDA)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CUDA)
                self.logger.info("✅ Using CUDA backend for person detection")
            else:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
                self.logger.info("✅ Using CPU backend for person detection")
            
            self.is_loaded = True
            self.logger.info("✅ YOLO Person Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load YOLO person detector: {e}")
            return False
    
    async def _load_ssd(self) -> bool:
        """Load SSD person detection model"""
        try:
            model_path = Path(self.model_path)
            
            # For SSD, expect .pb or .prototxt/.caffemodel
            if model_path.suffix == '.pb':
                # TensorFlow model
                self.net = cv2.dnn.readNetFromTensorflow(str(model_path))
            else:
                # Caffe model
                prototxt_path = model_path.parent / f"{model_path.stem}.prototxt"
                caffemodel_path = model_path
                
                if not prototxt_path.exists():
                    self.logger.error(f"Prototxt file not found: {prototxt_path}")
                    return False
                
                self.net = cv2.dnn.readNetFromCaffe(str(prototxt_path), str(caffemodel_path))
            
            # Set backend/target
            if self.device == "cuda" and cv2.cuda.getCudaEnabledDeviceCount() > 0:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_CUDA)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CUDA)
            else:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
            
            self.is_loaded = True
            self.logger.info("✅ SSD Person Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load SSD person detector: {e}")
            return False
    
    async def _load_hog(self) -> bool:
        """Load HOG+SVM person detector"""
        try:
            # Use OpenCV's built-in HOG descriptor
            self.hog = cv2.HOGDescriptor()
            self.hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
            
            self.is_loaded = True
            self.logger.info("✅ HOG+SVM Person Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load HOG person detector: {e}")
            return False
    
    async def detect(self, image: np.ndarray, **kwargs) -> List[Detection]:
        """Detect persons in image"""
        if not self.is_loaded:
            self.logger.error("Person detector not loaded")
            return []
        
        try:
            if self.model_type == 'yolo':
                return await self._detect_yolo(image)
            elif self.model_type == 'ssd':
                return await self._detect_ssd(image)
            elif self.model_type == 'hog':
                return await self._detect_hog(image)
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Person detection failed: {e}")
            return []
    
    async def _detect_yolo(self, image: np.ndarray) -> List[Detection]:
        """Detect persons using YOLO"""
        detections = []
        h, w = image.shape[:2]
        
        # Create blob
        blob = cv2.dnn.blobFromImage(
            image, 
            scalefactor=1/255.0,
            size=self.input_size,
            swapRB=True,
            crop=False
        )
        
        # Run inference
        self.net.setInput(blob)
        layer_outputs = self.net.forward(self.net.getUnconnectedOutLayersNames())
        
        boxes = []
        confidences = []
        
        # Process outputs
        for output in layer_outputs:
            for detection in output:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                
                # Only person class (class_id = 0 in COCO)
                if class_id == self.person_class_id and confidence > self.confidence_threshold:
                    # Get bounding box
                    center_x = int(detection[0] * w)
                    center_y = int(detection[1] * h)
                    width = int(detection[2] * w)
                    height = int(detection[3] * h)
                    
                    x = int(center_x - width / 2)
                    y = int(center_y - height / 2)
                    
                    boxes.append([x, y, width, height])
                    confidences.append(float(confidence))
        
        # Apply NMS
        indices = cv2.dnn.NMSBoxes(boxes, confidences, self.confidence_threshold, self.nms_threshold)
        
        if len(indices) > 0:
            for i in indices.flatten():
                x, y, width, height = boxes[i]
                confidence = confidences[i]
                
                bbox = BoundingBox(
                    x=float(x), 
                    y=float(y),
                    width=float(width),
                    height=float(height)
                )
                
                detection = Detection(
                    class_id=self.person_class_id,
                    class_name='person',
                    confidence=confidence,
                    bbox=bbox
                )
                
                detections.append(detection)
        
        return detections
    
    async def _detect_ssd(self, image: np.ndarray) -> List[Detection]:
        """Detect persons using SSD"""
        detections = []
        h, w = image.shape[:2]
        
        # Create blob
        blob = cv2.dnn.blobFromImage(
            image, 
            scalefactor=0.007843,
            size=self.input_size,
            mean=(127.5, 127.5, 127.5),
            swapRB=False
        )
        
        # Run inference
        self.net.setInput(blob)
        outputs = self.net.forward()
        
        # Process detections
        for i in range(outputs.shape[2]):
            class_id = int(outputs[0, 0, i, 1])
            confidence = outputs[0, 0, i, 2]
            
            # Only person class (depends on model - usually class 15 for COCO)
            if class_id == 15 and confidence > self.confidence_threshold:  # person class in COCO SSD
                # Get bounding box coordinates
                x1 = int(outputs[0, 0, i, 3] * w)
                y1 = int(outputs[0, 0, i, 4] * h)
                x2 = int(outputs[0, 0, i, 5] * w)
                y2 = int(outputs[0, 0, i, 6] * h)
                
                bbox = BoundingBox(
                    x=float(x1), 
                    y=float(y1),
                    width=float(x2 - x1),
                    height=float(y2 - y1)
                )
                
                detection = Detection(
                    class_id=self.person_class_id,
                    class_name='person',
                    confidence=float(confidence),
                    bbox=bbox
                )
                
                detections.append(detection)
        
        return detections
    
    async def _detect_hog(self, image: np.ndarray) -> List[Detection]:
        """Detect persons using HOG+SVM"""
        detections = []
        
        # Detect people
        (rects, weights) = self.hog.detectMultiScale(
            image,
            winStride=(4, 4),
            padding=(8, 8),
            scale=1.05
        )
        
        # Process detections
        for i, (x, y, w, h) in enumerate(rects):
            confidence = float(weights[i]) if i < len(weights) else 0.8
            
            if confidence > self.confidence_threshold:
                bbox = BoundingBox(
                    x=float(x), 
                    y=float(y),
                    width=float(w),
                    height=float(h)
                )
                
                detection = Detection(
                    class_id=self.person_class_id,
                    class_name='person',
                    confidence=confidence,
                    bbox=bbox
                )
                
                detections.append(detection)
        
        return detections
    
    async def unload(self) -> bool:
        """Unload person detection model"""
        try:
            self.net = None
            self.hog = None
            self.is_loaded = False
            
            self.logger.info("✅ Person Detector unloaded")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unload person detector: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get person detector information"""
        return {
            'name': 'Person Detector',
            'type': self.model_type,
            'model_path': self.model_path,
            'device': self.device,
            'confidence_threshold': self.confidence_threshold,
            'input_size': self.input_size,
            'classes': self.class_names,
            'is_loaded': self.is_loaded
        }