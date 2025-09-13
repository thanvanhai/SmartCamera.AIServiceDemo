# services/ai_worker/detectors/vehicle_detector.py
import cv2
import numpy as np
import logging
from typing import List, Dict, Any, Optional
import asyncio
from pathlib import Path

from services.ai_worker.detectors.base_detector import BaseDetector
from core.models.detection_models import Detection, BoundingBox


class VehicleDetector(BaseDetector):
    """Vehicle Detection for cars, trucks, motorcycles, buses"""
    
    def __init__(self, model_path: str, config: Dict, device: str = "cpu"):
        super().__init__(model_path, config, device)
        self.logger = logging.getLogger(__name__)
        
        # Model configuration
        self.confidence_threshold = config.get('confidence_threshold', 0.5)
        self.nms_threshold = config.get('nms_threshold', 0.4)
        self.input_size = config.get('input_size', (416, 416))
        
        # Model type (yolo, ssd, cascade)
        self.model_type = config.get('type', 'yolo')
        
        # Model components
        self.net = None
        self.car_cascade = None
        
        # Vehicle classes in COCO dataset
        self.vehicle_classes = {
            2: 'car',
            3: 'motorcycle', 
            5: 'bus',
            7: 'truck'
        }
        self.class_names = list(self.vehicle_classes.values())
    
    async def load(self) -> bool:
        """Load vehicle detection model"""
        try:
            self.logger.info(f"🔧 Loading Vehicle Detector: {self.model_type}")
            
            if self.model_type == 'yolo':
                return await self._load_yolo()
            elif self.model_type == 'ssd':
                return await self._load_ssd()
            elif self.model_type == 'cascade':
                return await self._load_cascade()
            else:
                self.logger.error(f"Unsupported vehicle detection type: {self.model_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load vehicle detector: {e}")
            return False
    
    async def _load_yolo(self) -> bool:
        """Load YOLO vehicle detection model"""
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
                self.logger.info("✅ Using CUDA backend for vehicle detection")
            else:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
                self.logger.info("✅ Using CPU backend for vehicle detection")
            
            self.is_loaded = True
            self.logger.info("✅ YOLO Vehicle Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load YOLO vehicle detector: {e}")
            return False
    
    async def _load_ssd(self) -> bool:
        """Load SSD vehicle detection model"""
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
            self.logger.info("✅ SSD Vehicle Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load SSD vehicle detector: {e}")
            return False
    
    async def _load_cascade(self) -> bool:
        """Load Haar Cascade vehicle detector"""
        try:
            model_path = Path(self.model_path)
            
            if not model_path.exists():
                self.logger.error(f"Cascade file not found: {model_path}")
                return False
            
            self.car_cascade = cv2.CascadeClassifier(str(model_path))
            
            if self.car_cascade.empty():
                self.logger.error("Failed to load vehicle cascade classifier")
                return False
            
            self.is_loaded = True
            self.logger.info("✅ Haar Cascade Vehicle Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load cascade vehicle detector: {e}")
            return False
    
    async def detect(self, image: np.ndarray, **kwargs) -> List[Detection]:
        """Detect vehicles in image"""
        if not self.is_loaded:
            self.logger.error("Vehicle detector not loaded")
            return []
        
        try:
            if self.model_type == 'yolo':
                return await self._detect_yolo(image)
            elif self.model_type == 'ssd':
                return await self._detect_ssd(image)
            elif self.model_type == 'cascade':
                return await self._detect_cascade(image)
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Vehicle detection failed: {e}")
            return []
    
    async def _detect_yolo(self, image: np.ndarray) -> List[Detection]:
        """Detect vehicles using YOLO"""
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
        class_ids = []
        
        # Process outputs
        for output in layer_outputs:
            for detection in output:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                
                # Only vehicle classes
                if class_id in self.vehicle_classes and confidence > self.confidence_threshold:
                    # Get bounding box
                    center_x = int(detection[0] * w)
                    center_y = int(detection[1] * h)
                    width = int(detection[2] * w)
                    height = int(detection[3] * h)
                    
                    x = int(center_x - width / 2)
                    y = int(center_y - height / 2)
                    
                    boxes.append([x, y, width, height])
                    confidences.append(float(confidence))
                    class_ids.append(class_id)
        
        # Apply NMS
        indices = cv2.dnn.NMSBoxes(boxes, confidences, self.confidence_threshold, self.nms_threshold)
        
        if len(indices) > 0:
            for i in indices.flatten():
                x, y, width, height = boxes[i]
                confidence = confidences[i]
                class_id = class_ids[i]
                class_name = self.vehicle_classes[class_id]
                
                bbox = BoundingBox(
                    x=float(x), 
                    y=float(y),
                    width=float(width),
                    height=float(height)
                )
                
                detection = Detection(
                    class_id=class_id,
                    class_name=class_name,
                    confidence=confidence,
                    bbox=bbox
                )
                
                detections.append(detection)
        
        return detections
    
    async def _detect_ssd(self, image: np.ndarray) -> List[Detection]:
        """Detect vehicles using SSD"""
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
            
            # Map SSD COCO classes to our vehicle classes
            ssd_to_coco = {7: 2, 8: 3, 6: 5, 8: 7}  # SSD class mapping
            coco_class_id = ssd_to_coco.get(class_id)
            
            if coco_class_id in self.vehicle_classes and confidence > self.confidence_threshold:
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
                    class_id=coco_class_id,
                    class_name=self.vehicle_classes[coco_class_id],
                    confidence=float(confidence),
                    bbox=bbox
                )
                
                detections.append(detection)
        
        return detections
    
    async def _detect_cascade(self, image: np.ndarray) -> List[Detection]:
        """Detect vehicles using Haar Cascade"""
        detections = []
        
        # Convert to grayscale
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # Detect vehicles
        vehicles = self.car_cascade.detectMultiScale(
            gray,
            scaleFactor=1.1,
            minNeighbors=3,
            minSize=(50, 50)
        )
        
        for (x, y, w, h) in vehicles:
            bbox = BoundingBox(
                x=float(x), 
                y=float(y),
                width=float(w),
                height=float(h)
            )
            
            # Haar cascade doesn't provide confidence or class, use defaults
            detection = Detection(
                class_id=2,  # Default to car
                class_name='car',
                confidence=0.8,  # Default confidence for cascade
                bbox=bbox
            )
            
            detections.append(detection)
        
        return detections
    
    async def unload(self) -> bool:
        """Unload vehicle detection model"""
        try:
            self.net = None
            self.car_cascade = None
            self.is_loaded = False
            
            self.logger.info("✅ Vehicle Detector unloaded")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unload vehicle detector: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get vehicle detector information"""
        return {
            'name': 'Vehicle Detector',
            'type': self.model_type,
            'model_path': self.model_path,
            'device': self.device,
            'confidence_threshold': self.confidence_threshold,
            'input_size': self.input_size,
            'vehicle_classes': self.vehicle_classes,
            'class_names': self.class_names,
            'is_loaded': self.is_loaded
        }
    
    def get_supported_vehicles(self) -> List[str]:
        """Get list of supported vehicle types"""
        return self.class_names
    
    async def detect_specific_vehicle(self, image: np.ndarray, vehicle_type: str) -> List[Detection]:
        """Detect specific vehicle type only"""
        all_detections = await self.detect(image)
        
        # Filter by vehicle type
        specific_detections = [
            det for det in all_detections 
            if det.class_name == vehicle_type
        ]
        
        return specific_detections