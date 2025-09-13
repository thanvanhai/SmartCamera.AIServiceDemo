# services/ai_worker/detectors/face_detector.py
import cv2
import numpy as np
import logging
from typing import List, Dict, Any, Optional, Tuple
import asyncio
from pathlib import Path

from services.ai_worker.detectors.base_detector import BaseDetector
from core.models.detection_models import Detection, BoundingBox


class FaceDetector(BaseDetector):
    """Face Detection using OpenCV DNN or MTCNN"""
    
    def __init__(self, model_path: str, config: Dict, device: str = "cpu"):
        super().__init__(model_path, config, device)
        self.logger = logging.getLogger(__name__)
        
        # Model configuration
        self.confidence_threshold = config.get('confidence_threshold', 0.5)
        self.nms_threshold = config.get('nms_threshold', 0.4)
        self.input_size = config.get('input_size', (300, 300))
        
        # Model type (opencv_dnn, mtcnn)
        self.model_type = config.get('type', 'opencv_dnn')
        
        # Model components
        self.net = None
        self.face_cascade = None
        self.mtcnn = None
        
        # Class names
        self.class_names = ['face']
    
    async def load(self) -> bool:
        """Load face detection model"""
        try:
            self.logger.info(f"🔧 Loading Face Detector: {self.model_type}")
            
            if self.model_type == 'opencv_dnn':
                return await self._load_opencv_dnn()
            elif self.model_type == 'mtcnn':
                return await self._load_mtcnn()
            elif self.model_type == 'cascade':
                return await self._load_cascade()
            else:
                self.logger.error(f"Unsupported face detection type: {self.model_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load face detector: {e}")
            return False
    
    async def _load_opencv_dnn(self) -> bool:
        """Load OpenCV DNN face detector (ResNet-SSD)"""
        try:
            model_path = Path(self.model_path)
            
            # Expect .prototxt and .caffemodel files
            prototxt_path = model_path.parent / f"{model_path.stem}.prototxt"
            caffemodel_path = model_path.parent / f"{model_path.stem}.caffemodel"
            
            if not prototxt_path.exists() or not caffemodel_path.exists():
                # Try alternative naming
                prototxt_path = model_path.parent / "deploy.prototxt"
                caffemodel_path = model_path.parent / "res10_300x300_ssd_iter_140000.caffemodel"
            
            if not prototxt_path.exists() or not caffemodel_path.exists():
                self.logger.error(f"Face detection model files not found: {prototxt_path}, {caffemodel_path}")
                return False
            
            # Load DNN model
            self.net = cv2.dnn.readNetFromCaffe(str(prototxt_path), str(caffemodel_path))
            
            if self.device == "cuda" and cv2.cuda.getCudaEnabledDeviceCount() > 0:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_CUDA)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CUDA)
                self.logger.info("✅ Using CUDA backend for face detection")
            else:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
                self.logger.info("✅ Using CPU backend for face detection")
            
            self.is_loaded = True
            self.logger.info("✅ OpenCV DNN Face Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load OpenCV DNN face detector: {e}")
            return False
    
    async def _load_mtcnn(self) -> bool:
        """Load MTCNN face detector"""
        try:
            # Try to import MTCNN
            try:
                from mtcnn import MTCNN
                self.mtcnn = MTCNN()
                self.is_loaded = True
                self.logger.info("✅ MTCNN Face Detector loaded successfully")
                return True
            except ImportError:
                self.logger.error("MTCNN not installed. Install with: pip install mtcnn")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load MTCNN face detector: {e}")
            return False
    
    async def _load_cascade(self) -> bool:
        """Load Haar Cascade face detector"""
        try:
            # Use built-in OpenCV cascade
            cascade_path = cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'
            self.face_cascade = cv2.CascadeClassifier(cascade_path)
            
            if self.face_cascade.empty():
                self.logger.error("Failed to load Haar cascade classifier")
                return False
            
            self.is_loaded = True
            self.logger.info("✅ Haar Cascade Face Detector loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load Haar cascade face detector: {e}")
            return False
    
    async def detect(self, image: np.ndarray, **kwargs) -> List[Detection]:
        """Detect faces in image"""
        if not self.is_loaded:
            self.logger.error("Face detector not loaded")
            return []
        
        try:
            if self.model_type == 'opencv_dnn':
                return await self._detect_opencv_dnn(image)
            elif self.model_type == 'mtcnn':
                return await self._detect_mtcnn(image)
            elif self.model_type == 'cascade':
                return await self._detect_cascade(image)
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Face detection failed: {e}")
            return []
    
    async def _detect_opencv_dnn(self, image: np.ndarray) -> List[Detection]:
        """Detect faces using OpenCV DNN"""
        detections = []
        h, w = image.shape[:2]
        
        # Create blob from image
        blob = cv2.dnn.blobFromImage(
            image, 
            scalefactor=1.0,
            size=self.input_size,
            mean=(104.0, 177.0, 123.0)
        )
        
        # Run inference
        self.net.setInput(blob)
        outputs = self.net.forward()
        
        # Process detections
        for i in range(outputs.shape[2]):
            confidence = outputs[0, 0, i, 2]
            
            if confidence > self.confidence_threshold:
                # Get bounding box coordinates
                x1 = int(outputs[0, 0, i, 3] * w)
                y1 = int(outputs[0, 0, i, 4] * h)
                x2 = int(outputs[0, 0, i, 5] * w)
                y2 = int(outputs[0, 0, i, 6] * h)
                
                # Create bounding box
                bbox = BoundingBox(
                    x=float(x1), 
                    y=float(y1),
                    width=float(x2 - x1),
                    height=float(y2 - y1)
                )
                
                # Create detection
                detection = Detection(
                    class_id=0,
                    class_name='face',
                    confidence=float(confidence),
                    bbox=bbox
                )
                
                detections.append(detection)
        
        return detections
    
    async def _detect_mtcnn(self, image: np.ndarray) -> List[Detection]:
        """Detect faces using MTCNN"""
        detections = []
        
        # Convert BGR to RGB (MTCNN expects RGB)
        rgb_image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        
        # Detect faces
        result = self.mtcnn.detect_faces(rgb_image)
        
        for face in result:
            confidence = face['confidence']
            
            if confidence > self.confidence_threshold:
                # Get bounding box
                x, y, width, height = face['box']
                
                # Create bounding box
                bbox = BoundingBox(
                    x=float(x), 
                    y=float(y),
                    width=float(width),
                    height=float(height)
                )
                
                # Create detection with keypoints info
                additional_info = {
                    'keypoints': face.get('keypoints', {}),
                    'landmarks': face.get('keypoints', {})
                }
                
                detection = Detection(
                    class_id=0,
                    class_name='face',
                    confidence=float(confidence),
                    bbox=bbox,
                    additional_info=additional_info
                )
                
                detections.append(detection)
        
        return detections
    
    async def _detect_cascade(self, image: np.ndarray) -> List[Detection]:
        """Detect faces using Haar Cascade"""
        detections = []
        
        # Convert to grayscale
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # Detect faces
        faces = self.face_cascade.detectMultiScale(
            gray,
            scaleFactor=1.1,
            minNeighbors=5,
            minSize=(30, 30)
        )
        
        for (x, y, w, h) in faces:
            # Create bounding box
            bbox = BoundingBox(
                x=float(x), 
                y=float(y),
                width=float(w),
                height=float(h)
            )
            
            # Haar cascade doesn't provide confidence, use default
            detection = Detection(
                class_id=0,
                class_name='face',
                confidence=0.9,  # Default confidence for cascade
                bbox=bbox
            )
            
            detections.append(detection)
        
        return detections
    
    async def unload(self) -> bool:
        """Unload face detection model"""
        try:
            self.net = None
            self.face_cascade = None
            self.mtcnn = None
            self.is_loaded = False
            
            self.logger.info("✅ Face Detector unloaded")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unload face detector: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get face detector information"""
        return {
            'name': 'Face Detector',
            'type': self.model_type,
            'model_path': self.model_path,
            'device': self.device,
            'confidence_threshold': self.confidence_threshold,
            'input_size': self.input_size,
            'classes': self.class_names,
            'is_loaded': self.is_loaded
        }
    
    async def preprocess_image(self, image: np.ndarray) -> np.ndarray:
        """Preprocess image for face detection"""
        # Face detection usually works better with good contrast
        if len(image.shape) == 3:
            # Apply histogram equalization to improve detection
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            gray = cv2.equalizeHist(gray)
            image = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
        
        return image