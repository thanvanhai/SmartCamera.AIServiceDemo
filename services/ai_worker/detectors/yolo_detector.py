# services/ai_worker/detectors/yolo_detector.py
import time
import logging
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
import cv2

from .base_detector import BaseDetector, DetectionOutput, ModelConfig
from core.models import Detection
from core.enums import ModelType


class YOLODetector(BaseDetector):
    """YOLO Object Detector using ONNX Runtime"""
    
    def __init__(self, model_config: ModelConfig):
        super().__init__(model_config)
        self.model_type = ModelType.YOLO
        self.supports_batch = True
        
        # YOLO specific settings
        self.stride = 32
        self.session = None
        self.input_name = None
        self.output_names = None
        
        # Class names for COCO dataset (default)
        if not self.config.class_names:
            self.config.class_names = self._get_coco_class_names()
    
    async def load_model(self) -> bool:
        """Load YOLO model using ONNX Runtime"""
        try:
            import onnxruntime as ort
            
            # Configure ONNX Runtime providers
            providers = []
            if self.config.device == 'cuda':
                providers.append('CUDAExecutionProvider')
            providers.append('CPUExecutionProvider')
            
            # Create inference session
            self.session = ort.InferenceSession(
                self.config.model_path,
                providers=providers
            )
            
            # Get input/output names
            self.input_name = self.session.get_inputs()[0].name
            self.output_names = [output.name for output in self.session.get_outputs()]
            
            # Get input shape
            input_shape = self.session.get_inputs()[0].shape
            if input_shape[2] != -1 and input_shape[3] != -1:
                self.config.input_size = (input_shape[2], input_shape[3])
            
            self.is_loaded = True
            logging.info(f"YOLO model loaded: {self.config.model_path}")
            logging.info(f"Input shape: {input_shape}")
            logging.info(f"Output names: {self.output_names}")
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to load YOLO model: {e}")
            self.is_loaded = False
            return False
    
    async def detect(self, frame: np.ndarray, **kwargs) -> DetectionOutput:
        """Detect objects trong single frame"""
        start_time = time.time()
        
        try:
            if not self.is_loaded:
                await self.load_model()
            
            if not self.validate_frame(frame):
                raise ValueError("Invalid input frame")
            
            # Preprocess frame
            processed_frame, scale_factor = self.preprocess_frame(frame)
            
            # Run inference
            outputs = self.session.run(
                self.output_names,
                {self.input_name: processed_frame}
            )
            
            # Postprocess outputs
            detections = self.postprocess_output(
                outputs[0], 
                frame.shape[:2], 
                scale_factor
            )
            
            # Filter by confidence threshold
            confidence_threshold = kwargs.get('confidence_threshold', self.config.confidence_threshold)
            filtered_detections = [
                det for det in detections 
                if det.confidence >= confidence_threshold
            ]
            
            processing_time = time.time() - start_time
            self._update_statistics(processing_time)
            
            return DetectionOutput(
                detections=filtered_detections,
                processing_time=processing_time,
                model_confidence=max([det.confidence for det in filtered_detections]) if filtered_detections else 0.0,
                inference_metadata={
                    'input_size': self.config.input_size,
                    'scale_factor': scale_factor,
                    'raw_detections_count': len(detections),
                    'filtered_detections_count': len(filtered_detections)
                }
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logging.error(f"YOLO detection failed: {e}")
            
            return DetectionOutput(
                detections=[],
                processing_time=processing_time,
                model_confidence=0.0,
                inference_metadata={'error': str(e)}
            )
    
    async def detect_batch(self, frames: np.ndarray, **kwargs) -> List[DetectionOutput]:
        """Detect objects trong batch frames"""
        start_time = time.time()
        batch_size = len(frames)
        
        try:
            if not self.is_loaded:
                await self.load_model()
            
            # Preprocess all frames
            processed_frames = []
            scale_factors = []
            
            for frame in frames:
                if not self.validate_frame(frame):
                    processed_frames.append(np.zeros((3, *self.config.input_size), dtype=np.float32))
                    scale_factors.append(1.0)
                    continue
                
                processed_frame, scale_factor = self.preprocess_frame(frame)
                processed_frames.append(processed_frame[0])  # Remove batch dimension
                scale_factors.append(scale_factor)
            
            # Stack into batch
            batch_input = np.array(processed_frames)
            
            # Run batch inference
            outputs = self.session.run(
                self.output_names,
                {self.input_name: batch_input}
            )
            
            # Process each output in batch
            results = []
            confidence_threshold = kwargs.get('confidence_threshold', self.config.confidence_threshold)
            
            for i in range(batch_size):
                try:
                    # Extract single frame output
                    frame_output = outputs[0][i:i+1]  # Keep batch dimension
                    
                    # Postprocess
                    detections = self.postprocess_output(
                        frame_output,
                        frames[i].shape[:2],
                        scale_factors[i]
                    )
                    
                    # Filter by confidence
                    filtered_detections = [
                        det for det in detections 
                        if det.confidence >= confidence_threshold
                    ]
                    
                    result = DetectionOutput(
                        detections=filtered_detections,
                        processing_time=(time.time() - start_time) / batch_size,  # Average time
                        model_confidence=max([det.confidence for det in filtered_detections]) if filtered_detections else 0.0,
                        inference_metadata={
                            'batch_index': i,
                            'input_size': self.config.input_size,
                            'scale_factor': scale_factors[i],
                            'raw_detections_count': len(detections),
                            'filtered_detections_count': len(filtered_detections)
                        }
                    )
                    
                    results.append(result)
                    
                except Exception as e:
                    logging.error(f"Batch postprocessing failed for frame {i}: {e}")
                    results.append(DetectionOutput(
                        detections=[],
                        processing_time=0,
                        model_confidence=0.0,
                        inference_metadata={'error': str(e), 'batch_index': i}
                    ))
            
            total_time = time.time() - start_time
            self._update_statistics(total_time)
            
            return results
            
        except Exception as e:
            logging.error(f"YOLO batch detection failed: {e}")
            
            # Return empty results for all frames
            return [
                DetectionOutput(
                    detections=[],
                    processing_time=0,
                    model_confidence=0.0,
                    inference_metadata={'error': str(e), 'batch_index': i}
                )
                for i in range(batch_size)
            ]
    
    def preprocess_frame(self, frame: np.ndarray) -> Tuple[np.ndarray, float]:
        """Preprocess frame cho YOLO inference"""
        # Resize with padding
        resized_frame, scale_factor = self._resize_frame(frame, self.config.input_size)
        
        # Normalize
        normalized_frame = self._normalize_frame(resized_frame)
        
        # Convert to tensor format (NCHW)
        tensor_frame = self._to_tensor_format(normalized_frame)
        
        return tensor_frame, scale_factor
    
    def postprocess_output(self, 
                          raw_output: np.ndarray, 
                          frame_shape: Tuple[int, int],
                          scale_factor: float = 1.0) -> List[Detection]:
        """Postprocess YOLO output thành Detection objects"""
        detections = []
        
        try:
            # YOLO output shape: (batch_size, num_detections, 85) for COCO
            # 85 = 4 (bbox) + 1 (confidence) + 80 (classes)
            
            if len(raw_output.shape) == 3:
                output = raw_output[0]  # Remove batch dimension
            else:
                output = raw_output
            
            # Apply confidence threshold early
            conf_mask = output[:, 4] >= self.config.confidence_threshold
            output = output[conf_mask]
            
            if len(output) == 0:
                return detections
            
            # Extract bbox coordinates and confidence
            boxes = output[:, :4]  # x_center, y_center, width, height
            confidences = output[:, 4]
            class_scores = output[:, 5:]
            
            # Get class predictions
            class_ids = np.argmax(class_scores, axis=1)
            class_confidences = np.max(class_scores, axis=1)
            
            # Final confidence = objectness * class_confidence
            final_confidences = confidences * class_confidences
            
            # Convert center format to corner format
            x_centers = boxes[:, 0]
            y_centers = boxes[:, 1]
            widths = boxes[:, 2]
            heights = boxes[:, 3]
            
            x1 = x_centers - widths / 2
            y1 = y_centers - heights / 2
            x2 = x_centers + widths / 2
            y2 = y_centers + heights / 2
            
            # Scale back to original frame size
            h, w = frame_shape
            input_h, input_w = self.config.input_size
            
            # Calculate padding offsets
            scale = min(input_w / w, input_h / h)
            pad_x = (input_w - w * scale) / 2
            pad_y = (input_h - h * scale) / 2
            
            # Unpad and scale coordinates
            x1 = (x1 - pad_x) / scale
            y1 = (y1 - pad_y) / scale
            x2 = (x2 - pad_x) / scale
            y2 = (y2 - pad_y) / scale
            
            # Apply NMS
            if self.config.nms_threshold > 0:
                indices = self._apply_nms(
                    np.column_stack([x1, y1, x2, y2]),
                    final_confidences,
                    self.config.nms_threshold
                )
            else:
                indices = range(len(x1))
            
            # Create Detection objects
            for i in indices:
                if final_confidences[i] >= self.config.confidence_threshold:
                    detection = self._create_detection(
                        class_ids[i],
                        final_confidences[i],
                        [x1[i], y1[i], x2[i], y2[i]],
                        frame_shape
                    )
                    detections.append(detection)
            
        except Exception as e:
            logging.error(f"YOLO postprocessing failed: {e}")
        
        return detections
    
    def _apply_nms(self, boxes: np.ndarray, scores: np.ndarray, nms_threshold: float) -> List[int]:
        """Apply Non-Maximum Suppression"""
        try:
            # Convert to x1, y1, x2, y2 format for cv2.dnn.NMSBoxes
            x1, y1, x2, y2 = boxes[:, 0], boxes[:, 1], boxes[:, 2], boxes[:, 3]
            w = x2 - x1
            h = y2 - y1
            
            # cv2.dnn.NMSBoxes expects (x, y, w, h) format
            boxes_xywh = np.column_stack([x1, y1, w, h]).tolist()
            scores_list = scores.tolist()
            
            indices = cv2.dnn.NMSBoxes(
                boxes_xywh,
                scores_list,
                self.config.confidence_threshold,
                nms_threshold
            )
            
            if len(indices) > 0:
                return indices.flatten().tolist()
            else:
                return []
                
        except Exception as e:
            logging.error(f"NMS failed: {e}")
            return list(range(len(boxes)))
    
    def _get_coco_class_names(self) -> List[str]:
        """Get COCO dataset class names"""
        return [
            'person', 'bicycle', 'car', 'motorcycle', 'airplane', 'bus', 'train', 'truck', 'boat',
            'traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench', 'bird', 'cat',
            'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear', 'zebra', 'giraffe', 'backpack',
            'umbrella', 'handbag', 'tie', 'suitcase', 'frisbee', 'skis', 'snowboard', 'sports ball',
            'kite', 'baseball bat', 'baseball glove', 'skateboard', 'surfboard', 'tennis racket',
            'bottle', 'wine glass', 'cup', 'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple',
            'sandwich', 'orange', 'broccoli', 'carrot', 'hot dog', 'pizza', 'donut', 'cake', 'chair',
            'couch', 'potted plant', 'bed', 'dining table', 'toilet', 'tv', 'laptop', 'mouse',
            'remote', 'keyboard', 'cell phone', 'microwave', 'oven', 'toaster', 'sink', 'refrigerator',
            'book', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier', 'toothbrush'
        ]


# Register YOLO detector
from .base_detector import DetectorFactory
DetectorFactory.register_detector(ModelType.YOLO, YOLODetector)