# services/results/processor.py
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

class ResultProcessor:
    """Xử lý kết quả AI detection"""
    
    def __init__(self):
        self.processed_count = 0
        self.last_processed_time = None
    
    async def process_result(
        self, 
        result_data: Dict[str, Any], 
        alerts: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Xử lý kết quả AI detection cuối cùng
        
        Args:
            result_data: Kết quả đã qua deduplication và enrichment
            alerts: Danh sách cảnh báo từ alert engine
            
        Returns:
            Kết quả đã xử lý hoàn chỉnh
        """
        try:
            # Add processing metadata
            processed_result = {
                **result_data,
                'alerts': alerts or [],
                'processed_at': datetime.utcnow().isoformat(),
                'processor_version': '1.0.0',
                'processing_id': f"proc_{self.processed_count}_{int(datetime.utcnow().timestamp())}"
            }
            
            # Calculate statistics
            detections = result_data.get('detections', [])
            if detections:
                confidences = [d.get('confidence', 0) for d in detections]
                processed_result['avg_confidence'] = sum(confidences) / len(confidences)
                processed_result['max_confidence'] = max(confidences)
                processed_result['min_confidence'] = min(confidences)
            
            # Add detection summary
            detection_summary = self._create_detection_summary(detections)
            processed_result['summary'] = detection_summary
            
            # Update counters
            self.processed_count += 1
            self.last_processed_time = datetime.utcnow()
            
            logger.debug(f"✅ Processed result - Camera: {result_data.get('camera_id')}, "
                        f"Detections: {len(detections)}, Alerts: {len(alerts or [])}")
            
            return processed_result
            
        except Exception as e:
            logger.error(f"❌ Error processing result: {e}")
            # Return original data with error flag
            return {
                **result_data,
                'processing_error': str(e),
                'processed_at': datetime.utcnow().isoformat(),
                'alerts': alerts or []
            }
    
    def _create_detection_summary(self, detections: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Tạo tóm tắt detection"""
        summary = {
            'total_detections': len(detections),
            'classes': {},
            'high_confidence_count': 0,
            'medium_confidence_count': 0,
            'low_confidence_count': 0
        }
        
        for detection in detections:
            # Count by class
            class_name = detection.get('class_name', 'unknown')
            summary['classes'][class_name] = summary['classes'].get(class_name, 0) + 1
            
            # Count by confidence level
            confidence = detection.get('confidence', 0)
            if confidence >= 0.8:
                summary['high_confidence_count'] += 1
            elif confidence >= 0.5:
                summary['medium_confidence_count'] += 1
            else:
                summary['low_confidence_count'] += 1
        
        return summary