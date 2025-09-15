# services/results/deduplicator.py
import logging
from typing import Dict, List, Any, Set
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger(__name__)

class ResultDeduplicator:
    """Loại bỏ detection trùng lặp"""
    
    def __init__(self, time_window: int = 5):
        self.time_window = time_window  # seconds
        self.recent_detections: Dict[str, Dict] = {}  # camera_id -> detections
        self.cleanup_interval = 60  # seconds
        self.last_cleanup = datetime.utcnow()
    
    async def remove_duplicates(self, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Loại bỏ detection trùng lặp
        
        Args:
            result_data: Kết quả detection gốc
            
        Returns:
            Kết quả đã loại bỏ trùng lặp
        """
        try:
            camera_id = result_data.get('camera_id')
            detections = result_data.get('detections', [])
            
            if not camera_id or not detections:
                return result_data
            
            # Cleanup old entries periodically
            await self._cleanup_old_entries()
            
            # Filter duplicates
            unique_detections = []
            for detection in detections:
                if not self._is_duplicate(camera_id, detection):
                    unique_detections.append(detection)
                    self._add_to_recent(camera_id, detection)
            
            # Create result with unique detections
            deduplicated_result = {
                **result_data,
                'detections': unique_detections,
                'original_detection_count': len(detections),
                'unique_detection_count': len(unique_detections),
                'duplicates_removed': len(detections) - len(unique_detections),
                'deduplicated_at': datetime.utcnow().isoformat()
            }
            
            if len(unique_detections) != len(detections):
                logger.debug(f"🔄 Removed {len(detections) - len(unique_detections)} duplicates "
                           f"from {camera_id}")
            
            return deduplicated_result
            
        except Exception as e:
            logger.error(f"❌ Error in deduplication: {e}")
            return result_data  # Return original if deduplication fails
    
    def _is_duplicate(self, camera_id: str, detection: Dict[str, Any]) -> bool:
        """Kiểm tra detection có trùng lặp không"""
        if camera_id not in self.recent_detections:
            return False
        
        detection_hash = self._calculate_detection_hash(detection)
        recent_camera_detections = self.recent_detections[camera_id]
        
        # Check if similar detection exists in time window
        current_time = datetime.utcnow()
        for recent_hash, recent_data in recent_camera_detections.items():
            time_diff = (current_time - recent_data['timestamp']).total_seconds()
            
            if time_diff <= self.time_window:
                # Check spatial similarity
                if self._are_spatially_similar(detection, recent_data['detection']):
                    # Check class similarity
                    if detection.get('class_name') == recent_data['detection'].get('class_name'):
                        return True
        
        return False
    
    def _calculate_detection_hash(self, detection: Dict[str, Any]) -> str:
        """Tính hash cho detection"""
        # Use class, confidence, and rough position for hash
        hash_data = {
            'class': detection.get('class_name', ''),
            'confidence_bucket': round(detection.get('confidence', 0) * 10) / 10,  # Round to 0.1
            'bbox_center': self._get_bbox_center(detection.get('bounding_box', {}))
        }
        
        hash_string = str(sorted(hash_data.items()))
        return hashlib.md5(hash_string.encode()).hexdigest()[:12]
    
    def _are_spatially_similar(self, det1: Dict[str, Any], det2: Dict[str, Any]) -> bool:
        """Kiểm tra hai detection có gần nhau về vị trí không"""
        bbox1 = det1.get('bounding_box', {})
        bbox2 = det2.get('bounding_box', {})
        
        if not bbox1 or not bbox2:
            return False
        
        center1 = self._get_bbox_center(bbox1)
        center2 = self._get_bbox_center(bbox2)
        
        # Calculate distance between centers
        distance = ((center1[0] - center2[0])**2 + (center1[1] - center2[1])**2)**0.5
        
        # Consider similar if centers are within 50 pixels
        return distance < 50
    
    def _get_bbox_center(self, bbox: Dict[str, Any]) -> tuple:
        """Lấy tâm của bounding box"""
        x = bbox.get('x', 0)
        y = bbox.get('y', 0)
        width = bbox.get('width', 0)
        height = bbox.get('height', 0)
        
        center_x = x + width / 2
        center_y = y + height / 2
        
        return (center_x, center_y)
    
    def _add_to_recent(self, camera_id: str, detection: Dict[str, Any]):
        """Thêm detection vào recent cache"""
        if camera_id not in self.recent_detections:
            self.recent_detections[camera_id] = {}
        
        detection_hash = self._calculate_detection_hash(detection)
        self.recent_detections[camera_id][detection_hash] = {
            'detection': detection,
            'timestamp': datetime.utcnow()
        }
    
    async def _cleanup_old_entries(self):
        """Dọn dẹp các entry cũ"""
        current_time = datetime.utcnow()
        
        if (current_time - self.last_cleanup).total_seconds() < self.cleanup_interval:
            return
        
        cutoff_time = current_time - timedelta(seconds=self.time_window * 2)
        
        for camera_id in list(self.recent_detections.keys()):
            camera_detections = self.recent_detections[camera_id]
            
            # Remove old entries
            old_hashes = [
                hash_key for hash_key, data in camera_detections.items()
                if data['timestamp'] < cutoff_time
            ]
            
            for hash_key in old_hashes:
                del camera_detections[hash_key]
            
            # Remove empty camera entries
            if not camera_detections:
                del self.recent_detections[camera_id]
        
        self.last_cleanup = current_time
        logger.debug(f"🧹 Cleanup completed - {len(self.recent_detections)} cameras with recent detections")