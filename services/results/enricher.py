# services/results/enricher.py
import logging
from typing import Dict, Any
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

class ResultEnricher:
    """Bổ sung thông tin cho kết quả AI"""
    
    def __init__(self):
        self.camera_info_cache = {}
        self.weather_cache = {}
    
    async def enrich_result(self, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Bổ sung thông tin cho kết quả detection
        
        Args:
            result_data: Kết quả detection gốc
            
        Returns:
            Kết quả đã bổ sung thông tin
        """
        try:
            enriched_result = {**result_data}
            
            # Add enrichment timestamp
            enriched_result['enriched_at'] = datetime.utcnow().isoformat()
            
            # Enrich detections
            detections = result_data.get('detections', [])
            enriched_detections = []
            
            for detection in detections:
                enriched_detection = await self._enrich_single_detection(detection)
                enriched_detections.append(enriched_detection)
            
            enriched_result['detections'] = enriched_detections
            
            # Add camera context (if available)
            camera_id = result_data.get('camera_id')
            if camera_id:
                camera_context = await self._get_camera_context(camera_id)
                enriched_result['camera_context'] = camera_context
            
            # Add temporal context
            temporal_context = self._get_temporal_context()
            enriched_result['temporal_context'] = temporal_context
            
            logger.debug(f"🔍 Enriched result for camera {camera_id}")
            
            return enriched_result
            
        except Exception as e:
            logger.error(f"❌ Error enriching result: {e}")
            return result_data  # Return original if enrichment fails
    
    async def _enrich_single_detection(self, detection: Dict[str, Any]) -> Dict[str, Any]:
        """Bổ sung thông tin cho một detection"""
        enriched = {**detection}
        
        try:
            # Add detection metadata
            enriched['detection_id'] = f"det_{int(datetime.utcnow().timestamp() * 1000)}"
            enriched['enriched_at'] = datetime.utcnow().isoformat()
            
            # Calculate bounding box area
            bbox = detection.get('bounding_box', {})
            if bbox and 'width' in bbox and 'height' in bbox:
                area = bbox['width'] * bbox['height']
                enriched['area'] = area
                enriched['size_category'] = self._categorize_size(area)
            
            # Add confidence category
            confidence = detection.get('confidence', 0)
            enriched['confidence_category'] = self._categorize_confidence(confidence)
            
            # Add position information
            if bbox:
                center_x = bbox.get('x', 0) + bbox.get('width', 0) / 2
                center_y = bbox.get('y', 0) + bbox.get('height', 0) / 2
                enriched['center_point'] = {'x': center_x, 'y': center_y}
                enriched['position_category'] = self._categorize_position(center_x, center_y)
            
        except Exception as e:
            logger.error(f"❌ Error enriching detection: {e}")
        
        return enriched
    
    async def _get_camera_context(self, camera_id: str) -> Dict[str, Any]:
        """Lấy thông tin context của camera"""
        if camera_id in self.camera_info_cache:
            return self.camera_info_cache[camera_id]
        
        # Mock camera context (in real implementation, get from database/API)
        context = {
            'location': 'Main Entrance',
            'zone': 'Security Zone A',
            'resolution': '1920x1080',
            'fps': 30,
            'field_of_view': 'wide',
            'lighting_condition': 'good'
        }
        
        # Cache for 5 minutes
        self.camera_info_cache[camera_id] = context
        return context
    
    def _get_temporal_context(self) -> Dict[str, Any]:
        """Lấy thông tin thời gian"""
        now = datetime.utcnow()
        
        return {
            'hour': now.hour,
            'day_of_week': now.weekday(),
            'is_weekend': now.weekday() >= 5,
            'is_business_hours': 8 <= now.hour <= 17,
            'time_category': self._categorize_time(now.hour)
        }
    
    def _categorize_size(self, area: int) -> str:
        """Phân loại kích thước object"""
        if area < 1000:
            return 'small'
        elif area < 10000:
            return 'medium'
        else:
            return 'large'
    
    def _categorize_confidence(self, confidence: float) -> str:
        """Phân loại độ tin cậy"""
        if confidence >= 0.9:
            return 'very_high'
        elif confidence >= 0.7:
            return 'high'
        elif confidence >= 0.5:
            return 'medium'
        else:
            return 'low'
    
    def _categorize_position(self, center_x: float, center_y: float) -> str:
        """Phân loại vị trí trong frame (giả sử frame 1920x1080)"""
        # Simple 3x3 grid
        if center_x < 640:
            x_pos = 'left'
        elif center_x < 1280:
            x_pos = 'center'
        else:
            x_pos = 'right'
        
        if center_y < 360:
            y_pos = 'top'
        elif center_y < 720:
            y_pos = 'middle'
        else:
            y_pos = 'bottom'
        
        return f"{y_pos}_{x_pos}"
    
    def _categorize_time(self, hour: int) -> str:
        """Phân loại thời gian trong ngày"""
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 22:
            return 'evening'
        else:
            return 'night'