# services/results/alert_engine.py
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AlertEngine:
    """Tạo cảnh báo dựa trên kết quả AI"""
    
    def __init__(self):
        self.alert_rules = {
            'person_count_high': {
                'threshold': 5,
                'severity': 'high',
                'message': 'High person count detected'
            },
            'person_in_restricted_area': {
                'severity': 'critical',
                'message': 'Person detected in restricted area'
            },
            'vehicle_speeding': {
                'threshold': 50,  # km/h
                'severity': 'medium',
                'message': 'Vehicle exceeding speed limit'
            },
            'unattended_object': {
                'duration': 300,  # seconds
                'severity': 'medium',
                'message': 'Unattended object detected'
            }
        }
    
    async def generate_alerts(self, result_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Tạo cảnh báo dựa trên kết quả detection
        
        Args:
            result_data: Kết quả AI detection
            
        Returns:
            Danh sách cảnh báo
        """
        alerts = []
        detections = result_data.get('detections', [])
        camera_id = result_data.get('camera_id')
        
        try:
            # Rule 1: Person count alert
            person_count = sum(1 for d in detections if d.get('class_name') == 'person')
            if person_count >= self.alert_rules['person_count_high']['threshold']:
                alerts.append(self._create_alert(
                    'person_count_high',
                    f"High person count: {person_count} people detected",
                    camera_id,
                    {'person_count': person_count}
                ))
            
            # Rule 2: High confidence vehicle detection
            vehicles = [d for d in detections if d.get('class_name') in ['car', 'truck', 'motorcycle']]
            high_conf_vehicles = [v for v in vehicles if v.get('confidence', 0) > 0.9]
            if high_conf_vehicles:
                alerts.append(self._create_alert(
                    'vehicle_detection',
                    f"{len(high_conf_vehicles)} vehicle(s) detected with high confidence",
                    camera_id,
                    {'vehicle_count': len(high_conf_vehicles)},
                    severity='low'
                ))
            
            # Rule 3: Multiple object classes detected simultaneously
            unique_classes = set(d.get('class_name') for d in detections)
            if len(unique_classes) >= 3:
                alerts.append(self._create_alert(
                    'multiple_classes',
                    f"Multiple object types detected: {', '.join(unique_classes)}",
                    camera_id,
                    {'classes': list(unique_classes)},
                    severity='low'
                ))
            
            logger.debug(f"🚨 Generated {len(alerts)} alerts for camera {camera_id}")
            
        except Exception as e:
            logger.error(f"❌ Error generating alerts: {e}")
        
        return alerts
    
    def _create_alert(
        self, 
        alert_type: str, 
        message: str, 
        camera_id: str,
        metadata: Dict[str, Any] = None,
        severity: str = None
    ) -> Dict[str, Any]:
        """Tạo alert object"""
        rule = self.alert_rules.get(alert_type, {})
        
        return {
            'id': f"alert_{int(datetime.utcnow().timestamp())}_{camera_id}_{alert_type}",
            'type': alert_type,
            'severity': severity or rule.get('severity', 'medium'),
            'message': message,
            'camera_id': camera_id,
            'timestamp': datetime.utcnow().isoformat(),
            'metadata': metadata or {},
            'acknowledged': False
        }