# ================================
# infrastructure/external/webapi_client.py
# ================================
import asyncio
import aiohttp
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from core.models.detection_models import Detection, DetectionResult
from core.models.camera_models import CameraConfig, CameraStatus
from core.exceptions import WebAPIError, AuthenticationError
from shared.utils.validation import validate_response
from shared.decorators.retry import retry_on_failure
from shared.decorators.timing import measure_time
from app.settings import settings
import logging

logger = logging.getLogger(__name__)

class AIServiceStatus(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping" 
    ERROR = "error"
    MAINTENANCE = "maintenance"

@dataclass
class WebAPIConfig:
    base_url: str
    api_key: str
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0

class WebAPIClient:
    """
    Client for communicating with SmartCamera.WebApiDemo (.NET Core)
    Handles authentication, API calls, and error handling
    """
    
    def __init__(self, config: WebAPIConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.auth_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
    async def __aenter__(self):
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def start(self):
        """Initialize HTTP session and authenticate"""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'SmartCamera-AIService/1.0'
            }
        )
        
        # Authenticate on startup
        await self._authenticate()
        logger.info("WebAPI client initialized and authenticated")
        
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            logger.info("WebAPI client closed")
            
    async def _authenticate(self):
        """Authenticate with WebAPI and get JWT token"""
        try:
            auth_data = {
                'apiKey': self.config.api_key,
                'serviceType': 'AIService',
                'version': '1.0.0'
            }
            
            async with self.session.post(
                f"{self.config.base_url}/api/auth/service-login",
                json=auth_data
            ) as response:
                
                if response.status == 200:
                    data = await response.json()
                    self.auth_token = data.get('token')
                    
                    # Parse token expiration (assuming JWT)
                    import jwt
                    decoded = jwt.decode(self.auth_token, options={"verify_signature": False})
                    self.token_expires_at = datetime.fromtimestamp(decoded.get('exp', 0))
                    
                    # Update session headers
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.auth_token}'
                    })
                    
                    logger.info(f"Authentication successful, token expires at {self.token_expires_at}")
                else:
                    raise AuthenticationError(f"Authentication failed: {response.status}")
                    
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise AuthenticationError(f"Failed to authenticate: {e}")
            
    async def _ensure_authenticated(self):
        """Ensure we have a valid authentication token"""
        if not self.auth_token or not self.token_expires_at:
            await self._authenticate()
            return
            
        # Refresh token if expiring in next 5 minutes
        if datetime.now() >= self.token_expires_at.replace(minute=self.token_expires_at.minute - 5):
            logger.info("Token expiring soon, refreshing...")
            await self._authenticate()
            
    @retry_on_failure(max_retries=3, delay=1.0)
    @measure_time
    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make HTTP request with error handling and retries"""
        
        await self._ensure_authenticated()
        
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        try:
            async with self.session.request(
                method=method.upper(),
                url=url,
                json=data if data else None,
                params=params
            ) as response:
                
                response_text = await response.text()
                
                if response.status == 401:
                    # Token expired, re-authenticate and retry
                    logger.warning("Received 401, re-authenticating...")
                    await self._authenticate()
                    raise AuthenticationError("Token expired, retrying...")
                    
                elif response.status >= 400:
                    logger.error(f"API Error {response.status}: {response_text}")
                    raise WebAPIError(
                        f"API request failed: {response.status}",
                        status_code=response.status,
                        response_text=response_text
                    )
                    
                # Parse JSON response
                if response_text:
                    return await response.json()
                else:
                    return {}
                    
        except aiohttp.ClientError as e:
            logger.error(f"HTTP client error: {e}")
            raise WebAPIError(f"HTTP request failed: {e}")
            
    # ================================
    # Camera Management APIs
    # ================================
    
    async def get_camera_config(self, camera_id: str) -> Optional[CameraConfig]:
        """Get camera configuration from WebAPI"""
        try:
            response = await self._make_request(
                'GET', 
                f'/api/cameras/{camera_id}/config'
            )
            
            if response:
                return CameraConfig(**response)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get camera config for {camera_id}: {e}")
            return None
            
    async def get_all_cameras(self) -> List[CameraConfig]:
        """Get all camera configurations"""
        try:
            response = await self._make_request('GET', '/api/cameras')
            
            cameras = []
            for camera_data in response.get('cameras', []):
                cameras.append(CameraConfig(**camera_data))
                
            return cameras
            
        except Exception as e:
            logger.error(f"Failed to get cameras: {e}")
            return []
            
    async def update_camera_ai_status(
        self, 
        camera_id: str, 
        status: CameraStatus,
        additional_info: Optional[Dict] = None
    ) -> bool:
        """Update camera AI processing status in WebAPI"""
        try:
            status_data = {
                'cameraId': camera_id,
                'aiStatus': status.value,
                'timestamp': datetime.now().isoformat(),
                'serviceInfo': {
                    'serviceName': 'AIService',
                    'version': '1.0.0',
                    'instanceId': settings.INSTANCE_ID
                }
            }
            
            if additional_info:
                status_data['additionalInfo'] = additional_info
                
            await self._make_request(
                'PUT',
                f'/api/cameras/{camera_id}/ai-status',
                data=status_data
            )
            
            logger.debug(f"Updated AI status for camera {camera_id}: {status.value}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update AI status for {camera_id}: {e}")
            return False
            
    # ================================
    # Detection Results APIs
    # ================================
    
    async def send_detection_results(
        self, 
        camera_id: str, 
        results: List[Detection],
        frame_info: Optional[Dict] = None
    ) -> bool:
        """Send AI detection results to WebAPI"""
        try:
            detection_data = {
                'cameraId': camera_id,
                'timestamp': datetime.now().isoformat(),
                'detections': [detection.to_dict() for detection in results],
                'totalDetections': len(results),
                'processingInfo': {
                    'serviceId': settings.INSTANCE_ID,
                    'modelVersion': frame_info.get('model_version') if frame_info else None,
                    'processingTimeMs': frame_info.get('processing_time_ms') if frame_info else None,
                    'confidence': frame_info.get('avg_confidence') if frame_info else None
                }
            }
            
            if frame_info:
                detection_data['frameInfo'] = {
                    'frameId': frame_info.get('frame_id'),
                    'width': frame_info.get('width'),
                    'height': frame_info.get('height'),
                    'format': frame_info.get('format', 'jpeg')
                }
                
            await self._make_request(
                'POST',
                '/api/detections/results',
                data=detection_data
            )
            
            logger.debug(f"Sent {len(results)} detection results for camera {camera_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send detection results for {camera_id}: {e}")
            return False
            
    async def send_alert(
        self,
        camera_id: str,
        alert_type: str,
        severity: str,
        message: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """Send alert to WebAPI"""
        try:
            alert_data = {
                'cameraId': camera_id,
                'alertType': alert_type,
                'severity': severity,
                'message': message,
                'timestamp': datetime.now().isoformat(),
                'source': 'AIService',
                'metadata': metadata or {}
            }
            
            await self._make_request(
                'POST',
                '/api/alerts',
                data=alert_data
            )
            
            logger.info(f"Sent alert for camera {camera_id}: {alert_type} - {message}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert for {camera_id}: {e}")
            return False
            
    # ================================
    # System Status APIs  
    # ================================
    
    async def register_ai_service(self) -> bool:
        """Register this AI service instance with WebAPI"""
        try:
            service_data = {
                'serviceId': settings.INSTANCE_ID,
                'serviceName': 'AIService',
                'version': '1.0.0',
                'status': AIServiceStatus.STARTING.value,
                'capabilities': {
                    'supportedModels': ['YOLO', 'PersonDetection', 'FaceRecognition'],
                    'maxConcurrentStreams': settings.MAX_CONCURRENT_STREAMS,
                    'processingFormats': ['RTSP', 'HTTP'],
                    'outputFormats': ['JSON', 'XML']
                },
                'healthEndpoint': f"http://{settings.API_HOST}:{settings.API_PORT}/health",
                'startedAt': datetime.now().isoformat()
            }
            
            await self._make_request(
                'POST',
                '/api/services/ai-service/register',
                data=service_data
            )
            
            logger.info(f"AI Service {settings.INSTANCE_ID} registered successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register AI service: {e}")
            return False
            
    async def update_service_status(self, status: AIServiceStatus, metrics: Optional[Dict] = None) -> bool:
        """Update AI service status and metrics"""
        try:
            status_data = {
                'serviceId': settings.INSTANCE_ID,
                'status': status.value,
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics or {}
            }
            
            await self._make_request(
                'PUT',
                f'/api/services/ai-service/{settings.INSTANCE_ID}/status',
                data=status_data
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update service status: {e}")
            return False
            
    async def heartbeat(self) -> bool:
        """Send heartbeat to WebAPI"""
        try:
            heartbeat_data = {
                'serviceId': settings.INSTANCE_ID,
                'timestamp': datetime.now().isoformat(),
                'status': 'healthy'
            }
            
            await self._make_request(
                'POST',
                '/api/services/ai-service/heartbeat',
                data=heartbeat_data
            )
            
            return True
            
        except Exception as e:
            logger.debug(f"Heartbeat failed: {e}")  # Debug level since this is frequent
            return False
            
    # ================================
    # Configuration APIs
    # ================================
    
    async def get_ai_config(self, camera_id: Optional[str] = None) -> Dict[str, Any]:
        """Get AI configuration from WebAPI"""
        try:
            endpoint = '/api/config/ai'
            if camera_id:
                endpoint += f'?cameraId={camera_id}'
                
            response = await self._make_request('GET', endpoint)
            return response
            
        except Exception as e:
            logger.error(f"Failed to get AI config: {e}")
            return {}
            
    async def update_processing_metrics(
        self, 
        camera_id: str, 
        metrics: Dict[str, Any]
    ) -> bool:
        """Update processing metrics for a camera"""
        try:
            metrics_data = {
                'cameraId': camera_id,
                'serviceId': settings.INSTANCE_ID,
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics
            }
            
            await self._make_request(
                'POST',
                '/api/metrics/processing',
                data=metrics_data
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update processing metrics: {e}")
            return False