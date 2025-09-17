# /home/haicoi/SmartCamera.AIServiceDemo/infrastructure/external/webapi_client.py

import httpx
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from shared.decorators.retry import retry
from shared.decorators.error_handling import handle_errors
from app.settings import settings

logger = logging.getLogger(__name__)


def format_timestamp(ts: Any) -> str:
    """Format timestamp to proper ISO 8601 format for .NET Core API"""
    try:
        if isinstance(ts, (int, float)):
            # Convert milliseconds to seconds if needed
            if ts > 1e10:  # If timestamp is in milliseconds
                ts = ts / 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        elif isinstance(ts, str):
            # Try to parse string timestamp
            try:
                # Handle milliseconds timestamp as string
                if ts.isdigit() and len(ts) > 10:
                    ts_num = int(ts) / 1000 if len(ts) == 13 else int(ts)
                    dt = datetime.fromtimestamp(ts_num, tz=timezone.utc)
                else:
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except:
                dt = datetime.now(timezone.utc)
        elif isinstance(ts, datetime):
            dt = ts.astimezone(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        
        # Return ISO format that .NET Core can parse
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'  # Remove last 3 microseconds digits
        
    except Exception as e:
        logger.warning(f"⚠️ Error formatting timestamp {ts}: {e}, using current time")
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'


class WebApiClient:
    def __init__(self, base_url: str = None, api_key: str = None, timeout: int = 30):
        self.base_url = base_url or settings.WEBAPI_BASE_URL
        self.api_key = api_key or settings.WEBAPI_API_KEY
        self.timeout = timeout or settings.WEBAPI_TIMEOUT

        # HTTP client config
        self.client_config = {
            "timeout": httpx.Timeout(self.timeout),
            "headers": {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "SmartCamera-AIService/1.0",
            },
            "verify": False,  # 👈 tạm thời bỏ verify SSL
        }

        # Stats
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.last_request_time: Optional[datetime] = None

    # -----------------------------
    # Live detection (khớp AIResultDto)
    # -----------------------------
    @handle_errors(default_return=False)
    @retry(max_attempts=3, delay=1.0)
    async def send_live_detection(self, detection_result: Dict[str, Any]) -> bool:
        """
        Gửi kết quả AI detection trực tiếp lên WebAPI (ResultsController) cho real-time display
        """
        endpoint = f"{self.base_url}/api/Results/ai-detection"

        required_fields = ["cameraId", "timestamp", "detections"]
        for field in required_fields:
            if field not in detection_result:
                logger.error(f"❌ Missing required field: {field}")
                return False

        try:
            # Prepare detections with proper format
            detections = detection_result.get("detections", [])
            
            # Create proper result object instead of empty dict
            result_data = {
                "success": True,
                "message": f"Processed {len(detections)} detections",
                "processedAt": format_timestamp(datetime.now(timezone.utc)),
                "metadata": {
                    "version": "1.0",
                    "source": "ai-service"
                }
            }
            
            # If no detections, still send valid result
            if len(detections) == 0:
                result_data["message"] = "No detections found"

            live_payload = {
                "cameraId": str(detection_result["cameraId"]),
                "workerId": detection_result.get("workerId", "python-worker"),
                "timestamp": format_timestamp(detection_result["timestamp"]),
                "processingTimeMs": float(detection_result.get("processingTimeMs", 0.0)),
                "detectionCount": len(detections),
                "detections": detections,
                "result": result_data  # 🔥 FIX: Provide proper result object instead of {}
            }

            logger.debug(f"📤 Payload gửi đi: {live_payload}")

            self.request_count += 1
            self.last_request_time = datetime.utcnow()

            async with httpx.AsyncClient(**self.client_config) as client:
                response = await client.post(endpoint, json=live_payload)

            if response.status_code == 200:
                self.success_count += 1
                logger.debug(
                    f"✅ Live detection sent - Camera: {live_payload['cameraId']}"
                )
                return True
            else:
                self.error_count += 1
                logger.warning(
                    f"⚠️ WebAPI returned {response.status_code}: {response.text}"
                )
                return False

        except httpx.TimeoutException:
            self.error_count += 1
            logger.error(
                f"⏰ Timeout sending live detection - Camera: {detection_result['cameraId']}"
            )
            return False

        except httpx.ConnectError:
            self.error_count += 1
            logger.error(f"🔌 Connection error to WebAPI: {endpoint}")
            return False

        except Exception as e:
            self.error_count += 1
            logger.error(f"💥 Unexpected error sending live detection: {e}")
            return False

    # -----------------------------
    # Alerts
    # -----------------------------
    @retry(max_attempts=2, delay=0.5)
    async def send_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Gửi cảnh báo khẩn cấp (high priority)"""
        endpoint = f"{self.base_url}/api/ai/alert"

        try:
            payload = {
                **alert_data,
                "sentAt": format_timestamp(datetime.now(timezone.utc)),
                "priority": "high",
            }

            async with httpx.AsyncClient(**self.client_config) as client:
                response = await client.post(endpoint, json=payload)

            if response.status_code == 200:
                logger.info(
                    f"🚨 Alert sent - Type: {alert_data.get('type')} - Camera: {alert_data.get('cameraId')}"
                )
                return True
            else:
                logger.error(f"❌ Failed to send alert: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"💥 Error sending alert: {e}")
            return False

    # -----------------------------
    # Camera status
    # -----------------------------
    async def update_camera_ai_status(
        self, camera_id: str, status: Dict[str, Any]
    ) -> bool:
        """Cập nhật trạng thái AI processing của camera"""
        endpoint = f"{self.base_url}/api/cameras/{camera_id}/ai-status"

        try:
            # Ensure timestamp format
            if "lastUpdated" in status:
                status["lastUpdated"] = format_timestamp(status["lastUpdated"])

            async with httpx.AsyncClient(**self.client_config) as client:
                response = await client.patch(endpoint, json=status)

            if response.status_code == 200:
                logger.debug(f"📊 Camera AI status updated - {camera_id}")
                return True
            else:
                logger.warning(
                    f"⚠️ Failed to update camera status: {response.status_code}"
                )
                return False

        except Exception as e:
            logger.error(f"💥 Error updating camera status: {e}")
            return False

    # -----------------------------
    # Camera config
    # -----------------------------
    async def get_camera_configuration(self, camera_id: str) -> Optional[Dict[str, Any]]:
        """Lấy cấu hình camera từ WebAPI"""
        endpoint = f"{self.base_url}/api/cameras/{camera_id}/config"

        try:
            async with httpx.AsyncClient(**self.client_config) as client:
                response = await client.get(endpoint)

            if response.status_code == 200:
                config = response.json()
                logger.debug(f"📋 Camera config retrieved - {camera_id}")
                return config
            else:
                logger.warning(
                    f"⚠️ Failed to get camera config: {response.status_code}"
                )
                return None

        except Exception as e:
            logger.error(f"💥 Error getting camera config: {e}")
            return None

    # -----------------------------
    # Health check
    # -----------------------------
    async def health_check(self) -> bool:
        """Kiểm tra kết nối WebAPI"""
        endpoint = f"{self.base_url}/api/health"

        try:
            async with httpx.AsyncClient(**self.client_config) as client:
                response = await client.get(endpoint)
            return response.status_code == 200

        except Exception:
            return False

    # -----------------------------
    # Batch detections (nếu backend bổ sung sau)
    # -----------------------------
    async def batch_send_detections(self, detections: List[Dict[str, Any]]) -> int:
        """Gửi nhiều detection cùng lúc (cho high throughput)"""
        endpoint = f"{self.base_url}/api/ai/batch-detections"

        if not detections:
            return 0

        try:
            payload = {
                "detections": detections,
                "batchSize": len(detections),
                "sentAt": format_timestamp(datetime.now(timezone.utc)),
            }

            async with httpx.AsyncClient(**self.client_config) as client:
                response = await client.post(endpoint, json=payload)

            if response.status_code == 200:
                result = response.json()
                processed = result.get("processed", len(detections))
                logger.info(
                    f"📦 Batch sent - {processed}/{len(detections)} detections"
                )
                return processed
            else:
                logger.error(f"❌ Batch send failed: {response.status_code}")
                return 0

        except Exception as e:
            logger.error(f"💥 Error in batch send: {e}")
            return 0

    # -----------------------------
    # Stats
    # -----------------------------
    def get_stats(self) -> Dict[str, Any]:
        """Lấy thống kê client"""
        success_rate = (
            (self.success_count / self.request_count * 100)
            if self.request_count > 0
            else 0
        )

        return {
            "total_requests": self.request_count,
            "successful_requests": self.success_count,
            "failed_requests": self.error_count,
            "success_rate_percent": round(success_rate, 2),
            "last_request_time": self.last_request_time.isoformat()
            if self.last_request_time
            else None,
            "webapi_url": self.base_url,
        }

    # -----------------------------
    # Debug method - test payload
    # -----------------------------
    def create_test_payload(self, camera_id: str) -> Dict[str, Any]:
        """Tạo payload test để debug"""
        return {
            "cameraId": str(camera_id),
            "workerId": "python-worker",
            "timestamp": format_timestamp(datetime.now(timezone.utc)),
            "processingTimeMs": 15.5,
            "detectionCount": 2,
            "detections": [
                {
                    "id": "det_001",
                    "type": "person",
                    "confidence": 0.95,
                    "boundingBox": {"x": 100, "y": 50, "width": 200, "height": 300}
                },
                {
                    "id": "det_002", 
                    "type": "vehicle",
                    "confidence": 0.87,
                    "boundingBox": {"x": 300, "y": 100, "width": 150, "height": 100}
                }
            ],
            "result": {
                "success": True,
                "message": "Test detection completed",
                "processedAt": format_timestamp(datetime.now(timezone.utc)),
                "metadata": {
                    "version": "1.0",
                    "source": "ai-service-test"
                }
            }
        }