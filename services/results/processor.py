import asyncio
import cv2
import base64
import numpy as np
from datetime import datetime
import logging # Thêm logging để theo dõi

logger = logging.getLogger(__name__)

class ResultsProcessor:
    # THAY ĐỔI 1: Cập nhật hàm __init__ để nhận 'topic_to_consume'
    def __init__(self, consumer, topic_to_consume, minio_client, webapi_client, cameras_ws_map):
        self.consumer = consumer
        self.topic_to_consume = topic_to_consume # <-- Lưu lại tên topic
        self.minio_client = minio_client
        self.webapi_client = webapi_client
        self.cameras_ws_map = cameras_ws_map

    async def run(self):
        # THAY ĐỔI 2: Kết nối consumer trước khi bắt đầu vòng lặp
        await self.consumer.connect()
        logger.info(f"Processor bắt đầu lắng nghe topic: {self.topic_to_consume}")

        # THAY ĐỔI 3: Sử dụng tên topic đã lưu
        async for message in self.consumer.consume(self.topic_to_consume):
            try:
                result = message.value  # dict: camera_id, frame_path, detections, etc.
                camera_id = result.get("camera_id")

                if not camera_id:
                    logger.warning(f"Bỏ qua message không có camera_id: {result}")
                    continue

                logger.debug(f"Đang xử lý kết quả cho camera {camera_id}")

                # Logic xử lý của bạn (giữ nguyên)
                # Lưu ý: Code cũ của bạn có 'frame_bytes', nhưng producer gửi 'frame_path'.
                # Tạm thời tôi sẽ giả định bạn sẽ đọc file từ path đó.
                # Nếu bạn thực sự gửi frame_bytes trực tiếp qua Kafka, hãy bỏ comment phần dưới.

                # --- Ví dụ nếu bạn đọc file từ đường dẫn (giống publisher) ---
                # frame_path = result.get("frame_path")
                # frame = cv2.imread(f"./frames/{frame_path}") # Điều chỉnh đường dẫn gốc
                # if frame is None:
                #    logger.error(f"Không thể đọc frame tại: {frame_path}")
                #    continue

                # --- Logic cũ của bạn nếu bạn gửi 'frame_bytes' qua Kafka ---
                frame = self._decode_frame(result["frame_bytes"])
                frame_with_overlay = self._draw_overlay(frame, result.get("detections", []))
                snapshot_bytes = cv2.imencode(".jpg", frame_with_overlay)[1].tobytes()
                # -----------------------------------------------------------

                # Upload to MinIO (cần kiểm tra self.minio_client có tồn tại không)
                if self.minio_client:
                    snapshot_name = f"{camera_id}/{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.jpg"
                    self.minio_client.upload(snapshot_name, snapshot_bytes)

                # Send to WebAppDemo
                snapshot_b64 = base64.b64encode(snapshot_bytes).decode("utf-8")
                await self._send_to_webapp(camera_id, snapshot_b64, result.get("detections", []))

            except Exception as e:
                logger.error(f"Lỗi khi đang xử lý message: {e}", exc_info=True)


    def _decode_frame(self, frame_bytes_b64):
        # Frame thường được encode base64 khi gửi qua JSON
        frame_bytes = base64.b64decode(frame_bytes_b64)
        nparr = np.frombuffer(frame_bytes, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    def _draw_overlay(self, frame, detections):
        if frame is None or not detections:
            return frame
        # (Giữ nguyên logic vẽ của bạn)
        for det in detections:
            bbox = det.get("bbox")
            if not bbox or len(bbox) != 4: continue
            x1, y1, x2, y2 = map(int, bbox)
            label = det.get("label", "obj")
            conf = det.get("confidence", 0)
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(frame, f"{label}:{conf:.2f}", (x1, y1 - 10),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
        return frame

    async def _send_to_webapp(self, camera_id, snapshot_b64, detections):
        ws_list = self.cameras_ws_map.get(camera_id, [])
        if not ws_list:
            return

        message_to_send = {
            "camera_id": camera_id,
            "snapshot": snapshot_b64,
            "detections": detections
        }
        # Gửi đồng thời tới tất cả các client đang xem camera này
        tasks = [ws.send_json(message_to_send) for ws in ws_list]
        await asyncio.gather(*tasks, return_exceptions=True)