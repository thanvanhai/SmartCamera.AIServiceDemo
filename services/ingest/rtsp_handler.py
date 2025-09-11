import cv2
import logging

logger = logging.getLogger(__name__)

class RTSPHandler:
    def __init__(self, rtsp_url: str):
        self.rtsp_url = rtsp_url
        self.capture = None

    def open(self) -> bool:
        # Kiểm tra rtsp_url trước khi mở
        if not self.rtsp_url or self.rtsp_url.strip() == "":
            logger.error("RTSP URL is empty or None")
            return False
            
        try:
            self.capture = cv2.VideoCapture(self.rtsp_url)
            # Thêm timeout và buffer size để tối ưu
            self.capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            
            if not self.capture.isOpened():
                logger.error("Failed to open RTSP stream: %s", self.rtsp_url)
                return False
                
            logger.info("RTSP stream opened: %s", self.rtsp_url)
            return True
        except Exception as e:
            logger.error("Exception while opening RTSP stream %s: %s", self.rtsp_url, str(e))
            return False

    def read_frame(self):
        if self.capture is None or not self.capture.isOpened():
            return None
        try:
            ret, frame = self.capture.read()
            return frame if ret else None
        except Exception as e:
            logger.error("Error reading frame: %s", str(e))
            return None

    def close(self):
        if self.capture:
            self.capture.release()
            logger.info("RTSP stream closed: %s", self.rtsp_url)