import cv2
import logging

logger = logging.getLogger(__name__)

class RTSPHandler:
    def __init__(self, rtsp_url: str):
        self.rtsp_url = rtsp_url
        self.capture = None

    def open(self) -> bool:
        self.capture = cv2.VideoCapture(self.rtsp_url)
        if not self.capture.isOpened():
            logger.error("Failed to open RTSP stream: %s", self.rtsp_url)
            return False
        logger.info("RTSP stream opened: %s", self.rtsp_url)
        return True

    def read_frame(self):
        if self.capture is None:
            return None
        ret, frame = self.capture.read()
        return frame if ret else None

    def close(self):
        if self.capture:
            self.capture.release()
            logger.info("RTSP stream closed: %s", self.rtsp_url)
