# core/exceptions.py
from typing import Optional

class SmartCameraException(Exception):
    """Base exception for SmartCamera AI Service"""
    pass

class WebAPIError(SmartCameraException):
    """WebAPI related errors"""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

class AuthenticationError(WebAPIError):
    """Authentication related errors"""
    pass

class CameraConfigurationError(SmartCameraException):
    """Camera configuration related errors"""
    pass
