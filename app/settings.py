# ================================
# app/settings.py (WebAPI related settings)
# ================================
from pydantic_settings import BaseSettings
from typing import List
import uuid

class Settings(BaseSettings):
    # ... other settings ...
    
    # WebAPI Integration
    WEBAPI_BASE_URL: str = "http://localhost:5000"
    WEBAPI_API_KEY: str = "your-api-key-here"
    WEBAPI_TIMEOUT: int = 30
    WEBAPI_MAX_RETRIES: int = 3
    WEBAPI_HEARTBEAT_INTERVAL: int = 30  # seconds
    
    # AI Service Instance
    INSTANCE_ID: str = Field(default_factory=lambda: str(uuid.uuid4()))
    MAX_CONCURRENT_STREAMS: int = 10
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    
    class Config:
        env_file = ".env"
        case_sensitive = True

print("WebAPI Integration created successfully!")
print("\nKey components:")
print("1. WebAPIClient - Main client for communicating with .NET Core API")  
print("2. WebAPIManager - Connection management and health monitoring")
print("3. Authentication - JWT token handling and refresh")
print("4. Error handling - Comprehensive error handling and retries")
print("5. Camera management - Get configs, update status")
print("6. Detection results - Send AI results to backend")
print("7. Alerts - Send alerts and notifications")
print("8. Service registration - Register AI service with backend")
print("9. Health monitoring - Heartbeat and status updates")