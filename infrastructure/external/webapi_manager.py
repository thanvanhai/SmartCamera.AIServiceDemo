# ================================
# infrastructure/external/webapi_manager.py  
# ================================
import asyncio
from typing import Optional
from contextlib import asynccontextmanager

from .webapi_client import WebAPIClient, WebAPIConfig, AIServiceStatus
from app.settings import settings
import logging

logger = logging.getLogger(__name__)

class WebAPIManager:
    """
    Manager for WebAPI client with connection pooling and health monitoring
    """
    
    def __init__(self):
        self.client: Optional[WebAPIClient] = None
        self.config = WebAPIConfig(
            base_url=settings.WEBAPI_BASE_URL,
            api_key=settings.WEBAPI_API_KEY,
            timeout=settings.WEBAPI_TIMEOUT,
            max_retries=settings.WEBAPI_MAX_RETRIES
        )
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._is_running = False
        
    async def start(self):
        """Start WebAPI manager and client"""
        self.client = WebAPIClient(self.config)
        await self.client.start()
        
        # Register service
        await self.client.register_ai_service()
        await self.client.update_service_status(AIServiceStatus.RUNNING)
        
        # Start heartbeat
        self._is_running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        logger.info("WebAPI Manager started")
        
    async def stop(self):
        """Stop WebAPI manager"""
        self._is_running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
                
        if self.client:
            # Update status before closing
            await self.client.update_service_status(AIServiceStatus.STOPPING)
            await self.client.close()
            
        logger.info("WebAPI Manager stopped")
        
    async def _heartbeat_loop(self):
        """Send periodic heartbeats to WebAPI"""
        while self._is_running:
            try:
                if self.client:
                    await self.client.heartbeat()
                await asyncio.sleep(settings.WEBAPI_HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5)  # Wait before retry
                
    @asynccontextmanager
    async def get_client(self):
        """Get WebAPI client with context management"""
        if not self.client:
            raise RuntimeError("WebAPI Manager not started")
            
        yield self.client

# ================================
# Global WebAPI manager instance
# ================================
webapi_manager = WebAPIManager()

# Convenience functions
async def get_webapi_client() -> WebAPIClient:
    """Get the global WebAPI client"""
    if not webapi_manager.client:
        raise RuntimeError("WebAPI Manager not initialized")
    return webapi_manager.client
