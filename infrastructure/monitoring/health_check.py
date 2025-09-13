import asyncio
import logging
import time
from typing import Dict, Any


class HealthChecker:
    """Theo dõi tình trạng health của AI Worker"""
    
    def __init__(self, worker_id: str, check_interval: int = 10):
        self.worker_id = worker_id
        self.check_interval = check_interval
        self.logger = logging.getLogger(f"health_checker_{worker_id}")
        
        self._is_running = False
        self._task: asyncio.Task = None
        
        # Health state
        self._last_check = 0.0
        self._status = "starting"
        self._details: Dict[str, Any] = {}

    async def start(self):
        """Start health checker loop"""
        if self._is_running:
            return

        self._is_running = True
        self.logger.info(f"🩺 HealthChecker started for {self.worker_id}")
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        """Stop health checker loop"""
        if not self._is_running:
            return

        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.logger.info(f"🩺 HealthChecker stopped for {self.worker_id}")

    async def _run(self):
        """Background loop cập nhật health status"""
        try:
            while self._is_running:
                await self._check_health()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            pass

    async def _check_health(self):
        """Thực hiện health check (giả lập, có thể mở rộng sau)"""
        self._last_check = time.time()
        
        # Ở đây có thể thêm check GPU/CPU/memory hay kết nối Kafka/Redis
        self._status = "healthy"
        self._details = {
            "timestamp": self._last_check,
            "uptime_seconds": int(time.time() - self._last_check),
            "worker_id": self.worker_id
        }

        self.logger.debug(f"Health check OK for {self.worker_id}")

    def get_status(self) -> Dict[str, Any]:
        """Lấy health status hiện tại"""
        return {
            "status": self._status,
            "last_check": self._last_check,
            "details": self._details
        }
