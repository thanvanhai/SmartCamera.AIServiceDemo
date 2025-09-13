# services/ai_worker/main.py
import asyncio
import logging
import signal
import sys
from typing import Dict, Any
import uvloop

from app.settings import settings
from services.ai_worker.inference_engine import InferenceEngine
from infrastructure.monitoring.health_check import HealthChecker
from infrastructure.monitoring.metrics import ModelMetrics, setup_prometheus_metrics, update_prometheus_metrics

class AIWorkerService:
    """Main AI Worker Service - Entry point cho AI Worker"""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.config = settings.ai_worker # Load cấu hình từ settings
        self.logger = logging.getLogger(f"ai_worker_service_{worker_id}")
        
        # Core components
        self.inference_engine = InferenceEngine(worker_id, self.config)
        self.health_checker = HealthChecker(worker_id)
        
        # Service state
        self.is_running = False
        self._shutdown_event = asyncio.Event()
        
    async def start(self):
        """Start AI Worker Service"""
        self.logger.info(f"🚀 Starting AI Worker Service {self.worker_id}")
        
        try:
            # Setup signal handlers
            self._setup_signal_handlers()
            
            # Setup monitoring
            setup_prometheus_metrics(f"ai_worker_{self.worker_id}")
            
            # Start health checker
            await self.health_checker.start()
            
            # Start inference engine
            await self.inference_engine.start()
            
            self.is_running = True
            self.logger.info(f"✅ AI Worker {self.worker_id} started successfully")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start AI Worker {self.worker_id}: {e}")
            raise
    
    async def shutdown(self):
        """Graceful shutdown"""
        self.logger.info(f"🛑 Shutting down AI Worker {self.worker_id}")
        
        self.is_running = False
        
        # Stop components
        await self.inference_engine.stop()
        await self.health_checker.stop()
        
        self._shutdown_event.set()
        self.logger.info(f"✅ AI Worker {self.worker_id} shutdown completed")
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        loop = asyncio.get_running_loop()
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))


async def main():
    """Main entry point"""
    # Sử dụng uvloop cho performance tốt hơn
    uvloop.install()
    
    # Get worker ID từ args hoặc env
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker_1"
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start service
    service = AIWorkerService(worker_id)
    await service.start()


if __name__ == "__main__":
    asyncio.run(main())