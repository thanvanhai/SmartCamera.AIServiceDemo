import asyncio
import logging
import signal
import sys
import os
from typing import Dict, Any
import uvloop

from app.settings import settings
# Đảm bảo đường dẫn import này là chính xác cho cấu trúc dự án của bạn
from services.ai_worker.engine import InferenceEngine 
from infrastructure.monitoring.health_check import HealthChecker
from infrastructure.monitoring.metrics import setup_prometheus_metrics

def setup_logging(worker_id: str, level: int = logging.INFO):
    """
    Cấu hình logging cho AI Worker.
    Tự động tạo thư mục log nếu chưa tồn tại.
    """
    log_dir = getattr(settings.ai_worker, 'log_dir', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, f'ai_worker_{worker_id}.log')
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file_path, mode='a')
        ]
    )
    logging.info(f"📝 Logging configured. Log file at: {log_file_path}")

class AIWorkerService:
    """Main AI Worker Service - Simple & Clean"""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        
        # SỬA LỖI TRIỆT ĐỂ: Chuyển đổi object settings thành dictionary
        # để đáp ứng yêu cầu của InferenceEngine (config: Dict[str, Any]).
        # Giả định settings.ai_worker là một Pydantic model nên có hàm .dict().
        try:
            config_dict = settings.ai_worker.dict()
        except AttributeError:
            # Fallback nếu không phải Pydantic model, dùng vars()
            config_dict = vars(settings.ai_worker)

        self.logger = logging.getLogger(f"ai_worker_{worker_id}")
        
        # Core components
        # Truyền config_dict (dictionary) vào InferenceEngine
        self.inference_engine = InferenceEngine(worker_id, config_dict)
        self.health_checker = HealthChecker(worker_id)
        
        # Service state
        self.is_running = False
        self._shutdown_event = asyncio.Event()
        
    async def start(self):
        """Start AI Worker Service"""
        self.logger.info(f"🚀 Starting AI Worker {self.worker_id}")
        
        try:
            self._setup_signal_handlers()
            setup_prometheus_metrics(f"ai_worker_{self.worker_id}")
            await self.health_checker.start()
            await self.inference_engine.start()
            
            self.is_running = True
            self.logger.info(f"✅ AI Worker {self.worker_id} started successfully")
            
            await self._shutdown_event.wait()
            
        except Exception as e:
            self.logger.exception(f"❌ Failed to start: {e}")
            await self.shutdown()
            raise
    
    async def shutdown(self):
        """Graceful shutdown"""
        if not self.is_running:
            return
            
        self.logger.info(f"🛑 Shutting down AI Worker {self.worker_id}")
        self.is_running = False
        
        await self.inference_engine.stop()
        await self.health_checker.stop()
        
        self._shutdown_event.set()
        self.logger.info("✅ Shutdown completed")
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        loop = asyncio.get_running_loop()
        
        def handle_shutdown(signum):
            self.logger.info(f"Received signal {signum}")
            asyncio.create_task(self.shutdown())
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: handle_shutdown(s))

async def main():
    """Main entry point"""
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker_1"
    log_level = logging.DEBUG if "--debug" in sys.argv else logging.INFO
    
    setup_logging(worker_id, log_level)
    logger = logging.getLogger(__name__)
    
    try:
        uvloop.install()
        logger.info("✅ uvloop installed")
    except Exception as e:
        logger.warning(f"⚠️ Could not install uvloop: {e}")
    
    logger.info(f"🎬 Starting AI Worker {worker_id}")
    
    service = AIWorkerService(worker_id)
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("👋 Interrupted by user")
    except Exception:
        logger.exception("💥 Fatal error in service execution")
        sys.exit(1)
    finally:
        logger.info("🏁 AI Worker completed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 AI Worker interrupted")
    except Exception as e:
        print(f"💥 Fatal error during initial setup: {e}")
        sys.exit(1)

