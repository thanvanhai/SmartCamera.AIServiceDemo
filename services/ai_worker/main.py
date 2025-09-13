# services/ai_worker/main.py
import asyncio
import logging
import signal
import sys
from typing import Dict, Any
import uvloop

from app.settings import settings
from services.ai_worker.engine import InferenceEngine  # Updated import path
from infrastructure.monitoring.health_check import HealthChecker
from infrastructure.monitoring.metrics import ModelMetrics, setup_prometheus_metrics, update_prometheus_metrics

class AIWorkerService:
    """Main AI Worker Service - Entry point cho AI Worker"""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.config = settings.ai_worker  # Load cấu hình từ settings
        self.logger = logging.getLogger(f"ai_worker_service_{worker_id}")
        
        # Core components - Updated to use refactored engine
        self.inference_engine = InferenceEngine(worker_id, self.config)
        self.health_checker = HealthChecker(worker_id)
        
        # Service state
        self.is_running = False
        self._shutdown_event = asyncio.Event()
        
        # Metrics tracking
        self._metrics_task = None
        
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
            
            # Register inference engine for health checking
            self.health_checker.register_component(
                "inference_engine", 
                self.inference_engine.health_check
            )
            
            # Start inference engine
            await self.inference_engine.start()
            
            # Start metrics update task
            self._metrics_task = asyncio.create_task(
                self._metrics_update_loop(),
                name="metrics_updater"
            )
            
            self.is_running = True
            self.logger.info(f"✅ AI Worker {self.worker_id} started successfully")
            
            # Log performance info in test mode
            if self.config.get('test_mode', False):
                self.logger.info("🧪 Running in test mode with enhanced logging")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start AI Worker {self.worker_id}: {e}", exc_info=True)
            await self.shutdown()  # Ensure cleanup on startup failure
            raise
    
    async def shutdown(self):
        """Graceful shutdown"""
        if not self.is_running:
            return  # Already shutting down
            
        self.logger.info(f"🛑 Shutting down AI Worker {self.worker_id}")
        self.is_running = False
        
        # Cancel metrics task
        if self._metrics_task and not self._metrics_task.done():
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
        
        # Stop components in reverse order
        try:
            await self.inference_engine.stop()
            self.logger.info("✅ Inference engine stopped")
        except Exception as e:
            self.logger.error(f"Error stopping inference engine: {e}")
        
        try:
            await self.health_checker.stop()
            self.logger.info("✅ Health checker stopped")
        except Exception as e:
            self.logger.error(f"Error stopping health checker: {e}")
        
        self._shutdown_event.set()
        self.logger.info(f"✅ AI Worker {self.worker_id} shutdown completed")
    
    async def _metrics_update_loop(self):
        """Periodically update Prometheus metrics"""
        self.logger.info("📊 Starting metrics update loop")
        
        while self.is_running:
            try:
                # Get performance stats from inference engine
                if hasattr(self.inference_engine, 'get_performance_stats'):
                    stats = self.inference_engine.get_performance_stats()
                    
                    # Update Prometheus metrics
                    if stats:
                        update_prometheus_metrics(f"ai_worker_{self.worker_id}", {
                            'current_fps': stats.get('current_fps', 0),
                            'processed_frames': stats.get('processed_frames', 0),
                            'memory_usage_mb': stats.get('memory_usage_mb', 0),
                            'gpu_utilization': stats.get('gpu_utilization', 0),
                            'job_queue_size': stats.get('job_queue_size', 0),
                            'result_queue_size': stats.get('result_queue_size', 0),
                            'uptime_seconds': stats.get('uptime_seconds', 0)
                        })
                
                # Update every 10 seconds
                await asyncio.sleep(10)
                
            except asyncio.CancelledError:
                self.logger.info("📊 Metrics update loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error updating metrics: {e}")
                await asyncio.sleep(10)  # Continue despite errors
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        try:
            loop = asyncio.get_running_loop()
            
            def signal_handler(signum):
                self.logger.info(f"Received signal {signum}, initiating shutdown...")
                asyncio.create_task(self.shutdown())
            
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
                
            self.logger.info("✅ Signal handlers setup completed")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not setup signal handlers: {e}")
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current service status (useful for monitoring)"""
        try:
            # Get health check results
            health = await self.inference_engine.health_check()
            
            # Get performance stats
            performance = {}
            if hasattr(self.inference_engine, 'get_performance_stats'):
                performance = self.inference_engine.get_performance_stats()
            
            return {
                'worker_id': self.worker_id,
                'is_running': self.is_running,
                'health': health,
                'performance': performance,
                'config': {
                    'test_mode': self.config.get('test_mode', False),
                    'enable_batch_processing': self.config.get('enable_batch_processing', True),
                    'max_threads': self.config.get('max_threads', 4),
                    'queue_size': self.config.get('queue_size', 200)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting service status: {e}")
            return {
                'worker_id': self.worker_id,
                'is_running': self.is_running,
                'error': str(e)
            }


async def main():
    """Main entry point"""
    # Sử dụng uvloop cho performance tốt hơn
    try:
        uvloop.install()
        logging.info("✅ uvloop installed for better performance")
    except Exception as e:
        logging.warning(f"⚠️ Could not install uvloop: {e}")
    
    # Get worker ID từ args hoặc env
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker_1"
    
    # Setup logging
    log_level = logging.DEBUG if len(sys.argv) > 2 and sys.argv[2] == "--debug" else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'logs/ai_worker_{worker_id}.log', mode='a')
        ]
    )
    
    logger = logging.getLogger("ai_worker_main")
    logger.info(f"🎬 Starting AI Worker {worker_id} with log level {log_level}")
    
    # Start service
    service = AIWorkerService(worker_id)
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("👋 Received KeyboardInterrupt")
    except Exception as e:
        logger.error(f"💥 Unhandled error in main: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("🏁 AI Worker main completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 AI Worker interrupted by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)