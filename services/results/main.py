# services/results/main.py
import asyncio
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
import argparse

from infrastructure.messaging.kafka.consumer import KafkaConsumer
from infrastructure.external.webapi_client import WebApiClient
from core.models.result_models import AIResult, Detection
from shared.config.logging_config import setup_logging
from .processor import ResultProcessor
from .alert_engine import AlertEngine
from .enricher import ResultEnricher
from .deduplicator import ResultDeduplicator

logger = logging.getLogger(__name__)

class ResultsProcessorService:
    def __init__(self, live_mode: bool = False):
        self.live_mode = live_mode
        # Fix: Truyền config dict thay vì list topics
        self.kafka_consumer = KafkaConsumer()
        self.topics = ['ai_results']  # Topics sẽ subscribe riêng
        self.webapi_client = WebApiClient()
        
        # Components
        self.processor = ResultProcessor()
        self.alert_engine = AlertEngine()
        self.enricher = ResultEnricher()
        self.deduplicator = ResultDeduplicator()
        
        # Live mode stats
        self.processed_count = 0
        self.last_processed_time = datetime.utcnow()
        
    async def start(self):
        """Khởi động Results Processor"""
        logger.info(f"🚀 Starting Results Processor - Live Mode: {self.live_mode}")
        
        # Start Kafka consumer
        await self.kafka_consumer.start()
        
        # Subscribe to topics
        await self.kafka_consumer.subscribe(self.topics)
        
        # Start processing loop
        await self.process_results_loop()
    
    async def process_results_loop(self):
        """Main processing loop"""
        try:
            # Fix: Sử dụng consume() thay vì iterate trực tiếp
            async for message in self.kafka_consumer.consume():
                await self.process_single_result(message)
                
        except Exception as e:
            logger.error(f"❌ Error in processing loop: {e}")
            # Restart after delay
            await asyncio.sleep(5)
            await self.process_results_loop()
    
    async def process_single_result(self, message):
        """Xử lý một kết quả AI từ Kafka"""
        try:
            # Parse message từ Kafka (message.value đã được deserialize)
            result_data = message.value
            logger.debug(f"📨 Received result: {result_data.get('camera_id')} - {len(result_data.get('detections', []))} detections")
            
            # Step 1: Deduplication - loại bỏ detection trùng lặp
            deduplicated_result = await self.deduplicator.remove_duplicates(result_data)
            
            # Step 2: Enrichment - bổ sung thông tin
            enriched_result = await self.enricher.enrich_result(deduplicated_result)
            
            # Step 3: Alert generation - tạo cảnh báo
            alerts = await self.alert_engine.generate_alerts(enriched_result)
            
            # Step 4: Process final result
            processed_result = await self.processor.process_result(
                enriched_result, 
                alerts=alerts
            )
            
            if self.live_mode:
                # LIVE MODE: Gửi trực tiếp lên WebAPI
                await self.send_live_result(processed_result)
            else:
                # NORMAL MODE: Lưu vào ClickHouse/MinIO trước
                await self.save_to_storage(processed_result)
                await self.notify_webapi(processed_result)
            
            # Update stats
            self.processed_count += 1
            self.last_processed_time = datetime.utcnow()
            
            if self.processed_count % 10 == 0:
                logger.info(f"📊 Processed {self.processed_count} results")
                
        except Exception as e:
            logger.error(f"❌ Error processing result: {e}")
    
    async def send_live_result(self, result: Dict[str, Any]):
        """LIVE MODE: Gửi trực tiếp lên WebAPI cho real-time display"""
        try:
            # Format cho WebAPI
            live_payload = {
                'cameraId': result['camera_id'],
                'timestamp': result['timestamp'],
                'detections': result['detections'],
                'alerts': result.get('alerts', []),
                'frameId': result.get('frame_id'),
                'processingTime': result.get('processing_time_ms'),
                'confidence': result.get('avg_confidence'),
                'metadata': {
                    'detector_version': result.get('detector_version'),
                    'model_name': result.get('model_name'),
                    'fps': result.get('fps')
                }
            }
            
            # Gửi lên WebAPI endpoint cho live detection
            success = await self.webapi_client.send_live_detection(live_payload)
            
            if success:
                logger.debug(f"✅ Live result sent - Camera: {result['camera_id']}")
            else:
                logger.warning(f"⚠️ Failed to send live result - Camera: {result['camera_id']}")
                
        except Exception as e:
            logger.error(f"❌ Error sending live result: {e}")
    
    async def save_to_storage(self, result: Dict[str, Any]):
        """NORMAL MODE: Lưu vào ClickHouse + MinIO"""
        # TODO: Implement storage logic
        pass
    
    async def notify_webapi(self, result: Dict[str, Any]):
        """NORMAL MODE: Thông báo WebAPI sau khi lưu storage"""
        # TODO: Implement notification logic
        pass
    
    async def get_stats(self) -> Dict[str, Any]:
        """Lấy thống kê xử lý"""
        return {
            'processed_count': self.processed_count,
            'last_processed_time': self.last_processed_time.isoformat(),
            'mode': 'live' if self.live_mode else 'normal',
            'status': 'running'
        }

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Results Processor Service')
    parser.add_argument(
        '--live-mode', 
        action='store_true',
        help='Skip storage and send results directly to WebAPI for real-time display'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(level=args.log_level)
    
    # Create and start service
    service = ResultsProcessorService(live_mode=args.live_mode)
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("🛑 Results Processor stopped by user")
    except Exception as e:
        logger.error(f"💥 Results Processor crashed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())