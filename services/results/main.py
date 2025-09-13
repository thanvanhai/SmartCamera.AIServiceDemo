# services/results/main.py

import asyncio
import logging
import os
from .processor import ResultsProcessor
from infrastructure.messaging.kafka.consumer import KafkaConsumer
# THAY ĐỔI 1: Import thêm lớp WebAPIConfig
from infrastructure.external.webapi_client import WebAPIClient, WebAPIConfig

# --- Cấu hình tập trung ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092")
RESULTS_TOPIC = "smartcamera.ai_results"
CONSUMER_GROUP_ID = "ai_results_processor_group"
WEBAPI_BASE_URL = os.getenv('WEBAPI_BASE_URL', "http://webapi.local")
WEBAPI_API_KEY = os.getenv('WEBAPI_API_KEY', "YOUR_API_KEY")

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    """
    Hàm chính để khởi tạo và chạy service.
    """
    logger.info("🚀 Bắt đầu khởi tạo service xử lý kết quả AI...")

    # Khởi tạo Kafka Consumer
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset='earliest'
    )

    # --- THAY ĐỔI 2: Sửa lại cách khởi tạo WebAPIClient ---

    # 2a. Tạo đối tượng cấu hình WebAPIConfig trước
    webapi_config = WebAPIConfig(
        base_url=WEBAPI_BASE_URL,
        api_key=WEBAPI_API_KEY,
        timeout=45 # Có thể tùy chỉnh các giá trị khác nếu muốn
    )

    # 2b. Truyền đối tượng config vào WebAPIClient
    webapi_client = WebAPIClient(config=webapi_config)
    
    # ----------------------------------------------------
    
    # Khởi tạo các thành phần khác
    cameras_ws_map = {}

    # Khởi tạo bộ xử lý chính (Processor)
    processor = ResultsProcessor(
        consumer=consumer,
        topic_to_consume=RESULTS_TOPIC,
        minio_client=None,
        webapi_client=webapi_client,
        cameras_ws_map=cameras_ws_map
    )

    try:
        logger.info(f"Bắt đầu lắng nghe và xử lý từ topic '{RESULTS_TOPIC}'...")
        # Sử dụng WebAPIClient như một context manager để tự động kết nối và ngắt kết nối
        async with webapi_client:
            await processor.run()

    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng trong quá trình chạy processor: {e}", exc_info=True)
    finally:
        logger.info("Dọn dẹp và đóng các kết nối...")
        await consumer.close()
        # webapi_client.close() sẽ được tự động gọi nhờ "async with"


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Chương trình đã dừng bởi người dùng (Ctrl+C).")