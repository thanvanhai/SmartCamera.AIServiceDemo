import asyncio
import logging
from infrastructure.messaging.rabbitmq.subscriber import RabbitMQSubscriber
from services.ingest.rtsp_handler import RTSPHandler
from services.ingest.kafka_publisher import KafkaPublisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBIT_URL = "amqp://guest:guest@localhost/"
KAFKA_BROKERS = "localhost:9092"

# Giữ task ingest theo camera_id
active_ingests = {}

async def ingest_camera(camera_id: str, rtsp_url: str, publisher: KafkaPublisher):
    # Validate inputs
    if not camera_id or not rtsp_url:
        logger.error("❌ Invalid camera_id (%s) or rtsp_url (%s)", camera_id, rtsp_url)
        return
        
    handler = RTSPHandler(rtsp_url)

    if not handler.open():
        logger.error("❌ Cannot open RTSP stream for camera %s with URL: %s", camera_id, rtsp_url)
        return

    logger.info("✅ Start ingest for camera %s with URL: %s", camera_id, rtsp_url)

    try:
        consecutive_failures = 0
        max_failures = 10
        
        while True:
            frame = handler.read_frame()
            if frame is None:
                consecutive_failures += 1
                logger.warning("⚠️ Frame read failed for camera %s (failures: %d)", 
                             camera_id, consecutive_failures)
                
                if consecutive_failures >= max_failures:
                    logger.error("❌ Too many consecutive failures for camera %s, stopping ingest", camera_id)
                    break
                    
                await asyncio.sleep(1)
                continue
            
            # Reset failure counter on successful read
            consecutive_failures = 0
            
            await publisher.publish_frame(camera_id, frame)
            await asyncio.sleep(0.05)  # 20 FPS
            
    except asyncio.CancelledError:
        logger.info("🛑 Ingest stopped for camera %s", camera_id)
    except Exception as e:
        logger.error("❌ Unexpected error in ingest for camera %s: %s", camera_id, str(e))
    finally:
        handler.close()

async def handle_event(routing_key: str, message: dict, publisher: KafkaPublisher):
    # Validate message structure
    if not isinstance(message, dict):
        logger.error("Invalid message format: %s", message)
        return
    
    camera_id = message.get("id")
    rtsp_url = message.get("rtsp_url")
    
    # Convert camera_id to string and validate
    if camera_id is None:
        logger.error("Missing 'Id' in message: %s", message)
        return
    
    camera_id = str(camera_id)
    
    logger.info("📨 Received event %s for camera %s with URL: %s", 
                routing_key, camera_id, rtsp_url)

    if routing_key == "camera.registered":
        if not rtsp_url:
            logger.error("❌ Missing RTSP URL for camera registration: %s", camera_id)
            return
            
        if camera_id in active_ingests:
            logger.info("Camera %s already ingesting, skipping", camera_id)
            return

        task = asyncio.create_task(ingest_camera(camera_id, rtsp_url, publisher))
        active_ingests[camera_id] = task

    elif routing_key == "camera.deleted":
        task = active_ingests.pop(camera_id, None)
        if task:
            task.cancel()
            logger.info("🗑️ Cancelled ingest for deleted camera %s", camera_id)

    elif routing_key == "camera.updated":
        if not rtsp_url:
            logger.error("❌ Missing RTSP URL for camera update: %s", camera_id)
            return
            
        # restart ingest
        task = active_ingests.pop(camera_id, None)
        if task:
            task.cancel()
            logger.info("🔄 Restarting ingest for updated camera %s", camera_id)
            
        task = asyncio.create_task(ingest_camera(camera_id, rtsp_url, publisher))
        active_ingests[camera_id] = task

    else:
        logger.info("❓ Unhandled event %s: %s", routing_key, message)

async def main():
    try:
        subscriber = RabbitMQSubscriber(RABBIT_URL)
        await subscriber.connect()

        publisher = KafkaPublisher(KAFKA_BROKERS)
        await publisher.connect()

        async def callback(routing_key, msg):
            try:
                await handle_event(routing_key, msg, publisher)
            except Exception as e:
                logger.error("Error handling event %s: %s", routing_key, str(e))

        # Lắng nghe tất cả camera.* event
        await subscriber.subscribe("camera.*", callback)
        
    except Exception as e:
        logger.error("Fatal error in main: %s", str(e))
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
    except Exception as e:
        logger.error("Application crashed: %s", str(e))