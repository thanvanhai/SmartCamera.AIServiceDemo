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
    handler = RTSPHandler(rtsp_url)

    if not handler.open():
        logger.error("❌ Cannot open RTSP stream for camera %s", camera_id)
        return

    logger.info("✅ Start ingest for camera %s", camera_id)

    try:
        while True:
            frame = handler.read_frame()
            if frame is None:
                logger.warning("⚠️ Frame read failed for camera %s", camera_id)
                await asyncio.sleep(1)
                continue

            await publisher.publish_frame(camera_id, frame)
            await asyncio.sleep(0.05)  # 20 FPS
    except asyncio.CancelledError:
        logger.info("🛑 Ingest stopped for camera %s", camera_id)
    finally:
        handler.close()

async def handle_event(routing_key: str, message: dict, publisher: KafkaPublisher):
    camera_id = str(message.get("Id"))
    rtsp_url = message.get("RtspUrl")

    if routing_key == "camera.registered":
        if camera_id in active_ingests:
            logger.info("Camera %s already ingesting, skipping", camera_id)
            return

        task = asyncio.create_task(ingest_camera(camera_id, rtsp_url, publisher))
        active_ingests[camera_id] = task

    elif routing_key == "camera.deleted":
        task = active_ingests.pop(camera_id, None)
        if task:
            task.cancel()

    elif routing_key == "camera.updated":
        # restart ingest
        task = active_ingests.pop(camera_id, None)
        if task:
            task.cancel()
        task = asyncio.create_task(ingest_camera(camera_id, rtsp_url, publisher))
        active_ingests[camera_id] = task

    else:
        logger.info("Unhandled event %s: %s", routing_key, message)

async def main():
    subscriber = RabbitMQSubscriber(RABBIT_URL)
    await subscriber.connect()

    publisher = KafkaPublisher(KAFKA_BROKERS)
    await publisher.connect()

    async def callback(routing_key, msg):
        await handle_event(routing_key, msg, publisher)

    await subscriber.start_consuming(callback)

if __name__ == "__main__":
    asyncio.run(main())
# Lệnh để chạy file này: python -m services.ingest.main