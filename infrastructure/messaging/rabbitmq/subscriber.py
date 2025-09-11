import asyncio
import aio_pika
import json
import logging
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)

class RabbitMQSubscriber:
    def __init__(self, amqp_url: str, exchange_name: str = "smartcamera"):
        self._url = amqp_url
        self._exchange_name = exchange_name
        self._connection = None
        self._channel = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=10)
        logger.info("Connected to RabbitMQ at %s", self._url)

    async def subscribe(
        self,
        routing_key: str,
        callback: Callable[[str, dict], Awaitable[None]],
        queue_name: str = ""
    ):
        """
        Subscribe vào exchange với routing_key
        Callback: async (routing_key: str, payload: dict)
        """
        exchange = await self._channel.declare_exchange(
            self._exchange_name, aio_pika.ExchangeType.TOPIC,
               durable=True  # Đặt true cho khớp với exchange đã tồn tại
        )
        queue = await self._channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        payload = json.loads(message.body.decode())
                        logger.info(f"📩 Received message: {payload}")  # 👈 thêm log debug
                        await callback(message.routing_key, payload)
                    except Exception as e:
                        logger.error("Error handling message: %s", e)
