import json
import logging
import asyncio
from typing import Callable, Any, Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from backend.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

class KafkaClient:
    def __init__(self):
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        self._consumers = []

    async def start_producer(self):
        if not self.producer:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            logger.info("Kafka Producer started.")

    async def stop_producer(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped.")

    async def send_message(self, topic: str, message: dict):
        if not self.producer:
            await self.start_producer()
        await self.producer.send_and_wait(topic, value=message)

    async def start_consumer(self, topic: str, group_id: str, callback: Callable[[dict], Any]):
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await consumer.start()
        self._consumers.append(consumer)
        logger.info(f"Kafka Consumer started for topic {topic}.")

        async def consume():
            try:
                async for msg in consumer:
                    await callback(msg.value)
            except asyncio.CancelledError:
                pass
            finally:
                await consumer.stop()

        asyncio.create_task(consume())

kafka_client = KafkaClient()
