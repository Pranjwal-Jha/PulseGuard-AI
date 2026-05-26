"""Kafka stub implementation for local development without actual Kafka."""

from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
from abc import ABC, abstractmethod
import asyncio
import uuid
from backend.utils.logging import get_logger

logger = get_logger("kafka_stub")


@dataclass
class KafkaMessage:
    """Represents a Kafka message."""
    key: str
    value: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    offset: int = field(default=0)
    partition: int = field(default=0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "offset": self.offset,
            "partition": self.partition
        }


class KafkaTopic:
    """In-memory Kafka topic implementation."""
    
    def __init__(self, name: str, num_partitions: int = 3):
        """Initialize topic."""
        self.name = name
        self.num_partitions = num_partitions
        self.messages: List[KafkaMessage] = []
        self.subscribers: Dict[str, List[Callable]] = {}  # group_id -> list of callbacks
        self.offset_commits: Dict[str, int] = {}  # group_id -> last offset
        self.lock = asyncio.Lock()
    
    async def publish(self, key: str, value: Dict[str, Any]) -> int:
        """Publish message to topic."""
        async with self.lock:
            message = KafkaMessage(
                key=key,
                value=value,
                offset=len(self.messages),
                partition=hash(key) % self.num_partitions
            )
            self.messages.append(message)
            logger.debug(f"Published message to {self.name}: key={key}, offset={message.offset}")
            
            # Notify subscribers
            for consumer_group, callbacks in self.subscribers.items():
                for callback in callbacks:
                    try:
                        await callback(message)
                    except Exception as e:
                        logger.error(f"Error in subscriber callback: {e}")
            
            return message.offset
    
    async def subscribe(self, group_id: str, callback: Callable):
        """Subscribe to topic updates."""
        if group_id not in self.subscribers:
            self.subscribers[group_id] = []
        self.subscribers[group_id].append(callback)
        logger.debug(f"Subscriber {group_id} subscribed to {self.name}")
    
    async def get_messages(self, group_id: str, from_offset: int = 0) -> List[KafkaMessage]:
        """Get messages from offset (consumer group tracking)."""
        async with self.lock:
            offset = self.offset_commits.get(group_id, from_offset)
            messages = self.messages[offset:]
            if messages:
                self.offset_commits[group_id] = offset + len(messages)
            return messages
    
    async def commit_offset(self, group_id: str, offset: int):
        """Commit offset for consumer group."""
        self.offset_commits[group_id] = offset
    
    def get_stats(self) -> Dict[str, Any]:
        """Get topic statistics."""
        return {
            "name": self.name,
            "num_partitions": self.num_partitions,
            "message_count": len(self.messages),
            "subscriber_count": sum(len(v) for v in self.subscribers.values()),
            "consumer_groups": list(self.subscribers.keys())
        }


class KafkaProducer:
    """Kafka producer stub."""
    
    def __init__(self, broker: "KafkaBroker"):
        """Initialize producer."""
        self.broker = broker
        self.producer_id = str(uuid.uuid4())[:8]
    
    async def send(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        """Send message to topic."""
        return await self.broker.publish(topic, key, value)
    
    async def send_batch(self, topic: str, messages: List[tuple]) -> List[int]:
        """Send multiple messages."""
        offsets = []
        for key, value in messages:
            offset = await self.send(topic, key, value)
            offsets.append(offset)
        return offsets
    
    async def close(self):
        """Close producer."""
        logger.debug(f"Producer {self.producer_id} closed")


class KafkaConsumer:
    """Kafka consumer stub."""
    
    def __init__(self, broker: "KafkaBroker", topics: List[str], group_id: str):
        """Initialize consumer."""
        self.broker = broker
        self.topics = topics
        self.group_id = group_id
        self.consumer_id = str(uuid.uuid4())[:8]
        self.is_consuming = False
    
    async def consume(self, timeout_ms: int = 1000) -> Optional[KafkaMessage]:
        """Consume single message."""
        for topic in self.topics:
            messages = await self.broker.get_messages(topic, self.group_id)
            if messages:
                return messages[0]
        return None
    
    async def consume_batch(self, max_records: int = 100, timeout_ms: int = 1000) -> List[KafkaMessage]:
        """Consume batch of messages."""
        all_messages = []
        for topic in self.topics:
            messages = await self.broker.get_messages(topic, self.group_id, max_records - len(all_messages))
            all_messages.extend(messages)
            if len(all_messages) >= max_records:
                break
        return all_messages[:max_records]
    
    async def subscribe(self, callback: Callable):
        """Subscribe to topic updates with callback."""
        for topic in self.topics:
            await self.broker.subscribe(topic, self.group_id, callback)
    
    async def commit(self, topic: str, offset: int):
        """Commit offset."""
        await self.broker.commit_offset(topic, self.group_id, offset)
    
    async def close(self):
        """Close consumer."""
        self.is_consuming = False
        logger.debug(f"Consumer {self.consumer_id} (group={self.group_id}) closed")


class KafkaBroker:
    """Central Kafka broker managing topics and message distribution."""
    
    def __init__(self):
        """Initialize broker."""
        self.topics: Dict[str, KafkaTopic] = {}
        self.lock = asyncio.Lock()
    
    async def create_topic(self, name: str, num_partitions: int = 3) -> KafkaTopic:
        """Create a new topic."""
        async with self.lock:
            if name in self.topics:
                return self.topics[name]
            topic = KafkaTopic(name, num_partitions)
            self.topics[name] = topic
            logger.info(f"Topic created: {name} with {num_partitions} partitions")
            return topic
    
    async def get_topic(self, name: str) -> Optional[KafkaTopic]:
        """Get existing topic."""
        return self.topics.get(name)
    
    async def publish(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        """Publish message to topic."""
        t = await self.get_topic(topic)
        if not t:
            t = await self.create_topic(topic)
        return await t.publish(key, value)
    
    async def get_messages(self, topic: str, group_id: str, limit: int = 100) -> List[KafkaMessage]:
        """Get messages from topic."""
        t = await self.get_topic(topic)
        if not t:
            return []
        return await t.get_messages(group_id, 0)
    
    async def subscribe(self, topic: str, group_id: str, callback: Callable):
        """Subscribe to topic."""
        t = await self.get_topic(topic)
        if not t:
            t = await self.create_topic(topic)
        await t.subscribe(group_id, callback)
    
    async def commit_offset(self, topic: str, group_id: str, offset: int):
        """Commit offset."""
        t = await self.get_topic(topic)
        if t:
            await t.commit_offset(group_id, offset)
    
    def create_producer(self) -> KafkaProducer:
        """Create a producer."""
        return KafkaProducer(self)
    
    def create_consumer(self, topics: List[str], group_id: str) -> KafkaConsumer:
        """Create a consumer."""
        return KafkaConsumer(self, topics, group_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get broker statistics."""
        return {
            "topics": {name: topic.get_stats() for name, topic in self.topics.items()},
            "total_topics": len(self.topics),
            "total_messages": sum(len(topic.messages) for topic in self.topics.values())
        }


# Global broker instance
_broker: Optional[KafkaBroker] = None


def get_kafka_broker() -> KafkaBroker:
    """Get or create global Kafka broker instance."""
    global _broker
    if _broker is None:
        _broker = KafkaBroker()
    return _broker
