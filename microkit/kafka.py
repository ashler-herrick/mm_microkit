from typing import Optional, AsyncIterator, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from .config import settings

# ————— Async Consumer ——————————————————————————————————————

_async_consumers: Dict[str, AIOKafkaConsumer] = {}


def get_async_consumer(group: str) -> AIOKafkaConsumer:
    """
    Returns the singleton AIOKafkaConsumer.
    Call init_async_consumer() on startup before using.
    """
    _async_consumer = _async_consumers.get(group)
    if not _async_consumer:
        raise RuntimeError(
            f"Async consumer not initialized for group {group}; call init_async_consumer(group)"
        )
    return _async_consumer


async def init_async_consumer(group: str, topic: Optional[str]) -> None:
    """
    Create & start the singleton AIOKafkaConsumer.
    """
    if not topic:
        topic = settings.kafka_api_raw_topic

    _async_consumer = _async_consumers.get(group)
    if _async_consumer is None:
        _async_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_servers.split(","),
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await _async_consumer.start()
        _async_consumers[group] = _async_consumer


async def close_async_consumer(group: str) -> None:
    """
    Stop & cleanup the singleton AIOKafkaConsumer.
    """
    _async_consumer = _async_consumers[group]
    if _async_consumer is not None:
        await _async_consumer.stop()
        _async_consumer = None


async def iterate_messages() -> AsyncIterator[bytes]:
    """
    Async generator over raw message bytes.
    """
    consumer = get_async_consumer()
    try:
        async for msg in consumer:
            yield msg.value
    finally:
        # note: we do not auto-stop here; use close_async_consumer()
        return


# ————— Async Producer ——————————————————————————————————————

_async_producer: Optional[AIOKafkaProducer] = None


def get_async_producer() -> AIOKafkaProducer:
    """
    Returns the singleton AIOKafkaProducer.
    Call init_async_producer() on startup before using.
    """
    global _async_producer
    if _async_producer is None:
        raise RuntimeError("Async producer not initialized; call init_async_producer()")
    return _async_producer


async def init_async_producer() -> None:
    """
    Create & start the singleton AIOKafkaProducer.
    """
    global _async_producer
    if _async_producer is None:
        _async_producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_servers.split(","),
            value_serializer=lambda v: v,  # expect bytes
        )
        await _async_producer.start()


async def close_async_producer() -> None:
    """
    Stop & cleanup the singleton AIOKafkaProducer.
    """
    global _async_producer
    if _async_producer is not None:
        await _async_producer.stop()
        _async_producer = None


def _get_avail_consumer_groups():
    return _async_consumers.keys()
