import json
import os
from aiokafka import AIOKafkaProducer

KAFKA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")

producer: AIOKafkaProducer | None = None

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

async def start_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        linger_ms=10,
        batch_size=32768,
        acks="all",
        retries=5,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        callback=delivery_report,
    )
    await producer.start()


async def publish_event(topic: str, event: dict, key: str | None = None):
    if not producer:
        return

    await producer.send(
        topic,
        value=event,
        key=key.encode("utf-8") if key else None,
    )


async def stop_producer():
    if producer:
        await producer.flush()
        await producer.stop()
