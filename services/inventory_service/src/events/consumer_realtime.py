import os
import json
from aiokafka import AIOKafkaConsumer
from handlers.realtime_handler import handle_realtime_event

KAFKA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
TOPIC = "inventory.realtime.events"
GROUP_ID = "inventory-realtime"


async def start_consumer():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    await consumer.start()
    try:
        async for msg in consumer:
            try:
                await handle_realtime_event(msg.value)
                await consumer.commit()
            except Exception as e:
                print(f"Realtime handler failed: {e}")
    finally:
        await consumer.stop()
