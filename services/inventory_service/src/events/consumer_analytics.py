import os
import json
from aiokafka import AIOKafkaConsumer
from handlers.inventory_analytics_handler import handle_inventory_event

KAFKA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
TOPIC = "inventory.analytics.events"
GROUP_ID = "inventory-analytics"


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
                await handle_inventory_event(msg.value)
                await consumer.commit()
            except Exception as e:
                print(f"Analytics handler failed: {e}")
    finally:
        await consumer.stop()
