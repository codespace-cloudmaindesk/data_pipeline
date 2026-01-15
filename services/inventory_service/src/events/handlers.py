import asyncio
import logging
from messaging.producer import publish_event
from topics import Topics

logger = logging.getLogger(__name__)


async def handle_event(event: dict):
    """
    Orchestrates events to two separate Kafka topics:
    - Analytics
    - Real-time dashboard
    """

    tasks = [
        publish_event(Topics.INVENTORY_ANALYTICS, event),
        publish_event(Topics.INVENTORY_REALTIME, event),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            logger.error("Failed sending event to topic", exc_info=result)
