import unittest
from unittest.mock import patch, AsyncMock
from src.events.producer import publish_event, start_producer, stop_producer

class TestProducer(unittest.IsolatedAsyncioTestCase):
    
    @patch('src.events.producer.AIOKafkaProducer')
    async def test_publish_event(self, mock_kafka_producer):
        # Setup mock
        mock_instance = AsyncMock()
        mock_kafka_producer.return_value = mock_instance
        
        await start_producer()
        
        event = {"test": "data"}
        await publish_event(event)
        
        # Verify send_and_wait called
        mock_instance.send_and_wait.assert_called_once()
        args, _ = mock_instance.send_and_wait.call_args
        self.assertEqual(args[1], event)
        
        await stop_producer()
