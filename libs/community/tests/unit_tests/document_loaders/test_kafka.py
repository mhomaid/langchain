import unittest
from unittest.mock import MagicMock, patch
from confluent_kafka import Message
from langchain_community.document_loaders.kafka import KafkaDocumentLoader

class TestKafkaDocumentLoader(unittest.TestCase):
    @patch('confluent_kafka.Consumer')
    def test_load(self, mock_consumer):
        # Create a mock Kafka message
        mock_message = MagicMock(spec=Message)
        mock_message.value.return_value = b'test message'
        mock_message.error.return_value = None

        # Set the mock consumer's poll method to return the mock message
        mock_consumer.return_value.poll.return_value = mock_message

        # Create a KafkaDocumentLoader instance
        kafka_loader = KafkaDocumentLoader(
            bootstrap_servers='localhost:9092',
            topic='test_topic',
            group_id='test_group',
            auto_offset_reset='earliest',
            record_handler=lambda record, id: record.value().decode('utf-8')
        )

        # Call the load method and get the first document
        document = next(kafka_loader.load())

        # Check if the document is correctly created from the Kafka message
        self.assertEqual(document, 'test message')

        # Check if the Kafka consumer's poll method is called
        mock_consumer.return_value.poll.assert_called_once()

        # Close the loader
        kafka_loader.close()