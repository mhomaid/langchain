import unittest
from unittest.mock import MagicMock, patch
from confluent_kafka import Message
from langchain_core.documents import Document
from langchain_community.document_loaders.kafka import KafkaDocumentLoader

class TestKafkaDocumentLoader(unittest.TestCase):
    @patch('confluent_kafka.Consumer')
    def test_load_data(self, mock_consumer_class):
        # Create mock Kafka messages
        mock_message1 = MagicMock(spec=Message)
        mock_message1.value.return_value = b'{"content": "This is the first document.", "metadata": {"source": "document1.txt"}}'
        mock_message1.error.return_value = None

        mock_message2 = MagicMock(spec=Message)
        mock_message2.value.return_value = b'{"content": "This is the second document.", "metadata": {"source": "document2.txt"}}'
        mock_message2.error.return_value = None

        mock_message3 = MagicMock(spec=Message)
        mock_message3.value.return_value = b'{"content": "This is the third document.", "metadata": {"source": "document3.txt"}}'
        mock_message3.error.return_value = None

        # Create a mock consumer instance
        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [mock_message1, mock_message2, mock_message3, None]
        mock_consumer_class.return_value = mock_consumer

        # Create a KafkaDocumentLoader instance
        kafka_loader = KafkaDocumentLoader(
            bootstrap_servers='localhost:9092',
            topic='test_topic',
            group_id='test_group',
            auto_offset_reset='earliest'
        )

        # Call the load method and get the documents
        documents = kafka_loader.load()

        # Check if the documents are correctly created from the Kafka messages
        expected_documents = [
            Document(page_content='{"content": "This is the first document.", "metadata": {"source": "document1.txt"}}'),
            Document(page_content='{"content": "This is the second document.", "metadata": {"source": "document2.txt"}}'),
            Document(page_content='{"content": "This is the third document.", "metadata": {"source": "document3.txt"}}')
        ]
        self.assertEqual(documents, expected_documents)

        # Check if the Kafka consumer's poll method is called
        self.assertEqual(mock_consumer.poll.call_count, 4)

        # Check if the Kafka consumer is closed
        mock_consumer.close.assert_called_once()