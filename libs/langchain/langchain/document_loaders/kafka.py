from confluent_kafka import Consumer, KafkaException, KafkaError
import json

class KafkaDocumentLoader:
    def __init__(self, config):
        """
        Initialize Kafka consumer with given configuration.
        """
        try:
            self.consumer = Consumer(config)
        except Exception as e:
            print(f"Failed to connect Kafka server with the provided configuration: {e}")
            raise

    def subscribe(self, topic):
        """
        Subscribe to a Kafka topic.
        """
        try:
            self.consumer.subscribe([topic])
        except Exception as e:
            print(f"Failed to subscribe to topic {topic}: {e}")
            raise

    def load_document(self):
        """
        Load document from a Kafka topic.
        """
        try:
            # Wait for up to 1s for a message
            msg = self.consumer.poll(1.0)

            if msg is None:
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                # Assume the message is a JSON string
                # This may need to be adapted depending on your use case
                return json.loads(msg.value().decode('utf-8'))

        except KeyboardInterrupt:
            pass

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()