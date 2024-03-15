from confluent_kafka import Producer

# Kafka broker configuration
broker_config = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'my-producer',
    'security.protocol': 'PLAINTEXT'
}

# Create a Kafka producer instance
producer = Producer(broker_config)

# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Send messages to Kafka
for i in range(5):
    message = f'Message {i}'
    producer.produce('my-topic', value=message.encode('utf-8'), callback=delivery_report)

# Flush the producer to ensure all messages are sent
producer.flush()