from confluent_kafka import Consumer, KafkaError

from config import *


def kafka_consumer():
    # Create Consumer instance
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "your_consumer_group_id",
            "auto.offset.reset": "earliest",
        }
    )

    # Subscribe to the Kafka topic
    consumer.subscribe([KAFKA_TOPIC])

    while True:
        # Read the message from the Kafka topic
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition")
            else:
                print(f"Error: {message.error()}")
        else:
            print(f"Received message: {message.value()}")

