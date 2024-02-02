from confluent_kafka import Consumer, KafkaError, KafkaException
from config import KAFKA_BROKER, KAFKA_TOPIC


def kafka_consumer():
    try:
        print("Kafka consumer started.")

        conf = {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "my_consumer_group",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print("Received message: {}".format(msg.value().decode("utf-8")))

        consumer.close()

    except KafkaException as e:
        print(f"Kafka Exception: {e}")

    except Exception as ex:
        print(f"Exception: {ex}")
