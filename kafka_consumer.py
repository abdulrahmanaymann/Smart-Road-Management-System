from confluent_kafka import Consumer, KafkaError, KafkaException
from Config.config import KAFKA_BROKER, KAFKA_TOPIC
from Config.Logger import LOGGER


def kafka_consumer():
    try:
        LOGGER.info("Kafka consumer started.")

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
                    LOGGER.error(msg.error())
                    break

            LOGGER.info("Received message: %s", msg.value().decode("utf-8"))

        consumer.close()

    except KafkaException as e:
        LOGGER.error("Kafka Exception: %s", e)

    except Exception as ex:
        LOGGER.error("Exception: %s", ex)
