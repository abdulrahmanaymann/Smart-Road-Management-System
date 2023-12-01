from confluent_kafka import Producer, Consumer
import nipyapi

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "my_topic"

# NiFi configuration
nifi_url = "http://localhost:8080/nifi-api"
nifi_processor_name = "PutKafka"

# Create a Kafka producer
producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})

# Create a Kafka consumer
consumer = Consumer(
    {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": "my_consumer_group",
        "auto.offset.reset": "earliest",
    }
)

# Connect to NiFi
nipyapi.config.nifi_config.host = nifi_url

# Get the NiFi processor by name
processor = nipyapi.canvas.get_processor(nifi_processor_name)


# Publish a message to Kafka
def publish_message(message):
    producer.produce(kafka_topic, message.encode("utf-8"))
    producer.flush()


# Consume messages from Kafka
def consume_messages():
    consumer.subscribe([kafka_topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        print(f"Received message: {msg.value().decode('utf-8')}")


# Start consuming messages from Kafka
consume_messages()
