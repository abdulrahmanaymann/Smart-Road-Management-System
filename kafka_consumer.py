from confluent_kafka import Consumer, KafkaError
from config import *
from confluent_kafka import Consumer, KafkaException

def kafka_consumer():
    conf = {
        'bootstrap.servers':KAFKA_BROKER,
        'group.id': 'your_group_id',
        # other consumer configuration properties...
    }

    # Print the group id
    print("Group ID:", conf['group.id'])

    consumer = Consumer(conf)

    topics = [KAFKA_TOPIC]
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(msg.error())
                    break
            # process the message...
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
