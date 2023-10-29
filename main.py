import multiprocessing
import redis
from confluent_kafka import Producer


from GUI import *
from data_handler import *
from config import *
from excel_reader import *
from kafka_consumer import *


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    df_governorates, df_vehicles, df_travels = read_excel_sheets(EXCEL_FILE)

    governorates_dict = insert_governorates_data(r, df_governorates)
    insert_vehicles_data(r, df_vehicles)

    # Start the Kafka consumer in a separate process
    kafka_consumer_process = multiprocessing.Process(target=kafka_consumer)
    kafka_consumer_process.start()

    # Start the GUI
    df_travels = add_travel_record(df_travels)

    # Process the updated data, including the new records
    process_travels_data(df_travels, governorates_dict, r, producer)

    r.close()
    producer.flush()


if __name__ == "__main__":
    main()
