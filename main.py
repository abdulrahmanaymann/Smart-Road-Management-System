import redis
from GUI import *
from data_handler import *
from config import *
from excel_reader import *
from kafka_consumer import *
import multiprocessing

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    df_governorates, df_vehicles, _, _ = read_excel_sheets(EXCEL_FILE)
    governorates_dict = insert_governorates_data(r, df_governorates)
    insert_vehicles_data(r, df_vehicles)

    add_travel_record()

    kafka_consumer_process = multiprocessing.Process(target=kafka_consumer)
    kafka_consumer_process.start()

    r.close()

if __name__ == "__main__":
    main()
