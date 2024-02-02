import multiprocessing
from GUI import *
from GUI_logout import search_and_insert_violations
from config import *
from data_handler import *
from MYSQL import *
from kafka_consumer import *


def main():
    r = get_redis_connection()
    df_governorates, df_vehicles, _, _ = read_excel_sheets(EXCEL_FILE)
    governorates_dict = insert_governorates_data(r, df_governorates)

    insert_vehicles_data(r, df_vehicles)

     # Create a process for adding travel record GUI
    add_travel_record_process = multiprocessing.Process(target=add_travel_record)
    add_travel_record_process.start()

    # Create a process for search and insert violations GUI
    search_insert_violations_process = multiprocessing.Process(target=search_and_insert_violations(CONN))
    search_insert_violations_process.start()

    # Wait for both processes to finish
    add_travel_record_process.join()
    search_insert_violations_process.join()

    r.close()


if __name__ == "__main__":
    main()
