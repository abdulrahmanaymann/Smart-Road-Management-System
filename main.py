from GUI import *
from Config.config import *
from data_handler import *
from kafka_consumer import *
from Config.Logger import LOGGER


def main():
    try:
        r = get_redis_connection()

        df_governorates, df_vehicles, _, _ = read_excel_sheets(EXCEL_FILE)

        insert_governorates_data(r, df_governorates)

        insert_vehicles_data(r, df_vehicles)

        add_travel_record()

    except Exception as e:
        LOGGER.error(f"Error: {e}")

    finally:
        if r:
            r.close()


if __name__ == "__main__":
    main()
