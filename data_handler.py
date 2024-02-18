from datetime import datetime, timedelta
import json
import pandas as pd
from Config.config import *
from MYSQL import *
from confluent_kafka import Producer
import redis
from Config.Logger import LOGGER


def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def get_kafka_producer(bootstrap_servers):
    return Producer({"bootstrap.servers": bootstrap_servers})


def send_to_kafka(producer, topic, data):
    try:
        producer.produce(topic, value=json.dumps(data))
        producer.flush()
        LOGGER.info(f"Data sent successfully to Kafka topic: {topic}")
    except Exception as e:
        LOGGER.error(f"Error occurred while sending data to Kafka: {e}")


def insert_governorates_data(redis_connection, df_governorates):
    governorates_dict = {}

    try:
        for _, row in df_governorates.iterrows():
            governorate = row["Governorate"]
            code = row["Code"]
            governorates_dict[governorate] = code

            data = {
                "Code": code,
                "Governorate": governorate,
                "Distance": row["Distance"],
            }
            # Save the data in the hash
            redis_connection.hset(f"governorate:{code}", mapping=data)

        LOGGER.info("# Governorates Data saved to Redis.")
        return governorates_dict

    except Exception as e:
        LOGGER.error(f"Error occurred while inserting governorates data: {e}")
        return None


def insert_vehicles_data(redis_connection, df_vehicles):
    try:
        for _, row in df_vehicles.iterrows():
            key = row["Type"]
            data = {
                "Type": key,
                "Legal Speed": row["Legal Speed"],
            }
            # Save the data in the hash
            redis_connection.hset(f"vehicle_data:{key}", mapping=data)

        LOGGER.info("# Vehicles Data saved to Redis.")

    except Exception as e:
        LOGGER.error(f"Error occurred while inserting vehicle data: {e}")


def calculate_ttl(
    distance, vehicle_type, end_gate, governorates_dict, redis_connection
):
    try:
        legal_speed = redis_connection.hget(
            f"vehicle_data:{vehicle_type}", "Legal Speed"
        )
        if legal_speed is None:
            return None, None

        distance = float(distance)
        legal_speed = float(legal_speed)
        ttl_seconds = distance / legal_speed
        ttl_seconds *= 3600
        end_gate_code = governorates_dict.get(end_gate, None)

        return ttl_seconds, end_gate_code

    except ValueError:
        LOGGER.error("Error: Invalid input data for distance or legal speed.")
        return None, None

    except Exception as e:
        LOGGER.error(f"Error occurred: {e}")
        return None, None


def process_travels_data(id, min_value, min_key, df_governorates, redis_connection):
    try:
        governorates_dict = insert_governorates_data(redis_connection, df_governorates)

        if governorates_dict is None:
            LOGGER.error("Error: Failed to insert governorates data into Redis.")
            return

        ttl, end_gate_code = calculate_ttl(
            min_value, id.split("_")[1], min_key, governorates_dict, redis_connection
        )

        if ttl is not None and end_gate_code is not None:
            travel_id_with_code = f"{id}-{end_gate_code}"
            start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            violation_data = {
                "ID": travel_id_with_code,
                "Start Date": start_date,
                "TTL (seconds)": ttl,
            }
            redis_connection.hset(f"{travel_id_with_code}", mapping=violation_data)
            travel_key = f"{travel_id_with_code}"
            redis_connection.expire(travel_key, int(ttl))  # Set TTL in seconds
            last_travel_key = f"last_travel:{id.split('_')[1]}"
            last_travel_data = {
                "ID": travel_id_with_code,
                "TTL (seconds)": ttl,
            }
            redis_connection.hset(last_travel_key, mapping=last_travel_data)
            LOGGER.info(
                f"Travel ID: {travel_id_with_code}, Start Date: {start_date}, TTL (seconds): {ttl :.2f}"
            )
        else:
            LOGGER.error(f"Invalid data for Travel ID: {id}")

        LOGGER.info("# Travels Data saved to Redis.")

    except Exception as e:
        LOGGER.error(f"Error occurred: {e}")


def process_new_travel_data(
    id, start_gate, end_gate, distance, producer, redis_connection
):
    try:
        kafka_message = {
            "ID": id,
            "Start Gate": start_gate,
            "End Gate": end_gate,
            "Distance": distance,
        }
        send_to_kafka(producer, KAFKA_TOPIC, kafka_message)

        conn, cursor = DB_Connection(
            MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
        )
        if conn and cursor:
            try:
                query = "INSERT INTO travels (ID, Start_Gate, End_Gate, Distance) VALUES (%s ,%s ,%s ,%s)"
                value = (id, start_gate, end_gate, distance)
                cursor.execute(query, value)
                conn.commit()
                LOGGER.info("Data inserted into MySQL successfully.")
            finally:
                cursor.close()
                conn.close()

            old_records = pd.read_excel(EXCEL_FILE, sheet_name=SHEET3)
            new_record = pd.DataFrame(
                {
                    "ID": [id],
                    "Start Gate": [start_gate],
                    "End Gate": [end_gate],
                    "Distance (KM)": [distance],
                }
            )
            new_records = pd.concat([old_records, new_record], ignore_index=True)

            with pd.ExcelWriter(
                EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace"
            ) as writer:
                new_records.to_excel(writer, SHEET3, index=False)

            LOGGER.info("Excel file updated.")

            existing_travel_keys = redis_connection.keys(f"{id}-*")
            travel_records_count = 0
            latest_travel_key = f"{id}-{start_gate}"

            for key in existing_travel_keys:
                ttl = redis_connection.ttl(key)
                if ttl > 0:
                    travel_records_count += 1
                    if ttl > redis_connection.ttl(latest_travel_key):
                        latest_travel_key = key

            if travel_records_count > 1:
                start_date = datetime.now()
                end_date = start_date + timedelta(
                    seconds=redis_connection.ttl(latest_travel_key)
                )
                LOGGER.warning(
                    f"Violation: Vehicle ID {id} has multiple existing travel records with TTL not expired. "
                    f"Start Date: {start_date}, End Date: {end_date}"
                )
                LOGGER.info("Latest travel key: %s", latest_travel_key)

                for key in existing_travel_keys:
                    if key != latest_travel_key:
                        redis_connection.delete(key)
                        LOGGER.info(f"Key {key} removed from Redis.")

                conn, cursor = DB_Connection(
                    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
                )
                if conn and cursor:
                    try:
                        query = "INSERT INTO violations (Car_ID, Start_Date, End_Date) VALUES (%s ,%s ,%s)"
                        value = (id, start_date, end_date)
                        cursor.execute(query, value)
                        conn.commit()
                        LOGGER.info(
                            "Violation record inserted into MySQL successfully."
                        )
                    finally:
                        cursor.close()
                        conn.close()

        return new_record

    except Exception as e:
        LOGGER.error(f"Error occurred: {e}")
        return None


def Calaulate_Lowest_Distance(start_gate, dict):
    min_key = None
    min_value = float("inf")

    start_index = list(dict.keys()).index(start_gate)
    total_gates = len(dict)

    for i in range(total_gates):
        current_gate_index = (start_index + i) % total_gates
        current_gate = list(dict.keys())[current_gate_index]

        if pd.notna(dict[current_gate][start_gate]):
            distance_to_gate = dict[current_gate][start_gate]
            if distance_to_gate == 0:
                continue
            if distance_to_gate < min_value:
                min_value = distance_to_gate
                min_key = current_gate

    if min_key is None:
        raise ValueError("No valid end gate found for the entered start gate.")

    end_gate_index = (start_index + total_gates - 1) % total_gates
    end_gate = list(dict.keys())[end_gate_index]

    return min_key, end_gate, min_value
