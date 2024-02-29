from datetime import datetime, timedelta
import json
import pandas as pd
from Config.config import *
from MYSQL import *
from confluent_kafka import Producer
import redis
from Config.Logger import LOGGER
import PySimpleGUI as sg


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


def process_travels_data(
    id, start_gate, distance, end_gate, df_governorates, redis_connection
):
    try:
        governorates_dict = insert_governorates_data(redis_connection, df_governorates)

        if governorates_dict is None:
            LOGGER.error("Error: Failed to insert governorates data into Redis.")
            return

        ttl, end_gate_code = calculate_ttl(
            distance, id.split("_")[1], end_gate, governorates_dict, redis_connection
        )

        if ttl is not None and end_gate_code is not None:
            travel_id_with_code = f"{id}-{end_gate_code}"
            start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            violation_data = {
                "ID": travel_id_with_code,
                "Start Date": start_date,
                "Start Gate": start_gate,
                "End Gate": end_gate,
                "TTL (seconds)": ttl,
            }
            redis_connection.hset(f"{travel_id_with_code}", mapping=violation_data)
            travel_key = f"{travel_id_with_code}"
            redis_connection.expire(travel_key, int(ttl))

            LOGGER.info(
                f"Travel ID: {travel_id_with_code}, Start Date: {start_date}, TTL (seconds): {ttl :.2f}"
            )

        else:
            LOGGER.error(f"Invalid data for Travel ID: {id}")

        LOGGER.info("# Travels Data saved to Redis.")

    except Exception as e:
        LOGGER.error(f"Error occurred: {e}")


def process_new_travel_data(
    id, start_gate, end_gate, distance, producer, df_governorates
):
    try:
        governorates_dict = insert_governorates_data(
            get_redis_connection(), df_governorates
        )
        if governorates_dict is None:
            LOGGER.error("Error: Failed to insert governorates data into Redis.")
            return

        start_gate_code = governorates_dict.get(start_gate, None)
        if start_gate_code is None:
            LOGGER.error(f"Error: Start gate code not found for '{start_gate}'.")
            return

        formatted_id = f"{id}-{start_gate_code}"

        kafka_message = {
            "ID": formatted_id,
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
                value = (formatted_id, start_gate, end_gate, distance)
                cursor.execute(query, value)
                conn.commit()
                LOGGER.info("Data inserted into MySQL successfully.")
            finally:
                cursor.close()
                conn.close()

            old_records = pd.read_excel(EXCEL_FILE, sheet_name=SHEET3)
            new_record = pd.DataFrame(
                {
                    "ID": [formatted_id],
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

        return new_record

    except Exception as e:
        LOGGER.error(f"Error occurred: {e}")
        return None


def Calaulate_Lowest_Distance(start_gate, dict):
    min_key = None
    min_value = float("inf")

    start_index = list(dict.keys()).index(start_gate)

    valid_gates = list(dict.keys())[start_index + 1 :]

    for entry_key, entry_values in dict.items():
        if entry_key in valid_gates and pd.notna(entry_values[start_gate]):
            if entry_values[start_gate] == 0:
                continue
            if entry_values[start_gate] < min_value:
                min_value = entry_values[start_gate]
                min_key = entry_key

    if min_key is None:
        raise ValueError("No valid end gate found for the entered start gate.")

    end_gate_index = (start_index + 1) % len(dict)
    end_gate = list(dict.keys())[end_gate_index]

    return min_key, end_gate, min_value


def To_Nifi(r, df_governorates, id, start_Gate, end_Gate, distance):
    keys = r.keys(f"{id}-*")

    if keys:
        for key in keys:
            Full_Data = r.hgetall(key)
            decoded_data = {
                key.decode(): value.decode() for key, value in Full_Data.items()
            }
            val = list(decoded_data.values())
            if start_Gate == val[2]:
                print("This travel is recored before")
            else:
                print(f"{decoded_data}")
                r.delete(key)
                process_travels_data(
                    id, start_Gate, distance, end_Gate, df_governorates, r
                )
                print(
                    f"Violation Detection!!!! \nthe previous key {key.decode('utf-8')} is DELETED FROM REDIS",
                )

    else:
        process_travels_data(id, start_Gate, distance, end_Gate, df_governorates, r)
        print(
            f"THERE IS NO such DATA in REDIS, BUT DATA SAVED TO REDIS ",
        )
