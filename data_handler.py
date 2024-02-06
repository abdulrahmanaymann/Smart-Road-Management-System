from datetime import datetime
import json
import pandas as pd
from config import *
from MYSQL import *
from confluent_kafka import Producer
import redis


def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def get_kafka_producer(bootstrap_servers):
    return Producer({"bootstrap.servers": bootstrap_servers})


def send_to_kafka(producer, topic, data):
    try:
        producer.produce(topic, value=json.dumps(data))
        producer.flush()
        print(f"Data sent successfully to Kafka topic: {topic}")
    except Exception as e:
        print(f"Error occurred while sending data to Kafka: {e}")


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

        print("# Governorates Data saved to Redis.")
        return governorates_dict

    except Exception as e:
        print(f"Error occurred while inserting governorates data: {e}")
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

        print("# Vehicles Data saved to Redis.")

    except Exception as e:
        print(f"Error occurred while inserting vehicle data: {e}")


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
        print("Error: Invalid input data for distance or legal speed.")
        return None, None

    except Exception as e:
        print(f"Error occurred: {e}")
        return None, None


def process_travels_data(id, min_value, min_key, df_governorates, redis_connection):
    try:
        governorates_dict = insert_governorates_data(redis_connection, df_governorates)

        if governorates_dict is None:
            print("Error: Failed to insert governorates data into Redis.")
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
            print(
                f"Travel ID: {travel_id_with_code}, Start Date: {start_date}, TTL (seconds): {ttl :.2f}"
            )
        else:
            print(f"Invalid data for Travel ID: {id}")

        print("# Travels Data saved to Redis.")

    except Exception as e:
        print(f"Error occurred: {e}")


def process_new_travel_data(id, start_gate, end_gate, distance, producer):
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
                print("Data inserted into MySQL successfully.")
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

            print("Excel file updated.")
            return new_record
        else:
            print("Failed to establish connection to MySQL database.")
            return None

    except Exception as e:
        print(f"Error occurred: {e}")
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
