from datetime import datetime
import json
import pandas as pd
from config import *
from MYSQL import *
from confluent_kafka import Producer
import redis
producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def insert_governorates_data(redis_connection, df_governorates):
    governorates_dict = {}

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


def insert_vehicles_data(redis_connection, df_vehicles):
    for _, row in df_vehicles.iterrows():
        key = row["Type"]
        data = {
            "Type": key,
            "Legal Speed": row["Legal Speed"],
        }
        # Save the data in the hash
        redis_connection.hset(f"vehicle_data:{key}", mapping=data)

    print("# Vehicles Data saved to Redis.")


def calculate_ttl(distance, vehicle_type, end_gate, governorates_dict, redis_connection):
    legal_speed = redis_connection.hget(f"vehicle_data:{vehicle_type}", "Legal Speed")
    if legal_speed is None:
        return None, None

    try:
        distance = float(distance)
        legal_speed = float(legal_speed)
        ttl_seconds = distance / legal_speed
        ttl_seconds = ttl_seconds * 3600
        end_gate_code = governorates_dict.get(end_gate, None)

        return ttl_seconds, end_gate_code
    except ValueError:
        return None, None


def process_travels_data(id , min_value,min_key,df_governorates):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    governorates_dict = insert_governorates_data(r, df_governorates)
    ttl, end_gate_code = calculate_ttl(min_value, id.split("_")[1], min_key, governorates_dict, r)
    if ttl is not None and end_gate_code is not None:
        travel_id_with_code = f"{id}-{end_gate_code}"
        start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        violation_data = {
            "ID": travel_id_with_code,
            "Start Date": start_date,
            "TTL (seconds)": ttl,
        }
        r.hset(f"{travel_id_with_code}", mapping=violation_data)
        travel_key = f"{travel_id_with_code}"
        r.expire(travel_key, int(ttl))  # Set TTL in seconds
        last_travel_key = f"last_travel:{id.split('_')[1]}"
        last_travel_data = {
            "ID": travel_id_with_code,
            "TTL (seconds)": ttl,
        }
        r.hset(last_travel_key, mapping=last_travel_data)
        print(
            f"Travel ID: {travel_id_with_code}, Start Date: {start_date}, TTL (seconds): {ttl :.2f}"
        )
    else:
        print(f"Invalid data for Travel ID: {id}")

    print("# Travels Data saved to Redis.")


# Create a message to Kafka
def send_to_kafka(bootstrap_servers, topic, data):
    try:
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        producer.produce(topic, value=json.dumps(data))
        producer.flush()
        print(f"Data send successfully to Kafka topic :{topic}")
    except Exception as e:
        print(f"Error in kafka!!\n{e}")



def process_new_travel_data(id, start_gate, end_gate, distance):
    # Send to kafka topic
    kafka_message = {
        "ID": id,
        "Start Gate": start_gate,
        "End Gate": end_gate,
        "Distance": distance
    }
    send_to_kafka(KAFKA_BROKER, KAFKA_TOPIC, kafka_message)

    # add data to database mysql
    try:
        conn = DB_Connection(
            MYSQL_HOST,
            MYSQL_PORT,
            MYSQL_USER,
            MYSQL_PASSWORD,
            MYSQL_DATABASE,
        )
        cursor = conn.cursor()
        query = "INSERT INTO travels (ID, Start_Gate, End_Gate, Distance) VALUES (%s ,%s ,%s ,%s)"
        value = [(id, start_gate, end_gate, distance)]
        cursor.executemany(query, value)
        conn.commit()
        print("DATA INSERTED SUCCESSFULLY :)")
        
        
        old_records = pd.read_excel(EXCEL_FILE, sheet_name=SHEET3)

        new_record = pd.DataFrame({
            "ID": [id],
            "Start Gate": [start_gate],
            "End Gate": [end_gate],
            "Distance (KM)": [distance],
        })

        new_records = pd.concat([old_records, new_record], ignore_index=True)
        
        with pd.ExcelWriter(
            EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace"
        ) as writer:
            new_records.to_excel(writer, SHEET3, index=False)

        return new_record
    
    except Exception as e:
        print(f"ERROR ==>\t{e}")


    