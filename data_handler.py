from datetime import datetime
import pandas as pd
from config import *


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


def calculate_ttl(
    distance, vehicle_type, end_gate, governorates_dict, redis_connection
):
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


def process_travels_data(df_travels, governorates_dict, r):
    for _ , row in df_travels.iterrows():
        travel_id = row["ID"]
        vehicle_type = travel_id.split("_")[1]
        end_gate = row["End Gate"]
        distance = row["Distance (KM)"]

        ttl, end_gate_code = calculate_ttl(
            distance, vehicle_type, end_gate, governorates_dict, r
        )

        if ttl is not None and end_gate_code is not None:
            travel_id_with_code = f"{travel_id}-{end_gate_code}"

            # Check if the same vehicle has already traveled
            last_travel_key = f"last_travel:{vehicle_type}"
            last_travel_data = r.hgetall(last_travel_key)

            if last_travel_data and "ID" in last_travel_data:
                last_travel_id = last_travel_data["ID"].decode("utf-8")
                last_travel_ttl = float(last_travel_data.get("TTL (seconds)", 0))

                # If the TTL for the last travel is still found, add it to violations
                if last_travel_ttl > 0:
                    last_travel_end_gate_code = last_travel_id.split("-")[-1]
                    violation_key = f"{last_travel_id}"
                    violation_data = {
                        "ID": last_travel_id,
                        "End Gate": last_travel_end_gate_code,
                        "TTL (seconds)": last_travel_ttl,
                    }
                    r.hset(violation_key, mapping=violation_data)
                    print(
                        f"Adding Travels for Travel ID: {last_travel_id}, End Gate: {last_travel_end_gate_code}, TTL (seconds): {last_travel_ttl:.2f}"
                    )

            start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            violation_data = {
                "ID": travel_id_with_code,
                "Start Date": start_date,
                "TTL (seconds)": ttl,
            }

            r.hset(f"{travel_id_with_code}", mapping=violation_data)

            # Set an expiration time for the travel record key based on the calculated TTL
            travel_key = f"{travel_id_with_code}"
            r.expire(travel_key, int(ttl * 3600))  # Set TTL in seconds

            # Update the last travel data for the vehicle
            last_travel_data = {
                "ID": travel_id_with_code,
                "TTL (seconds)": ttl,
            }
            r.hset(last_travel_key, mapping=last_travel_data)

            print(
                f"Travel ID: {travel_id_with_code}, Start Date: {start_date}, TTL (seconds): {ttl :.2f}"
            )
        else:
            print(f"Invalid data for Travel ID: {travel_id}")

    print("# Travels Data saved to Redis.")


def process_new_travel_data(df_travels, id, start_gate, end_gate, distance):
    # Create a new DataFrame with the new travel record
    new_record = pd.DataFrame(
        {
            "ID": [id],
            "Start Gate": [start_gate],
            "End Gate": [end_gate],
            "Distance (KM)": [distance],
        }
    )
    df_travels = pd.concat([df_travels, new_record], ignore_index=True)
    with pd.ExcelWriter(
        EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace"
    ) as writer:
        df_travels.to_excel(writer, sheet_name=SHEET3, index=False)

    return df_travels