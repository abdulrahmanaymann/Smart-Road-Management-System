from datetime import datetime
import hashlib
import secrets
import sys

sys.path.append("D:\\GP\\graduation project\\GP")

from flask import *
from kafka import KafkaProducer
from data_handler import *
from Config.config import *
from excel_reader import read_excel_sheets

# from Spark_stream import run_spark_job


r = get_redis_connection()
p = get_kafka_producer(KAFKA_BROKER)

df_governorates, df_vehicles, df_travels, df_all_government = read_excel_sheets(
    EXCEL_FILE
)
governorates_dict = insert_governorates_data(r, df_governorates)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

topic_name = "travel-data"
conn, cursor = DB_Connection(
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
)

df_all_government.set_index("Start_gate\End_gate", inplace=True)
df_dict = df_all_government.to_dict(orient="index")


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def extract_vehicle_type(id):
    start_index = id.find("_") + 1
    end_index = id.find("-")
    if start_index != -1 and end_index != -1:
        return id[start_index:end_index]
    else:
        return None


def check_type(e_mail):
    user_type = e_mail.split("@")[1].split(".")[0]
    if user_type == "admin":
        return "admin"
    elif user_type == "gmail":
        return "driver"
    else:
        return "invalid email"


def is_json_file(file_name):
    return file_name.endswith(".json")


def get_driver_info(name, conn, cursor):
    try:
        if conn and cursor:
            query_driver = "SELECT * FROM drivers WHERE name = %s"
            cursor.execute(query_driver, (name,))
            driver_data = cursor.fetchone()

            if driver_data:
                driver_id = driver_data[0]

                query_travels = """
                    SELECT Travel_id, Start_Gate, End_Gate, Distance, Start_Travel_Date, End_Travel_Date
                    FROM travels
                    WHERE Vehicle_id IN (SELECT id FROM vehicles WHERE driver_id = %s)
                """
                cursor.execute(query_travels, (driver_id,))
                travel_data = cursor.fetchall()

                query_violations = """
                    SELECT Car_ID, Start_Gate, End_Gate, Start_Date, Arrival_End_Date, Payment_Status
                    FROM violations
                    WHERE Car_id IN (SELECT Number_type FROM vehicles WHERE driver_id = %s) AND Payment_Status = 'unpaid'
                """
                cursor.execute(query_violations, (driver_id,))
                violation_data = cursor.fetchall()

                travels = []
                for travel in travel_data:
                    travels.append(
                        {
                            "Travel_ID": travel[0],
                            "Start_Gate": travel[1],
                            "End_Gate": travel[2],
                            "Distance": travel[3],
                            "Start_Travel_Date": travel[4],
                            "End_Travel_Date": travel[5],
                        }
                    )

                violations = []
                for violation in violation_data:
                    violations.append(
                        {
                            "Travel_ID": violation[0],
                            "Start_Gate": violation[1],
                            "End_Gate": violation[2],
                            "Travel_Start_Date": violation[3],
                            "Travel_Arrival_Date": violation[4],
                            "Payment_Status": violation[5],
                        }
                    )

                return travels, violations
    except Exception as e:
        print(f"Error fetching travel and violation data: {e}")

    return None, None


def get_driver_profile(name, conn, cursor):
    if conn and cursor:
        try:
            query_driver = "SELECT * FROM drivers WHERE name = %s"
            cursor.execute(query_driver, (name,))
            driver_data = cursor.fetchone()

            if driver_data:
                driver_id = driver_data[0]
                email = driver_data[2]

                query_cars = "SELECT Number_type FROM vehicles WHERE driver_id = %s"
                cursor.execute(query_cars, (driver_id,))
                car_data = cursor.fetchall()
                car_ids = [car[0] for car in car_data]

                return email, car_ids
            else:
                return None, None
        except Exception as e:
            print(f"Error fetching driver profile: {e}")

    return None, None


def get_paid_violations(conn, cursor):
    if conn and cursor:
        try:
            query_violations = """
                    SELECT d.name as Driver_Name, v.Car_ID, v.Start_Gate, v.End_Gate, v.Start_Date, v.Arrival_End_Date, v.Payment_Status
                    FROM violations v
                    JOIN vehicles ve ON v.Car_ID = ve.Number_type
                    JOIN drivers d ON ve.driver_id = d.id
                    WHERE v.Payment_Status = 'paid'
                """
            cursor.execute(query_violations)
            violation_data = cursor.fetchall()

            violations = []
            for violation in violation_data:
                violations.append(
                    {
                        "Driver_Name": violation[0],
                        "Car_ID": violation[1],
                        "Start_Gate": violation[2],
                        "End_Gate": violation[3],
                        "Start_Date": violation[4],
                        "Arrival_End_Date": violation[5],
                        "Payment_Status": violation[6]
                    }
                )

            return violations
        except Exception as e:
            print(f"Error fetching paid violations: {e}")

    return None

