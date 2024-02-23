import mysql.connector
import sys
from datetime import datetime
import ast
import PySimpleGUI as sg
from Config.config import *
from data_handler import get_kafka_producer

sg.ChangeLookAndFeel("DarkGrey10")

try:
    p = get_kafka_producer(KAFKA_BROKER)

    connection = mysql.connector.connect(
        host="localhost", port=3306, user="root", password="", database="py_test_db"
    )
    cursor = connection.cursor()

    if connection.is_connected():
        for line in sys.stdin:
            try:
                word = line.strip()
                dictionary = ast.literal_eval(word)
                val = list(dictionary.values())

                car_str = val[0]
                start_date = val[1]
                end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    "INSERT INTO violations (Car_ID, Start_Date, End_Date) VALUES (%s, %s, %s)",
                    (car_str, start_date, end_date),
                )
                connection.commit()

                print(
                    "DATA INSERTED SUCCESSFULLY :)",
                )
                #---------------------------------------------------------------
                kafka_message = {
                    "ID": car_str,
                    "Start Date": start_date,
                    "End Date": end_date,
                }
                p.produce(
                    VIOLATIONS_TOPIC,
                    key=car_str.encode("utf-8"),
                    value=str(kafka_message).encode("utf-8"),
                )
                p.flush()
                print("Data sent to Kafka topic:", KAFKA_TOPIC)
                #---------------------------------------------------------------
            except Exception as e:
                print(
                    "Error:",
                    e,
                )
    else:
        print(
            "Error: Failed to connect to the database.",
        )
except mysql.connector.Error as err:
    print(f"Error: {err}")
