from time import time
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
                if word.split("-")[0] == "(late)":
                    row = word.split(")-")[1]
                    dictionary = ast.literal_eval(row)
                    val = list(dictionary.values())

                    car_str = val[0].split("-")[0]
                    start_gate = val[2]
                    end_gate = val[3]
                    start_date = val[1]
                    end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    cursor.execute(
                        "INSERT INTO delays (Car_ID, Start_Gate, End_Gate, Start_Date, Arrival_End_Date) VALUES (%s, %s, %s, %s, %s)",
                        (car_str, start_gate, end_gate, start_date, end_date),
                    )
                    connection.commit()

                    sg.popup(
                        "DATA INSERTED SUCCESSFULLY :)",
                    )

                else:
                    dictionary = ast.literal_eval(word)
                    val = list(dictionary.values())

                    car_str = val[0].split("-")[0]
                    start_gate = val[2]
                    end_gate = val[3]
                    start_date = val[1]
                    end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    cursor.execute(
                        "INSERT INTO violations (Car_ID, Start_Gate, End_Gate, Start_Date, Arrival_End_Date	) VALUES (%s, %s, %s, %s, %s)",
                        (car_str, start_gate, end_gate, start_date, end_date),
                    )
                    connection.commit()

                    sg.popup(
                        "DATA INSERTED SUCCESSFULLY :)",
                    )

                    kafka_message = {
                        "ID": car_str,
                        "Start Gate": start_gate,
                        "End Gate": end_gate,
                        "Start Date": start_date,
                        "End Date": end_date,
                        "Timestamp": time(),
                    }

                    p.produce(
                        VIOLATIONS_TOPIC,
                        key=car_str.encode("utf-8"),
                        value=str(kafka_message).encode("utf-8"),
                    )
                    p.flush()
                    sg.popup("Data sent to Kafka topic:", VIOLATIONS_TOPIC)
                   
            except Exception as e:
                sg.popup(
                    "Error:",
                    e,
                )
    else:
        sg.popup(
            "Error: Failed to connect to the database.",
        )
except mysql.connector.Error as err:
    sg.popup(f"Error: {err}")
