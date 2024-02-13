import mysql.connector
import sys
from datetime import datetime
import ast
import PySimpleGUI as sg
from Config.config import *

sg.ChangeLookAndFeel("DarkGrey10")

try:
    connection = mysql.connector.connect(
        host="localhost", port=3306, user="root", password="", database="py_test_db"
    )
    cursor = connection.cursor()

    if connection.is_connected():
        cursor.execute("SELECT DATABASE() ;")
        db = cursor.fetchone()
        sg.popup(
            f"Connected to database {db} successfully",
            title="MySQL",
            icon=MYSQL_ICON,
        )
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

                sg.popup(
                    "DATA INSERTED SUCCESSFULLY :)",
                    title="MySQL",
                    icon=MYSQL_ICON,
                )
            except Exception as e:
                sg.popup(
                    "Error:",
                    e,
                    title="Oops!",
                    icon=DIALOG_ERROR_ICON,
                )
    else:
        sg.popup(
            "Error: Failed to connect to the database.",
            title="Oops!",
            icon=DIALOG_ERROR_ICON,
        )
except mysql.connector.Error as err:
    print(f"Error: {err}")
