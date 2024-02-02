from datetime import datetime
import PySimpleGUI as sg
from config import *
from data_handler import get_redis_connection

def search_and_insert_violations(conn):
    try:
        r = get_redis_connection()

        # GUI
        layout = [
            [sg.Text("Enter your search query:"), sg.InputText()],
            [sg.Button("Search"), sg.Button("Cancel")],
        ]
        window = sg.Window("Search in Redis", layout)

        while True:
            event, values = window.read()
            if event == "Search":
                value = values[0]

                print(f'Searching for "{value}"...')
                if value is not None:
                    if r.exists(value):
                        sg.popup(f"The {value} is found")

                        print(value + "\n")

                        data = r.hgetall(value)

                        decoded_data = {
                            key.decode(): value.decode() for key, value in data.items()
                        }
                        print(f"{decoded_data}")

                        key = list(decoded_data.keys())
                        for k in key:
                            print(f"Key: {k}, Value: {decoded_data[k]}")

                        val = list(decoded_data.values())
                        car_str = val[0]  
                        Car_ID = car_str.split("-")
                        CarID = Car_ID[0]
                        start_date = val[1]
                        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # Unpack the tuple into separate variables
                        db_conn, cursor = conn

                        cursor.execute("INSERT INTO violations (Car_ID, Start_Date, End_Date) VALUES (%s, %s, %s)",
                                       (CarID, start_date, end_date))
                        db_conn.commit()

                        print("DATA INSERTED SUCCESSFULLY :)")
                        sg.popup("Violations inserted to DataBase Successfully")
                    else:
                        sg.popup(f"The {value} is not found")
                else:
                    sg.popup("Please, Enter key to search")

                continue
            if event == sg.WIN_CLOSED or event == "Cancel":
                break

        window.close()

    except Exception as e:
        sg.popup_error(f"An error occurred: {e}")
