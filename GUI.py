from confluent_kafka import Producer
from tkinter import messagebox
from MYSQL import *
from data_handler import *
from config import *
from excel_reader import read_excel_sheets
import pandas as pd
import PySimpleGUI as sg

df_governorates, df_vehicles, df_travels, df_all_government = read_excel_sheets(
    EXCEL_FILE
)


def add_travel_record(df_travels):
    # convert all_government to nested dictionary
    df_all_government.set_index("Start_gate\End_gate", inplace=True)
    df_dict = df_all_government.to_dict(orient="index")

    # GUI
    layout = [
        [sg.Text("Enter ID:"), sg.InputText(key="ID")],
        [sg.Text("Enter Start Gate:"), sg.InputText(key="Start Gate")],
        [sg.Button("Login"), sg.Button("Cancel")],
    ]
    window = sg.Window("LOGIN", layout, element_justification="center")

    Excel_Data = {"ID": [], "start_gate": [], "End_gate": [], "distance": []}

    while True:
        event, values = window.read()
        key = values["Start Gate"]  # Replace with the entered start gate

        # Initialize variables to keep track of the minimum value and its key
        min_key = None
        min_value = float("inf")  # Set to positive infinity initially

        # Iterate through the dictionary
        for entry_key, entry_values in df_dict.items():
            # Check if the specified key is present and the corresponding value is numeric
            if key in entry_values and pd.notna(entry_values[key]):
                # Update the minimum value and its key if a smaller value is found
                if entry_values[key] == 0:
                    continue
                if entry_values[key] < min_value:
                    min_value = entry_values[key]
                    min_key = entry_key
            else:
                print(f"key {key} you Entered not exist !!!")

        if event == sg.WINDOW_CLOSED or event == "Cancel":
            break
        elif event == "Login":
            # input your data
            try:
                id = values["ID"]
                start_gate = values["Start Gate"]
                end_gate = min_key
                distance = min_value

                Excel_Data = {
                    "ID": [],
                    "start_gate": [],
                    "End_gate": [],
                    "distance": [],
                }

                if not id or not start_gate or not end_gate or not distance:
                    sg.popup_error(
                        "Please fill in all fields (or you Entered start_gate incorrectly)."
                    )
                    continue
                Excel_Data["ID"].append(id)
                Excel_Data["start_gate"].append(start_gate)
                Excel_Data["End_gate"].append(end_gate)
                Excel_Data["distance"].append(distance)

                # Create a message to Kafka
                # kafka_message = f"Travel ID: {id}, Start Gate: {start_gate}, End Gate: {end_gate}, Distance: {distance} KM"
                # producer.produce(KAFKA_TOPIC, key=id, value=kafka_message)

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
                    query = f"INSERT INTO travels (ID, Start_Gate, End_Gate, Distance) VALUES (%s ,%s ,%s ,%s)"
                    value = [(id, start_gate, end_gate, distance)]
                    cursor.executemany(query, value)
                    conn.commit()
                    print("DATA INSERTED SUCCESSFULLY :)")
                except Exception as e:
                    print(f"ERROR ==>\t{e}")

                # Add the new travel record to the DataFrame
                df_travels = process_new_travel_data(
                    df_travels, id, start_gate, end_gate, distance
                )
                messagebox.showinfo("Success", "Travel record added successfully.")

            except Exception as e:
                print(f"Error!!\n{e}")
        return df_travels
    window.close()