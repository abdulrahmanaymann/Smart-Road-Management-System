import threading
from MYSQL import *
from data_handler import *
from config import *
from excel_reader import read_excel_sheets
import pandas as pd
import PySimpleGUI as sg

from kafka_consumer import kafka_consumer

df_governorates, df_vehicles, df_travels, df_all_government = read_excel_sheets(
    EXCEL_FILE
)


def start_kafka_consumer():
    # Start Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.daemon = True
    kafka_thread.start()


def add_travel_record():
    # convert all_government to nested dictionary
    df_all_government.set_index("Start_gate\End_gate", inplace=True)
    df_dict = df_all_government.to_dict(orient="index")

    r = get_redis_connection()
    p = get_kafka_producer(KAFKA_BROKER)
    # Start Kafka consumer
    start_kafka_consumer()

    # GUI
    layout = [
        [sg.Text("Enter Car ID:"), sg.InputText(key="ID")],
        [sg.Text("Enter Start Gate:"), sg.InputText(key="Start Gate")],
        [sg.Button("Add Record"), sg.Button("Close")],
        [sg.Output(size=(50, 10))],
    ]
    window = sg.Window("Add Travel Record", layout, element_justification="center")

    while True:
        event, values = window.read()

        if event == sg.WINDOW_CLOSED or event == "Close":
            window.close()
            break
        elif event == "Add Record":
            try:
                id = values["ID"]
                start_gate = values["Start Gate"]

                min_key = None
                min_value = float("inf")
                for entry_key, entry_values in df_dict.items():
                    if start_gate in entry_values and pd.notna(
                        entry_values[start_gate]
                    ):
                        if entry_values[start_gate] == 0:
                            continue
                        if entry_values[start_gate] < min_value:
                            min_value = entry_values[start_gate]
                            min_key = entry_key

                if min_key is None:
                    raise ValueError(
                        "No valid end gate found for the entered start gate."
                    )

                # Add the new travel record to Redis
                process_travels_data(id, min_value, min_key, df_governorates, r)

                # Process new travel data
                process_new_travel_data(id, start_gate, min_key, min_value, p)

                # Display success message
                sg.popup("Travel record added successfully!", title="Success")

            except ValueError as ve:
                sg.popup_error(f"Error: {ve}", title="Value Error")
            except Exception as e:
                sg.popup_error(f"Error: {e}", title="Error")

    # Close Kafka producer
    p.flush()
    p.close()
