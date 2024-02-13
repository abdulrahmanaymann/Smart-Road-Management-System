import threading
from MYSQL import *
from data_handler import *
from Config.config import *
from excel_reader import read_excel_sheets
import PySimpleGUI as sg

from kafka_consumer import kafka_consumer

df_governorates, df_vehicles, df_travels, df_all_government = read_excel_sheets(
    EXCEL_FILE
)


def start_kafka_consumer():
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.daemon = True
    kafka_thread.start()


def add_travel_record():
    df_all_government.set_index("Start_gate\End_gate", inplace=True)
    df_dict = df_all_government.to_dict(orient="index")

    r = get_redis_connection()
    p = get_kafka_producer(KAFKA_BROKER)

    # Start Kafka consumer
    start_kafka_consumer()

    sg.ChangeLookAndFeel("DarkGrey10")
    # GUI
    layout = [
        [sg.Text("Enter Car ID:"), sg.InputText(key="ID")],
        [sg.Text("Enter Start Gate:"), sg.InputText(key="Start Gate")],
        [sg.Button("Add Record"), sg.Button("Cancel")],
        [sg.Output(size=(50, 10))],
    ]
    window = sg.Window("Add Travel Record", layout, element_justification="center")
    window.set_icon(ADD_TRAVEL_ICON)
    while True:
        event, values = window.read()

        if event == sg.WINDOW_CLOSED or event == "Cancel":
            window.close()
            break
        elif event == "Add Record":
            try:
                id = values["ID"]
                start_gate = values["Start Gate"]

                min_key, end_gate, min_value = Calaulate_Lowest_Distance(
                    start_gate, df_dict
                )

                process_travels_data(id, min_value, end_gate, df_governorates, r)

                process_new_travel_data(id, start_gate, min_key, min_value, p, r)

                sg.popup(
                    "Travel record added successfully!",
                    title="Success",
                    icon=EMBLEM_DEFAULT_ICON,
                )

            except ValueError as ve:
                sg.popup(
                    f"Error: {ve}",
                    title="Value Error",
                    icon=DIALOG_ERROR_ICON,
                )
            except Exception as e:
                sg.popup(
                    f"Error: {e}",
                    title="Error",
                    icon=DIALOG_ERROR_ICON,
                )
    p.flush()
