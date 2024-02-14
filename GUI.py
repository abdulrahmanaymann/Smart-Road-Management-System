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

    start_kafka_consumer()

    sg.theme("DarkGrey10")

    sg.theme_text_color("white")
    sg.theme_background_color("#2B2B2B")
    sg.theme_element_background_color("#2B2B2B")

    input_text_font = ("Helvetica", 12)
    input_text_color = "white"
    button_color = ("white", "#407294")
    cancel_button_color = ("white", "#B52B65")

    layout = [
        [
            sg.Text("Car ID:", font=input_text_font, text_color=input_text_color),
            sg.InputText(key="ID", size=(30, 1), font=input_text_font),
        ],
        [
            sg.Text("Start Gate:", font=input_text_font, text_color=input_text_color),
            sg.InputText(key="Start Gate", size=(30, 1), font=input_text_font),
        ],
        [
            sg.Button("Add Record", size=(15, 1), button_color=button_color),
            sg.Button("Cancel", size=(15, 1), button_color=cancel_button_color),
        ],
    ]

    window = sg.Window(
        "Add Travel Record",
        layout,
        element_justification="center",
        icon=ADD_TRAVEL_ICON,
        size=(400, 200),
    )

    added_records = set()

    while True:
        event, values = window.read()

        if event == sg.WINDOW_CLOSED or event == "Cancel":
            window.close()
            break
        elif event == "Add Record":
            car_id = values["ID"].strip()
            start_gate = values["Start Gate"].strip()

            if not car_id:
                sg.popup_error("Car ID cannot be empty.")
                continue

            if not start_gate:
                sg.popup_error("Start Gate cannot be empty.")
                continue

            if (car_id, start_gate) in added_records:
                sg.popup_error(
                    "This vehicle ID and start gate combination already exists."
                )
                continue

            try:
                min_key, end_gate, min_value = Calaulate_Lowest_Distance(
                    start_gate, df_dict
                )

                process_travels_data(car_id, min_value, end_gate, df_governorates, r)

                process_new_travel_data(car_id, start_gate, min_key, min_value, p, r)

                added_records.add((car_id, start_gate))

                sg.popup_ok(
                    "Travel record added successfully!",
                    title="Success",
                    icon=EMBLEM_DEFAULT_ICON,
                )

            except ValueError as ve:
                sg.popup_error(f"Value Error: {ve}")
            except Exception as e:
                sg.popup_error(f"Error: {e}")

    p.flush()
