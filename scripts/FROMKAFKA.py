import PySimpleGUI as sg
from kafka import KafkaProducer
import mysql.connector
from GUI import *

# Define the Kafka broker address
bootstrap_servers = "localhost:9092"

# Kafka topic to send messages
topic_name = "travel-data"

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

r = get_redis_connection()
p = get_kafka_producer(KAFKA_BROKER)

df_governorates, df_vehicles, df_travels, df_all_government = read_excel_sheets(
    EXCEL_FILE
)

# convert all_government to nested dictionary
df_all_government.set_index("Start_gate\End_gate", inplace=True)
df_dict = df_all_government.to_dict(orient="index")


def get_end_gates(car_id):
    try:
        connection = mysql.connector.connect(
            host="localhost", port=3306, user="root", password="", database="py_test_db"
        )
        cursor = connection.cursor()

        cursor.execute(
            "SELECT DISTINCT Start_Gate, End_Gate  FROM travels WHERE ID LIKE %s",
            (car_id + "%",),
        )
        gates = cursor.fetchall()

        cursor.close()
        connection.close()

        return gates

    except mysql.connector.Error as err:
        sg.popup_error(f"Error: {err}")


def FROM_KAFKA():
    try:
        sg.ChangeLookAndFeel("DarkGrey10")
        # GUI
        layout = [
            [sg.Text("Enter your key:"), sg.InputText(key="key")],
            [
                sg.Button("Send"),
                sg.Button("Show Gates"),
                sg.Button("Add new travel"),
                sg.Button("Cancel", pad=((133, 10), (10, 10))),
            ],
        ]
        window = sg.Window("Send to Kafka topic", layout=layout)
        while True:
            event, values = window.read()

            # If the user closes the window or clicks "Cancel"
            if event == sg.WINDOW_CLOSED or event == "Cancel":
                break

            # If the user clicks "Send"
            if event == "Send":
                val = values["key"]

                # Send the message to the Kafka topic
                producer.send(
                    topic_name, val.replace('"', "").replace("'", "").encode("utf-8")
                )

                # Flush the producer to ensure all messages are sent
                producer.flush()

                sg.popup("Message sent successfully!", title="Success")
                print(
                    f"data send successfully to topic : {topic_name} with value {val}"
                )

            if event == "Show Gates":
                car_id = values["key"]
                Car_ID = car_id.split("-")[0].replace("'", "").replace('"', "")
                end_gates = get_end_gates(Car_ID)

                if end_gates:
                    end_gate_str = set()
                    for gate in end_gates:
                        end_gate_str.update(gate)
                    sg.popup(
                        f"Gates that Car: {Car_ID} is registered:\n{', '.join(list(sorted(end_gate_str)))}",
                        title="Gates",
                    )
                else:
                    sg.popup(
                        f"No end gates found for Car ID {car_id}",
                        title="Gates",
                    )
            if event == "Add new travel":
                government = values["key"]
                if r.exists(government):
                    travel_data = (
                        government.replace("'", "").replace('"', "").split("-")
                    )
                    travel_ID = travel_data[0]
                    gover = travel_data[1]
                    Govern_key = f"governorate:{gover}"
                    gover_list = r.hgetall(Govern_key)
                    decoded_data = {
                        key.decode(): value.decode()
                        for key, value in gover_list.items()
                    }
                    val = list(decoded_data.values())
                    Start_gate = val[1]

                    min_key, min_value = Calaulate_Lowest_Distance(Start_gate, df_dict)

                    # Add the new travel record to Redis
                    process_travels_data(
                        travel_ID, min_value, min_key, df_governorates, r
                    )

                    # Process new travel data
                    process_new_travel_data(
                        travel_ID, Start_gate, min_key, min_value, p
                    )

                    # Display success message
                    sg.popup("Travel record added successfully!", title="Success")
                else:
                    sg.popup("Travel key not exists!", title="Redis")

        producer.close()
        window.close()

    except Exception as e:
        print(e)
