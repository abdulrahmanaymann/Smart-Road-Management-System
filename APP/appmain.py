from datetime import datetime
import sys

sys.path.append("D:\\graduation project\\GP")

from flask import Flask, redirect, request, render_template, url_for
from kafka import KafkaProducer
from data_handler import *
from Config.config import *
from excel_reader import read_excel_sheets

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


app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def APP():
    if request.method == "POST":
        try:
            car_id = request.form.get("ID")

            start_gate = request.form.get("StartGate")

            start_date = datetime.now()
            car_type = car_id.split("_")[1]
            print(f"id :{car_id} , startgate:{start_gate}")
            min_key, end_gate, min_value = Calaulate_Lowest_Distance(
                start_gate, df_dict
            )
            ttl, _ = calculate_ttl(min_value, car_type, end_gate, governorates_dict, r)
            actual_end_date = start_date + timedelta(seconds=ttl)

            s_d = start_date.strftime("%Y-%m-%d %H:%M:%S")
            e_d = actual_end_date.strftime("%Y-%m-%d %H:%M:%S")

            process_new_travel_data(
                car_id,
                start_gate,
                end_gate,
                min_value,
                p,
                df_governorates,
                s_d,
                e_d,
            )

            mes = (
                f"id:{car_id},s_g:{start_gate},e_g:{end_gate},D:{min_value},s_d: {s_d}"
            )

            producer.send(topic_name, mes.encode("utf-8"))
            producer.flush()

        except Exception as e:
            return f"Erroooooooooooor!\n{e}"

        if car_id == "":
            return " there is not data"
        elif start_gate == "":
            return " there is not data"

        else:
            return render_template("index.html")
    return render_template("index.html")


def extract_vehicle_type(id):
    start_index = id.find("_") + 1
    end_index = id.find("-")
    if start_index != -1 and end_index != -1:
        return id[start_index:end_index]
    else:
        return None


def get_travel_info(id):

    if conn and cursor:
        try:
            query = "SELECT ID FROM travels WHERE ID = %s"
            cursor.execute(query, (f"{id}",))
            travel_id = cursor.fetchall()

            if travel_id:
                for ids in travel_id:
                    ex_id = ids[0].split("_")[0]

                    query = "SELECT Start_Gate, End_Gate, Distance, Start_Travel_Date, End_Travel_Date FROM travels WHERE ID = %s"
                    cursor.execute(query, (ids[0],))
                    travel_data = cursor.fetchall()
                    print("Travel data:", travel_data)

                    if travel_data:
                        travels = []
                        for travel in travel_data:
                            vehicle_type = extract_vehicle_type(ids[0])
                            travels.append(
                                {
                                    "Start_Gate": travel[0],
                                    "End_Gate": travel[1],
                                    "Distance": travel[2],
                                    "Start_Travel_Date": travel[3],
                                    "End_Travel_Date": travel[4],
                                    "Vehicle_Type": vehicle_type,
                                }
                            )

                        violation_query = "SELECT Car_ID, Start_Gate, End_Gate, Start_Date, Arrival_End_Date FROM violations WHERE Car_ID = %s"
                        cursor.execute(violation_query, (f"{ex_id}%",))
                        violations_data = cursor.fetchall()

                        violations = []
                        for violation_data in violations_data:
                            violations.append(
                                {
                                    "Car_ID": violation_data[0],
                                    "Start_Gate": violation_data[1],
                                    "End_Gate": violation_data[2],
                                    "Start_Date": violation_data[3],
                                    "Arrival_End_Date": violation_data[4],
                                }
                            )

                        return travels
            else:
                return None, None

        except mysql.connector.Error as e:
            print(f"Error fetching travel data: {e}")
        finally:
            cursor.close()
            conn.close()


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        id = request.form["id"]
        if conn and cursor:
            try:
                query = "SELECT * FROM travels WHERE ID = %s"
                cursor.execute(query, (id,))
                travel_data = cursor.fetchone()
                if travel_data:
                    travel_info = get_travel_info(id)
                    if travel_info:
                        return redirect(url_for("travel_info", id=id))
                    else:
                        return "Error fetching travel information."
                else:
                    return "Invalid ID. Please try again."
            except mysql.connector.Error as e:
                print(f"Error fetching data from travels table: {e}")
            finally:
                cursor.close()
                conn.close()
    return render_template("login.html")


@app.route("/travel_info/<id>")
def travel_info(id):

    travel_info = get_travel_info(id)
    if travel_info:
        return render_template("vehicle_info.html", travel=travel_info)
    else:
        return "Travel information not found."


if __name__ == "__main__":
    app.run(debug=True)
