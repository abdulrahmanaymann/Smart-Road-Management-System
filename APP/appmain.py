from datetime import datetime, timedelta
import hashlib
from http.client import HTTPException
import secrets

import sys
import threading

from flask_socketio import SocketIO

sys.path.append("D:\\GP\\graduation project\\GP")

from flask import (
    Flask,
    jsonify,
    redirect,
    request,
    render_template,
    session,
    url_for,
)
from kafka import KafkaProducer
from data_handler import *
from Config.config import *
from excel_reader import read_excel_sheets
from Spark_stream import run_spark_job
from Flask_Functions import *
from assignedVehicles import *

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
socketio = SocketIO(app)


def is_json_file(file_name):
    return file_name.endswith(".json")


@app.route("/json_data")
def json_data():
    delay_dir = "output/delay"
    travel_dir = "output/travel"
    joined_dir = "output/joined"

    delay_data = []
    travel_data = []
    joined_data = []

    if os.path.exists(delay_dir):
        delay_files = [f for f in os.listdir(delay_dir) if is_json_file(f)]
        for file_name in delay_files:
            file_path = os.path.join(delay_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as delay_file:
                    delay_data.extend(
                        [json.loads(line) for line in delay_file.readlines()]
                    )
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    if os.path.exists(travel_dir):
        travel_files = [f for f in os.listdir(travel_dir) if is_json_file(f)]
        for file_name in travel_files:
            file_path = os.path.join(travel_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as travel_file:
                    travel_data.extend(
                        [json.loads(line) for line in travel_file.readlines()]
                    )
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    if os.path.exists(joined_dir):
        joined_files = [f for f in os.listdir(joined_dir) if is_json_file(f)]
        for file_name in joined_files:
            file_path = os.path.join(joined_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as joined_file:
                    joined_data.extend(
                        [json.loads(line) for line in joined_file.readlines()]
                    )
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    return jsonify(
        {
            "delay_records": delay_data,
            "travel_records": travel_data,
            "joined_records": joined_data,
        }
    )


@app.route("/streaming")
def streaming_data():
    data_dir = "output/stream"  # violations json
    streaming_data = []

    if os.path.exists(data_dir):
        data_files = [f for f in os.listdir(data_dir) if is_json_file(f)]
        for file_name in data_files:
            file_path = os.path.join(data_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as data_file:
                    streaming_data.extend(
                        [json.loads(line) for line in data_file.readlines()]
                    )
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    return jsonify({"streaming_data": streaming_data})


@app.route("/travels")
def _data():
    data_dir = "output/stream2"  # travels json
    streaming_data = []

    if os.path.exists(data_dir):
        data_files = [f for f in os.listdir(data_dir) if is_json_file(f)]
        for file_name in data_files:
            file_path = os.path.join(data_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as data_file:
                    streaming_data.extend(
                        [json.loads(line) for line in data_file.readlines()]
                    )
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    return jsonify({"travels_data": streaming_data})


@app.route("/delays")
def delays():
    data_dir = "output/stream3"  # delays json
    streaming_data = []

    if os.path.exists(data_dir):
        data_files = [f for f in os.listdir(data_dir) if is_json_file(f)]
        for file_name in data_files:
            file_path = os.path.join(data_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as data_file:
                    streaming_data.extend(
                        [json.loads(line) for line in data_file.readlines()]
                    )
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    return jsonify({"delays_data": streaming_data})


@app.route("/car_types")
def car_types():
    data_dir = "output/stream"  # types json
    car_types = {}

    if os.path.exists(data_dir):
        data_files = [f for f in os.listdir(data_dir) if is_json_file(f)]
        for file_name in data_files:
            file_path = os.path.join(data_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as data_file:
                    streaming_data = [
                        json.loads(line) for line in data_file.readlines()
                    ]
                    for item in streaming_data:
                        id_parts = item["ID"].split("_")
                        car_type = id_parts[1].split("-")[0]
                        car_types[car_type] = car_types.get(car_type, 0) + 1
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    return jsonify({"car_types": car_types})


@app.route("/types_delay")
def car_types_delays():
    data_dir = "output/stream3"  # types json delay
    car_types = {}

    if os.path.exists(data_dir):
        data_files = [f for f in os.listdir(data_dir) if is_json_file(f)]
        for file_name in data_files:
            file_path = os.path.join(data_dir, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as data_file:
                    streaming_data = [
                        json.loads(line) for line in data_file.readlines()
                    ]
                    for item in streaming_data:
                        car_id = item.get("ID")
                        if car_id:
                            car_type = car_id.split("_")[1]
                            car_types[car_type] = car_types.get(car_type, 0) + 1
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")

    return jsonify({"car_types": car_types})


@app.route("/Dashboard")
def streaming_page():
    return render_template("streaming.html")


app.secret_key = secrets.token_hex(16)


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


@app.route("/Enroll", methods=["GET", "POST"])
def APP():
    if request.method == "POST":
        try:
            if "image" in request.files and request.files["image"]:
                uploaded_image = request.files["image"]

                filename = uploaded_image.filename

                image_directory = "Datasets\\Egyptain Cars\\Before Cropping\\"

                uploaded_image_path = os.path.join(image_directory, filename)

                print("Image uploaded")
                print("Uploaded image filename:", uploaded_image_path)

                car_id = Get_Vehicle_ID(uploaded_image_path)
                print(f"Extracted ID from image: {car_id}")
            else:
                car_id = request.form.get("ID")
            start_gate = request.form.get("StartGate")

            start_date = datetime.now()
            car_type = car_id.split("_")[1]
            print(f"id :{car_id} , startgate:{start_gate}")

            print(f"start_gate: {start_gate}")
            min_key, end_gate, min_value = Calaulate_Lowest_Distance(
                start_gate, df_dict
            )
            print(f"min_key: {min_key}")
            print(f"end_gate: {end_gate}")

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

            socketio.emit(
                "violation_alert",
                {"message": f"New violation for car ID {car_id}", "car_id": car_id},
            )

        except Exception as e:
            sg.popup(f"Error occurred: {e}", icon=DIALOG_ERROR_ICON, title="ERROR")
            return render_template("index.html")

        else:
            return render_template("index.html")
    return render_template("index.html")


@app.route("/driver/<name>")
def driver_info(name):
    conn, cursor = DB_Connection(
        MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    )
    travel_info, violations = get_driver_info(name, conn, cursor)
    email, car_ids = get_driver_profile(name, conn, cursor)

    if travel_info:
        return render_template(
            "vehicle_info.html",
            driver_profile={"name": name, "email": email, "car_ids": car_ids},
            travel=travel_info,
            violation=violations,
        )
    else:
        return "Driver information not found."


@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        name = request.form["name"]
        password = hash_password(request.form["password"])
        types = check_type(email)
        if types == "admin":
            cursor.execute(
                "SELECT * FROM admins WHERE email = %s AND password = %s",
                (email, password),
            )
            user = cursor.fetchone()
            if user:
                session["user_id"] = user[0]
                return redirect(url_for("APP"))
            else:
                return "sorry! user not exists"
        elif types == "driver":
            cursor.execute(
                "SELECT * FROM drivers WHERE email = %s AND password = %s",
                (email, password),
            )
            user = cursor.fetchone()
            if user:
                session["user_id"] = user[0]
                return redirect(url_for("driver_info", name=name))
            else:
                return "sorry! user not exists"
        else:
            return "Invalid email or password. Please try again."

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("user_id", None)
    return redirect(url_for("login"))


@app.route("/paid")
def paid():
    return render_template("payment.html")


@app.route("/update", methods=["POST"])
def button():
    if request.method == "POST":
        start_date = request.form["button"]
        value = "paid"
        query = "UPDATE violations SET payment_status = %s WHERE Start_Date = %s"
        cursor.execute(query, (value, start_date))
        conn.commit()
        return redirect(url_for("paid"))
    return jsonify({"success": False, "message": "Invalid request method"})


@app.route("/register/driver", methods=["GET", "POST"])
def register_driver():
    if request.method == "POST":
        email = request.form["email"]
        name = request.form["name"]
        password = hash_password(request.form["password"])

        cursor.execute(
            "INSERT INTO drivers (email, name, password) VALUES (%s, %s, %s)",
            (email, name, password),
        )
        conn.commit()
        return redirect(url_for("login"))
    return render_template("register_driver.html")


@app.route("/register/admin", methods=["GET", "POST"])
def register_admin():
    if request.method == "POST":
        email = request.form["email"]
        name = request.form["name"]
        password = hash_password(request.form["password"])

        cursor.execute(
            "INSERT INTO admins (email, name, password) VALUES (%s, %s, %s)",
            (email, name, password),
        )
        conn.commit()
        return redirect(url_for("login"))
    return render_template("register_admin.html")


@app.route("/paid_violations")
def paid_violations():
    conn, cursor = DB_Connection(
        MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    )

    if conn and cursor:
        try:
            paid_violations = get_paid_violations(conn, cursor)

            if paid_violations:
                return render_template(
                    "paid_violations.html", paid_violations=paid_violations
                )
            else:
                return "No paid violations found."
        except Exception as e:
            print(f"Error fetching paid violations: {e}")
            return "An error occurred while fetching paid violations."
        finally:
            cursor.close()
            conn.close()
    else:
        return "Database connection failed."


if __name__ == "__main__":
    #spark_thread = threading.Thread(target=run_spark_job)
    #spark_thread.start()

    socketio.run(app, debug=True)
