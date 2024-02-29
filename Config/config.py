import os
from MYSQL import DB_Connection

# ----------------- REDIS CONSTANTS ----------------- #
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# ----------------- EXCEL CONSTANTS ----------------- #
EXCEL_FILE = "D:\\GP\\graduation project\\GP\\Data\\New data.xlsx"
SHEET1 = "governorates"
SHEET2 = "vehicles"
SHEET3 = "travels"
SHEET4 = "all_governments"

# ----------------------------- KAFKA CONSTANTS -----------------------------#
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "travel_data"
VIOLATIONS_TOPIC = "violations"

# ----------------------------- MYSQL CONSTANTS -----------------------------#
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "py_test_db"
MYSQL_PORT = 3306
TRAVELS = "travels"
VIOLATIONS = "violations"

CONN = DB_Connection(
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

# ----------------------------- SPARK CONSTANTS -----------------------------#
STDERR_REDIRECT_COMMAND = "2>/dev/null" if os.name == "posix" else "2>nul"
HADOOP_PATH = "C:\\hadoop"
TEMP_PATH = "d:\\tmp"
TEMP = "C:\\Users\\abdel\\AppData\\Local\\Temp\\spark-93aadcea-55e8-42ad-b06b-51056bf0ee11\\pyspark-35748fd3-1191-4155-a195-16188d767d27"
MYSQL_CONNECTOR_PATH = (
    "C:\\Spark\\spark-3.5.0-bin-hadoop3\\jars\\mysql-connector-j-8.3.0.jar"
)

KAFKA_CONNECTOR_PATH = (
    "C:\\Spark\\spark-3.5.0-bin-hadoop3\\jars\\spark-sql-kafka-0-10_2.12:3.5.0.jar"
)

SPARK_SQL_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

# ----------------- ICONS PATHS ----------------- #
ADD_TRAVEL_ICON = "ICONS\\add_travel.ico"
DIALOG_ERROR_ICON = "ICONS\\dialogerror.ico"
EMBLEM_DEFAULT_ICON = "ICONS\\emblemdefaul.ico"
MYSQL_ICON = "ICONS\\mysql.ico"
DOWNLOAD_ICON = "ICONS\\download.ico"
REDIS_ICON = "ICONS\\redis.ico"
GATE_ICON = "ICONS\\gate.ico"
MYSQL1_ICON = "ICONS\\mysql1.ico"
