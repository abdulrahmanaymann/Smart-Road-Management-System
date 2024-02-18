from MYSQL import DB_Connection

# ----------------- REDIS CONSTANTS ----------------- #
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# ----------------- EXCEL CONSTANTS ----------------- #
EXCEL_FILE = "New data.xlsx"
SHEET1 = "governorates"
SHEET2 = "vehicles"
SHEET3 = "travels"
SHEET4 = "all_governments"

# ----------------------------- KAFKA CONSTANTS -----------------------------#
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "travel_data"

# ----------------------------- MYSQL CONSTANTS -----------------------------#
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "py_test_db"
MYSQL_PORT = 3306

CONN = DB_Connection(
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

# ----------------- ICONS PATHS ----------------- #
ADD_TRAVEL_ICON = "ICONS\\add_travel.ico"
DIALOG_ERROR_ICON = "ICONS\\dialogerror.ico"
EMBLEM_DEFAULT_ICON = "ICONS\\emblemdefaul.ico"
MYSQL_ICON = "ICONS\\mysql.ico"
DOWNLOAD_ICON = "ICONS\\download.ico"
REDIS_ICON = "ICONS\\redis.ico"
GATE_ICON = "ICONS\\gate.ico"
MYSQL1_ICON = "ICONS\\mysql1.ico"
