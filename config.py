from MYSQL import DB_Connection

# ----------------- Global Constants ----------------- #
# Redis Constants
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Excel Sheets Constants
EXCEL_FILE = "Data/New data.xlsx"
SHEET1 = "governorates"
SHEET2 = "vehicles"
SHEET3 = "travels"
SHEET4 = "all_governments"

# Kafka Constants
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "travel_data"

# MYSQL Constants
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
