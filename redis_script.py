import sys
import redis
import PySimpleGUI as sg
from Config.config import *
from data_handler import *
from excel_reader import read_excel_sheets

r = redis.StrictRedis(host="localhost", port=6379, db=0)
p = get_kafka_producer(KAFKA_BROKER)

df_governorates, df_vehicles, df_travels, df_all_government = read_excel_sheets(
    EXCEL_FILE
)

for line in sys.stdin:
    try:
        word = line.strip()

        id = word.split(",")[0].split(":")[1]
        start_Gate = word.split(",")[1].split(":")[1]
        end_Gate = word.split(",")[2].split(":")[1]
        distance = word.split(",")[3].split(":")[1]
        start_date = word.split(",")[4].split(": ")[1]

        To_Nifi(r, df_governorates, id, start_Gate, end_Gate, distance, start_date)

    except Exception as e:
        print("Error:", e)
