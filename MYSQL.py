import mysql.connector
from config import *

# from excel_reader import read_excel_sheets

# df_governorates, df_vehicles, df_travels = read_excel_sheets(EXCEL_FILE)

        # print(f"ID :{travel_id} ,num :{id} ,vehicle :{vehicle_type} ,end :{end_gate} ,start :{start_gate} ,distance :{distance}\n")

def DB_Connection(host, port, user, password, database):
    connection = mysql.connector.connect(host=host,
                                         port=port,
                                         user=user,
                                         password=password,
                                         database=database)
    Cursor = connection.cursor()
    if connection.is_connected():
        Cursor.execute("SELECT DATABASE() ;")
        db = Cursor.fetchone()
        print(f"connected to database {db} successfully")

    else:
        print("Error is just happened !!")

    return connection

# conn = DB_Connection(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD,
#                      MYSQL_DATABASE)
# Cursor = conn.cursor()
# try:
#     Cursor.execute("CREATE TABLE violations (Car_ID VARCHAR(255), Start_Date VARCHAR(255), End_Date VARCHAR(255))")

#     print("created success ")
# except Exception as e:
#     print("error :(")
# conn.commit()
# for _, row in df_travels.iterrows():
#     travel_id = row["ID"]
#     id = travel_id.split("_")[1]
#     vehicle_type = travel_id.split("_")[0]
#     end_gate = row["End Gate"]
#     distance = row["Distance (KM)"]
#     start_gate =row["Start Gate"]
#     query = f"INSERT INTO travels (ID ,vehicle ,start_gate ,end_gate) VALUES (%s ,%s ,%s ,%s)"
#     value= [(id , vehicle_type ,start_gate ,end_gate)]
#     Cursor.executemany(query , value)    

# conn.commit()

# res = Cursor.fetchall()

# for i in res:
#     print("ID",i[0])
#     print("name",i[1] ,"\n")

# DB_Connection(MYSQL_HOST, MYSQL_PORT, MYSQL_USER ,MYSQL_PASSWORD ,MYSQL_DATABASE)