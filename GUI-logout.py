from datetime import datetime
import PySimpleGUI as sg
import redis
from config import *
from MYSQL import *

# Establish a connection to the Redis server
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# GUI (Logout)
layout = [[sg.Text('Enter your search query:'),
           sg.InputText()], [sg.Button('Search'),
                             sg.Button('Cancel')]]
window = sg.Window('Search in Redis', layout)

while True:
    event, values = window.read()
    if event == 'Search':
        value = values[0]
        # key = r.keys(value)
        # Do something with the search query
        print(f'Searching for "{value}"...')
        if value is not None:
            if r.exists(value):
                button = sg.popup(f'fortunately, The {value} is found',
                                  custom_text='Violated',
                                  modal=True)
                if button == 'Violated':
                    print(value + "\n")

                    data = r.hgetall(value)

                    decoded_data = {
                        key.decode(): value.decode()
                        for key, value in data.items()
                    }
                    key = list(decoded_data.keys())
                    val = list(decoded_data.values())
                    str = str(val[0])
                    Car_ID = str.split('-')
                    CarID= Car_ID[0]
                    start_date = val[1]
                    end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Car_ID : {CarID}\t Start_date : {start_date}\t End_date : {end_date}")
                    conn = DB_Connection(MYSQL_HOST, MYSQL_PORT, MYSQL_USER,
                                         MYSQL_PASSWORD, MYSQL_DATABASE)
                    Cursor = conn.cursor()
                    query = 'INSERT INTO violations (Car_ID, Start_Date, End_Date) VALUES (%s, %s ,%s)'
                    Values =[(CarID, start_date, end_date)]
                    Cursor.executemany(query, Values)
                    conn.commit()
                    print("DATA INSERTED SUCCESSFULLY :)")
                    sg.popup('Violations inserted to DataBase Successfully')
                else:
                    print("no action")
            else:
                sg.popup(f'unfortunately, The {value} is not found')
        else:
            sg.popup('Please, Enter key to search')

        continue
    if event == sg.WIN_CLOSED or event == 'Cancel':
        # Exit the program
        break

window.close()