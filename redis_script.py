import sys
import redis
import PySimpleGUI as sg

from Config.config import *

# Connect to Redis
r = redis.StrictRedis(host="localhost", port=6379, db=0)
sg.ChangeLookAndFeel("DarkGrey10")

# Read data from stdin (FlowFile content)
for line in sys.stdin:
    # Assuming each line contains a single word
    try:
        # Get Key from flow file
        word = line.strip()

        # Check if key is existing or not
        if r.exists(word):
            x = r.hgetall(word)
            decoded_data = {key.decode(): value.decode() for key, value in x.items()}
            print(f"{decoded_data}")
            sg.popup(
                "Travel is existing and sent successfully",
                title="Success",
                icon=EMBLEM_DEFAULT_ICON,
            )
        else:
            sg.popup(
                f"Sorry! ,it isn't exists (may be {word} is ran out or key isn't correct)",
                title="Oops!",
                icon=DIALOG_ERROR_ICON,
            )

    except Exception as e:
        print("Error:", e)
