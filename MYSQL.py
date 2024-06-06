import mysql.connector
from Config.Logger import LOGGER


def DB_Connection(host, port, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=database
        )
        cursor = connection.cursor()

        if connection.is_connected():
            return connection, cursor
        else:
            LOGGER.error("Failed to connect to the database.")
            return None, None

    except mysql.connector.Error as err:
        LOGGER.error("Error: %s", err)
        return None, None
