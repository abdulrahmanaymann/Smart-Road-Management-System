import mysql.connector

def DB_Connection(host, port, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=database
        )
        cursor = connection.cursor()

        if connection.is_connected():
            cursor.execute("SELECT DATABASE() ;")
            db = cursor.fetchone()
            print(f"Connected to database {db} successfully")
            return connection, cursor
        else:
            print("Error: Failed to connect to the database.")
            return None, None

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None, None
