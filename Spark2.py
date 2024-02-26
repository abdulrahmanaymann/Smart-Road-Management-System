from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# Create SparkSession
spark = SparkSession.builder.appName("MySQLStreamProcessing").getOrCreate()

# Configure MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/py_test_db"
jdbc_driver = "com.mysql.jdbc.Driver"
jdbc_username = "root"
jdbc_password = ""


# Define function to read data from MySQL
def mysql_travels():
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", jdbc_driver)
        .option("dbtable", "travels")
        .option("user", jdbc_username)
        .option("password", jdbc_password)
        .load()
    )
    return df


def mysql_violations():
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", jdbc_driver)
        .option("dbtable", "violations")
        .option("user", jdbc_username)
        .option("password", jdbc_password)
        .load()
    )
    return df


violations_df = mysql_violations()


def Caculate_violated_routes(violation_df):
    count_violation = (
        violation_df.groupBy("Start_Gate", "End_Gate")
        .count()
        .orderBy(col("count").desc())
        .select("Start_gate", "End_gate", "count")
        .first()
    )
    print(
        f"Maximum violated routes from [ {count_violation['Start_gate']} ] >>----to----> [ {count_violation['End_gate']} ] with {count_violation['count']} time(s)"
    )


def Caculate_violated_vehichles(violation_df):
    id_df = violation_df.withColumn(
        "number of vehichles", split(violation_df["Car_ID"], "_")[1]
    )
    # Count the frequncy of each car
    car_counts = (
        id_df.groupBy("number of vehichles")
        .count()
        .orderBy(col("count").desc())
        .select("number of vehichles", "count")
        .first()
    )
    print(
        f"\nMaximum violated vehichles with type [ {car_counts['number of vehichles']} ] with {car_counts['count']} time(s)\n"
    )


previous_data = None


# Define a function to read data from JDBC source and print only when changed
def read_violation(violation):
    global previous_data
    # Read data from JDBC source into a DataFrame
    df = violation
    # Convert DataFrame to pandas DataFrame for easy comparison
    current_data = df.toPandas()
    # Compare current data with previous data
    if previous_data is None or not current_data.equals(previous_data):
        # Data has changed or it's the first fetch
        print("Data has changed:")
        Caculate_violated_routes(violation)
        Caculate_violated_vehichles(violation)
        # Update previous_data with current_data
        previous_data = current_data
    else:
        print("Data has not changed.")


while True:
    read_violation(violations_df)

    # Sleep for a certain duration before polling again
    sleep(10)  # Adjust the sleep duration as needed

# # Stop the SparkSession
spark.stop()
