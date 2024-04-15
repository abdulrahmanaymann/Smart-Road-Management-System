from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from Config.config import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


spark = SparkSession.Builder().appName("MYSQL").getOrCreate()

# # Configure MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/py_test_db"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_username = "root"
jdbc_password = ""
mysql_delays_table = "delays"
mysql_travels_table = "travels"


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


def mysql_delays():
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", jdbc_driver)
        .option("dbtable", "delays")
        .option("user", jdbc_username)
        .option("password", jdbc_password)
        .load()
    )
    return df


delay = mysql_delays()
travel = mysql_travels()
print("DELAYS:::\n")
delay.show()
print("TRAVELS:::\n")
travel.show()


spark = SparkSession.Builder().appName("MYSQL").getOrCreate()

# # Configure MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/py_test_db"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_username = "root"
jdbc_password = ""
mysql_delays_table = "delays"
mysql_travels_table = "travels"


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


def mysql_delays():
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", jdbc_driver)
        .option("dbtable", "delays")
        .option("user", jdbc_username)
        .option("password", jdbc_password)
        .load()
    )
    return df


delay = mysql_delays()
travel = mysql_travels()
print("DELAYS:::\n")
delay.show()
# CSV
delay.write.csv(f"{OUTPUT_PATH}/delay.csv", mode="append", header=True)

print("TRAVELS:::\n")
travel.show()
# CSV
travel.write.csv(f"{OUTPUT_PATH}/travel.csv", mode="append", header=True)

delay = delay.selectExpr(
    "Car_ID as Delay_Car_ID",
    "Start_Gate as Delay_Start_Gate",
    "End_Gate as Delay_End_Gate",
    "Start_Date as Delay_Start_Date",
    "Arrival_End_Date as Delay_Arrival_End_Date"
)


travel = travel.selectExpr(
    "ID",
    "Start_Gate as Travel_Start_Gate",
    "End_Gate as Travel_End_Gate",
    "Distance",
    "Start_Travel_Date",
    "End_Travel_Date"
)

joined_df = (
    travel.join(delay, 
                (travel["Travel_Start_Gate"] == delay["Delay_Start_Gate"]) &
                (travel["Start_Travel_Date"] == delay["Delay_Start_Date"]), 
                "inner")
    .withColumn("Arrival_End_Date", unix_timestamp("Delay_Arrival_End_Date"))
    .withColumn("End_Travel_Date", unix_timestamp("End_Travel_Date"))
    .withColumn("time_seconds", col("Arrival_End_Date") - col("End_Travel_Date"))
    .withColumn("time_hours", expr("float((time_seconds) / 3600)"))
    .withColumn("time_minutes", expr("float((time_seconds % 3600) / 60)"))
    .withColumn("Arrival_End_Date", from_unixtime("Arrival_End_Date"))
    .withColumn("End_Travel_Date", from_unixtime("End_Travel_Date"))
)


joined_df.show()

# CSV
joined_df.write.csv(f"{OUTPUT_PATH}/joined_df.csv", mode="append", header=True)

spark.stop()


spark.stop()
