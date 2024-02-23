import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import *
import os
from Config.Logger import *
from Config.config import *

# ----------------- Initialize and find Spark -----------------
findspark.init()
findspark.find()

# ----------------- Create SparkSession -----------------
spark = (
    SparkSession.builder.appName("Spark")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.jars", MYSQL_CONNECTOR_PATH)
    .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_PATH)
    .config("spark.sql.shuffle.partitions", 4)
    .master("local[*]")
    .getOrCreate()
)

# ----------------- Read travels table from MySQL -----------------
travels_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    .option("dbtable", "travels")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
)
travels_df.show()

# ----------------- Read violations table from MySQL -----------------
violations_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    .option("dbtable", "violations")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
)

violations_df.show()

# ----------------- Join the two tables violations id == travels id with travels start gate and end gate -----------------
"""
# Travels Data
 +-----+----------+--------+--------+
|   ID|Start_Gate|End_Gate|Distance|
+-----+----------+--------+--------+
|1_Car|     Cairo|    Giza|       7|
|1_Car|      Giza|Qalyubia|      32|
|2_Car|     Cairo|    Giza|       7|
|2_Car|      Giza|Qalyubia|      32|
|3_Car|     Cairo|    Giza|       7|
|3_Car|      Giza|Qalyubia|      32|
|4_Car|     Cairo|    Giza|       7|
|4_Car|      Giza|Qalyubia|      32|
|5_Car|     Cairo|    Giza|       7|
|5_Car|      Giza|Qalyubia|      32|
+-----+----------+--------+--------+

# Violations Data
+---------+-------------------+-------------------+
|   Car_ID|         Start_Date|           End_Date|
+---------+-------------------+-------------------+
|1_Car-QAL|2024-02-22 14:45:20|2024-02-22 14:46:43|
|1_Car-GIZ|2024-02-22 14:46:43|2024-02-22 14:46:48|
|2_Car-GIZ|2024-02-22 14:51:41|2024-02-22 14:51:45|
|3_Car-GIZ|2024-02-22 14:51:53|2024-02-22 14:51:58|
|4_Car-GIZ|2024-02-22 14:52:05|2024-02-22 14:52:09|
|5_Car-GIZ|2024-02-22 14:52:15|2024-02-22 14:52:20|
|22_Car-MNF|2024-02-22 15:51:52|2024-02-22 15:52:04|
+---------+-------------------+-------------------+ 

# Joined Data
+---------+----------+--------+-------------------+-------------------+
|   Car_ID|Start_Gate|End_Gate|         Start_Date|           End_Date|
+---------+----------+--------+-------------------+-------------------+
|1_Car-GIZ|   Giza| Qalyubia|2024-02-22 14:46:43|2024-02-22 14:46:48|
|2_Car-GIZ|   Giza| Qalyubia|2024-02-22 14:51:41|2024-02-22 14:51:45|
|3_Car-GIZ|   Giza| Qalyubia|2024-02-22 14:51:53|2024-02-22 14:51:58|
|4_Car-GIZ|   Giza| Qalyubia|2024-02-22 14:52:05|2024-02-22 14:52:09|
|5_Car-GIZ|   Giza| Qalyubia|2024-02-22 14:52:15|2024-02-22 14:52:20|
|22_Car-MNF| Monufia|Gharbia|2024-02-22 15:51:52|2024-02-22 15:52:04|
+---------+----------+--------+-------------------+-------------------+

Most violated route: Row(Start_Gate='Giza', End_Gate='Qalyubia', Violation_Count=5)

"""
""" 
joined_df = travels_df.join(
    violations_df, travels_df["ID"] == violations_df["Car_ID"], "inner"
)

result_df = joined_df.select(
    violations_df["Car_ID"],
    travels_df["Start_Gate"],
    travels_df["End_Gate"],
    violations_df["Start_Date"],
    violations_df["End_Date"],
)

result_df = result_df.orderBy(col("Start_Date"))

result_df.show()

# Group by route (start gate to end gate) and count violations for each route
route_violations_df = joined_df.groupBy("Start_Gate", "End_Gate").agg(
    count("*").alias("Violation_Count")
)
most_violated_route = route_violations_df.orderBy(col("Violation_Count").desc()).first()

LOGGER.info(f"Most violated route: {most_violated_route}") """

# ----------------- Read Kafka Stream -----------------
travels = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Consumer: {"ID": "123_Car", "Start Gate": "Cairo", "End Gate": "Giza", "Distance": 6.9}
schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Start Gate", StringType(), True),
        StructField("End Gate", StringType(), True),
        StructField("Distance", DoubleType(), True),
    ]
)

# Convert the value column to a string
travels = travels.withColumn("value", travels["value"].cast(StringType()))
travels = travels.withColumn("value", from_json("value", schema))
travels = travels.select(col("value.*"))
LOGGER.info("Travels Schema")

violations = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", VIOLATIONS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Consumer: {'ID': '30_Car-GIZ', 'Start Date': '2024-02-23 01:49:34', 'End Date': '2024-02-23 01:49:43'}
violations_schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Start Date", StringType(), True),
        StructField("End Date", StringType(), True),
    ]
)

violations = violations.withColumn("value", violations["value"].cast(StringType()))
violations = violations.withColumn("value", from_json("value", violations_schema))
violations = violations.select(col("value.*"))
LOGGER.info("Violations Schema")

# ----------------- Join the two streams -----------------
joined_stream = travels.join(violations, travels["ID"] == violations["ID"], "inner")

result_stream = joined_stream.select(
    violations["ID"],
    travels["Start Gate"],
    travels["End Gate"],
    violations["Start Date"],
    violations["End Date"],
)
LOGGER.info("Joined Stream Schema")

# ----------------- Write the joined stream to the console -----------------
query = result_stream.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
query.stop()
