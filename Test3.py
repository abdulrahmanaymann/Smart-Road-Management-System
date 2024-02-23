import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import *
import os
from Config.Logger import *
from Config.config import *

# ----------------- Init and find Spark -----------------
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

# ----------------- Join the two tables -----------------
joined_df = (
    travels_df.join(violations_df, travels_df["ID"] == violations_df["Car_ID"], "inner")
    .select(
        violations_df["Car_ID"],
        travels_df["Start_Gate"],
        travels_df["End_Gate"],
        violations_df["Start_Date"],
        violations_df["End_Date"],
    )
    .orderBy(violations_df["Start_Date"])
)

joined_df.show()

# ----------------- Count the number of violations for each route -----------------
route_violations = (
    joined_df.groupBy("Start_Gate", "End_Gate")
    .count()
    .orderBy("count", ascending=False)
)

route_violations.show()

# ----------------- Read Kafka Stream -----------------
violations_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", VIOLATIONS_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Consumer: {'ID': '70_Car-GIZ', 'Start Gate': 'Giza', 'End Gate': 'Qalyubia', 'Start Date': '2024-02-23 14:52:59', 'End Date': '2024-02-23 14:53:06'}
violations_schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Start Gate", StringType(), True),
        StructField("End Gate", StringType(), True),
        StructField("Start Date", StringType(), True),
        StructField("End Date", StringType(), True),
    ]
)

violations_stream_df = (
    violations_stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json("value", violations_schema).alias("data"))
    .select("data.*")
)

# ----------------- Count the number of violations for each route and print start gate end gate count -----------------
route_violations_stream = (
    violations_stream_df.groupBy("Start Gate", "End Gate")
    .count()
    .orderBy("count", ascending=False)
)

query = (
    route_violations_stream.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()







""" 
# Violations count for each route from MYSQL
+----------+--------+-----+
|Start_Gate|End_Gate|count|
+----------+--------+-----+
|      Giza|Qalyubia|   33|
|   Monufia| Gharbia|    1|
+----------+--------+-----+

# Violations count for each route from Kafka stream
-------------------------------------------
Batch: 1
-------------------------------------------
+----------+--------+-----+
|Start Gate|End Gate|count|
+----------+--------+-----+
|Giza      |Qalyubia|1    |
+----------+--------+-----+ 
"""






















