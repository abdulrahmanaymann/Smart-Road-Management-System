from math import e
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, split, expr
from Config.config import *
from Config.Logger import *

# ** ----------------- Init and find Spark -----------------
findspark.init()
findspark.find()

# ** ----------------- Create SparkSession -----------------
spark = (
    SparkSession.builder.appName("Spark")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.jars", MYSQL_CONNECTOR_PATH)
    .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_PATH)
    .config("spark.sql.shuffle.partitions", 8)
    .master("local[*]")
    .getOrCreate()
)


# ** ----------------- Read tables from MySQL -----------------
def read_mysql_table(table_name):
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        .option("dbtable", table_name)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )


travels_df = read_mysql_table("travels").dropDuplicates().cache()
violations_df = read_mysql_table("violations").dropDuplicates().cache()

# ** ----------------- Join the two tables -----------------
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

# ** ----------------- Count the number of violations for each route ( MYSQL ) -----------------
route_violations = (
    joined_df.groupBy("Start_Gate", "End_Gate")
    .count()
    .orderBy("count", ascending=False)
    .cache()
)

# ** ----------------- Read Kafka Stream -----------------
violations_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", VIOLATIONS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

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

# ** ----------------- Count the number of violations for each route  -----------------
joined_df_stream = violations_stream_df.join(
    route_violations,
    (violations_stream_df["Start Gate"] == route_violations["Start_Gate"])
    & (violations_stream_df["End Gate"] == route_violations["End_Gate"]),
    "inner",
).select(
    violations_stream_df["Start Gate"],
    violations_stream_df["End Gate"],
    (route_violations["count"]).alias("Total Violations"),
)

# ** ----------------- Count the number of violations for each route  -----------------
joined_df = joined_df.withColumn("Vehicle_Type", split(joined_df["Car_ID"], "_")[1])
joined_df = joined_df.withColumn(
    "Vehicle_Type", split(joined_df["Vehicle_Type"], "-")[0]
)

route_vehicle_violations = (
    (
        joined_df.groupBy("Start_Gate", "End_Gate", "Vehicle_Type")
        .count()
        .orderBy("count", ascending=False)
    )
    .dropDuplicates(["Start_Gate", "End_Gate"])
    .cache()
)

stream_joined_df = violations_stream_df.join(
    route_vehicle_violations,
    (violations_stream_df["Start Gate"] == route_vehicle_violations["Start_Gate"])
    & (violations_stream_df["End Gate"] == route_vehicle_violations["End_Gate"]),
    "inner",
).select(
    violations_stream_df["Start Gate"],
    violations_stream_df["End Gate"],
    route_vehicle_violations["Vehicle_Type"],
    route_vehicle_violations["count"],
)

# ** ----------------- The First Query -----------------
try:
    query1 = (
        joined_df_stream.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query1.awaitTermination()
except Exception as e:
    LOGGER.error(f"Error in Query1: {e}")

# ** ----------------- The Second Query -----------------
total_violations = violations_df.count()  # Count the total number of violations

try:
    query2 = (
        violations_stream_df.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .foreachBatch(
            lambda df, epoch_id: print(
                f"Batch: {epoch_id}, Total Violations: {df.count() + total_violations}"
            )
        )
        .start()
    )

    query2.awaitTermination()
except Exception as e:
    LOGGER.error(f"Error in Query2: {e}")

# ** ----------------- The Third Query -----------------
try:
    query3 = (
        stream_joined_df.writeStream.outputMode("update")
        .format("console")
        .foreachBatch(
            lambda df, epoch_id: df.foreach(
                lambda row: print(
                    f"Epoch ID: {epoch_id}, Route: {row['Start Gate']} to {row['End Gate']}, Vehicle Type: {row['Vehicle_Type']}, Count: {row['count']}"
                )
            )
        )
        .option("truncate", "false")
        .start()
    )

    query3.awaitTermination()
except Exception as e:
    LOGGER.error(f"Error in Query3: {e}")
