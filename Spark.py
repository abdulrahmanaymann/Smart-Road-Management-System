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
    .config("spark.sql.shuffle.partitions", 4)
    .master("local[*]")
    .getOrCreate()
)

# ** ----------------- Read travels table from MySQL -----------------
travels_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    .option("dbtable", "travels")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
).dropDuplicates()

# // travels_df.show()
a = travels_df.count()
# // print(f"Number of records ( Travels ): {a}")

# ** ----------------- Read violations table from MySQL -----------------
violations_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    .option("dbtable", "violations")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
).dropDuplicates()

# // violations_df.show()
b = violations_df.count()
# // print(f"Number of records ( Violations ): {b}")

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
).dropDuplicates()

c = joined_df.count()
# // print(f"Number of records ( Joind ): {c}")
# // joined_df.show()

# ** ----------------- Count the number of violations for each route ( MYSQL ) -----------------
route_violations = (
    joined_df.groupBy("Start_Gate", "End_Gate")
    .count()
    .orderBy("count", ascending=False)
)

# // route_violations.show()

# ** ----------------- Read Kafka Stream -----------------
violations_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", VIOLATIONS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

""" 
Consumer: {'ID': '70_Car-GIZ', 'Start Gate': 'Giza', 'End Gate': 'Qalyubia', 'Start Date': '2024-02-23 14:52:59', 'End Date': '2024-02-23 14:53:06'}
"""
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

# // joined_df_stream.show()

# ** ----------------- The First Query -----------------
""" query = (
    joined_df_stream.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .foreachBatch(
        lambda df, epoch_id: print(
            f"Batch: {epoch_id}, Most Violated Route: {df.orderBy('Total Violations', ascending=False).first()['Start Gate']} to {df.orderBy('Total Violations', ascending=False).first()['End Gate']} with {df.orderBy('Total Violations', ascending=False).first()['Total Violations']} violations"
        )
    )
    .start()
)

query.awaitTermination()
spark.stop()
 """

# ** ----------------- Count the total number of violations -----------------
total_violations = violations_df.count()
# // print(f"Total Violations: {total_violations}")

# ** ----------------- The Second Query -----------------
""" query = (
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

query.awaitTermination()
spark.stop() """

# ** ----------------- Count the number of violations for each route  -----------------
joined_df = joined_df.withColumn("Vehicle_Type", split(joined_df["Car_ID"], "_")[1])
joined_df = joined_df.withColumn(
    "Vehicle_Type", split(joined_df["Vehicle_Type"], "-")[0]
)

route_vehicle_violations = (
    joined_df.groupBy("Start_Gate", "End_Gate", "Vehicle_Type")
    .count()
    .orderBy("count", ascending=False)
).dropDuplicates(["Start_Gate", "End_Gate"])

# // route_vehicle_violations.show()

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

# ** ----------------- The Third Query -----------------
query = (
    stream_joined_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
