import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, sum
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


# ** ----------------- Read from MYSQL -----------------
def read_from_mysql(table_name):
    df = (
        spark.read.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table_name)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    ).cache()

    return df


violations_df = read_from_mysql(VIOLATIONS)
violations_df.show()

route_violations = (  # ** count the number of violations for each route
    violations_df.groupBy("Start_Gate", "End_Gate")
    .count()
    .orderBy("count", ascending=False)
    .cache()
)
route_violations.show()

vehicle_violations = (  # ** count the number of violations for each vehicle type
    violations_df.withColumn("Vehicle_Type", F.split(violations_df["Car_ID"], "_|-")[1])
    .groupBy("Vehicle_Type")
    .count()
    .orderBy("count", ascending=False)
    .cache()
)

vehicle_violations.show()


# ** ----------------- Read from KAFKA -----------------
schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Start Gate", StringType(), True),
        StructField("End Gate", StringType(), True),
        StructField("Start Date", StringType(), True),
        StructField("End Date", StringType(), True),
    ]
)

violations_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", VIOLATIONS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json("value", schema).alias("data"))
    .select("data.*")
)

# ** ----------------- Count the number of violations for each route  -----------------
joined_df_stream = (
    violations_stream_df.join(
        route_violations,
        (violations_stream_df["Start Gate"] == route_violations["Start_Gate"])
        & (violations_stream_df["End Gate"] == route_violations["End_Gate"]),
        "left",
    )
    .withColumn(
        "count", F.when(col("ID").isNotNull(), col("count") + 1).otherwise(col("count"))
    )
    .select("Start Gate", "End Gate", "count")
)

# ** ----------------- First Query -----------------
query1 = (
    joined_df_stream.writeStream.outputMode("update")
    .format("console")
    .foreachBatch(
        lambda df, epoch_id: df.foreach(
            lambda row: print(
                f"Epoch ID: {epoch_id},Start Gate: {row['Start Gate']}, End Gate: {row['End Gate']}, Count: {row['count']}"
            )
        )
    )
    .option("truncate", "false")
    .start()
    .awaitTermination()
)

# ** ----------------- Count the number of violations for each vehicle type -----------------
joined_df_stream2 = (
    violations_stream_df.join(
        vehicle_violations,
        (
            F.split(violations_stream_df["ID"], "_|-")[1]
            == vehicle_violations["Vehicle_Type"]
        ),
        "left",
    )
    .withColumn(
        "count", F.when(col("ID").isNotNull(), col("count") + 1).otherwise(col("count"))
    )
    .select("Start Gate", "End Gate", "Vehicle_Type", "count")
)


# ** ----------------- Second Query -----------------
query2 = (
    joined_df_stream2.writeStream.outputMode("update")
    .format("console")
    .foreachBatch(
        lambda df, epoch_id: df.foreach(
            lambda row: print(
                f"Epoch ID: {epoch_id}, Route: {row['Start Gate']} to {row['End Gate']}, Vehicle Type: {row['Vehicle_Type']}, Violations Count: {row['count']}"
            )
        )
    )
    .option("truncate", "false")
    .start()
    .awaitTermination()
)
