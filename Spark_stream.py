from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json

from Config.config import *
from Config.Logger import *


checkpoint_location = "output/checkpoints"
checkpoint_location_violations = "output/checkpoints/violations"
checkpoint_location_travels = "output/checkpoints/travels"
checkpoint_location_delays = "output/checkpoints/delays"


def run_spark_job():
    spark = (
        SparkSession.builder.appName("Spark")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .config("spark.jars", MYSQL_CONNECTOR_PATH)
        .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_PATH)
        .config("spark.sql.shuffle.partitions", 8)
        .master("local[*]")
        .getOrCreate()
    )

    def read_from_mysql(table_name):
        df = (
            spark.read.format("jdbc")
            .option("url", MYSQL_JDBC_URL)
            .option("dbtable", table_name)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .load()
        ).cache()

        return df

    violations_df = read_from_mysql(VIOLATIONS)
    travels_df = read_from_mysql(TRAVELS)
    delays_df = read_from_mysql(DELAYS)

    #travels_df.show()
    #violations_df.show()
    #delays_df.show()

    """     #!! for test purposes only
        def send_to_kafka_topic(df, topic):
            kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            rows = df.collect()
            for row in rows:
                kafka_producer.send(topic, str(row).encode("utf-8"))
            kafka_producer.flush()
            kafka_producer.close()

        send_to_kafka_topic(violations_df, VIOLATIONS_TOPIC)
        send_to_kafka_topic(travels_df, KAFKA_TOPIC)
        send_to_kafka_topic(delays_df, DELAYS_TOPIC)  """

    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Start Gate", StringType(), True),
            StructField("End Gate", StringType(), True),
            StructField("Start Date", StringType(), True),
            StructField("End Date", StringType(), True),
        ]
    )

    delays_schema = StructType(
        [
            StructField("Car_ID", StringType(), True),
            StructField("Start_Gate", StringType(), True),
            StructField("End_Gate", StringType(), True),
            StructField("Start_Date", StringType(), True),
            StructField("Arrival_End_Date", StringType(), True),
        ]
    )

    violations_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", VIOLATIONS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) ", "CAST(timestamp AS STRING)")
        .select(from_json("value", schema).alias("data"), "timestamp")
        .select("data.*", "timestamp")
    )

    query1 = (
        violations_stream_df.writeStream.format("json")
        .outputMode("append")
        .option("path", "output/stream")
        .option("checkpointLocation", checkpoint_location_violations)
        .option("failOnDataLoss", "false")
        .start()
    )

    travels_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) ", "CAST(timestamp AS STRING)")
        .select(from_json("value", schema).alias("data"), "timestamp")
        .select("data.*", "timestamp")
    )

    query2 = (
        travels_stream_df.writeStream.format("json")
        .outputMode("append")
        .option("path", "output/stream2")
        .option("checkpointLocation", checkpoint_location_travels)
        .option("failOnDataLoss", "false")
        .start()
    )

    delays_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", DELAYS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) ", "CAST(timestamp AS STRING)")
        .select(from_json("value", delays_schema).alias("data"), "timestamp")
        .select("data.*", "timestamp")
    )

    query3 = (
        delays_stream_df.writeStream.format("json")
        .outputMode("append")
        .option("path", "output/stream3")
        .option("checkpointLocation", checkpoint_location_delays)
        .option("failOnDataLoss", "false")
        .start()
    )

    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()

    spark.stop()

""" if __name__ == "__main__":
    run_spark_job() """