import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import shutil
import os
from Config.config import *
from Config.Logger import *

# ---------------------------- This is program to Spark Streaming with MYSQL ---------------------------- #
try:
    os.system("command " + STDERR_REDIRECT_COMMAND)  # Redirecting the error to null
    findspark.init()  # Initializing the spark
    findspark.find()  # Finding the spark
    LOGGER.info("Spark Found Successfully")

    # Create SparkSession with kafka connector
    spark = (
        SparkSession.builder.appName("TestSparkSession")
        .config("spark.jars", KAFKA_CONNECTOR_PATH)
        .config("spark.driver.extraClassPath", KAFKA_CONNECTOR_PATH)
        .config("spark.local.dir", TEMP_PATH)
        .getOrCreate()
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Stop the spark session
    spark.stop()
    LOGGER.info("Spark Session Stopped Successfully")

    # Remove the temp directory
    shutil.rmtree(TEMP_PATH)
    LOGGER.info("Temp Directory Removed Successfully")

    LOGGER.info("Program Finished Successfully")

    LOGGER.info("------------------------------------------")

except Exception as e:
    LOGGER.error(f"Error: {e}")
