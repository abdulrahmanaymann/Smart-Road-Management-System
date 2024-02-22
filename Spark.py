import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    split,
    substring_index,
    countDistinct,
    trim,
    regexp_replace,
)
import os
import shutil
from Config.Logger import *
from Config.config import *

try:
    os.system("command " + STDERR_REDIRECT_COMMAND)
    findspark.init()
    findspark.find()

    # Set HADOOP_HOME to PATH
    os.environ["HADOOP_HOME"] = HADOOP_PATH
    os.environ["PATH"] += ";" + os.path.join(os.environ["HADOOP_HOME"], "bin")

    # Create SparkSession
    spark = (
        SparkSession.builder.appName("TestSparkSession")
        .config("spark.jars", MYSQL_CONNECTOR_PATH)
        .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_PATH)
        .config("spark.local.dir", TEMP_PATH)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Read travels table from MySQL as a streaming DataFrame
    travels_df = (
        spark.readStream.format("jdbc")
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        .option("dbtable", "travels")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )

    # Print streaming travels DataFrame
    travels_query = (
        travels_df.writeStream.format("console").outputMode("complete").start()
    )

    # Read violations table from MySQL as a streaming DataFrame
    violation_df = (
        spark.readStream.format("jdbc")
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        .option("dbtable", "violations")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )

    # Print streaming violation DataFrame
    violation_query = (
        violation_df.writeStream.format("console").outputMode("complete").start()
    )

    # ----------------- 1. Find the number of violations for each vehicle -----------------
    violations_count_df = violation_df.groupBy("Car_ID").agg(
        F.count("Car_ID").alias("Violations_Count")
    )

    # Print streaming violations count DataFrame
    violations_count_query = (
        violations_count_df.writeStream.format("console").outputMode("complete").start()
    )

    # ----------------- 2. Find the number of violations for each vehicle type -----------------
    violations_count_df = violation_df.withColumn(
        "Vehicle_Type", split(substring_index(col("Car_ID"), "_", -1), "-")[0]
    )
    violations_count_df = violations_count_df.groupBy("Vehicle_Type").agg(
        F.count("Car_ID").alias("Violations_Count")
    )

    # Print streaming violations count by vehicle type DataFrame
    violations_count_by_type_query = (
        violations_count_df.writeStream.format("console").outputMode("complete").start()
    )

    # ----------------- 3. Find the number of each start and end gate -----------------
    travels_df = travels_df.withColumn(
        "Start_Gate", trim(regexp_replace("Start_Gate", "[^a-zA-Z0-9 ]", ""))
    )
    start_gate_analysis = travels_df.groupBy("Start_Gate").agg(
        countDistinct("ID").alias("Start_Gate_Count")
    )

    # Print streaming start gate analysis DataFrame
    start_gate_analysis_query = (
        start_gate_analysis.writeStream.format("console").outputMode("complete").start()
    )

    # Wait for streaming queries to finish
    travels_query.awaitTermination()
    violation_query.awaitTermination()
    violations_count_query.awaitTermination()
    violations_count_by_type_query.awaitTermination()
    start_gate_analysis_query.awaitTermination()

except Exception as e:
    LOGGER.error(f"An error occurred: {str(e)}")
