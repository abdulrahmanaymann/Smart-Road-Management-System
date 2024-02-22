import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import *
import os
from Config.Logger import *
from Config.config import *

os.system("command " + STDERR_REDIRECT_COMMAND)
# Set HADOOP_HOME to PATH
os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["PATH"] += ";" + os.path.join(os.environ["HADOOP_HOME"], "bin")

findspark.init()
findspark.find()

spark = (
    SparkSession.builder.appName("Streaming from Kafka")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.shuffle.partitions", 4)
    .master("local[*]")
    .getOrCreate()
)
LOGGER.info("----------------- Spark Session Created -----------------")

# Set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# subscribe to the topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)
LOGGER.info("----------------- Data Frame Created -----------------")

""" # Consumer: {"ID": "123_Car", "Start Gate": "Cairo", "End Gate": "Giza", "Distance": 6.9}
schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Start Gate", StringType(), True),
        StructField("End Gate", StringType(), True),
        StructField("Distance", DoubleType(), True),
    ]
)

# Convert the value column to a string
df = df.withColumn("value", df["value"].cast(StringType()))
df = df.withColumn("value", from_json("value", schema))
df = df.select(col("value.*"))

query = df.writeStream.outputMode("append").format("console").start()
LOGGER.info("----------------- Streaming Started -----------------")

query.awaitTermination()

# Stop the Spark Session
spark.stop()
 """
