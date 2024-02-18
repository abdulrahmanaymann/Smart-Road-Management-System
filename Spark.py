from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MySQL Spark Streaming") \
    .getOrCreate()

# Configure MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "py_test_db"
MYSQL_PORT = 3306

# Define the JDBC URL
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

# Define the connection properties
connection_properties = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.jdbc.Driver"
}

# Read data from MySQL table using Spark Streaming
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "travels") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.jdbc.Driver") \
    .load()

# Perform further transformations or actions on the DataFrame
# For example, you can print the schema and show the data
df.printSchema()
df.show()

# Start the Spark Streaming job
# ...

# Stop the SparkSession
spark.stop()