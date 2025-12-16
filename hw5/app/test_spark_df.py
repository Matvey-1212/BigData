from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder.appName("CSVStats").getOrCreate()

df = spark.read.csv("./data/flights.csv", header=True, inferSchema=True)

df.groupBy("DayofMonth").agg(
    count("*").alias("rows"),
    avg("DepDelay").alias("avg_DepDelay")
).orderBy("DayofMonth").show(
    n=10,
    truncate=False,
    vertical=False
)

spark.stop()
