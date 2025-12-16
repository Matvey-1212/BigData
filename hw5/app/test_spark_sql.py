from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLJoin").getOrCreate()

flights = spark.read.csv("./data/flights.csv", header=True, inferSchema=True)
carriers = spark.read.csv("./data/airports.csv", header=True, inferSchema=True)

flights.createOrReplaceTempView("flights")
carriers.createOrReplaceTempView("airports")

spark.sql("""
    SELECT
        a1.name AS origin_airport,
        a2.name AS destination_airport,
        ROUND(AVG(f.DepDelay),2) AS avg_dep_delay
    FROM flights f
    JOIN airports a1 ON f.OriginAirportID = a1.airport_id
    JOIN airports a2 ON f.DestAirportID = a2.airport_id
    WHERE f.DepDelay IS NOT NULL
    GROUP BY a1.name, a2.name
    HAVING AVG(f.DepDelay) > 30
    ORDER BY avg_dep_delay DESC;
""").show(
    n=10,
    truncate=False,
    vertical=False
)

spark.stop()
