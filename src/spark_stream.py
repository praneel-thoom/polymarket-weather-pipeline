from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

polymarket_schema = StructType([
    StructField("ts", StringType()),
    StructField("market_id", StringType()),
    StructField("token_id", StringType()),
    StructField("yes_price", DoubleType())
])

weather_schema = StructType([
    StructField("ts", StringType()),
    StructField("location_id", StringType()),
    StructField("forecast_time", StringType()),
    StructField("windSpeed", DoubleType()),
    StructField("windGust", DoubleType()),
    StructField("windDirection", DoubleType()),
    StructField("pressureSeaLevel", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("precipitationProbability", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("weatherCode", IntegerType())
])

spark = SparkSession.builder \
    .appName("PolymarketWeatherStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

polymarket_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "polymarket_events") \
    .option("startingOffsets", "earliest") \
    .load()

polymarket_df = polymarket_raw \
    .select(from_json(col("value").cast("string"), polymarket_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("ts"))) \
    .withWatermark("event_time", "10 minutes")

weather_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_forecasts") \
    .option("startingOffsets", "earliest") \
    .load()

weather_df = weather_raw \
    .select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("ts"))) \
    .withWatermark("event_time", "10 minutes")

polymarket_query = polymarket_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/polymarket_bronze") \
    .start("data/bronze/polymarket_events")

weather_query = weather_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/weather_bronze") \
    .start("data/bronze/weather_forecasts")

print("Spark streaming started. Writing to Delta Lake bronze layer...")
spark.streams.awaitAnyTermination()
