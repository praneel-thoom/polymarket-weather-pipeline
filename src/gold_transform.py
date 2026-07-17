import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs, round as spark_round, when, unix_timestamp, avg, count
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("GoldTransform") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ['AWS_REGION']}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_KEY"]) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

poly_silver = spark.read.format("delta").load(f"s3a://{os.environ['AWS_BUCKET']}/silver/polymarket_events")
weather_silver = spark.read.format("delta").load(f"s3a://{os.environ['AWS_BUCKET']}/silver/weather_forecasts")

significant_moves = poly_silver \
    .filter(spark_abs(col("price_change_pct")) > 5) \
    .withColumn("abs_change_pct", spark_round(spark_abs(col("price_change_pct")), 2)) \
    .select(
        "event_time", "market_id", "yes_price",
        "price_change", "price_change_pct", "abs_change_pct", "price_direction"
    ) \
    .orderBy("event_time")

significant_moves.write.format("delta").mode("overwrite").save(f"s3a://{os.environ['AWS_BUCKET']}/gold/significant_moves")
print(f"Significant price moves: {significant_moves.count()}")
significant_moves.show(10)

poly_with_window = poly_silver \
    .withColumn("window_ts", (unix_timestamp("event_time") / 1800).cast("long") * 1800)

weather_with_window = weather_silver \
    .withColumn("window_ts", (unix_timestamp("event_time") / 1800).cast("long") * 1800)

poly_agg = poly_with_window.groupBy("window_ts", "market_id").agg(
    avg("yes_price").alias("avg_yes_price"),
    count("*").alias("tick_count")
)

weather_agg = weather_with_window.groupBy("window_ts", "location_id").agg(
    avg("windSpeed").alias("avg_wind_speed"),
    avg("pressureSeaLevel").alias("avg_pressure"),
    avg("humidity").alias("avg_humidity"),
    avg("precipitationProbability").alias("avg_precip_prob")
)

market_weather = poly_agg.join(weather_agg, on="window_ts", how="inner") \
    .withColumn("avg_yes_price", spark_round(col("avg_yes_price"), 4)) \
    .withColumn("avg_wind_speed", spark_round(col("avg_wind_speed"), 2)) \
    .withColumn("avg_pressure", spark_round(col("avg_pressure"), 2)) \
    .withColumn("avg_humidity", spark_round(col("avg_humidity"), 1)) \
    .withColumn("avg_precip_prob", spark_round(col("avg_precip_prob"), 1)) \
    .orderBy("window_ts", "location_id")

market_weather.write.format("delta").mode("overwrite").save(f"s3a://{os.environ['AWS_BUCKET']}/gold/market_weather_signals")
print(f"Market weather signal rows: {market_weather.count()}")
market_weather.show(10)

print("Gold layer complete.")
