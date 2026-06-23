from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, round as spark_round, when
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SilverTransform") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

poly_bronze = spark.read.format("delta").load("s3a://polymarket-weather/bronze/polymarket_events")

poly_window = Window.partitionBy("market_id").orderBy("event_time")

poly_silver = poly_bronze \
    .withColumn("prev_price", lag("yes_price", 1).over(poly_window)) \
    .withColumn("price_change", spark_round(col("yes_price") - col("prev_price"), 4)) \
    .withColumn("price_direction",
        when(col("price_change") > 0, "UP")
        .when(col("price_change") < 0, "DOWN")
        .otherwise("FLAT")
    ) \
    .withColumn("price_change_pct",
        spark_round(
            when(col("prev_price").isNotNull() & (col("prev_price") != 0),
                (col("yes_price") - col("prev_price")) / col("prev_price") * 100
            ).otherwise(None), 2
        )
    ) \
    .select(
        "event_time", "market_id", "yes_price",
        "prev_price", "price_change", "price_change_pct", "price_direction"
    ) \
    .dropna(subset=["prev_price"])

poly_silver.write.format("delta").mode("overwrite").save("s3a://polymarket-weather/silver/polymarket_events")
print(f"Polymarket silver rows: {poly_silver.count()}")
poly_silver.show(5)

weather_bronze = spark.read.format("delta").load("s3a://polymarket-weather/bronze/weather_forecasts")

weather_silver = weather_bronze \
    .withColumn("wind_severity",
        when(col("windSpeed") >= 33, "HURRICANE")
        .when(col("windSpeed") >= 17, "GALE")
        .when(col("windSpeed") >= 7, "MODERATE")
        .otherwise("CALM")
    ) \
    .withColumn("pressure_anomaly",
        spark_round(col("pressureSeaLevel") - 1013.25, 2)
    ) \
    .select(
        "event_time", "location_id", "forecast_time",
        "windSpeed", "windGust", "windDirection",
        "pressureSeaLevel", "pressure_anomaly",
        "humidity", "precipitationProbability",
        "temperature", "weatherCode", "wind_severity"
    )

weather_silver.write.format("delta").mode("overwrite").save("s3a://polymarket-weather/silver/weather_forecasts")
print(f"Weather silver rows: {weather_silver.count()}")
weather_silver.show(5)

print("Silver layer complete.")
