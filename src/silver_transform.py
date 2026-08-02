import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

BUCKET = os.environ["AWS_BUCKET"]
BRONZE_POLY = f"s3a://{BUCKET}/bronze/polymarket_events"
BRONZE_WEATHER = f"s3a://{BUCKET}/bronze/weather_forecasts"
SILVER_POLY = f"s3a://{BUCKET}/silver/polymarket_events"
SILVER_WEATHER = f"s3a://{BUCKET}/silver/weather_forecasts"
CHECKPOINT_POLY = f"s3a://{BUCKET}/checkpoints/silver_polymarket_events"
CHECKPOINT_WEATHER = f"s3a://{BUCKET}/checkpoints/silver_weather_forecasts"

poly_silver_schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("market_id", StringType()),
    StructField("yes_price", DoubleType()),
    StructField("prev_price", DoubleType()),
    StructField("price_change", DoubleType()),
    StructField("price_change_pct", DoubleType()),
    StructField("price_direction", StringType())
])

spark = SparkSession.builder \
    .appName("SilverStream") \
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

# --- Cross-batch state for "previous price per market" ---
# foreachBatch runs on the driver of a single long-lived streaming query,
# so a plain dict here persists across micro-batches for the life of the job.
# On job restart we rebuild it from whatever is already in the silver table,
# so a redeploy doesn't produce a bogus first price_change.
last_price_state = {}

def load_last_prices():
    try:
        existing = spark.read.format("delta").load(SILVER_POLY)
        rows = (existing.groupBy("market_id")
                .agg({"event_time": "max"})
                .withColumnRenamed("max(event_time)", "event_time")
                .join(existing, ["market_id", "event_time"])
                .select("market_id", "yes_price")
                .collect())
        for r in rows:
            last_price_state[r["market_id"]] = r["yes_price"]
        print(f"Loaded last prices for {len(last_price_state)} market(s): {last_price_state}")
    except Exception as e:
        print(f"No existing silver/polymarket_events table yet, starting fresh: {e}")

load_last_prices()

def process_poly_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = batch_df.orderBy("event_time").collect()
    out_rows = []
    for r in rows:
        market_id = r["market_id"]
        yes_price = r["yes_price"]
        prev_price = last_price_state.get(market_id)
        if prev_price is not None:
            price_change = round(yes_price - prev_price, 4)
            price_change_pct = round((yes_price - prev_price) / prev_price * 100, 2) if prev_price != 0 else None
            price_direction = "UP" if price_change > 0 else "DOWN" if price_change < 0 else "FLAT"
            out_rows.append((r["event_time"], market_id, yes_price, prev_price,
                              price_change, price_change_pct, price_direction))
        last_price_state[market_id] = yes_price
    if not out_rows:
        return
    out_df = spark.createDataFrame(out_rows, schema=poly_silver_schema)
    out_df.write.format("delta").mode("append").save(SILVER_POLY)
    print(f"[poly batch {batch_id}] appended {out_df.count()} silver row(s)")

poly_bronze_stream = spark.readStream.format("delta").load(BRONZE_POLY)

poly_query = poly_bronze_stream.writeStream \
    .foreachBatch(process_poly_batch) \
    .option("checkpointLocation", CHECKPOINT_POLY) \
    .trigger(processingTime="30 seconds") \
    .start()

# --- Weather transform is stateless (no lag/window needed), so it can be a
# plain append streaming query straight from bronze to silver. ---
weather_bronze_stream = spark.readStream.format("delta").load(BRONZE_WEATHER)

weather_silver_stream = weather_bronze_stream \
    .withColumn("wind_severity",
        when(col("windSpeed") >= 33, "HURRICANE")
        .when(col("windSpeed") >= 17, "GALE")
        .when(col("windSpeed") >= 7, "MODERATE")
        .otherwise("CALM")
    ) \
    .withColumn("pressure_anomaly", spark_round(col("pressureSeaLevel") - 1013.25, 2)) \
    .select(
        "event_time", "location_id", "forecast_time",
        "windSpeed", "windGust", "windDirection",
        "pressureSeaLevel", "pressure_anomaly",
        "humidity", "precipitationProbability",
        "temperature", "weatherCode", "wind_severity"
    )

weather_query = weather_silver_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_WEATHER) \
    .trigger(processingTime="60 seconds") \
    .start(SILVER_WEATHER)

print("Silver streaming started: bronze -> silver, continuous.")
spark.streams.awaitAnyTermination()