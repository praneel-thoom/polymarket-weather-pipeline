import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("BackfillIngest") \
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

weather_backfill = spark.read \
    .schema(weather_schema) \
    .json("data/backfill_weather.jsonl") \
    .withColumn("event_time", to_timestamp(col("ts")))

weather_backfill.write \
    .format("delta") \
    .mode("append") \
    .save(f"s3a://{os.environ['AWS_BUCKET']}/bronze/weather_forecasts")

print(f"Appended {weather_backfill.count()} weather backfill rows to bronze")
print("Backfill ingest complete.")
