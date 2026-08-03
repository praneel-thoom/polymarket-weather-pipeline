import os
from pyspark.sql import SparkSession

BUCKET = os.environ["AWS_BUCKET"]

spark = SparkSession.builder \
    .appName("SilverCompaction") \
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

for table in ["polymarket_events", "weather_forecasts"]:
    path = f"s3a://{BUCKET}/silver/{table}"
    print(f"Compacting {path} ...")
    spark.sql(f"OPTIMIZE delta.`{path}`")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{path}` RETAIN 1 HOURS")
    print(f"Done: {path}")

spark.stop()
print("Compaction complete.")
