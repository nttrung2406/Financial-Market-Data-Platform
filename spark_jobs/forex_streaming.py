import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

DELTA_OUTPUT = "s3a://processed/forex"
CHECKPOINT = "s3a://processed/_checkpoints/forex"

FOREX_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("high_24h", DoubleType()),
    StructField("low_24h", DoubleType()),
    StructField("volume_24h", DoubleType()),
    StructField("quote_volume", DoubleType()),
    StructField("timestamp", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CryptoStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", "crypto_raw")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), FOREX_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("price") > 0)
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("ingested_at", current_timestamp())
        .drop("timestamp")
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema", "true")
        .partitionBy("symbol")
        .start(DELTA_OUTPUT)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
