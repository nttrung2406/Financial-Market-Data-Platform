import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, lit, row_number,
    when, lag, greatest,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "finance")
PG_USER = os.getenv("POSTGRES_USER", "finance")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "finance")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("DeltaToPostgres")
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


def compute_moving_averages(df):
    """Add MA5, MA20, MA50 columns using a time-ordered window per symbol."""
    w5 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-4, 0)
    w20 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-19, 0)
    w50 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-49, 0)
    return (
        df
        .withColumn("ma5", avg("price").over(w5))
        .withColumn("ma20", avg("price").over(w20))
        .withColumn("ma50", avg("price").over(w50))
    )


def compute_rsi(df, period: int = 14):
    """Approximate RSI-14 using Wilder's method over a row-based window."""
    w = Window.partitionBy("symbol").orderBy("event_time")
    df = df.withColumn("prev_price", lag("price", 1).over(w))
    df = df.withColumn("change", col("price") - col("prev_price"))
    df = df.withColumn("gain", when(col("change") > 0, col("change")).otherwise(lit(0.0)))
    df = df.withColumn("loss", when(col("change") < 0, -col("change")).otherwise(lit(0.0)))

    wRsi = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-period + 1, 0)
    df = df.withColumn("avg_gain", avg("gain").over(wRsi))
    df = df.withColumn("avg_loss", avg("loss").over(wRsi))
    df = df.withColumn(
        "rsi",
        when(col("avg_loss") == 0, lit(100.0))
        .otherwise(100.0 - (100.0 / (1.0 + col("avg_gain") / greatest(col("avg_loss"), lit(1e-10)))))
    )
    return df.drop("prev_price", "change", "gain", "loss", "avg_gain", "avg_loss")


def process_stocks(spark: SparkSession):
    df = spark.read.format("delta").load("s3a://processed/stocks")
    df = compute_moving_averages(df)
    df = compute_rsi(df)
    df = df.withColumn("loaded_at", current_timestamp())

    # Keep only latest row per symbol for fact table (full history also written)
    w_latest = Window.partitionBy("symbol").orderBy(col("event_time").desc())
    latest = (
        df.withColumn("_rn", row_number().over(w_latest))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # Write full history
    df.write.jdbc(JDBC_URL, "fact_stock_price", mode="append", properties=JDBC_PROPS)

    # Upsert-like: write latest to dim_company (symbol metadata)
    latest.select(
        col("symbol"),
        col("currency"),
        current_timestamp().alias("updated_at"),
    ).write.jdbc(JDBC_URL, "dim_company", mode="append", properties=JDBC_PROPS)


def process_crypto(spark: SparkSession):
    df = spark.read.format("delta").load("s3a://processed/crypto")
    df = compute_moving_averages(df.withColumnRenamed("price", "price"))
    df = compute_rsi(df)
    df = df.withColumn("loaded_at", current_timestamp())
    df.write.jdbc(JDBC_URL, "fact_crypto", mode="append", properties=JDBC_PROPS)


def process_forex(spark: SparkSession):
    df = spark.read.format("delta").load("s3a://processed/forex")
    df = df.withColumn("loaded_at", current_timestamp())
    df.write.jdbc(JDBC_URL, "fact_forex", mode="append", properties=JDBC_PROPS)


def populate_dim_time(spark: SparkSession):
    """Populate dim_time from the range of event_times seen in stocks."""
    df = spark.read.format("delta").load("s3a://processed/stocks").select("event_time")
    from pyspark.sql.functions import (
        dayofmonth, dayofweek, dayofyear, hour, minute,
        month, quarter, year,
    )
    dim = (
        df.select("event_time").distinct()
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("quarter", quarter("event_time"))
        .withColumn("day", dayofmonth("event_time"))
        .withColumn("day_of_week", dayofweek("event_time"))
        .withColumn("day_of_year", dayofyear("event_time"))
        .withColumn("hour", hour("event_time"))
        .withColumn("minute", minute("event_time"))
    )
    dim.write.jdbc(JDBC_URL, "dim_time", mode="append", properties=JDBC_PROPS)


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    process_stocks(spark)
    process_crypto(spark)
    process_forex(spark)
    populate_dim_time(spark)

    spark.stop()


if __name__ == "__main__":
    run()
