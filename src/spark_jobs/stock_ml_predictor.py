"""
Spark MLlib batch job: train 3 classifiers with hyperparameter tuning,
apply majority voting, and write the top-5 predicted risers / droppers
to PostgreSQL (ml_stock_predictions) and Kafka (ml_predictions topic).

Data source: fact_stock_price in PostgreSQL (already contains MA/RSI).
The job runs entirely as a local PySpark session so it can be triggered
by the Airflow worker without connecting to the Spark cluster.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, row_number

from src.spark.predictors.stock_predictor import (
    NUMERIC_FEATURES,
    add_labels,
    build_gbt_cv,
    build_lr_cv,
    build_rf_cv,
    engineer_features,
    extract_rise_prob,
    majority_vote,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("StockMLPredictor")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "finance")
PG_USER = os.getenv("POSTGRES_USER", "finance")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "finance")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "ml_predictions"

LOOKBACK_DAYS = int(os.getenv("ML_LOOKBACK_DAYS", "30"))
CV_FOLDS = int(os.getenv("ML_CV_FOLDS", "3"))
MIN_ROWS = int(os.getenv("ML_MIN_ROWS", "100"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockMLPredictor")
        .master("local[*]")
        # Disable Arrow-based pandas conversion to avoid version conflicts
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_stock_data(spark: SparkSession):
    """Read recent stock history from PostgreSQL into a Spark DataFrame."""
    query = f"""
        SELECT symbol, price, open_price, high_price, low_price, close_price,
               volume, ma5, ma20, ma50, rsi, event_time
        FROM fact_stock_price
        WHERE event_time >= NOW() - INTERVAL '{LOOKBACK_DAYS} days'
          AND price > 0
          AND ma5   IS NOT NULL
          AND rsi   IS NOT NULL
        ORDER BY symbol, event_time
    """
    with _pg_conn() as conn:
        pdf = pd.read_sql(query, conn)

    if pdf.empty:
        return None

    # Ensure correct types before converting to Spark DF
    for num_col in ["price", "open_price", "high_price", "low_price",
                    "close_price", "volume", "ma5", "ma20", "ma50", "rsi"]:
        pdf[num_col] = pd.to_numeric(pdf[num_col], errors="coerce")

    pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)

    return spark.createDataFrame(pdf)


# ── Result persistence ────────────────────────────────────────────────────────

def _write_predictions(result_pdf: pd.DataFrame) -> None:
    """Upsert prediction rows into ml_stock_predictions."""
    rows = [
        (
            str(r["symbol"]),
            str(r["prediction"]),
            float(r["confidence"]),
            float(r["rf_probability"]),
            float(r["gbt_probability"]),
            float(r["lr_probability"]),
            int(r["model_votes"]),
            float(r["current_price"]),
            r["predicted_at"],
        )
        for _, r in result_pdf.iterrows()
    ]
    sql = """
        INSERT INTO ml_stock_predictions
            (symbol, prediction, confidence, rf_probability, gbt_probability,
             lr_probability, model_votes, current_price, predicted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    log.info("Inserted %d prediction rows into ml_stock_predictions.", len(rows))


def _publish_to_kafka(result_pdf: pd.DataFrame) -> None:
    """Produce top-5 rise and top-5 drop records to the ml_predictions Kafka topic."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        log.warning("kafka-python not installed; skipping Kafka publish.")
        return

    rise_top5 = result_pdf.nlargest(5, "confidence")
    drop_top5 = result_pdf.nsmallest(5, "confidence")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    ts = datetime.now(timezone.utc).isoformat()
    total = 0
    for category, rows in [("TOP_RISE", rise_top5), ("TOP_DROP", drop_top5)]:
        for _, row in rows.iterrows():
            producer.send(KAFKA_TOPIC, {
                "@timestamp": ts,
                "category": category,
                "symbol": row["symbol"],
                "prediction": row["prediction"],
                "confidence": float(row["confidence"]),
                "rf_probability": float(row["rf_probability"]),
                "gbt_probability": float(row["gbt_probability"]),
                "lr_probability": float(row["lr_probability"]),
                "model_votes": int(row["model_votes"]),
                "current_price": float(row["current_price"]),
            })
            total += 1

    producer.flush()
    producer.close()
    log.info("Published %d records to Kafka topic '%s'.", total, KAFKA_TOPIC)


# ── Main job ──────────────────────────────────────────────────────────────────

def run() -> None:
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Loading stock data (last %d days)…", LOOKBACK_DAYS)
    df = _load_stock_data(spark)
    if df is None:
        log.warning("No data returned from fact_stock_price; aborting.")
        spark.stop()
        return

    df.cache()
    row_count = df.count()
    log.info("Loaded %d rows across %d symbols.", row_count, df.select("symbol").distinct().count())

    if row_count < MIN_ROWS:
        log.warning("Insufficient data (%d rows < MIN_ROWS=%d); aborting.", row_count, MIN_ROWS)
        spark.stop()
        return

    log.info("Engineering features…")
    df = engineer_features(df)
    df = df.dropna(subset=NUMERIC_FEATURES)

    # Latest row per symbol — used for prediction (no forward label available)
    w_latest = Window.partitionBy("symbol").orderBy(col("event_time").desc())
    latest_df = (
        df.withColumn("_rn", row_number().over(w_latest))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .cache()
    )
    symbols = sorted(r.symbol for r in latest_df.select("symbol").collect())
    log.info("Symbols for prediction: %s", symbols)

    # Labeled data for training (all rows except the last per symbol, which has no future)
    labeled_df = add_labels(df).dropna(subset=NUMERIC_FEATURES + ["label"])
    labeled_count = labeled_df.count()
    log.info("Training on %d labeled rows.", labeled_count)

    # ── Train three models ────────────────────────────────────────────────────
    log.info("Training RandomForest (CV)…")
    rf_cv_model = build_rf_cv(CV_FOLDS).fit(labeled_df)
    log.info("  RF   best AUC = %.4f", max(rf_cv_model.avgMetrics))

    log.info("Training GBT (CV)…")
    gbt_cv_model = build_gbt_cv(CV_FOLDS).fit(labeled_df)
    log.info("  GBT  best AUC = %.4f", max(gbt_cv_model.avgMetrics))

    log.info("Training LogisticRegression (CV)…")
    lr_cv_model = build_lr_cv(CV_FOLDS).fit(labeled_df)
    log.info("  LR   best AUC = %.4f", max(lr_cv_model.avgMetrics))

    # ── Predict on latest snapshot ────────────────────────────────────────────
    log.info("Generating predictions on latest snapshot…")

    rf_preds = (
        rf_cv_model.transform(latest_df)
        .transform(lambda d: extract_rise_prob(d, "probability", "rf_rise_prob"))
        .select("symbol", col("price").alias("current_price"), "rf_rise_prob")
    )
    gbt_preds = (
        gbt_cv_model.transform(latest_df)
        .transform(lambda d: extract_rise_prob(d, "probability", "gbt_rise_prob"))
        .select("symbol", "gbt_rise_prob")
    )
    lr_preds = (
        lr_cv_model.transform(latest_df)
        .transform(lambda d: extract_rise_prob(d, "probability", "lr_rise_prob"))
        .select("symbol", "lr_rise_prob")
    )

    # ── Majority voting ───────────────────────────────────────────────────────
    combined = (
        rf_preds
        .join(gbt_preds, "symbol")
        .join(lr_preds, "symbol")
        .transform(majority_vote)
        .withColumn("predicted_at", current_timestamp())
        .select(
            "symbol",
            "prediction",
            "confidence",
            col("rf_rise_prob").alias("rf_probability"),
            col("gbt_rise_prob").alias("gbt_probability"),
            col("lr_rise_prob").alias("lr_probability"),
            "model_votes",
            "current_price",
            "predicted_at",
        )
    )

    result_pdf = combined.toPandas()
    log.info("Predictions:\n%s", result_pdf[["symbol", "prediction", "confidence", "model_votes"]].to_string())

    # ── Persist ───────────────────────────────────────────────────────────────
    _write_predictions(result_pdf)
    _publish_to_kafka(result_pdf)

    spark.stop()
    log.info("ML prediction job complete.")


if __name__ == "__main__":
    run()
