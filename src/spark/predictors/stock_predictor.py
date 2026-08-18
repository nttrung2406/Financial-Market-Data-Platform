"""

Each model predicts a binary label:
  1 = next tick price rises  (RISE)
  0 = next tick price falls  (DROP)

Models are wrapped in CrossValidator for hyperparameter tuning.
"""
from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import avg, col, lag, lead, lit, stddev, when

# Numeric feature columns produced by engineer_features()
NUMERIC_FEATURES = [
    "price_return_1",   # 1-tick return
    "price_return_5",   # 5-tick return
    "volatility_10",    # rolling 10-tick std of 1-tick return
    "volume_ratio",     # volume / 20-tick avg volume
    "ma5_signal",       # price/MA5 - 1
    "ma20_signal",      # price/MA20 - 1
    "ma50_signal",      # price/MA50 - 1
    "rsi_norm",         # RSI / 100
    "ohlc_body",        # (close - open) / open
    "high_low_range",   # (high - low) / close
]



def engineer_features(df: DataFrame) -> DataFrame:
    """Append feature columns to a stock DataFrame that already has MA/RSI columns."""
    w = Window.partitionBy("symbol").orderBy("event_time")
    w10 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-9, 0)
    w20 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-19, 0)

    return (
        df
        .withColumn("_lag1", lag("price", 1).over(w))
        .withColumn("_lag5", lag("price", 5).over(w))
        .withColumn("_avg_vol20", avg("volume").over(w20))
        .withColumn(
            "price_return_1",
            when(col("_lag1") > 0, (col("price") - col("_lag1")) / col("_lag1"))
            .otherwise(lit(0.0)),
        )
        .withColumn(
            "price_return_5",
            when(col("_lag5") > 0, (col("price") - col("_lag5")) / col("_lag5"))
            .otherwise(lit(0.0)),
        )
        .withColumn("volatility_10", stddev("price_return_1").over(w10))
        .withColumn(
            "volume_ratio",
            when(col("_avg_vol20") > 0, col("volume") / col("_avg_vol20"))
            .otherwise(lit(1.0)),
        )
        .withColumn(
            "ma5_signal",
            when(col("ma5") > 0, col("price") / col("ma5") - 1.0).otherwise(lit(0.0)),
        )
        .withColumn(
            "ma20_signal",
            when(col("ma20") > 0, col("price") / col("ma20") - 1.0).otherwise(lit(0.0)),
        )
        .withColumn(
            "ma50_signal",
            when(col("ma50") > 0, col("price") / col("ma50") - 1.0).otherwise(lit(0.0)),
        )
        .withColumn("rsi_norm", col("rsi") / 100.0)
        .withColumn(
            "ohlc_body",
            when(col("open_price") > 0,
                 (col("close_price") - col("open_price")) / col("open_price"))
            .otherwise(lit(0.0)),
        )
        .withColumn(
            "high_low_range",
            when(col("close_price") > 0,
                 (col("high_price") - col("low_price")) / col("close_price"))
            .otherwise(lit(0.0)),
        )
        .drop("_lag1", "_lag5", "_avg_vol20")
    )


def add_labels(df: DataFrame) -> DataFrame:
    """Append binary label using lead(): 1 if next tick price rises."""
    w = Window.partitionBy("symbol").orderBy("event_time")
    return (
        df
        .withColumn("_next_price", lead("price", 1).over(w))
        .withColumn("label", when(col("_next_price") > col("price"), 1.0).otherwise(0.0))
        .filter(col("_next_price").isNotNull())
        .drop("_next_price")
    )



def _base_stages():
    """StringIndexer → OHE (symbol) → VectorAssembler → StandardScaler."""
    si = StringIndexer(inputCol="symbol", outputCol="_sym_idx", handleInvalid="keep")
    ohe = OneHotEncoder(inputCols=["_sym_idx"], outputCols=["_sym_vec"])
    asm = VectorAssembler(
        inputCols=NUMERIC_FEATURES + ["_sym_vec"],
        outputCol="raw_features",
        handleInvalid="keep",
    )
    # withMean=False preserves sparsity from OHE
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features",
        withStd=True, withMean=False,
    )
    return si, ohe, asm, scaler



def build_rf_cv(num_folds: int = 3) -> CrossValidator:
    si, ohe, asm, scaler = _base_stages()
    clf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
    grid = (
        ParamGridBuilder()
        .addGrid(clf.numTrees, [20, 50])
        .addGrid(clf.maxDepth, [5, 10])
        .build()
    )
    return CrossValidator(
        estimator=Pipeline(stages=[si, ohe, asm, scaler, clf]),
        estimatorParamMaps=grid,
        evaluator=BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC"),
        numFolds=num_folds,
        seed=42,
        parallelism=1,
    )


def build_gbt_cv(num_folds: int = 3) -> CrossValidator:
    si, ohe, asm, scaler = _base_stages()
    clf = GBTClassifier(labelCol="label", featuresCol="features", seed=42)
    grid = (
        ParamGridBuilder()
        .addGrid(clf.maxIter, [20, 50])
        .addGrid(clf.maxDepth, [5, 8])
        .build()
    )
    return CrossValidator(
        estimator=Pipeline(stages=[si, ohe, asm, scaler, clf]),
        estimatorParamMaps=grid,
        evaluator=BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC"),
        numFolds=num_folds,
        seed=42,
        parallelism=1,
    )


def build_lr_cv(num_folds: int = 3) -> CrossValidator:
    si, ohe, asm, scaler = _base_stages()
    # LR does not accept seed= parameter
    clf = LogisticRegression(labelCol="label", featuresCol="features")
    grid = (
        ParamGridBuilder()
        .addGrid(clf.regParam, [0.01, 0.1])
        .addGrid(clf.maxIter, [50, 100])
        .build()
    )
    return CrossValidator(
        estimator=Pipeline(stages=[si, ohe, asm, scaler, clf]),
        estimatorParamMaps=grid,
        evaluator=BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC"),
        numFolds=num_folds,
        seed=42,
        parallelism=1,
    )



def extract_rise_prob(df: DataFrame, prob_col: str, out_alias: str) -> DataFrame:
    """Extract P(label=1) = probability vector index 1 into a scalar column."""
    return df.withColumn(out_alias, vector_to_array(col(prob_col)).getItem(1))


def majority_vote(df: DataFrame) -> DataFrame:
    """
    Combine rf_rise_prob, gbt_rise_prob, lr_rise_prob into an ensemble signal.

    confidence  – mean P(RISE) across the three models
    model_votes – count of models voting RISE (threshold 0.5)
    prediction  – RISE if model_votes >= 2, else DROP
    """
    return (
        df
        .withColumn(
            "confidence",
            (col("rf_rise_prob") + col("gbt_rise_prob") + col("lr_rise_prob")) / 3.0,
        )
        .withColumn(
            "model_votes",
            (
                when(col("rf_rise_prob") > 0.5, 1).otherwise(0)
                + when(col("gbt_rise_prob") > 0.5, 1).otherwise(0)
                + when(col("lr_rise_prob") > 0.5, 1).otherwise(0)
            ).cast("integer"),
        )
        .withColumn(
            "prediction",
            when(col("model_votes") >= 2, lit("RISE")).otherwise(lit("DROP")),
        )
    )
