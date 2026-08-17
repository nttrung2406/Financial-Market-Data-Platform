from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="stock_ml_prediction",
    schedule="0 * * * *",   # every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "stock", "prediction"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
)
def stock_ml_prediction_dag():

    @task
    def run_ml_predictor():
        from src.spark_jobs.stock_ml_predictor import run
        run()

    run_ml_predictor()


stock_ml_prediction_dag()
