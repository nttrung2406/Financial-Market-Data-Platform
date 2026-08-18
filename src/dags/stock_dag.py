import asyncio
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="stock_ingestion",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "stock"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def stock_ingestion_dag():

    @task
    def ingest_stocks():
        from src.ingestion.config import Settings
        from src.ingestion.base.kafka_producer import KafkaProducerService
        from src.ingestion.base.minio_client import MinIOStorageService
        from src.ingestion.providers.twelve_data import TwelveDataProvider
        from src.ingestion.services.stock_ingestion import StockIngestionService
        from src.ingestion.pipelines.stock_pipeline import StockPipeline

        cfg = Settings()
        kafka = KafkaProducerService(bootstrap_servers=cfg.kafka_bootstrap_servers)
        minio = MinIOStorageService(
            endpoint=cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            bucket_name=cfg.minio_bucket,
            secure=cfg.minio_secure,
        )
        provider = TwelveDataProvider(
            endpoint=cfg.twelve_data_endpoint,
            api_key=cfg.twelve_data_api_key,
        )
        service = StockIngestionService(
            provider=provider,
            kafka_producer=kafka,
            minio_storage=minio,
            kafka_topic=cfg.kafka_topic_stocks,
        )
        pipeline = StockPipeline(
            service=service,
            symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"],
        )
        asyncio.run(pipeline.run())
        kafka.close()

    ingest_stocks()


stock_ingestion_dag()
