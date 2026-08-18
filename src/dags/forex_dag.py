import asyncio
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="forex_ingestion",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "forex"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
)
def forex_ingestion_dag():

    @task
    def ingest_forex():
        from src.ingestion.config import Settings
        from src.ingestion.base.kafka_producer import KafkaProducerService
        from src.ingestion.base.minio_client import MinIOStorageService
        from src.ingestion.providers.exchange_rate import ExchangeRateProvider
        from src.ingestion.services.forex_ingestion import ForexIngestionService
        from src.ingestion.pipelines.forex_pipeline import ForexPipeline

        cfg = Settings()
        kafka = KafkaProducerService(bootstrap_servers=cfg.kafka_bootstrap_servers)
        minio = MinIOStorageService(
            endpoint=cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            bucket_name=cfg.minio_bucket,
            secure=cfg.minio_secure,
        )
        provider = ExchangeRateProvider(
            endpoint=cfg.exchange_rate_endpoint,
            api_key=cfg.exchange_rate_api_key,
        )
        service = ForexIngestionService(
            provider=provider,
            kafka_producer=kafka,
            minio_storage=minio,
            kafka_topic=cfg.kafka_topic_forex,
        )
        pipeline = ForexPipeline(
            service=service,
            base_currencies=["USD", "EUR", "GBP", "JPY"],
        )
        asyncio.run(pipeline.run())
        kafka.close()

    ingest_forex()


forex_ingestion_dag()
