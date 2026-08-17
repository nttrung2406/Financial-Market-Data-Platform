import asyncio
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="crypto_ingestion",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "crypto"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def crypto_ingestion_dag():

    @task
    def ingest_crypto():
        from src.ingestion.config import Settings
        from src.ingestion.base.kafka_producer import KafkaProducerService
        from src.ingestion.base.minio_client import MinIOStorageService
        from src.ingestion.providers.binance import BinanceProvider
        from src.ingestion.services.crypto_ingestion import CryptoIngestionService
        from src.ingestion.pipelines.crypto_pipeline import CryptoPipeline

        cfg = Settings()
        kafka = KafkaProducerService(bootstrap_servers=cfg.kafka_bootstrap_servers)
        minio = MinIOStorageService(
            endpoint=cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            bucket_name=cfg.minio_bucket,
            secure=cfg.minio_secure,
        )
        provider = BinanceProvider(
            endpoint=cfg.binance_endpoint,
            api_key=cfg.binance_api_key,
            api_secret_key=cfg.binance_api_secret_key,
            binance_hmac=cfg.binance_hmac,
        )
        service = CryptoIngestionService(
            provider=provider,
            kafka_producer=kafka,
            minio_storage=minio,
            kafka_topic=cfg.kafka_topic_crypto,
        )
        pipeline = CryptoPipeline(
            service=service,
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
        )
        asyncio.run(pipeline.run())
        kafka.close()

    ingest_crypto()


crypto_ingestion_dag()
