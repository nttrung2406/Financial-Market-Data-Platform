import asyncio
from src.ingestion.config import settings
from src.ingestion.base.kafka_producer import KafkaProducerService
from src.ingestion.base.minio_client import MinIOStorageService
from src.ingestion.providers.twelve_data import TwelveDataProvider
from src.ingestion.providers.binance import BinanceProvider
from src.ingestion.providers.exchange_rate import ExchangeRateProvider
from src.ingestion.services.stock_ingestion import StockIngestionService
from src.ingestion.services.crypto_ingestion import CryptoIngestionService
from src.ingestion.services.forex_ingestion import ForexIngestionService
from src.ingestion.pipelines.stock_pipeline import StockPipeline
from src.ingestion.pipelines.crypto_pipeline import CryptoPipeline
from src.ingestion.pipelines.forex_pipeline import ForexPipeline
from src.ingestion.base.logger import get_logger

logger = get_logger("Main")


async def main():
    logger.info("Initializing Data Ingestion System...")

    # 1. Initialize Infrastructure Drivers
    kafka_producer = KafkaProducerService(bootstrap_servers=settings.kafka_bootstrap_servers)
    minio_storage = MinIOStorageService(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        bucket_name=settings.minio_bucket,
        secure=settings.minio_secure,
    )

    # 2. Initialize Data Providers
    twelve_data_provider = TwelveDataProvider(
        endpoint=settings.twelve_data_endpoint,
        api_key=settings.twelve_data_api_key,
    )
    binance_provider = BinanceProvider(
        endpoint=settings.binance_endpoint,
        api_key=settings.binance_api_key,
        api_secret_key=settings.binance_api_secret_key,
        binance_hmac=settings.binance_hmac,
    )
    exchange_rate_provider = ExchangeRateProvider(
        endpoint=settings.exchange_rate_endpoint,
        api_key=settings.exchange_rate_api_key,
    )

    # 3. Initialize Ingestion Services
    stock_service = StockIngestionService(
        provider=twelve_data_provider,
        kafka_producer=kafka_producer,
        minio_storage=minio_storage,
        kafka_topic=settings.kafka_topic_stocks,
    )
    crypto_service = CryptoIngestionService(
        provider=binance_provider,
        kafka_producer=kafka_producer,
        minio_storage=minio_storage,
        kafka_topic=settings.kafka_topic_crypto,
    )
    forex_service = ForexIngestionService(
        provider=exchange_rate_provider,
        kafka_producer=kafka_producer,
        minio_storage=minio_storage,
        kafka_topic=settings.kafka_topic_forex,
    )

    # 4. Initialize Pipelines
    stock_pipeline = StockPipeline(service=stock_service, symbols=["AAPL", "MSFT", "GOOGL"])
    crypto_pipeline = CryptoPipeline(service=crypto_service, symbols=["BTCUSDT", "ETHUSDT"])
    forex_pipeline = ForexPipeline(service=forex_service, base_currencies=["USD", "EUR"])

    # 5. Execute Pipelines Concurrently
    logger.info("Executing Ingestion Pipelines...")
    await asyncio.gather(
        stock_pipeline.run(),
        crypto_pipeline.run(),
        forex_pipeline.run(),
    )

    # 6. Cleanup
    kafka_producer.close()
    logger.info("Ingestion System finished tasks successfully.")


if __name__ == "__main__":
    asyncio.run(main())