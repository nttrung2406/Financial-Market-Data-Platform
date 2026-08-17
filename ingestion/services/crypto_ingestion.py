from typing import List
from datetime import datetime
from src.ingestion.interfaces import BaseIngestionService
from src.ingestion.providers.binance import BinanceProvider
from src.ingestion.base.kafka_producer import KafkaProducerService
from src.ingestion.base.minio_client import MinIOStorageService
from src.ingestion.models import CryptoData
from src.ingestion.base.logger import get_logger

logger = get_logger("CryptoIngestionService")


class CryptoIngestionService(BaseIngestionService):
    """Service to process and store Cryptocurrency market data"""

    def __init__(
        self,
        provider: BinanceProvider,
        kafka_producer: KafkaProducerService,
        minio_storage: MinIOStorageService,
        kafka_topic: str,
    ):
        self.provider = provider
        self.kafka_producer = kafka_producer
        self.minio_storage = minio_storage
        self.kafka_topic = kafka_topic

    async def process_and_publish(self, symbols: List[str]) -> List[CryptoData]:
        records: List[CryptoData] = []

        for symbol in symbols:
            raw_data = await self.provider.fetch_data(symbol=symbol)
            try:
                price = float(raw_data.get("lastPrice", 0.0))
                high = float(raw_data.get("highPrice", 0.0)) if raw_data.get("highPrice") else None
                low = float(raw_data.get("lowPrice", 0.0)) if raw_data.get("lowPrice") else None
                volume = float(raw_data.get("volume", 0.0)) if raw_data.get("volume") else None
                quote_volume = float(raw_data.get("quoteVolume", 0.0)) if raw_data.get("quoteVolume") else None

                record = CryptoData(
                    symbol=symbol,
                    price=price,
                    high_24h=high,
                    low_24h=low,
                    volume_24h=volume,
                    quote_volume=quote_volume,
                    timestamp=datetime.utcnow(),
                    raw_payload=raw_data,
                )

                # Persist raw payload to MinIO
                ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.minio_storage.upload_json(
                    object_name=f"crypto/{symbol}/{ts_str}.json",
                    data=record.model_dump(),
                )

                # Publish message to Kafka
                self.kafka_producer.send(
                    topic=self.kafka_topic,
                    key=symbol,
                    value=record.model_dump(),
                )

                records.append(record)
            except Exception as e:
                logger.error(f"Error parsing crypto symbol {symbol}: {e}")

        return records