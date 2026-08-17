from typing import List
from datetime import datetime
from src.ingestion.interfaces import BaseIngestionService
from src.ingestion.providers.twelve_data import TwelveDataProvider
from src.ingestion.base.kafka_producer import KafkaProducerService
from src.ingestion.base.minio_client import MinIOStorageService
from src.ingestion.models import StockData
from src.ingestion.base.logger import get_logger

logger = get_logger("StockIngestionService")


class StockIngestionService(BaseIngestionService):
    """Service to process and store Stock market data"""

    def __init__(
        self,
        provider: TwelveDataProvider,
        kafka_producer: KafkaProducerService,
        minio_storage: MinIOStorageService,
        kafka_topic: str,
    ):
        self.provider = provider
        self.kafka_producer = kafka_producer
        self.minio_storage = minio_storage
        self.kafka_topic = kafka_topic

    async def process_and_publish(self, symbols: List[str]) -> List[StockData]:
        records: List[StockData] = []

        for symbol in symbols:
            raw_data = await self.provider.fetch_data(symbol=symbol)
            try:
                price = float(raw_data.get("close") or raw_data.get("price") or 0.0)
                open_p = float(raw_data.get("open", 0.0)) if raw_data.get("open") else None
                high_p = float(raw_data.get("high", 0.0)) if raw_data.get("high") else None
                low_p = float(raw_data.get("low", 0.0)) if raw_data.get("low") else None
                volume = float(raw_data.get("volume", 0.0)) if raw_data.get("volume") else None

                record = StockData(
                    symbol=symbol,
                    price=price,
                    open_price=open_p,
                    high_price=high_p,
                    low_price=low_p,
                    close_price=price,
                    volume=volume,
                    currency=raw_data.get("currency", "USD"),
                    timestamp=datetime.utcnow(),
                    raw_payload=raw_data,
                )

                # Persist raw payload to MinIO
                ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.minio_storage.upload_json(
                    object_name=f"stocks/{symbol}/{ts_str}.json",
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
                logger.error(f"Error parsing stock symbol {symbol}: {e}")

        return records