from typing import List
from datetime import datetime
from src.ingestion.interfaces import BaseIngestionService
from src.ingestion.providers.exchange_rate import ExchangeRateProvider
from src.ingestion.base.kafka_producer import KafkaProducerService
from src.ingestion.base.minio_client import MinIOStorageService
from src.ingestion.models import ForexData
from src.ingestion.base.logger import get_logger

logger = get_logger("ForexIngestionService")


class ForexIngestionService(BaseIngestionService):
    """Service to process and store Forex rate data"""

    def __init__(
        self,
        provider: ExchangeRateProvider,
        kafka_producer: KafkaProducerService,
        minio_storage: MinIOStorageService,
        kafka_topic: str,
    ):
        self.provider = provider
        self.kafka_producer = kafka_producer
        self.minio_storage = minio_storage
        self.kafka_topic = kafka_topic

    async def process_and_publish(self, base_currencies: List[str]) -> List[ForexData]:
        records: List[ForexData] = []

        for base in base_currencies:
            raw_data = await self.provider.fetch_data(base_currency=base)
            try:
                rates = raw_data.get("conversion_rates", {})
                for target, rate in rates.items():
                    record = ForexData(
                        base_currency=base,
                        target_currency=target,
                        rate=float(rate),
                        timestamp=datetime.now(),
                        raw_payload=raw_data,
                    )

                    pair = f"{base}_{target}"
                    self.kafka_producer.send(
                        topic=self.kafka_topic,
                        key=pair,
                        value=record.model_dump(),
                    )
                    records.append(record)

                # Store payload batch to MinIO
                ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.minio_storage.upload_json(
                    object_name=f"forex/{base}/{ts_str}.json",
                    data=raw_data,
                )
            except Exception as e:
                logger.error(f"Error processing forex base currency {base}: {e}")

        return records