from typing import List
from src.ingestion.interfaces import BasePipeline
from src.ingestion.services.forex_ingestion import ForexIngestionService
from src.ingestion.base.logger import get_logger

logger = get_logger("ForexPipeline")


class ForexPipeline(BasePipeline):
    """Orchestrates forex ingestion pipeline execution"""

    def __init__(self, service: ForexIngestionService, base_currencies: List[str]):
        self.service = service
        self.base_currencies = base_currencies

    async def run(self) -> None:
        logger.info(f"Starting Forex Pipeline for: {self.base_currencies}")
        results = await self.service.process_and_publish(self.base_currencies)
        logger.info(f"Forex Pipeline finished. Processed {len(results)} forex rate records.")