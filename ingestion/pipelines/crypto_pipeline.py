from typing import List
from src.ingestion.interfaces import BasePipeline
from src.ingestion.services.crypto_ingestion import CryptoIngestionService
from src.ingestion.base.logger import get_logger

logger = get_logger("CryptoPipeline")


class CryptoPipeline(BasePipeline):
    """Orchestrates crypto ingestion pipeline execution"""

    def __init__(self, service: CryptoIngestionService, symbols: List[str]):
        self.service = service
        self.symbols = symbols

    async def run(self) -> None:
        logger.info(f"Starting Crypto Pipeline for: {self.symbols}")
        results = await self.service.process_and_publish(self.symbols)
        logger.info(f"Crypto Pipeline finished. Processed {len(results)} crypto records.")