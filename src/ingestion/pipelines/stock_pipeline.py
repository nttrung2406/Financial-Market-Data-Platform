from typing import List
from src.ingestion.interfaces import BasePipeline
from src.ingestion.services.stock_ingestion import StockIngestionService
from src.ingestion.base.logger import get_logger

logger = get_logger("StockPipeline")


class StockPipeline(BasePipeline):
    """Orchestrates stock ingestion pipeline execution"""

    def __init__(self, service: StockIngestionService, symbols: List[str]):
        self.service = service
        self.symbols = symbols

    async def run(self) -> None:
        logger.info(f"Starting Stock Pipeline for: {self.symbols}")
        results = await self.service.process_and_publish(self.symbols)
        logger.info(f"Stock Pipeline finished. Processed {len(results)} stock records.")