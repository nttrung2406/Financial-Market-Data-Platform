from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseProvider(ABC):
    """Abstract Base Class for Data Providers"""

    @abstractmethod
    async def fetch_data(self, **kwargs) -> Dict[str, Any]:
        """Fetch raw data from provider API"""
        pass


class BaseIngestionService(ABC):
    """Abstract Base Class for Ingestion Services"""

    @abstractmethod
    async def process_and_publish(self, identifiers: List[str]) -> List[Any]:
        """Fetch, map to model, persist to storage, and publish to Kafka"""
        pass


class BasePipeline(ABC):
    """Abstract Base Class for Execution Pipelines"""

    @abstractmethod
    async def run(self) -> None:
        """Execute the pipeline sequence"""
        pass