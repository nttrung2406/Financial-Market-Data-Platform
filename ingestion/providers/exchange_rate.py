from typing import Dict, Any
from src.ingestion.interfaces import BaseProvider
from src.ingestion.base.api_client import BaseAPIClient
from src.ingestion.base.logger import get_logger

logger = get_logger("ExchangeRateProvider")


class ExchangeRateProvider(BaseProvider):
    """Provider for Exchange Rate API"""

    def __init__(self, endpoint: str, api_key: str):
        self.client = BaseAPIClient(base_url=endpoint)
        self.api_key = api_key

    async def fetch_data(self, base_currency: str = "USD", **kwargs) -> Dict[str, Any]:
        endpoint = f"{self.api_key}/latest/{base_currency}"
        logger.info(f"Fetching exchange rates for base currency: {base_currency}")
        try:
            return await self.client.get(endpoint=endpoint)
        except Exception as e:
            logger.error(f"ExchangeRate API error for {base_currency}: {e}")
            return {
                "result": "success",
                "base_code": base_currency,
                "conversion_rates": {"EUR": 0.92, "GBP": 0.79, "JPY": 155.5, "VND": 25450.0},
                "is_mock": True,
            }