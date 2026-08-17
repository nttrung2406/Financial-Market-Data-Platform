from typing import Dict, Any
from src.ingestion.interfaces import BaseProvider
from src.ingestion.base.api_client import BaseAPIClient
from src.ingestion.base.logger import get_logger

logger = get_logger("TwelveDataProvider")


class TwelveDataProvider(BaseProvider):
    """Provider for Twelve Data Stock API"""

    def __init__(self, endpoint: str, api_key: str):
        self.client = BaseAPIClient(base_url=endpoint)
        self.api_key = api_key

    async def fetch_data(self, symbol: str = "AAPL", **kwargs) -> Dict[str, Any]:
        endpoint = "quote"
        params = {"symbol": symbol, "apikey": self.api_key}
        logger.info(f"Fetching TwelveData quote for: {symbol}")
        try:
            return await self.client.get(endpoint=endpoint, params=params)
        except Exception as e:
            logger.error(f"TwelveData API error for {symbol}: {e}")
            return {
                "symbol": symbol,
                "close": "185.50",
                "open": "183.20",
                "high": "186.00",
                "low": "182.80",
                "volume": "45000000",
                "currency": "USD",
                "is_mock": True,
            }