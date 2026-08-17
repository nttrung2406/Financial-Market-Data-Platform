import hmac
import hashlib
from typing import Dict, Any, Optional
from src.ingestion.interfaces import BaseProvider
from src.ingestion.base.api_client import BaseAPIClient
from src.ingestion.base.logger import get_logger

logger = get_logger("BinanceProvider")


class BinanceProvider(BaseProvider):
    """Provider for Binance Market API"""

    def __init__(
        self,
        endpoint: str,
        api_key: Optional[str] = "",
        api_secret_key: Optional[str] = "",
        binance_hmac: Optional[str] = "",
    ):
        headers = {}
        if api_key:
            headers["X-MBX-APIKEY"] = api_key

        self.client = BaseAPIClient(base_url=endpoint, headers=headers)
        self.api_key = api_key
        self.api_secret_key = api_secret_key
        self.binance_hmac = binance_hmac

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return hmac.new(
            self.api_secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def fetch_data(self, symbol: str = "BTCUSDT", **kwargs) -> Dict[str, Any]:
        endpoint = "api/v3/ticker/24hr"
        params = {"symbol": symbol}
        logger.info(f"Fetching Binance 24hr ticker for: {symbol}")
        try:
            return await self.client.get(endpoint=endpoint, params=params)
        except Exception as e:
            logger.error(f"Binance API error for {symbol}: {e}")
            return {
                "symbol": symbol,
                "lastPrice": "67500.00",
                "highPrice": "68200.00",
                "lowPrice": "66100.00",
                "volume": "15200.5",
                "quoteVolume": "1026000000.00",
                "is_mock": True,
            }