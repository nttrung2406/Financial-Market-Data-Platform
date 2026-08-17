from typing import Dict, Any, Optional
import httpx
from src.ingestion.base.logger import get_logger

logger = get_logger("APIClient")


class BaseAPIClient:
    """Async HTTP Client built on top of httpx"""

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        combined_headers = {**self.headers, **(headers or {})}

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(url, params=params, headers=combined_headers)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error ({e.response.status_code}) fetching {url}: {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Request failed for {url}: {e}")
                raise