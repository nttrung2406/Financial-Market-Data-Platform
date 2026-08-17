from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class StockData(BaseModel):
    symbol: str
    price: float
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[float] = None
    currency: str = "USD"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    raw_payload: Optional[Dict[str, Any]] = None


class CryptoData(BaseModel):
    symbol: str
    price: float
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    volume_24h: Optional[float] = None
    quote_volume: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    raw_payload: Optional[Dict[str, Any]] = None


class ForexData(BaseModel):
    base_currency: str
    target_currency: str
    rate: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    raw_payload: Optional[Dict[str, Any]] = None
