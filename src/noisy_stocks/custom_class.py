from datetime import datetime
from typing import Optional

from pydantic import BaseModel

# Mark optional parameters by NotRequired

# Classes shoudl be PascalCase


class StockInfo(BaseModel):
    timestamp_fetch_stock: Optional[datetime] = None
    #
    market_cap_millions: Optional[int]
    point_change: Optional[int]


class Stocks(BaseModel):
    ticker: StockInfo
    timestamp_fetched_stocks: datetime
