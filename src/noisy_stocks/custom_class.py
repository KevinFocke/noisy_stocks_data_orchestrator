from datetime import datetime

from typing_extensions import NotRequired, TypedDict

# Mark optional parameters by NotRequired


class Stock_Info(TypedDict):
    timestamp_fetch_stock: NotRequired[datetime]  # optional
    market_cap_millions: NotRequired[int]
    point_change: NotRequired[int]


class Stocks(TypedDict):
    ticker: Stock_Info
    timestamp_fetched_stocks: datetime
