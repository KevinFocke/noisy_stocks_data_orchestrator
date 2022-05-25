from datetime import datetime
from sqlite3 import Timestamp

import pandas as pd
import pandera as pa
from pandera.dtypes import DateTime
from pandera.typing import DataFrame, Index, Series
from pydantic import BaseModel

# Purpose of primary datastructures:
# Pandas DataFrame for analysis
# JSON for moving information


# Typical data order analysis:
# 1. Fetch required timeseries aggregation from database (JSON)
# 2. Convert timeseries into Pandas DataFrame to analyze
# 3. Convert DataFrame to JSON for publishing


# Classes should be PascalCase
# Mark optional parameters by NotRequired


class StockSchema(pa.SchemaModel):
    timestamp: Index[DateTime] = pa.Field()
    state: Series[str]
    city: Series[str]
    price: Series[int] = pa.Field(in_range={"min_value": 5, "max_value": 20})


format = "%Y-%M-%D"

today = datetime.today()
# data to validate
df = pd.DataFrame(
    {
        "dates": [today, today, today, today, today],
        "price_in_millions": [-1.3, -1.4, -2.9, -10.1, -20.4],
    }
)

df["dates"] = df["dates"].dt.floor("d")  # Set time to 00:00:00 for performance


print(df)


# define schema
stock_schema = pa.DataFrameSchema(
    {
        "dates": pa.Column(Timestamp, coerce=True),
        "price_in_millions": pa.Column(float, checks=pa.Check.lt(-1.2)),
    },
    strict=True,
)

validated_df = stock_schema(df)
print(validated_df)

# Test, all arrays are same length
# Test, ValidationError
# Test if the units are consistent

# TODO: Check if there are duplicates; duplicate


def pd_dataframe_to_JSON(pd_dataframe: pd.DataFrame):

    # Convert dataframe to TimeSeriesDict for validation

    # Use built-in method
    # Validate
    # Converts a Pandas DataFrame to JSON file
    pass


class Stock(BaseModel):
    symbol: str  # Stock symbol is unique identifier
    time_series_data: DataFrame


def stock_to_JSON(stock: Stock):
    return stock.json()
