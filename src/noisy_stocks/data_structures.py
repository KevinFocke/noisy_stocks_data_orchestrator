import pandas as pd
import pandera as pa
from pandera.dtypes import Timestamp
from pandera.errors import SchemaError
from pandera.typing import DataFrame
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


# Load unvalidated DataFrame

df = pd.DataFrame(
    {
        "dates": ["1996-10-04", "1980-02-05", "1970-02-05", "1950-02-07", "1800-02-05"],
        "close_price": [1.3, 1.4, 0, 1, 5],
    }
)

# test for dates in the future

# Convert into validated DataFrame


# define stock schema

stock_schema = pa.DataFrameSchema(
    {
        "dates": pa.Column(Timestamp, coerce=True),
        "close_price": pa.Column(float, checks=pa.Check.greater_than_or_equal_to(0)),
    },
    strict=True,
)


def validate_df(df: pd.DataFrame):
    """Validate the dataframe values

    Args:
        df (pd.DataFrame): Unvalidated Pandas DataFrame

    Returns:
        df (pd.DataFrame): Validated Pandas DataFrame
    """

    try:
        validated_df = stock_schema(df)
        return validated_df
    except SchemaError as e:
        print(f"{e}")  # TODO: Log error & pass this stock


# Test, all arrays are same length
# Test, ValidationError
# Test if the units are consistent
# Test except SchemaError

# TODO: Check if there are duplicates dates; remove them; duplicate


class Stock(BaseModel):
    symbol: str  # Stock symbol is unique identifier
    time_series_data: DataFrame

    def stock_to_JSON(self):
        return self.json()
