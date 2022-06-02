import pandas as pd
import pandera as pa
from pandera.dtypes import Timestamp
from pandera.errors import SchemaError
from pydantic import BaseModel

# Classes should be PascalCase
# Check type using pydantic
# Mark optional parameters with NotRequired


# Typical data order analysis:
# 1. Fetch required timeseries aggregation from database (JSON)
# 2. Convert timeseries into Pandas DataFrame to analyze
# 3. Convert DataFrame to JSON for publishing


# TODO: Make tests
# Test, all dataframe arrays are same length
# Test, ValidationError
# Test if the units are consistent
# Test except SchemaError

# TODO: Check if there are duplicates dates in dataframe; remove them; duplicate


class Stock(BaseModel):
    symbol: str  # Stock symbol is unique identifier
    time_series_df: pd.DataFrame  # Check if type is DataFrame

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True

    def stock_to_JSON(self):
        return self.json()

    def __data_clean_df(self):

        # TODO: Refactor - seperation of concerns
        self.time_series_df.dropna(inplace=True)  # Remove missing rows
        self.__validate_ts_and_set_df()  # Validate time series & set
        self.time_series_df.sort_values(
            "timestamp", ascending=True, inplace=True
        )  # Sort by date (ascending)
        self.time_series_df.reset_index(drop=True)  # Reset index to newly sorted

    def __validate_ts_and_set_df(self):
        """Validate time series dataframe and set

        Args:
            df (pd.DataFrame): Unvalidated Pandas DataFrame

        Returns:
            int: Did the process succeed?
        """
        # TODO: Refactor for seperation of concerns? (Validate, then set)

        stock_df_schema = pa.DataFrameSchema(
            {
                "timestamp": pa.Column(Timestamp, coerce=True),
                "close_price": pa.Column(
                    float, checks=pa.Check.greater_than_or_equal_to(-1)
                ),
            },
        )
        try:
            self.time_series_df = stock_df_schema(self.time_series_df)
        except SchemaError as e:
            print(f"{e}")  # TODO: Log error & pass this stock
            return 1  # TODO: Return an error?

    def __init__(self, symbol: str, time_series_df: pd.DataFrame):

        # Initialize all variables #TODO: Rewrite using *args and **kwargs

        # Initialize object; inherit init from superclass
        super().__init__(symbol=symbol, time_series_df=time_series_df)
        # Dataclean upon initialization
        self.__data_clean_df()
