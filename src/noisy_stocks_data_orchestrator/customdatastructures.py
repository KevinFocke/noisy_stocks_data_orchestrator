from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.dtypes import Timestamp
from pandera.errors import SchemaError
from pydantic import BaseModel

# Classes should be PascalCase
# Check type using pydantic, check DataFrame using pandera
# Mark optional pydantic parameters with NotRequired


# Typical order of data analysis:
# 1. Fetch required timeseries aggregation from database (JSON)
# 2. Convert timeseries into Pandas DataFrame for analysis
# 3. Convert DataFrame to Markdown for publishing


class ObjectGenerationError(Exception):
    """Object cannot be created"""


class TimeSeries(BaseModel):
    name: str  # unique identifier
    dataset_profile_unique_id: int = -1  # Which dataset?
    time_series_df: pd.DataFrame  # Check if type is DataFrame

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True

    def stock_to_JSON(self):
        """create JSON

        Returns:
            JSON
        """
        return self.json()

    def __data_clean_df(self):
        """Clean the dataframe"""
        # Remove missing rows
        self.time_series_df.dropna(inplace=True)
        # Validate time series & set
        self.__validate_ts_and_set_df()
        # Drop duplicate dates
        self.time_series_df.drop_duplicates(
            subset="timestamp", keep="first", inplace=True
        )
        # Sort dates
        self.time_series_df.sort_values("timestamp", ascending=True, inplace=True)
        # Reset index
        self.time_series_df.reset_index(drop=True, inplace=True)

    def drop_failure_cases(self, failure_cases):
        """Drop failed cases from dataframe"""
        index_list = failure_cases["index"].values.tolist()
        return self.time_series_df.drop(index_list, inplace=True)

    def __validate_schema(self):
        """Validate pandera df schema"""
        time_series_df_schema = pa.DataFrameSchema(
            {
                "timestamp": pa.Column(Timestamp, coerce=True),
                "close_price": pa.Column(
                    float, checks=pa.Check.greater_than_or_equal_to(0)
                ),
            },
        )

        self.time_series_df = time_series_df_schema(self.time_series_df)

    def __validate_ts_and_set_df(self):
        """Validate time series dataframe and set

        Args:
            df (pd.DataFrame): Unvalidated Pandas DataFrame
        """

        try:
            self.__validate_schema()
        except SchemaError as se:
            # print(f"{e}")  # TODO: Log error

            # attempt fix by dropping rows
            self.drop_failure_cases(se.failure_cases)
            # revalidate
            try:
                self.__validate_schema()
            except SchemaError:
                raise ObjectGenerationError

    def __init__(self, *args, **kwargs):

        # Initialize object with Pydantic type checking
        # Inherit init from superclass

        try:
            super().__init__(*args, **kwargs)
            self.__data_clean_df()
        except ValueError:
            raise ObjectGenerationError
        except TypeError:
            raise ObjectGenerationError
