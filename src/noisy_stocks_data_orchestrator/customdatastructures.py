"""Custom data structures and their methods
    """

from datetime import datetime, strftime, strptime
from pathlib import Path
from typing import Optional

import pandas as pd
import pandera as pa
from pandas import DatetimeIndex
from pandera import Index
from pandera.dtypes import Timestamp
from prefect.tasks import task

# Classes should be PascalCase
# Check type using pydantic, check DataFrame using pandera
from pydantic import BaseModel, PrivateAttr, validate_arguments
from pydantic.types import PositiveInt


@validate_arguments
@task
def file_exists(path: Path):
    return path.is_file()


@validate_arguments
@task
def folder_exists(path: Path):
    return path.is_dir()


# Schema is always:
# 1. timestamp column
# 2. one or more columns that have integer values

# During analysis, two columns are compared

# Make one DataFrame per timestamp column pair


class DatabaseQuery(BaseModel):
    select_fields: list[str]
    from_database: str
    begin_timestamp: datetime
    end_timestamp: datetime

    def to_sql(self):
        pass

    def _output_date(self, date: datetime, date_output_format: str = r"%Y-%m-%d"):
        return date.strftime(date_output_format)

    def output_begin_timestamp(self, date_output_format: str = r"%Y-%m-%d"):
        self._output_date(
            date=self.begin_timestamp, date_output_format=date_output_format
        )

    def output_end_timestamp(self, date_output_format: str = r"%Y-%m-%d"):
        self._output_date(
            date=self.end_timestamp, date_output_format=date_output_format
        )


class TimeSeries(BaseModel):
    stock_symbol_name: Optional[str]  # stock symbol
    timestamp_index_name: str  # What is the name of the timestamp column?
    numeric_col_name: str  # What is the name of the numeric column? eg. price_close
    time_series_df: pd.DataFrame  # Check if type is DataFrame
    _time_series_df_schema: pa.DataFrameSchema = PrivateAttr()
    _longest_timeseries_sequence_start: DatetimeIndex = PrivateAttr()
    _longest_timeseries_sequence_end: DatetimeIndex = PrivateAttr()
    _longest_timeseries_sequence_length: PositiveInt = PrivateAttr()

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True

    def __create_custom_df_schema(self):
        # At minimum will compare timestamp + value
        # Base dict
        self._time_series_df_schema = pa.DataFrameSchema(
            {
                self.numeric_col_name: pa.Column(
                    float, checks=pa.Check.greater_than_or_equal_to(0)
                )
            },
            index=Index(Timestamp, coerce=True),
        )

    def __data_clean_df(self):
        """Clean the dataframe"""

        # Remove missing rows
        self.time_series_df.dropna(inplace=True)
        # Validate time series & set
        self.__validate_ts_and_set_df()

    def stock_to_JSON(self):
        """create JSON

        Returns:
            JSON
        """
        return self.json()

    def __validate_schema(self):
        """Validate pandera df schema"""
        time_series_df_schema = self._time_series_df_schema
        self.time_series_df = time_series_df_schema(self.time_series_df)

    def __validate_ts_and_set_df(self):
        """Validate time series dataframe and set

        Args:
            df (pd.DataFrame): Unvalidated Pandas DataFrame
        """

        self.__validate_schema()

    def __init__(self, *args, **kwargs):

        # Initialize object with Pydantic type checking
        # Inherit init from superclass
        super().__init__(*args, **kwargs)
        self.__create_custom_df_schema()
        self.__data_clean_df()
