from pathlib import Path
from queue import Empty, Full, SimpleQueue

import pandas as pd
import pandera as pa
from pandera.dtypes import Timestamp
from pandera.errors import SchemaError
from prefect.flows import flow
from prefect.tasks import task
from pydantic import BaseModel, PositiveInt

# Classes should be PascalCase
# Check type using pydantic, check DataFrame using pandera


class Resource(BaseModel):
    resource_type: str
    resource_location: str
    resource_schema: pa.DataFrameSchema

    class Config:
        arbitrary_types_allowed = True


class ExtractionQueue(BaseModel):
    timeout_pop: PositiveInt = 1
    _queue: SimpleQueue = SimpleQueue()  # Should not be accessed

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True
        orm_mode = True

    def pop(self):
        try:
            return self._queue.get(timeout=self.timeout_pop)
        except Empty:
            raise Empty

    def push(self, item):
        try:
            self._queue.put(item)
        except Full:
            raise Full


class FolderExtractionQueue(ExtractionQueue):
    pass


class FileExtractionQueue(ExtractionQueue):
    pass
    file_queue_dirty: SimpleQueue

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True


# TODO: Make TimeSeries method use prefect flows & tasks
# TODO: Add data cleaning


class TimeSeries(BaseModel):
    name: str  # unique identifier
    dataset_profile_unique_id: int = 0  # Which dataset?
    # TODO: Add parameter deciding schema
    time_series_df: pd.DataFrame  # Check if type is DataFrame

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True

    def __data_clean_df(self):
        """Clean the dataframe"""
        # Remove missing rows
        self.time_series_df.dropna(inplace=True)
        # Validate time series & set
        self.__validate_ts_and_set_df()
        # Drop duplicate dates for analysis
        self.time_series_df.drop_duplicates(
            subset="timestamp", keep="first", inplace=True
        )
        # Sort dates
        self.time_series_df.sort_values("timestamp", ascending=True, inplace=True)
        # Reset index
        self.time_series_df.reset_index(drop=True, inplace=True)

    def stock_to_JSON(self):
        """create JSON

        Returns:
            JSON
        """
        return self.json()

    def drop_failure_cases(self, failure_cases):
        """Drop failed cases from dataframe"""
        index_list = failure_cases["index"].values.tolist()
        return self.time_series_df.drop(index_list, inplace=True)

    # TODO: Refactor schema, can be provided to function
    # This works for the stock, but not for the avocados
    # Each dataset has a schema associated with it
    # stock_dataset schema
    # avocado_dataset_schema
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
                raise SchemaError

    def __init__(self, *args, **kwargs):

        # Initialize object with Pydantic type checking
        # Inherit init from superclass

        try:
            super().__init__(*args, **kwargs)
            self.__data_clean_df()
        except ValueError:
            raise ValueError
        except TypeError:
            raise TypeError
