from datetime import datetime
from pathlib import Path
from queue import SimpleQueue
from typing import Optional

import pandas as pd
import pandera as pa
from pandera.dtypes import Timestamp
from pandera.errors import SchemaError
from prefect.flows import flow
from prefect.tasks import task
from pydantic import BaseModel

from ingress import folder_exists

# Classes should be PascalCase
# Check type using pydantic, check DataFrame using pandera
# Mark optional pydantic parameters with NotRequired


# Typical order of data analysis:
# 1. Fetch required timeseries aggregation from database (JSON)
# 2. Convert timeseries into Pandas DataFrame for analysis
# 3. Convert DataFrame to Markdown for publishing


class ObjectGenerationError(Exception):
    """Object cannot be created"""


class FolderExtractionQueue(BaseModel):
    folder_queue_dirty: SimpleQueue

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True


class FileExtractionQueue(BaseModel):
    file_queue_dirty: SimpleQueue

    class Config:  # Pydantic configuration
        arbitrary_types_allowed = True

    @flow
    def _clean_file_queue(self, file_queue_dirty):
        # While the queue is not empty, check if it's a file
        pass


@task
def create_folder_extraction_queue(path: Path):
    # TODO: Add recursve folder extraction into a queue
    # check if is folder
    if not folder_exists(path):
        return ValueError(f"Folder {path} not found")

    # TODO: Pathlib GLOB IT UP! (to recursively open folders)


def clean_folder_queue():
    file_queue: SimpleQueue = SimpleQueue()
    pass
    # Check if url is a file, then add to queue
    # Implement simple queue structure https://github.com/python/cpython/blob/3.10/Lib/queue.py
    # https://docs.python.org/3/library/pathlib.html
    # urlpath = path + r"*" + file_suffix

    # df = dd.read_csv(r"urlpath/*.csv")
    # https://docs.dask.org/en/stable/generated/dask.dataframe.read_csv.html

    # base case; folder is empty or recursive_levels < 0

    # how many levels deep can you recurse into folder structure?


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
