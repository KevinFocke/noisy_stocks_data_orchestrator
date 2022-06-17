"""Custom data structures and their methods
    """

from pathlib import Path

import pandas as pd
import pandera as pa
from pandera.errors import SchemaError
from prefect.tasks import task
from pydantic import BaseModel, validate_arguments

# Classes should be PascalCase
# Check type using pydantic, check DataFrame using pandera


@validate_arguments
@task
def file_exists(path: Path):
    return path.is_file()


@validate_arguments
@task
def folder_exists(path: Path):
    return path.is_dir()


class Extractor(BaseModel):
    resource_type: str
    resource_location: str
    resource_schema: pa.DataFrameSchema
    file_suffix: str


class ExtractorFromURL(Extractor):
    pass
    # Downloads file from URL, (unzips), and then c


class ExtractorFromFilesystem(Extractor):
    pass
    # It extracts a file or folder from filesystem

    def output_filepath_list(self):
        pass

    pass


class ExtractorFactory(Extractor):
    # Depending on resource_type, create a fitting extractor

    def create_path_from_location(self):
        # self.resource_path = Path(self.resource_location)
        pass

    def create_extractor(self):
        # TODO: Add Url

        # If file or folder, create Path
        # If URL, validate URL

        pass


class TimeSeries(BaseModel):
    name: str  # unique identifier
    resource_unique_id: int = 0  # Which dataset?
    resource_schema: pa.DataFrameSchema
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

    # TODO: Refactor schema, should be provided to function
    # This works for the stock, but not for the avocados
    # Each dataset has a schema associated with it
    # stock_dataset schema
    # avocado_dataset_schema
    def __validate_schema(self):
        """Validate pandera df schema"""
        time_series_df_schema = self.resource_schema
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
            self.__validate_schema()

    # WISHLIST: Refactor data cleaning into seperate function

    def __init__(self, *args, **kwargs):

        # Initialize object with Pydantic type checking
        # Inherit init from superclass

        super().__init__(*args, **kwargs)
        self.__data_clean_df()
