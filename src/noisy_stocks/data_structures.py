import json
from datetime import datetime
from typing import Optional

import pandas as pd
from pydantic import BaseModel

# Purpose of primary datastructures:
# TimeSeriesDict for validation of the ingested info.
# Pandas DataFrame for analysis
# JSON for moving information

# Classes should be PascalCase
# Mark optional parameters by NotRequired


class TimeSeriesDict(BaseModel):
    # Used for validation of ingested information; Pandas has no native typing support
    time: datetime


def time_series_dict_to_JSON(dict: TimeSeriesDict):

    # Exports time series dict
    pass


def slice_pd_dataframe(
    pd_dataframe: pd.DataFrame, start_date, end_date
) -> pd.DataFrame:

    # Slice a timerange out of a pandas dataframe

    return pd_dataframe


def pd_dataframe_to_JSON(pd_dataframe: pd.DataFrame):

    # Convert dataframe to TimeSeriesDict for validation

    # Use built-in method
    # Validate
    # Converts a Pandas DataFrame to JSON file
    pass


class Stock(BaseModel):
    id: str  # Stock symbol is unique identifier
    exchange: Optional[str]
    data: TimeSeriesDict
