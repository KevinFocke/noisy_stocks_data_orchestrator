"""Custom data structures and their methods
    """

from datetime import datetime, timedelta
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
    """check if provided Path is a file"""
    return path.is_file()


@validate_arguments
@task
def folder_exists(path: Path):
    """check if provided Path is a folder"""
    return path.is_dir()


class DatabaseQuery(BaseModel):
    """values and variables related to analysis ingestion stage.

    Upon initialization creates begin and end timestamps in matter of priority:
    1. Based on process_begin_and_end_timestamp (begin, end)
    2. Based on target_date + interval_in_days
    3. Based on time.now() - provided days_ago + interval_in_days
    4. Based on time.now() - 20 years ago + interval_in_days"""

    select_fields: list[str]
    from_database: str
    interval_in_days: Optional[PositiveInt] = 5
    process_begin_and_end_timestamp: Optional[tuple[datetime, datetime]] = None
    _begin_timestamp: datetime = PrivateAttr()
    _end_timestamp: datetime = PrivateAttr()
    target_date: Optional[datetime] = None
    days_ago: Optional[PositiveInt] = None  # eg 5 means 5 days ago

    def to_sql(self):
        """output sql query"""
        # TODO: Rework to sqlalchemy query
        stocks_query = (
            r"SELECT "
            + self._unfold_select_fields()
            + r"FROM "
            + str(self.from_database)
            + " WHERE "
            + r"timestamp >= "
            + r"'"
            + str(self._begin_timestamp.date())
            + r"'"
            + r" and "
            + r"timestamp <= "
            + r"'"
            + str(self._end_timestamp.date())
            + r"'"
            + r";"
        )
        return stocks_query

    def calculate_target_date(self):
        # calculate target_date if not provided
        if self.target_date is None:
            if self.days_ago is None:
                years_ago = 20
                # TODO: Calculate leap years between now and date
                self.days_ago = (years_ago * 365) + 5
            today = datetime.now()
            return today - timedelta(days=self.days_ago)
        return self.target_date

    def _unfold_select_fields(self):
        """splat list into comma-seperated string; eg. ["Hello", "There"]
        becomes "Hello, there]"""
        unfolded_select_fields = ""
        for field in self.select_fields:
            unfolded_select_fields += field + r","
        return unfolded_select_fields[:-1] + r" "  # remove last comma

    def _output_date(
        self,
        date: datetime,
        date_output_format: str = r"%Y-%m-%d",
        as_string: bool = True,
    ):
        """output a timestamp
        if as_string == 1 then output a string format else output datetime"""
        if as_string is True:
            output_date = date.strftime(date_output_format)
        else:
            output_date = date
        return output_date

    def output_begin_timestamp(
        self,
        date_output_format: str = r"%Y-%m-%d",
        as_string: bool = True,
    ):
        """output the begin timestamp in string format"""
        return self._output_date(
            date=self._begin_timestamp, date_output_format=date_output_format
        )

    def output_end_timestamp(
        self, date_output_format: str = r"%Y-%m-%d", as_string: bool = True
    ):
        """output the end timestamp in string format"""
        return self._output_date(
            date=self._end_timestamp, date_output_format=date_output_format
        )

    def calculate_date_interval(
        self,
        date: datetime,
        interval_in_days: PositiveInt = 5,
    ) -> tuple[datetime, datetime]:
        """
        input: date in Year-Month-Day eg. 2022-07-20
        interval_in_days eg. 2
        output: dict eg {begindate:"2022-07-18"
        enddate:"2022-07-22"""

        begin_date = date - timedelta(int(interval_in_days))
        end_date = date + timedelta(int(interval_in_days))

        return (begin_date, end_date)

    def __init__(self, *args, **kwargs):

        # Initialize object with Pydantic type checking
        # Inherit init from superclass
        super().__init__(*args, **kwargs)
        if self.process_begin_and_end_timestamp is None:
            if self.target_date is None:
                self.target_date = self.calculate_target_date()
            self._begin_timestamp, self._end_timestamp = self.calculate_date_interval(
                date=self.target_date, interval_in_days=self.interval_in_days  # type: ignore
            )
        else:  # interval is provided by user
            self._begin_timestamp = self.process_begin_and_end_timestamp[0]
            self._end_timestamp = self.process_begin_and_end_timestamp[1]


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
        # Sort index
        self.time_series_df.sort_index(inplace=True)
        # Validate time series & set
        self.__validate_ts_and_set_df()

    def stock_to_JSON(self):
        """create JSON

        Returns:
            JSON
        """
        return self.json()

    def calc_longest_consecutive_days(
        self, treshold: PositiveInt = 20, min_consecutive_days: PositiveInt = 3
    ):
        """calculate the largest timeseries sequence without gaps inbetween in the existing df

        for stocks treshold should be 1"""

        # group by day & count of group
        grouped_dates_df = (
            self.time_series_df.groupby(self.timestamp_index_name)
            .size()
            .to_frame("size")
        )
        # filter based on threshold
        grouped_dates_df = grouped_dates_df[grouped_dates_df["size"] >= treshold]

        # prepare adding missing dates
        # sort dates in asc order
        grouped_dates_df = grouped_dates_df.sort_index(ascending=True)

        # get start & end date of df
        start_date = grouped_dates_df.index[0]
        end_date = grouped_dates_df.index[-1]

        # create date range to include missing dates

        date_range_including_missing = pd.date_range(
            start=start_date, end=end_date, freq="D"
        )

        # add missing dates
        grouped_dates_df = grouped_dates_df.reindex(date_range_including_missing, fill_value=0)  # type: ignore

        # sort dates in desc order

        # we go through the reversed dates
        # legend: x is a value > threshold
        # leftwise is the smallest date, right is biggest date
        # x-xx-x
        #      ^
        # increase cur_seq_count by one
        # save cur value to pd dataframe
        # x-xx-x
        #     ^
        # reset cur_seq_count
        # save cur value to pd dataframe

        grouped_dates_df = grouped_dates_df.sort_index(ascending=False)

        cur_seq_count = 0
        for df_index, df_row in grouped_dates_df.iterrows():
            if df_row[0] < treshold:
                cur_seq_count = 0
            else:
                cur_seq_count += 1
            grouped_dates_df.loc[df_index, "consecutive_days_sequence"] = cur_seq_count

        # filter based on threshold
        grouped_dates_df = grouped_dates_df[
            grouped_dates_df["consecutive_days_sequence"] >= min_consecutive_days
        ]

        valid_indexes = []

        for df_index, df_row in grouped_dates_df.iterrows():
            valid_indexes.append(df_index)

        # TODO: define return type tuple of Timestamps
        # TODO: write test
        # TODO: allow user to supply custom DataFrame?
        return tuple(valid_indexes)

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
