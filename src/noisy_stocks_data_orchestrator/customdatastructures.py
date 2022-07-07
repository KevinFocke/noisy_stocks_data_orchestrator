"""Custom data structures and their methods
    """

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pandera as pa
from pandas import DataFrame
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


# TODO: refactor to make it DatabaseQuery more versatile
class CorrDatabaseQuery(BaseModel):
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
            return date.strftime(date_output_format)
        else:
            return date  # TODO: add test

    def output_begin_timestamp(
        self,
        date_output_format: str = r"%Y-%m-%d",
        as_string: bool = True,
    ):
        """output the begin timestamp in string format"""
        return self._output_date(
            date=self._begin_timestamp,
            date_output_format=date_output_format,
            as_string=as_string,
        )

    def output_end_timestamp(
        self, date_output_format: str = r"%Y-%m-%d", as_string: bool = True
    ):
        """output the end timestamp in string format"""
        return self._output_date(
            date=self._end_timestamp,
            date_output_format=date_output_format,
            as_string=as_string,
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

    # df assumption: dates are aligned on day at 0:00 UTC
    timestamp_index_name: str  # What is the name of the timestamp column?
    numeric_col_name: str  # What is the name of the numeric column? eg. price_close
    time_series_df: pd.DataFrame  # DataFrame with timestamp as index, sorted DESC
    _time_series_df_schema: pa.DataFrameSchema = PrivateAttr()

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
        self.time_series_df = self.time_series_df.dropna()
        # Sort index
        self.time_series_df = self.time_series_df.sort_index(ascending=False)
        # Validate time series & set
        self.__validate_ts_and_set_df()

    def drop_row_except(self, keep_list: tuple[Timestamp]):
        """drop every row not within date range"""
        self.time_series_df = self.time_series_df[
            self.time_series_df.index.isin(keep_list)
        ]

    def drop_col_except(self, keep_list: list[str]):
        """drop every col not within keep_list"""
        self.time_series_df = self.time_series_df.filter(keep_list)

    def pivot_rows_to_cols(self, index, columns, values):
        """converts a long df into a wide df"""
        # pivot here based on date
        self.time_series_df = pd.pivot_table(
            data=self.time_series_df,
            index=index,
            columns=columns,
            values=values,
        )

        # drop cols with a null value
        self.time_series_df.dropna(axis=1, inplace=True)  # axis 1 means cols

    def calc_longest_consecutive_days_sequence(
        self,
        treshold: PositiveInt = 20,
        provided_time_series_df: Optional[DataFrame] = None,
    ) -> tuple[Timestamp]:
        """calculate the largest timeseries day sequence without gaps

        if no time time_series_df is provided, takes the self.time_series_df
        Note: for stocks the treshold should be 1 because
        there is only one value per date"""

        # TODO: Refactor
        # group by day & count of group
        if provided_time_series_df is None:  # df not provided
            grouped_dates_df = self.time_series_df
        else:
            grouped_dates_df = provided_time_series_df

        grouped_dates_df = (
            self.time_series_df.groupby(self.timestamp_index_name)
            .size()
            .to_frame("size")
        )

        # filter based on threshold
        grouped_dates_df = grouped_dates_df[grouped_dates_df["size"] >= treshold]

        # sort dates in asc order
        grouped_dates_df = grouped_dates_df.sort_index(ascending=True)
        print(grouped_dates_df)
        # get start & end date of df
        start_date = grouped_dates_df.index[0]
        end_date = grouped_dates_df.index[-1]

        # create date range to include missing dates

        date_range_including_missing = pd.date_range(
            start=start_date, end=end_date, freq="D"
        )

        # add missing dates
        grouped_dates_df = grouped_dates_df.reindex(date_range_including_missing, fill_value=0)  # type: ignore

        # algo
        # legend: x is a value > threshold
        # leftwise is the oldest date, right is newest date
        # x-xx-x
        #      ^
        # increase cur_seq_count by one
        # save cur value to pd dataframe
        # x-xx-x
        #     ^
        # reset cur_seq_count
        # save cur value to pd dataframe

        # sort by desc to itterows in reverse order
        grouped_dates_df = grouped_dates_df.sort_index(ascending=False)

        seq_count_col_name = "consecutive_days_sequence"

        cur_seq_count = 0
        for df_index, df_row in grouped_dates_df.iterrows():
            if df_row[0] < treshold:
                cur_seq_count = 0
            else:
                cur_seq_count += 1
            grouped_dates_df.loc[df_index, seq_count_col_name] = cur_seq_count

        # reset index
        grouped_dates_df.reset_index(inplace=True)  # maintains sorting
        # timestamp is renamed to index !
        grouped_dates_df = grouped_dates_df.rename(
            columns={"index": self.timestamp_index_name}
        )

        # grouped_dates_df now looks like (timestamp DESC)
        #   timestamp  size  consecutive_days_sequence
        # 0 2002-07-05   357                        1.0
        # 1 2002-07-04     0                        0.0

        max_seq_start_date_index = grouped_dates_df[seq_count_col_name].idxmax()
        max_seq_value = int(
            grouped_dates_df[seq_count_col_name].iloc[max_seq_start_date_index]
        )

        longest_timestamp_range = [
            grouped_dates_df[self.timestamp_index_name].iloc[
                max_seq_start_date_index - offset_from_max_seq_start_date_index
            ]
            for offset_from_max_seq_start_date_index in range(max_seq_value)
        ]
        # range upper bound not incl

        return tuple(longest_timestamp_range)  # type:ignore

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
        if self.time_series_df.empty:
            raise ValueError("Expected non-empty DataFrame")
        # drop row containing 0
        self.time_series_df = self.time_series_df[
            self.time_series_df[self.numeric_col_name] != 0
        ]
        self.__create_custom_df_schema()
        self.__data_clean_df()


class StockTimeSeries(TimeSeries):
    def find_movers_and_shakers(
        self,
        start_date: Timestamp,
        end_date: Timestamp,
        min_stocks_output=5,
        max_stocks_output=10,  # TODO: add as args to main_flow
    ) -> tuple[tuple[str, float]]:
        """calculates the biggest absolute difference between start date and end date

        input: expects a TimeSeries already processed by pivot_rows_to_cols

        output: ((stock_symbol_str, percentage_move_in_float))

        arg explanation:
        value_threshold # what should the minimum value difference be
        eg. 10 means the start date value will be 10 higher or lower than the end date value.

        min_stocks_output # how many stocks should the result contain at a minimum?

        max_stocks_output # how many stocks should the result contain at a maximum?
        eg. 20 means it will return 20 stocks at most.
        """

        # check min amount of stocks
        # expects pivot table with DateTimeIndex
        if min_stocks_output > max_stocks_output:
            raise ValueError(
                "arg min_stocks_output cannot be bigger than max_stocks_output"
            )

        if self.time_series_df.shape[1] < min_stocks_output:
            raise ValueError("Not enough stocks in df")

        start_date_index = self.time_series_df.index.get_loc(start_date)
        end_date_index = self.time_series_df.index.get_loc(end_date)

        start_series = self.time_series_df.iloc[start_date_index]
        end_series = self.time_series_df.iloc[end_date_index]

        # normalize based on start_series to find biggest % increase

        end_series = end_series.div(  # type: ignore
            start_series
        )  # calc end_series first otherwise we div by 1

        start_series = start_series.div(start_series)  # type: ignore

        abs_delta = start_series.sub(end_series).abs()

        abs_delta = abs_delta.sort_values(ascending=False)

        # -> list[tuple[str, PositiveInt]]
        abs_delta_df = abs_delta.to_frame()

        if abs_delta_df.shape[0] < max_stocks_output:
            pass  # no need for slicing
        else:
            abs_delta_df = abs_delta_df[:max_stocks_output]  # slice first 20 rows
        # get highest corr

        tuples_stock_and_rel_change = tuple(
            abs_delta_df.itertuples(index=True, name=None)
        )
        # tuple_list = tuple(abs_delta_df.reset_index().values.tolist())

        return tuples_stock_and_rel_change
