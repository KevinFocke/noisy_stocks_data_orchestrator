from math import sqrt
from typing import Optional

import numpy as np
import numpy.typing as npt
from numba import jit
from numba.typed import List as NumbaList
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic.types import PositiveInt
from sqlalchemy import create_engine

from customdatastructures import DatabaseQuery
from ingress import fetch_stocks_to_TimeSeries, fetch_weather_to_TimeSeries


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


def np_mean_per_col(np_array):
    return NumbaList(np_array.mean(axis=0).tolist())  # axis 0 is over rows


def np_stdev_per_row(np_array):
    return NumbaList(np_array.std(axis=0).tolist())
    # axis 0 is over rows
    # numba requires specific type list https://numba.readthedocs.io/en/stable/reference/deprecation.html#deprecation-of-reflection-for-list-and-set-types


@jit(nopython=True)
def pearson_corr(
    dataset_stdevs,
    stocks_stdevs,
    stocks_array=np.array([[]]),
    dataset_array=np.array([[]]),
):
    # TODO: can I cleanup types?

    (
        stocks_array_row_count,
        stocks_array_col_count,
    ) = stocks_array.shape  # how many cols?
    dataset_array_row_count, dataset_array_col_count = dataset_array.shape

    # sanity check, are there an equal amount of rows?
    if stocks_array_row_count != dataset_array_row_count:
        raise ValueError("rows should be of equal size")

    # iterate over stocks
    for stock_col_index in range(stocks_array_col_count):
        # many iterations here thus use njit
        datapoint_correlation = np.empty(dataset_array_col_count)
        # one wide series
        # cur_stock_mean = stocks_means[stock_col_index]
        cur_stock_array = stocks_array[
            0:, stock_col_index
        ]  # all rows, first col; in other words: current stock
        cur_stock_stdev = stocks_stdevs[stock_col_index]
        for datapoint_col_index in range(dataset_array_col_count):
            cur_datapoint_array = dataset_array[
                0:, datapoint_col_index
            ]  # array is empty?
            # cur_datapoint_mean = dataset_means[datapoint_col_index]
            cur_datapoint_stdev = dataset_stdevs[datapoint_col_index]
            numerator = np.cov(cur_datapoint_array, cur_stock_array, bias=True)[0][
                1
            ]  # bias True measn normalize by N
            # result is a long array
            denominator = cur_stock_stdev * cur_datapoint_stdev
            datapoint_correlation[datapoint_col_index] = numerator / denominator

        print(datapoint_correlation)
        print(datapoint_correlation.shape)
        # calc correlation

def correlate():

@flow(task_runner=SequentialTaskRunner(), name="stock_correlation_flow")
def stock_correlation_flow():

    # best practice for creating sqlalchemy engine
    # one connection per database
    # https://docs.sqlalchemy.org/en/14/core/connections.html#basic-usage
    stocks_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    )
    # preferences
    stock_select_fields = ["timestamp", "stock_symbol", "price_close"]
    stock_database_name = "stock_timedata"
    stock_interval_in_days = 5
    stocks_numeric_col_name = "price_close"
    weather_select_fields = ["timestamp", "longitude", "latitude", "precipitation"]
    weather_database_name = "weather"
    weather_numeric_col_name = "precipitation"

    # SQLAlchemy will not turn itself into a pickle from another process. DO NOT PICKLE!
    sql_alchemy_stock_engine = create_engine(stocks_db_conn_string)

    stocks_db_query_object = DatabaseQuery(
        select_fields=stock_select_fields,
        from_database=stock_database_name,
        interval_in_days=stock_interval_in_days,
    )

    # get TimeSeries
    stocks_time_series = fetch_stocks_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_stock_engine,
        query=stocks_db_query_object.to_sql(),
        numeric_col_name=stocks_numeric_col_name,
    ).result()

    longest_consecutive_days_sequence = (
        stocks_time_series.calc_longest_consecutive_days_sequence()
    )

    stocks_time_series.drop_row_except(longest_consecutive_days_sequence)

    # TODO: drop except should raise an error if dataframe is empty
    # if df.empty: Raise ValueError
    # #really it's just df.empty #very nice!

    stocks_time_series.pivot_rows_to_cols(
        index="timestamp", columns="stock_symbol", values="price_close"
    )

    # SPEED, minor: change order of find_movers_and_shakers and pivot_rows_to_col
    # (requires refactor)

    largest_stocks = stocks_time_series.find_movers_and_shakers(  # type: ignore
        start_date=longest_consecutive_days_sequence[0],
        end_date=longest_consecutive_days_sequence[-1],
    )

    stocks_time_series.drop_col_except([stock[0] for stock in largest_stocks])

    weather_db_query_object = DatabaseQuery(
        select_fields=weather_select_fields,
        from_database=weather_database_name,
        process_begin_and_end_timestamp=(
            longest_consecutive_days_sequence[0],  # first el
            longest_consecutive_days_sequence[-1],  # last el
        ),
    )

    datasets_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/datasets"
    )

    sql_alchemy_datasets_engine = create_engine(datasets_db_conn_string)

    weather_time_series = fetch_weather_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_datasets_engine,
        query=weather_db_query_object.to_sql(),
        numeric_col_name=weather_numeric_col_name,
        timeout=120,
    ).result()

    # TODO: pair long & latitude in single datatype instead of multi-index
    # ideally, the lon+lat would be stored in a POINT variable with Geopandas
    # however Geopandas x to y conversion worked dreadfully slow 1+ min
    weather_time_series.pivot_rows_to_cols(
        index="timestamp", columns=["longitude", "latitude"], values="precipitation"
    )
    print(stocks_time_series.time_series_df)
    print(weather_time_series.time_series_df)

    stock_col_list = list(stocks_time_series.time_series_df.columns)

    weather_numpy_array = weather_time_series.time_series_df.to_numpy()
    stocks_numpy_array = stocks_time_series.time_series_df.to_numpy()
    print(weather_numpy_array)
    print(weather_numpy_array.shape)

    if stocks_numpy_array.shape[0] != weather_numpy_array.shape[0]:
        raise ValueError("rows must be equal size!!")
    else:
        rowsize = stocks_numpy_array.shape[0]

    stocks_stdevs = np_stdev_per_row(stocks_numpy_array)
    # dataset_means = np_mean_per_col(dataset_array)
    dataset_stdevs = np_stdev_per_row(weather_numpy_array)
    pearson_corr(
        stocks_array=stocks_numpy_array,
        stocks_stdevs=stocks_stdevs,
        dataset_stdevs=dataset_stdevs,
        dataset_array=weather_numpy_array,
    )
    # TODO: move out of its column

    # print(corr_matrix)

    #    corr_matrix = weather_time_series.time_series_df.corrwith(
    #        stocks_time_series.time_series_df["TIF"], axis=0, drop=False, method="pearson"
    #    )  # axis 1 for row-wise calculation
    #    print(corr_matrix)

    sql_alchemy_stock_engine.dispose()

    # TODO: cleanup orion db files


if __name__ == "__main__":
    stock_correlation_flow()
