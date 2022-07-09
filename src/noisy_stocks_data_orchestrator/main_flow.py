from pathlib import Path

import numpy as np
from numba import jit
from numba.typed import List as NumbaList
from pandas import Series
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from pydantic import PositiveInt
from pytest import approx
from sqlalchemy import create_engine

from customdatastructures import CorrDatabaseQuery
from egress import corr_to_db_content, pickle_object_to_path, publish
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


# BUG: Cannot assign @task to it, or will receive error
# TypeError: cannot pickle '_nrt_python._MemInfo' object
@jit(nopython=True)
def pearson_corr(
    dataset_stdevs,
    stocks_stdevs,
    stocks_np_array=np.array([[]]),
    dataset_np_array=np.array([[]]),
):
    # TODO: can I cleanup types?

    if stocks_np_array.shape[0] != dataset_np_array.shape[0]:
        raise ValueError("rows must be equal size!!")

    (
        stocks_array_row_count,
        stocks_array_col_count,
    ) = stocks_np_array.shape  # how many cols?
    dataset_array_row_count, dataset_array_col_count = dataset_np_array.shape

    # sanity check, are there an equal amount of rows?
    if stocks_array_row_count != dataset_array_row_count:
        raise ValueError("rows should be of equal size")

    # iterate over stocks
    correlations = NumbaList()
    for stock_col_index in range(stocks_array_col_count):
        # many iterations here thus use njit
        corr_to_stock = np.empty(dataset_array_col_count)
        # one wide series
        # cur_stock_mean = stocks_means[stock_col_index]
        cur_stock_array = stocks_np_array[
            0:, stock_col_index
        ]  # all rows, first col; in other words: current stock
        cur_stock_stdev = stocks_stdevs[stock_col_index]
        for datapoint_col_index in range(dataset_array_col_count):
            cur_datapoint_array = dataset_np_array[
                0:, datapoint_col_index
            ]  # array is empty?
            # cur_datapoint_mean = dataset_means[datapoint_col_index]
            cur_datapoint_stdev = dataset_stdevs[datapoint_col_index]
            numerator = np.cov(cur_datapoint_array, cur_stock_array, bias=True)[0][
                1
            ]  # bias True measn normalize by N
            # result is a long array
            denominator = cur_stock_stdev * cur_datapoint_stdev
            corr_to_stock[datapoint_col_index] = numerator / denominator

        # print(corr_to_stock)
        # print(corr_to_stock.shape)
        correlations.append(corr_to_stock)
    return correlations  # list containing correlations per stock
    # calc correlation


@flow(task_runner=SequentialTaskRunner())
def correlate_datasets(*args, **kwargs):
    return list(
        pearson_corr(*args, **kwargs)
    )  # convert back from NumbaList to regular Python list


@flow(task_runner=SequentialTaskRunner(), name="stock_correlation_flow")
def stock_correlation_flow(
    corr_dict_pickle_storage_path=r"/home/kevin/coding_projects/noisy_stocks/persistent_data/corr_dicts",
    dataset_uid_col_name_list=[
        "latitude",
        "longitude",
    ],  # one or more values that uniquely identify a datapoint
    posts_per_day: PositiveInt = 10,
    stocks_db_conn_string=(
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    ),
    datasets_db_conn_string=(
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/datasets"
    ),
):

    # TODO: refactor preferences to arguments of func
    # best practice for creating sqlalchemy engine
    # one connection per database
    # https://docs.sqlalchemy.org/en/14/core/connections.html#basic-usage
    # preferences
    min_stocks_output = (
        posts_per_day * 6
    )  # why * 6? see lengthy explanation in function
    stock_select_fields = ["timestamp", "stock_symbol", "price_close"]
    stock_database_name = "stock_timedata"
    stock_interval_in_days = 5
    stocks_numeric_col_name = "price_close"
    dataset_select_fields = ["timestamp", "longitude", "latitude", "precipitation"]
    dataset_database_name = "weather"
    dataset_numeric_col_name = "precipitation"

    # SQLAlchemy will not turn itself into a pickle from another process. DO NOT PICKLE!
    sql_alchemy_stock_engine = create_engine(stocks_db_conn_string)

    stocks_db_query_object = CorrDatabaseQuery(
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

    # consideration: pivot_rows_to_cols works destructively on existing df,
    # but it is space efficient

    stocks_time_series.pivot_rows_to_cols(
        index="timestamp", columns="stock_symbol", values="price_close"
    )

    # SPEED, minor: change order of find_movers_and_shakers and pivot_rows_to_col
    # (requires refactor)

    largest_stocks = stocks_time_series.find_movers_and_shakers(  # type: ignore
        start_date=longest_consecutive_days_sequence[0],
        end_date=longest_consecutive_days_sequence[-1],
        min_stocks_output=min_stocks_output,
        max_stocks_output=min_stocks_output,
    )

    stocks_time_series.drop_col_except([stock[0] for stock in largest_stocks])

    dataset_db_query_object = CorrDatabaseQuery(
        select_fields=dataset_select_fields,
        from_database=dataset_database_name,
        process_begin_and_end_timestamp=(
            longest_consecutive_days_sequence[0],  # first el
            longest_consecutive_days_sequence[-1],  # last el
        ),
    )

    sql_alchemy_datasets_engine = create_engine(datasets_db_conn_string)

    dataset_time_series = fetch_weather_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_datasets_engine,
        query=dataset_db_query_object.to_sql(),
        numeric_col_name=dataset_numeric_col_name,
        timeout=120,
    ).result()

    # should be seperate function; works too
    dataset_time_series.pivot_rows_to_cols(
        index="timestamp", columns=dataset_uid_col_name_list, values="precipitation"
    )
    #   print(stocks_time_series.time_series_df)

    stock_col_list = list(stocks_time_series.time_series_df.columns)
    dataset_col_list = list(dataset_time_series.time_series_df.columns)

    # get stock correlations; list containing numpy arrays per stock, one numpy array

    correlations = correlate_datasets(
        stocks_np_array=stocks_time_series.time_series_df.to_numpy(),
        stocks_stdevs=np_stdev_per_row(stocks_time_series.time_series_df.to_numpy()),
        dataset_stdevs=np_stdev_per_row(dataset_time_series.time_series_df.to_numpy()),
        dataset_np_array=dataset_time_series.time_series_df.to_numpy(),
    ).result()

    # print(correlations)

    # sanity check
    assert len(correlations) == len(stock_col_list)

    stock_index = 0
    corr_dict = {}
    # get highest correlations
    # TODO: refactor into own function
    # TODO: limit posts per day. Might be a thorny problem because
    # longest_consecutive_days_sequence will cause overlapping periods
    # BUG: Ingesting new stocks to the stock database might publish more posts than requested; the upsert of export assumes the exact same stocks will be upserted each time
    # WORKAROUND: if the stock dataset ever changes, wait until all remaining posts are published.
    # deleting those posts is not a workaround because you woulnd't be able to determinstically build up the same database from the existing pickles

    for stock_corr in correlations:
        # sanity check
        assert len(stock_corr) == len(dataset_col_list)
        # grab index of highest abs correlation
        max_corr_index = np.argmax(np.abs(stock_corr))
        highest_corr = stock_corr[max_corr_index]
        dataset_uid = dataset_col_list[max_corr_index]
        stock = stock_col_list[stock_index]
        corr_dict[stock] = {
            "begin_date": longest_consecutive_days_sequence[0],
            "end_date": longest_consecutive_days_sequence[-1],
            "dataset_database_name": dataset_database_name,
            "highest_corr": highest_corr,
            "stock_database_name": stock_database_name,
            "stock_pd_series": stocks_time_series.time_series_df[stock],
            "stock_num_col": stocks_time_series.numeric_col_name,  # contains timestamps + values
            "dataset_uid": dataset_uid,
            "dataset_uid_col_name_list": dataset_uid_col_name_list,  # eg. (lat,lon)
            "dataset_pd_series": dataset_time_series.time_series_df[dataset_uid],
            "dataset_num_col": dataset_time_series.numeric_col_name,  # contains timestamps + values
        }
        assert isinstance(corr_dict[stock]["stock_pd_series"], Series)
        assert isinstance(corr_dict[stock]["dataset_pd_series"], Series)

        corr_pd = corr_dict[stock]["stock_pd_series"].corr(
            corr_dict[stock]["dataset_pd_series"]
        )
        assert corr_pd == approx(corr_dict[stock]["highest_corr"])

        # corr_dict fields:
        # dict of dicts,
        # first layer keys are stocks (APPL, AMZN)
        # add begindate and enddate, stock_data, geo_points_data
        stock_index += 1

    print(corr_dict)
    pickle_object_to_path(corr_dict, folder_path=Path(corr_dict_pickle_storage_path))
    # print(corr_dict)

    # TODO: once Orion is out of beta, create a dependency flow https://github.com/PrefectHQ/prefect/blob/b9503001f5de642d48d7d5248436d1e8861cffed/docs/core/idioms/flow-to-flow.md
    sql_alchemy_stock_engine.dispose()


@flow(task_runner=SequentialTaskRunner(), name="correlate_and_publish_flow")
def correlate_and_publish(
    corr_dict_pickle_storage_path=r"/home/kevin/coding_projects/noisy_stocks/persistent_data/corr_dicts",
    dataset_uid_col_name_list=[
        "longitude",
        "latitude",
    ],  # one or more values that uniquely identify a datapoint
    posts_per_day: PositiveInt = 10,
    stocks_db_conn_string: str = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    ),
    datasets_db_conn_string: str = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/datasets"
    ),
    content_db_conn_string: str = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/content",
):

    stock_correlation_flow(
        corr_dict_pickle_storage_path=corr_dict_pickle_storage_path,
        dataset_uid_col_name_list=dataset_uid_col_name_list,
        posts_per_day=posts_per_day,
        stocks_db_conn_string=stocks_db_conn_string,
        datasets_db_conn_string=datasets_db_conn_string,
    )

    corr_to_db_content(content_db_conn_string=content_db_conn_string)

    # publish()


if __name__ == "__main__":

    correlate_and_publish()
