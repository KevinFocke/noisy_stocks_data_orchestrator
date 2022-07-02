import pickle
from datetime import datetime, timedelta
from typing import Optional

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic.types import PositiveInt
from sqlalchemy import create_engine

from customdatastructures import DatabaseQuery
from ingress import (
    fetch_stocks_to_TimeSeries,
    fetch_weather_to_TimeSeries,
    query_database_to_TimeSeries,
)

# Convert dates to datetime
# https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html


# Worfklows contain tasks but no logic. Tasks do the work.


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


@flow(task_runner=SequentialTaskRunner())
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
    stocks_query = stocks_db_query_object.to_sql()

    # get TimeSeries
    stocks_time_series = fetch_stocks_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_stock_engine,
        query=stocks_query,
        numeric_col_name=stocks_numeric_col_name,
    ).result()

    # find continuous

    print(stocks_time_series)

    longest_consecutive_days_sequence = (
        stocks_time_series.calc_longest_consecutive_days_sequence()
    )

    # print(longest_consecutive_days_sequence)

    weather_db_query_object = DatabaseQuery(
        select_fields=weather_select_fields,
        from_database=weather_database_name,
        process_begin_and_end_timestamp=(
            longest_consecutive_days_sequence[0],
            longest_consecutive_days_sequence[-1],
        ),
    )

    datasets_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/datasets"
    )

    sql_alchemy_datasets_engine = create_engine(datasets_db_conn_string)

    weather_query = (
        "SELECT * FROM weather WHERE timestamp between '2002-01-01' and '2002-01-07';"
    )

    weather_time_series = fetch_weather_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_datasets_engine,
        query=weather_query,
        numeric_col_name=weather_numeric_col_name,
        timeout=120,
    ).result()

    print(weather_time_series.time_series_df)
    print(weather_time_series.time_series_df.memory_usage(deep=True).sum())

    # stocks_time_series.drop_except(keep=longest_consecutive_days_sequence)

    # # close the db connection

    sql_alchemy_stock_engine.dispose()

    # TODO: cleanup orion db files


if __name__ == "__main__":
    stock_correlation_flow()
