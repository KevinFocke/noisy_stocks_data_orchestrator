from typing import Optional

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic.types import PositiveInt
from sqlalchemy import create_engine

from customdatastructures import DatabaseQuery
from ingress import fetch_stocks_to_TimeSeries, fetch_weather_to_TimeSeries

# Convert dates to datetime
# https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html


# Worfklows contain tasks but no logic. Tasks do the work.


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


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

    # find continuous

    print(stocks_time_series)

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

    # SPEED: change order of find_movers_and_shakers and pivot_rows_to_col
    # (requires refactor)

    largest_stocks = stocks_time_series.find_movers_and_shakers(  # type: ignore
        start_date=longest_consecutive_days_sequence[0],
        end_date=longest_consecutive_days_sequence[-1],
    )
    # splat into collections then select first

    stocks_time_series.drop_col_except([el[0] for el in largest_stocks])

    print(stocks_time_series)

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

    print(weather_time_series.time_series_df)
    print(weather_time_series.time_series_df.memory_usage(deep=True).sum())

    # TODO: pair long & latitude as a string lon_lat
    weather_time_series.pivot_rows_to_cols(
        index="timestamp", columns=["longitude", "latitude"], values="precipitation"
    )

    # # close the db connection

    # weather_time_series.pivot_rows_to_cols(
    # index="timestamp", columns="geometry", values="precipitation"
    # )

    print(stocks_time_series.time_series_df)
    print(weather_time_series.time_series_df)
    # do this for every stock!
    corr_matrix = weather_time_series.time_series_df.corrwith(
        stocks_time_series.time_series_df["FLEX"], axis=0, drop=False, method="pearson"
    )  # axis 1 for row-wise calculation
    print(corr_matrix)

    # for every stock:
    # correlate the column
    # multi index giving problems?

    sql_alchemy_stock_engine.dispose()

    # TODO: cleanup orion db files


if __name__ == "__main__":
    stock_correlation_flow()
