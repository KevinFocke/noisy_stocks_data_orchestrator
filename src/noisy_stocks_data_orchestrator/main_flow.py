from datetime import datetime, timedelta
from typing import Optional

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic.types import PositiveInt
from sqlalchemy import create_engine

from customdatastructures import DatabaseQuery
from ingress import fetch_stocks_to_TimeSeries

# Convert dates to datetime
# https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html


# Worfklows contain tasks but no logic. Tasks do the work.


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


def calc_longest_timeseries_sequence(treshold: PositiveInt = 20):
    """calculate the largest timeseries sequence without gaps inbetween"""

    # validate pandera scheme
    # treshold could be 1 for individual stocks!
    cur_date_sequence_count = 0
    df = [5]

    # times_gaps = df.index - df.index.shift(1)
    for indexes in df:
        if total_dates_count < treshold:
            cur_date_sequence_count = 0
        else:
            cur_date_sequence_count += 1
        # df at index, not cur_date_sequence_count
    # take the max
    # 20 stocks minimum

    # group by date, count the # of stocks
    # then index shift
    # if prev exists,

    # sorted list of counts based on the date
    # continuous_count = 0
    # if datapoints_count < treshold

    # times_gaps = df.index - df.index.shift(1)
    # calculate date interval


@flow(task_runner=SequentialTaskRunner())
def stock_correlation_flow():

    # best practice for creating sqlalchemy engine
    # one connection per database
    # https://docs.sqlalchemy.org/en/14/core/connections.html#basic-usage
    stocks_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    )
    # preferences
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    interval_in_days = 5
    # SQLAlchemy will not turn itself into a pickle from another process. DO NOT PICKLE!
    sql_alchemy_stock_engine = create_engine(stocks_db_conn_string)

    db_query_object = DatabaseQuery(
        select_fields=select_fields,
        from_database=database_name,
        interval_in_days=interval_in_days,
    )
    stocks_query = db_query_object.to_sql()
    stocks_numeric_col_name = "price_close"

    # get TimeSeries
    stocks_time_series = fetch_stocks_to_TimeSeries(
        sql_alchemy_stock_engine=sql_alchemy_stock_engine,
        stocks_query=stocks_query,
        numeric_col_name=stocks_numeric_col_name,
    ).result()

    # find continuous

    print(stocks_time_series)

    # find a valid time series, minimum 4 consecutive days, prefer 5 consecutive days

    # from fin_analyze

    # select movers and shakers

    # based on the valid movers & shakers

    # datasets_db_conn_string = (
    #     "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/datasets"
    # )

    # sql_alchemy_datasets_engine = create_engine(datasets_db_conn_string)

    # # query datasets

    # query_weather = r"SELECT * FROM weather WHERE timestamp >= '2002-07-01'::timestamp - interval '5 day' and timestamp <= '2002-07-01'::timestamp + interval '5 day';"

    # "SELECT timestamp, price_close \
    #     FROM stock_timedata \
    #     WHERE stock_symbol = 'GE' and timestamp between '2009-01-01' and '2009-02-01'"
    # time_series2 = query_database_to_TimeSeries(
    #     sql_alchemy_engine=sql_alchemy_datasets_engine, query=query_weather, timeout=60
    # ).result()

    # print(time_series2)

    # # close the db connection

    sql_alchemy_stock_engine.dispose()


if __name__ == "__main__":
    # construct_stocks_query(2)
    stock_correlation_flow()
