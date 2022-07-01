from datetime import datetime, timedelta
from typing import Optional

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from sqlalchemy import create_engine

from ingress import query_database_to_TimeSeries

# Convert dates to datetime
# https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html


# Worfklows contain tasks but no logic. Tasks do the work.


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


@task
def calculate_date_interval(
    date: datetime, interval_in_days: int = 5, date_format: str = r"%Y-%m-%d"
) -> dict:
    """
    input: date in Year-Month-Day eg. 2022-07-20
    interval_in_days eg. 2
    output: dict eg {begindate:"2022-07-18"
    enddate:"2022-07-22"""

    # TODO: upgrade datetime to timezone aware

    begindate = date - timedelta(int(interval_in_days))
    enddate = date + timedelta(int(interval_in_days))

    return {
        "begin_date": begindate.strftime(date_format),
        "end_date": enddate.strftime(date_format),
    }
    # return {
    #    "begindate": "2022-06-29",
    #    "enddate": "2022-07-03",
    # }


@flow(task_runner=SequentialTaskRunner())
def construct_stocks_query(
    interval_in_days: int = 5,
    target_date: Optional[datetime] = None,
    days_ago: Optional[int] = None,
):

    years_ago = 20
    if days_ago is None:
        # TODO: Calculate leap years between now and date
        days_ago = years_ago * 365 + 5
    if target_date is None:
        today = datetime.now()
        target_date = today - timedelta(days=days_ago)

    date_interval = calculate_date_interval(
        date=target_date, interval_in_days=interval_in_days
    ).result()
    begin_date = date_interval.get("begin_date")
    end_date = date_interval.get("end_date")

    select_fields = ["timestamp", "stock_symbol", "price_close"]

    # CLUDGE
    unfolded_select_fields = ""
    for field in select_fields:
        unfolded_select_fields += field + r","
    unfolded_select_fields = unfolded_select_fields[:-1] + r" "  # remove last comma

    database_name = "stock_timedata"
    # TODO: Rework to sqlalchemy query
    stocks_query = (
        r"SELECT "
        + unfolded_select_fields
        + r"FROM "
        + str(database_name)
        + " WHERE "
        + r"timestamp >= "
        + r"'"
        + str(begin_date)
        + r"'"
        + r"and "
        + r"timestamp <= "
        + r"'"
        + str(end_date)
        + r"'"
        + r";"
    )
    # "SELECT * \
    # FROM stock_timedata \
    # WHERE timestamp >= '2002-07-01'::timestamp - interval '5 day' and \
    # timestamp <= '2002-07-01'::timestamp + interval '5 day';;"

    print("\n" + str(stocks_query) + "\n")

    return stocks_query


@flow(task_runner=SequentialTaskRunner())
def fetch_stocks_to_TimeSeries(
    sql_alchemy_stock_engine, stocks_query, numeric_col_name, timeout=60
):
    # query stocks
    stocks_time_series = query_database_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_stock_engine,
        query=stocks_query,
        numeric_col_name=numeric_col_name,
        timeout=timeout,
    ).result()

    return stocks_time_series


@flow(task_runner=SequentialTaskRunner())
def stock_correlation_flow():

    # create engine; one per database
    stocks_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    )
    # SQLAlchemy will not turn itself into a pickle from another process. DO NOT PICKLE!
    sql_alchemy_stock_engine = create_engine(stocks_db_conn_string)
    numeric_col_name = "price_close"

    # construct query
    stocks_query = "SELECT timestamp, stock_symbol, price_close \
        FROM stock_timedata \
        WHERE stock_symbol = 'IBM' and timestamp between '2009-01-01' and '2009-02-01';"

    stocks_time_series = fetch_stocks_to_TimeSeries(
        sql_alchemy_stock_engine=sql_alchemy_stock_engine,
        stocks_query=stocks_query,
        numeric_col_name=numeric_col_name,
    ).result()

    print(stocks_time_series)

    # find a valid time series, minimum 4 consecutive days, prefer 5 consecutive days

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
    construct_stocks_query(2)
    # stock_correlation_flow()
