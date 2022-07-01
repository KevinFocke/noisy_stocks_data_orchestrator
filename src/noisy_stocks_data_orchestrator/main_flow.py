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
def calculate_stock_query_date(date, interval):
    # TODO: write test
    pass


@flow(task_runner=SequentialTaskRunner())
def construct_stocks_query(interval):

    # calculate date
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    stocks_query = "SELECT * \
    FROM stock_timedata \
    WHERE timestamp >= '2002-07-01'::timestamp - interval '5 day' and \
    timestamp <= '2002-07-01'::timestamp + interval '5 day';;"
    pass


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
    stock_correlation_flow()
