from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
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


@flow(task_runner=SequentialTaskRunner())
def stock_correlation_flow():

    # create engine
    stocks_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    )
    # SQLAlchemy will not turn itself into a pickle from another process. DO NOT PICKLE!
    sql_alchemy_stock_engine = create_engine(stocks_db_conn_string)

    # query stocks
    query_stocks = "SELECT timestamp, stock_symbol, price_close \
        FROM stock_timedata \
        WHERE stock_symbol = 'IBM' and timestamp between '2009-01-01' and '2009-02-01';"

    stocks_time_series = query_database_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_stock_engine, query=query_stocks
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

    # close the db connection
    sql_alchemy_stock_engine.dispose()


if __name__ == "__main__":
    stock_correlation_flow()
