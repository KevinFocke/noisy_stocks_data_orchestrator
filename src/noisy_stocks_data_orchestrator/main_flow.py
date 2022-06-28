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
    db_conn_string = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"
    # SQLAlchemy will not turn itself into a pickle from another process. DO NOT PICKLE!
    sql_alchemy_engine = create_engine(db_conn_string)
    query2 = "SELECT timestamp, price_close \
        FROM stock_timedata \
        WHERE stock_symbol = 'IBM' and timestamp between '2009-01-01' and '2009-02-01'"

    time_series1 = query_database_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_engine, query=query2
    ).result()

    query3 = "SELECT timestamp, price_close \
        FROM stock_timedata \
        WHERE stock_symbol = 'GE' and timestamp between '2009-01-01' and '2009-02-01'"
    time_series2 = query_database_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_engine, query=query3
    ).result()

    print("hi")
    # TODO: Add date for running the flow; default to today roughly 8 years ago*
    # * 365 * years. Take into account leap days!
    # TODO: Update functions and arguments
    # Preanalysis


if __name__ == "__main__":
    stock_correlation_flow()
