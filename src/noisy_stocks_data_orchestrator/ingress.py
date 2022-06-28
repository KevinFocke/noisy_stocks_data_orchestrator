from pathlib import Path

import pandas as pd
from pandas import DataFrame
from prefect.tasks import task
from pydantic import validate_arguments
from sqlalchemy import create_engine, engine

from customdatastructures import folder_exists

"""Data Inflow Module
"""

# Best practice: One engine should handle ALL queries
# https://docs.sqlalchemy.org/en/14/core/connections.html#basic-usage
# Upsert table to ignore duplicate entries and make action idempotent
# If timestamp is already in database for the stock, do not add.
# https://docs.timescale.com/timescaledb/latest/how-to-guides/write-data/upsert/


# https://docs.dask.org/en/stable/generated/dask.dataframe.read_csv.html


class Config_Arbitrary_Types_Allowed:
    arbitrary_types_allowed = True


@validate_arguments
def create_database_engine(conn_string: str) -> engine.base.Engine:
    return create_engine(conn_string)


@validate_arguments(
    config=Config_Arbitrary_Types_Allowed
)  # No specific validator for engine, checks type
def query_database(sql_alchemy_engine: engine.base.Engine, query: str) -> DataFrame:
    connection = sql_alchemy_engine.connect()  # Connect to the database
    return pd.read_sql(query, connection)  # Run query and convert into pd DataFrame


conn_string = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/stocks"

query = "SELECT *\
    FROM stock_timedata \
    WHERE stock_symbol = 'IBM' and timestamp between '2009-01-01' and '2009-02-01'"

myengine = create_engine(conn_string)

result1 = query_database(myengine, query)
query2 = "SELECT *\
    FROM stock_timedata \
    WHERE stock_symbol = 'GIGA' and timestamp between '2009-01-01' and '2009-02-01'"

result2 = query_database(myengine, query2)

print(result1)
print(result2)


@validate_arguments
@task
def create_path_object(path: str):
    """Create normalized path

    Args:
        path (str): link to folder

    Returns:
        Path: object-oriented filesystem path
    """
    # optionally use .resolve() to make absolute link
    return Path(path)


@validate_arguments
@task
def create_folder(folder_url: Path):
    if not folder_exists(folder_url):
        print(f"Folder {folder_url} does not exist, creating it.")
        Path.mkdir(folder_url, parents=True)
        if folder_exists(folder_url):
            return True  # Folder created
        else:
            raise ValueError("Folder should have been created, but was not.")
    else:
        print("Folder already exists")
        return False  # No folder created
