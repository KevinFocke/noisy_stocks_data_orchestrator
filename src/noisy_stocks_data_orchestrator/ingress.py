from pathlib import Path

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments

from customdatastructures import folder_exists

"""Data Inflow Module
"""


# During load only exact duplicates should be dropped
# Same date is okay and even desirable
# Upsert table to ignore duplicate entries and make action idempotent
# If timestamp is already in database for the stock, do not add.
# https://docs.timescale.com/timescaledb/latest/how-to-guides/write-data/upsert/#create-a-table-with-a-unique-constraint


# (it would be a duplicate)
# Include ingestion origin data
# https://docs.dask.org/en/stable/generated/dask.dataframe.read_csv.html

# Open question: How to ensure dataset is not already ingested?

# How to add new entries to a partially ingested dataset?


@task
def query_database(query):
    # Query using https://prefecthq.github.io/prefect-sqlalchemy/
    # Might raise errors https://github.com/timescale/timescaledb/issues/2226
    # https://github.com/PrefectHQ/prefect-sqlalchemy/blob/main/prefect_sqlalchemy/credentials.py
    return


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


@task
def compress():
    """Compress database"""

    # WISHLIST: Add compression
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/manually-compress-chunks/#main-content
