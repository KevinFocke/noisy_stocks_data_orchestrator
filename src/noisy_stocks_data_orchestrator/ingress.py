from pathlib import Path

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments

from customdatastructures import folder_exists

"""Data Inflow Module
"""


# TODO: Add typical order of data inflow documentation

# TODO: Steps for folder ingestion:
# List path + subdirectories of path
# https://docs.python.org/3/library/pathlib.html
# For each (sub)directory, put directory in DirectoryExtractionQueue
# Get directory out of DirectoryExtractionQueue until empty
# For each directory, create the files list (no subdirectories!)
# For each file in file list, put in FileExtractionQueue
# Get file directory out of FileExtractionQueue until empty
# For each file: create a TimeSeries class
# Save the TimeSeries class to database, the key is the name

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
def transform():
    # General datacleaning
    # Unfortunately, it's very difficult to generalize for all datasets

    # Sort timestamps ascending
    # Normalize dataset based on start date
    return


@task
def load():
    """Load data into database"""

    # During load only exact duplicates should be dropped
    # Same date is okay and even desirable
    # Upsert table to ignore duplicate entries and make action idempotent
    # If timestamp is already in database for the stock, do not add.
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/write-data/upsert/#create-a-table-with-a-unique-constraint
    return


@flow(task_runner=SequentialTaskRunner())
def extract(resource_schema, resource_location, resource_type, file_suffix: str = ""):
    # Create relevant resource queue via ResourceFactory

    # Process queue
    pass


@flow(task_runner=SequentialTaskRunner())
def ingress_data(
    resource_schema, resource_location, resource_type, file_suffix: str = ""
):
    # ETL Dataset into database
    # assumption: resource_schema is uniform accross all files
    # TODO: Skip files not fitting
    # file_suffix is used to match files within folders

    #  initialize queue if not existing
    extract(
        resource_schema=resource_schema,
        resource_location=resource_location,
        resource_type=resource_type,
        file_suffix=file_suffix,
    )
    transform()
    load()
    compress()


@task
def compress():
    """Compress database"""

    # WISHLIST: Add compression
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/manually-compress-chunks/#main-content
