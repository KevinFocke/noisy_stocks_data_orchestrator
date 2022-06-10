from pathlib import Path
from queue import SimpleQueue

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments

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
# If timestamp is already in database for the stock, do not add.
# (it would be a duplicate)
# Include ingestion origin data
# https://docs.dask.org/en/stable/generated/dask.dataframe.read_csv.html

# Open question: How to ensure dataset is not already ingested?

# How to add new entries to a partially ingested dataset?


@task
def query_database(query):

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
def folder_exists(path: Path):
    return Path.is_dir(path)


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
    return


@task
def compress():
    """Compress database"""

    # WISHLIST: Add compression
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/manually-compress-chunks/#main-content


@task
def extraction_selector(resource_location, resource_type):
    # Choose the fitting extraction method for the resource_type
    pass

    # extraction_options = {"folder": folder_extraction}


@flow(task_runner=SequentialTaskRunner())
def ingest_data(name, time_series_col):
    # ETL Dataset into database

    extraction_selector()
    transform()
    load()
    compress()
