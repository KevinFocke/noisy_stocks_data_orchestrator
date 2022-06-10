from pathlib import Path
from queue import SimpleQueue

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments

from customdatastructures import ResourceFactory

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
def file_exists(path: Path):
    return path.is_file()


@validate_arguments
@task
def folder_exists(path: Path):
    return path.is_dir()


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
def folder_extraction(resource: ResourceFactory):

    pass


@flow(task_runner=SequentialTaskRunner())
def ingress_data(resource_schema, resource_location, resource_type):
    # ETL Dataset into database
    resource = ResourceFactory(
        resource_schema=resource_schema,
        resource_location=resource_location,
        resource_type=resource_type,
    )
    #  initialize queue if not existing
    resource.enqueue()
    resource.process_queue()
    transform()
    load()
    compress()


@task
def compress():
    """Compress database"""

    # WISHLIST: Add compression
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/manually-compress-chunks/#main-content
