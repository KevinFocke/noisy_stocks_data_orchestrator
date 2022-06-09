from pathlib import Path
from queue import SimpleQueue

from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments

"""Data Inflow Module
"""


@flow
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


def extract_folder(path, file_suffix_in_folder, recursive="n", recursive_levels=0):
    # TODO: stub
    folder_exists(path)
    # check if is folder

    # urlpath = path + r"*" + file_suffix

    # df = dd.read_csv(r"urlpath/*.csv")
    # https://docs.dask.org/en/stable/generated/dask.dataframe.read_csv.html

    # base case; folder is empty or recursive_levels < 0

    # how many levels deep can you recurse into folder structure?


@validate_arguments
@flow
def file_extraction_flow(
    source_type: str = "",
    path: str = "",
    file_suffix_in_folder: str = ".txt",
    recursive: str = "n",
):
    file_urls: SimpleQueue = SimpleQueue()
    # Implement simple queue structure https://github.com/python/cpython/blob/3.10/Lib/queue.py

    # WISHLIST: Define recursion levels ; PositiveInt pydantic type could be useful
    pass


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
