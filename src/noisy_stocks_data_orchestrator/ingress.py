from pathlib import Path

import dask as dd
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from pydantic import PositiveInt, validate_arguments

"""Data Inflow Module
"""


@flow
def query_database(query):

    return


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


@flow(task_runner=SequentialTaskRunner())
def check_and_create_folder(folder_url: Path, create: int = 1):
    # TODO: Refactor? Feels granular enough for now.
    if not Path.is_dir(folder_url):
        if create == 1:
            print(f"Folder {folder_url} does not exist, creating it.")
            Path.mkdir(folder_url, parents=True)
    return Path.is_dir(folder_url)


@validate_arguments
@flow
def download_kaggle_dataset(
    dataset_list: list[str] = [
        r"borismarjanovic/price-volume-data-for-all-us-stocks-etfs"
    ],
    download_folder_path: str = r"\.datasets",
):
    # eg. borismarjanovic/price-volume-data-for-all-us-stocks-etfs

    # authentication via kaggle.json in .kaggle
    # kaggle datasets download -d borismarjanovic/price-volume-data-for-all-us-stocks-etfs
    # Download zipped dataset

    # TODO: Refactor download_folder_path_object? Doesn't roll off the tongue that well…

    # TODO: Think through which kinds of errors can happen

    # download_folder_path_object = create_path_object(download_folder_path)
    # check_and_create_folder(download_folder_path_object, create=1)

    # kaggle_base_download = r"kaggle datasets download"

    # Unzip dataset
    pass


@task
def extract_url(url):
    # TODO: Write open_url functiona using requests
    # TODO: Enable recursive file fetching from URL

    pass


def path_exists(path):

    pass


def extract_file(path):

    # Does path exist?
    path_exists(path)

    #  df = dd.read_csv(r"urlpath/filename")

    pass


def extract_folder(path, file_suffix_in_folder, recursive="n", recursive_levels=0):
    # TODO: stub
    path_exists(path)
    # check if is folder

    # urlpath = path + r"*" + file_suffix

    # df = dd.read_csv(r"urlpath/*.csv")
    # https://docs.dask.org/en/stable/generated/dask.dataframe.read_csv.html

    # base case; folder is empty or recursive_levels < 0

    # how many levels deep can you recurse into folder structure?

    pass


@flow(task_runner=SequentialTaskRunner())
@validate_arguments
def extract(
    source_type: str = "",
    path: str = "",
    file_suffix_in_folder: str = ".txt",
    recursive: str = "n",
    recursive_levels: PositiveInt = 5,
):

    # TODO: Rename file_suffix_in_folder?

    # file suffix is used to glob match fol
    types = {
        "url": extract_url(path),
        "file": extract_file(path),
        "folder": extract_folder(
            path, file_suffix_in_folder, recursive, recursive_levels
        ),
    }

    extraction = None
    # Depending on type, call extraction method

    if source_type in types:
        pass
        # call associated method

    # Check if extraction was succesful; extraction not None

    # Return list of dask Dataframes


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
    return


@task
def compress():
    """Compress database"""
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/manually-compress-chunks/#main-content


@flow(task_runner=SequentialTaskRunner())
def ingest_data(name, time_series_col):
    # ETL Dataset into database

    extract()
    transform()
    load()
    compress()
