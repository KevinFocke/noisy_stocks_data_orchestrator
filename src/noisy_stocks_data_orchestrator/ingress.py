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
    # Datacleaning
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
