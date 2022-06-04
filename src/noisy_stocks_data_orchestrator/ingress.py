from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

"""Data Inflow Module
"""
import os


@flow
def query_database(query):

    return


@task
def open_url(url):
    # TODO: Write open_url function
    # TODO: Enable recursive file fetching from URL

    pass


def path_exists(path):
    # check using os library

    # os.path.exists(filepath)

    pass


def open_file(path):

    # Does path exist?
    path_exists(path)

    # check using os library

    # Go to path

    # if path is folder, open every child file
    # recursive into children folders
    pass


def open_folder(path, recursive="n", recursive_levels=0):

    path_exists(path)
    # check if is folder

    # base case; folder is empty

    # how many levels deep can you recurse into folder structure?

    pass


@flow(task_runner=SequentialTaskRunner())
def extract(type="", path="", recursive="n", recursive_levels=5):

    types = {
        "url": open_url(path),
        "file": open_file(path),
        "folder": open_folder(path, recursive, recursive_levels),
    }

    # Depending on type, call extraction method
    if type in types:
        pass
        # call associated method


@task
def transform():
    return


@task
def load():
    """Load data into database"""

    # Compress
    return


@task
def compress():
    """Compress database"""
    # https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/manually-compress-chunks/#main-content


@flow(task_runner=SequentialTaskRunner())
def ingest_dataset(name, time_series_col):
    # ETL Dataset into database

    extract()
    transform()
    load()
    compress()
