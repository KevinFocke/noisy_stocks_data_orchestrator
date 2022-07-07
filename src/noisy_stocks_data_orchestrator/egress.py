import hashlib
import io
import pickle
from datetime import datetime
from pathlib import Path

import reverse_geocoder as rg  # Might need to be installed locally via pip
import sqlalchemy as db
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from customdatastructures import folder_exists
from ingress import load_object_from_file_path

"""Data Outflow Module for exporting to website & database
"""

# When adding data, upsert table to ignore duplicate entries and make action idempotent
# If timestamp is already in database for the stock, do not add.
# https://docs.timescale.com/timescaledb/latest/how-to-guides/write-data/upsert/

# TODO: Set pandas backend to plotly https://plotly.com/python/pandas-backend/
# TODO: Static image export plotly https://plotly.com/python/static-image-export


@flow(task_runner=SequentialTaskRunner())
def write_object_to_path(object_to_save, folder_path: Path):
    """input: object, folderPath, the filename will be the current datetime"""
    today = datetime.now()
    folder_exists(folder_path)
    file_path = folder_path / (today.strftime(r"%Y_%m_%d_%H_%M_%S") + r".pickle")
    with file_path.open("wb") as fp:  # wb to write binary
        pickle.dump(object_to_save, fp)


def hash_file(filepath: Path, algo_name: str = "sha256"):
    """input : Path to file
    output: hexadecimal hash string"""
    with filepath.open("rb") as fp:
        binary_data = fp.read()
        return hashlib.new(name=algo_name, data=binary_data).hexdigest()


def upsert_corr_dict(
    filepath, connection, corr_dict, cols_not_represented_in_content_db, website_table
):
    for stock_symbol in corr_dict:

        unfolded_indexes = dict(
            zip(
                corr_dict[stock_symbol]["dataset_uid_col_name_list"],
                corr_dict[stock_symbol]["dataset_uid"],
            )
        )

        file_hash = hash_file(filepath=filepath, algo_name="sha256")

        stock_symbol_dict = {"stock_symbol": stock_symbol}
        pickle_name_dict = {
            "ingested_pickle_name": filepath.name,
            "ingested_pickle_name_hash": file_hash,
        }  # note: pickle is not guaranteed to run deterministically
        # however, this is not required for this particular use case
        # the only purpose it to ensure the exact same pickle is found

        merged_dict = {
            **stock_symbol_dict,
            **pickle_name_dict,
            **unfolded_indexes,
            **corr_dict[stock_symbol],
        }
        for key_to_remove in cols_not_represented_in_content_db:
            merged_dict.pop(key_to_remove)

        insert_query = insert(website_table).values(merged_dict)
        # do nothing if duplicate value
        upsert_query = insert_query.on_conflict_do_nothing(
            index_elements=["stock_symbol", "requested_publish_date"]
        )  # upserts, inserts a date if there is no entry for the (composite) key
        connection.execute(upsert_query)


def move_file_to_subfolder(file_path: Path, sub_folder_name: str):
    processed_folder = (
        file_path.parents[0] / sub_folder_name
    )  # parents[0] accesses the parent folder path,
    # in other words, everything except the filename
    file_path.rename(processed_folder / file_path.name)


def corr_dict_pickle_to_db(
    content_db_conn_string: str = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/content",
    cols_not_represented_in_content_db: list[str] = [
        "dataset_pd_series",
        "stock_pd_series",
        "dataset_uid_col_name_list",
        "dataset_uid",
    ],
    corr_dict_pickle_folder_path: Path = Path(
        r"/home/kevin/coding_projects/noisy_stocks/persistent_data/corr_dicts/"
    ),
):
    sql_alchemy_content_engine = db.create_engine(content_db_conn_string)
    connection = sql_alchemy_content_engine.connect()
    metadata = db.MetaData()
    website_table = db.Table(
        "website", metadata, autoload=True, autoload_with=sql_alchemy_content_engine
    )

    corr_dict_pickle_file_paths = list(corr_dict_pickle_folder_path.glob("*.pickle"))

    for corr_dict_file_path in corr_dict_pickle_file_paths:
        corr_dict = load_object_from_file_path(corr_dict_file_path).result()
        upsert_corr_dict(
            filepath=corr_dict_file_path,
            connection=connection,
            corr_dict=corr_dict,
            cols_not_represented_in_content_db=cols_not_represented_in_content_db,
            website_table=website_table,
        )
        mark_corr_dict_as_success(corr_dict_file_path)  # moves to processed folder


def reverse_geocoder(coordinates):
    """input: ((latitude, longitude),)
    output:
    coordinates expects a TUPLE of multiple values!
    eg. ((51.5214588, -0.1729636),)"""
    geo = rg.RGeocoder(
        mode=1,  # 1 = single processor, 2 = multi-processor
        verbose=True,
        stream=io.StringIO(
            open(r"egress_geonames_cities.csv", encoding="utf-8").read()
        ),
    )
    results = geo.query(coordinates)

    return results


@task()
def create_visualization():
    # TODO: Set maximum filesize limit for images (10 MB?)
    # TODO: Compress images exported by Plotly
    # TODO: Export chart images in 2 sizes:
    # Thumbnail is 640 x 360 , should be saved in .webp
    # Main image is 1600 x 900, should be saved in .webp

    pass


@task()
def create_markdown_files(visualization):
    pass


@task()
def export_markdown(markdown):
    pass

    # date format: year-month-dayThour:min:sec+tz_offset
    # eg. 2021-09-15T11:30:03+00:00
    # // +00:00 means 00:00 offset from UTC, thus UTC itself

    # sample post
    # title: "Why did APPL go up? 📈"
    # date: 2020-09-15T11:30:03+00:00
    # categories: ["stock goes up📈"]
    # description: "Desc Text."
    # canonicalURL: "https://noisystocks.com/$pretty-date/$page-title"
    # cover:
    # image: "graph-APPL-rainfall-2002-05-30.jpg"
    # alt: "chart showing the correlation between APPL and rainfall on " # alt for image
    # caption: "test" # display caption under cover
    # ---
    # Today, $yearcount years ago, the chart for $stock went $direction. What could have caused it? There could be a billion good reasons. We at NoisyStocks have no idea what those reasons are. Perhaps it was the weather, the phase of the moon, or the stance of the planets. We calculated our chart using a special "throw-spaghetti-at-a-wall-and-see-what-sticks" algorithm. Our tried-and-failed approach takes random variables and makes wildly spurious correlations.


@flow(task_runner=SequentialTaskRunner())
def publish():
    pass
    # load correlation from database

    # load defaults from website
    # Create visualization

    # Credit: Cities database from Geocities
    # Precipitation data from ...
    # Stock data from ...


if __name__ == "__main__":
    corr_dict_pickle_to_db()

    coordinates = ((51.5214588, -0.1729636),)
    results = reverse_geocoder(coordinates=coordinates)
    # TODO: Add country code lookup based on Geocities
    print(results)
