import io
import pickle
from datetime import datetime
from pathlib import Path

import reverse_geocoder as rg  # Might need to be installed locally via pip
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task

from ingress import file_exists

"""Data Outflow Module for exporting to website & database
"""

# When adding data, upsert table to ignore duplicate entries and make action idempotent
# If timestamp is already in database for the stock, do not add.
# https://docs.timescale.com/timescaledb/latest/how-to-guides/write-data/upsert/

# TODO: Set pandas backend to plotly https://plotly.com/python/pandas-backend/
# TODO: Static image export plotly https://plotly.com/python/static-image-export




def corr_dict_to_db():
    stocks_db_conn_string = (
        "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/content"
    )
    # preferences
    stock_select_fields = ["timestamp", "stock_symbol", "price_close"]
    stock_database_name = "stock_timedata"
    stock_interval_in_days = 5
    sql_alchemy_stock_engine = create_engine(stocks_db_conn_string)


def reverse_geocoder(coordinates):
    """input: ((latitude, longitude),)
    output:
    coordinates expects a TUPLE of multiple values!
    eg. ((51.5214588, -0.1729636),)"""
    geo = rg.RGeocoder(
        mode=1,  # 1 = single processor, 2 = multi-processor
        verbose=True,
        stream=io.StringIO(open("egress_geonames_cities.csv", encoding="utf-8").read()),
    )
    results = geo.query(coordinates)

    return results


coordinates = ((51.5214588, -0.1729636),)
results = reverse_geocoder(coordinates=coordinates)
# TODO: Add country code lookup based on Geocities
print(results)


@task()
def create_visualization():
    # TODO: Set maximum filesize limit for images (10 MB?)
    # TODO: Compress images exported by Plotly
    # TODO: Export chart images in 2 sizes:
    # Thumbnail is 640 x 360 , should be saved in .webp
    # Main image is 1600 x 900, should be saved in .webp

    return


@task()
def create_markdown_files(visualization):
    return


@task()
def export_markdown(markdown):


    # date format: year-month-dayThour:min:sec+tz_offset
    # eg. 2021-09-15T11:30:03+00:00 
    # // +00:00 means 00:00 offset from UTC, thus UTC itself

    # sample post
    #title: "Why did APPL go up? 📈"
    #date: 2020-09-15T11:30:03+00:00
    #categories: ["stock goes up📈"]
    #description: "Desc Text."
    #canonicalURL: "https://noisystocks.com/$pretty-date/$page-title"
    #cover:
        #image: "graph-APPL-rainfall-2002-05-30.jpg"
        #alt: "chart showing the correlation between APPL and rainfall on " # alt for image
        #caption: "test" # display caption under cover
    # ---
    # Today, $yearcount years ago, the chart for $stock went $direction. What could have caused it? There could be a billion good reasons. We at NoisyStocks have no idea what those reasons are. Perhaps it was the weather, the phase of the moon, or the stance of the planets. We calculated our chart using a special "throw-spaghetti-at-a-wall-and-see-what-sticks" algorithm. Our tried-and-failed approach takes random variables and makes wildly spurious correlations.


@flow(task_runner=SequentialTaskRunner())
def publish():

    # load correlation from database

    # load defaults from website
    # Create visualization

    # Credit: Cities database from Geocities
    # Precipitation data from ...
    # Stock data from ...

