import hashlib
import io
import pickle
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import reverse_geocoder as rg  # Might need to be installed locally via pip
import sqlalchemy as db
from plotly.subplots import make_subplots
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pytest import approx
from sqlalchemy.dialects.postgresql import insert

from customdatastructures import folder_exists
from ingress import load_object_from_file_path

"""Data Outflow Module for exporting to website & database
"""

# When adding data, upsert table to ignore duplicate entries and make action idempotent
# If timestamp is already in database for the stock, do not add.
# https://docs.timescale.com/timescaledb/latest/how-to-guides/write-data/upsert/

# TODO: Set pandas backend to plotly https://plotly.com/python/pandas-backend/
# TODO: Static image export plotly https://plotly.com/python/static-image-export


@flow
def visualize_corr(
    pd_series_stocks,
    pd_series_dataset,
    highest_corr,
    stock_symbol,
    dataset_uid,
    latitude,
    longitude,
):
    # TODO: refactor, export graph in other function
    mytuple = (float(latitude), float(longitude))  # lat lon
    coordinates = (mytuple,)
    results = reverse_geocoder(coordinates=coordinates)
    city = results[0]["name"]  # TODO: refactor city & country code to other func
    country_code = results[0]["cc"]
    pd.options.plotting.backend = "plotly"
    # sanity check, are the corrs correct?
    assert highest_corr == approx(pd_series_stocks.corr(pd_series_dataset))
    df1 = pd.DataFrame(pd_series_stocks)
    df2 = pd.DataFrame(pd_series_dataset)
    # convert timestamp to datetime
    df1.index = pd.to_datetime(df1.index)
    df2.index = pd.to_datetime(df2.index)
    print(df1)
    print(df2)
    print(df1.index)
    print(df2.index)
    merged_df = df1.join(df2)
    merged_df = merged_df.reset_index()

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(
            x=merged_df["timestamp"],
            y=merged_df[stock_symbol],
            name=f"rainfall in {city}",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=merged_df["timestamp"],
            y=merged_df[dataset_uid],
            name=stock_symbol,
        ),
        secondary_y=True,
    )

    # Add figure title
    fig.update_layout(
        title_text=f"NoisyStocks.com | Spurious stock correlation ({highest_corr.round(5)}%) between {stock_symbol} price and rainfall in {city}, {country_code}"
    )

    # Set x-axis title
    fig.update_xaxes(title_text="date")

    # Set y-axes titles
    fig.update_yaxes(title_text=f"{stock_symbol} close price", secondary_y=False)
    fig.update_yaxes(title_text=f"rainfall in {city}, {country_code}", secondary_y=True)

    fig.update_layout(
        xaxis_tickformat="%d %B (%a)<br>%Y",  # only plot available x
    )
    fig.write_image(
        r"/home/kevin/coding_projects/noisy_stocks/persistent_data/testimg/myimg.webp",
        width=1920,
        height=1080,
    )
    # fig.write_image(
    #    r"/home/kevin/coding_projects/noisy_stocks/persistent_data/testimg/myimg.webp"
    # )
    fig.show()
    graph_json = fig.to_json(pretty=True)
    return graph_json, city, country_code
    # thumbnail, full


@flow
def create_folder(folder_url: Path):
    if not folder_exists(folder_url).result():
        # print(f"Folder {folder_url} does not exist, creating it.")
        Path.mkdir(folder_url, parents=True)
        if folder_exists(folder_url).result():
            return True  # Folder created
        else:
            raise ValueError("Folder should have been created, but was not.")
    else:
        print("Folder already exists")
        return False  # No folder created


@flow(task_runner=SequentialTaskRunner())
def write_object_to_path(object_to_save, folder_path: Path):
    """input: object, folderPath, the filename will be the current datetime"""
    today = datetime.now()
    folder_exists(folder_path)
    file_path = folder_path / (today.strftime(r"%Y_%m_%d_%H_%M_%S") + r".pickle")
    with file_path.open("wb") as fp:  # wb to write binary
        pickle.dump(object_to_save, fp)


@flow
def hash_file(filepath: Path, algo_name: str = "sha256") -> str:
    """input : Path to file
    output: hexadecimal hash string"""
    with filepath.open("rb") as fp:
        binary_data = fp.read()
        return hashlib.new(name=algo_name, data=binary_data).hexdigest()


@flow
def move_file_to_subfolder(file_to_move: Path, sub_folder_name: str):
    """moves file to subfolder, creates the subfolder if not exists"""
    processed_folder = (
        file_to_move.parents[0] / sub_folder_name
    )  # parents[0] accesses the parent folder path,
    # in other words, everything except the filename
    create_folder(processed_folder)
    file_to_move.rename(processed_folder / file_to_move.name)


@flow
def corr_to_db_content(
    content_db_conn_string: str = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/content",
    cols_not_represented_in_content_db: list[str] = [
        "dataset_pd_series",
        "stock_pd_series",
        "dataset_uid_col_name_list",
        "dataset_uid",
        "requested_publish_date",
    ],
    corr_dict_pickle_folder_path: Path = Path(
        r"/home/kevin/coding_projects/noisy_stocks/persistent_data/corr_dicts/"
    ),
):
    corr_dict_pickle_file_paths = list(corr_dict_pickle_folder_path.glob("*.pickle"))
    if not corr_dict_pickle_file_paths:  # if there are no file paths
        return  # nothing to do

    # create visualization json

    # create sql_alchemy engine
    sql_alchemy_content_engine = db.create_engine(content_db_conn_string)
    connection = sql_alchemy_content_engine.connect()
    metadata = db.MetaData()
    website_table = db.Table(
        "website", metadata, autoload=True, autoload_with=sql_alchemy_content_engine
    )

    for corr_dict_file_path in corr_dict_pickle_file_paths:
        corr_dict = load_object_from_file_path(corr_dict_file_path).result()

        # TODO: refactor when Prefect 2.0 out of beta
        # the normal prefect library sqlalchemy works alright
        # but the dict of dicts REFUSES to be pickled!
        stock_index = 0
        for stock_symbol in corr_dict:
            unfolded_indexes = dict(
                zip(
                    corr_dict[stock_symbol]["dataset_uid_col_name_list"],
                    corr_dict[stock_symbol]["dataset_uid"],
                )
            )

            graph_json, city, country_code = visualize_corr(
                pd_series_stocks=corr_dict[stock_symbol]["stock_pd_series"],
                pd_series_dataset=corr_dict[stock_symbol]["dataset_pd_series"],
                highest_corr=corr_dict[stock_symbol]["highest_corr"],
                stock_symbol=stock_symbol,
                dataset_uid=corr_dict[stock_symbol]["dataset_uid"],
                longitude=unfolded_indexes["longitude"],
                latitude=unfolded_indexes["latitude"],
            ).result()

            file_hash = hash_file(
                filepath=corr_dict_file_path, algo_name="sha256"
            ).result()

            stock_symbol_dict = {"stock_symbol": stock_symbol}
            json_dict = {"graph_json": graph_json}
            geo_dict = {"city": city, "country_code": country_code}
            pickle_name_dict = {
                "ingested_pickle_filename": corr_dict_file_path.name,
                "ingested_pickle_hash": file_hash,
            }  # note: pickle is not guaranteed to run deterministically
            # however, this is not required for this particular use case
            # the only purpose it to ensure the exact same pickle is found

            upsertion_query_values = {
                **stock_symbol_dict,
                **json_dict,
                **geo_dict,
                **pickle_name_dict,
                **unfolded_indexes,
                **corr_dict[stock_symbol],
            }
            for key_to_remove in cols_not_represented_in_content_db:
                upsertion_query_values.pop(key_to_remove, None)

            insert_query = insert(website_table).values(upsertion_query_values)
            # do nothing if duplicate value
            upsert_query = insert_query.on_conflict_do_nothing(
                index_elements=[
                    "stock_symbol",
                    "begin_date",
                    "end_date",
                    "dataset_database_name",
                ]
            )  # upserts, inserts a date if there is no entry for the (composite) key
            connection.execute(upsert_query)

        move_file_to_subfolder(corr_dict_file_path, "processed")
        # moves to processed folder
        stock_index += 1


def reverse_geocoder(coordinates):
    """input: ((latitude, longitude),)
    output:
    coordinates expects a TUPLE of multiple values!
    eg. ((51.5214588, -0.1729636),)"""
    geo = rg.RGeocoder(
        mode=1,  # 1 = single processor, 2 = multi-processor
        verbose=True,
        stream=io.StringIO(
            open(
                r"/home/kevin/coding_projects/noisy_stocks/noisy_stocks_data_orchestrator/src/noisy_stocks_data_orchestrator/egress_geonames_cities.csv",
                encoding="utf-8",
            ).read()
        ),
    )
    results = geo.query(coordinates)

    return results


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
    # title: "Why did $STOCK go up? 📈"
    # date: 2020-09-15T11:30:03+00:00
    # categories: ["stock goes up📈"]
    # description: "Desc Text."
    # canonicalURL: "https://noisystocks.com/$pretty-date/$page-title"
    # cover:
    # image: "graph-APPL-rainfall-2002-05-30.jpg"
    # alt: "chart showing the correlation between APPL and rainfall on " # alt for image
    # caption: "test" # display caption under cover
    # ---
    # Today, $yearcount years ago, the chart for $stock went $direction. What could have caused it? There could be a billion good reasons. We at NoisyStocks have no idea what those reasons are. Perhaps it was the weather—there was a $correlation% correlation between the price of $stock and the $dataset_num_col_name of city.

    # We have calculated this chart using a special "throw-spaghetti-at-a-wall-and-see-what-sticks" algorithm. Our marvelous approach takes random variables and makes wildly spurious correlations.

    # original data, from datasets


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
    corr_to_db_content()

    # create graph based on pandas series

    # export
    mytuple = (float(52.3), float(40))  # lat lon

    coordinates = (mytuple,)
    # coordinates = ((51.5214588, -0.1729636),)
    results = reverse_geocoder(coordinates=coordinates)
    # TODO: Add country code lookup based on Geocities
    print(results)
