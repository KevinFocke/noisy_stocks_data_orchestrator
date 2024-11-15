import hashlib
import io
import pickle
from datetime import datetime, timedelta
from os import environ, linesep
from pathlib import Path

import pandas as pd
import plotly
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
    # TODO: This is encapsulation hell; I ran out of time, but it was a fun project!
    # TODO: refactor, export graph in other function
    mytuple = (float(latitude), float(longitude))  # lat lon
    coordinates = (mytuple,)
    results = reverse_geocoder(coordinates=coordinates)
    city = results[0]["name"]  # TODO: refactor city & country code to other func
    city_longitude = results[0]["lon"]
    city_latitude = results[0]["lat"]
    country_code = results[0]["cc"]
    pd.options.plotting.backend = "plotly"
    # sanity check, are the corrs correct?
    assert highest_corr == approx(pd_series_stocks.corr(pd_series_dataset))
    df1 = pd.DataFrame(pd_series_stocks)
    df2 = pd.DataFrame(pd_series_dataset)
    # convert timestamp to datetime
    df1.index = pd.to_datetime(df1.index)
    df2.index = pd.to_datetime(df2.index)

    merged_df = df1.join(df2)
    merged_df = merged_df.reset_index()
    x_axis_len = len(merged_df["timestamp"])

    merged_df = merged_df.sort_index()
    # check if direction went up or down
    # FIXME: broken code, stock direction does not get calculated correctly.
    if merged_df[stock_symbol].iloc[-1] > merged_df[stock_symbol].iloc[0]:
        price_direction = "up"
    elif merged_df[stock_symbol].iloc[-1] < merged_df[stock_symbol].iloc[0]:
        price_direction = "down"
    else:
        price_direction = "NEUTRAL CHAOS"

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
            name=f"{stock_symbol} close price",
        ),
        secondary_y=True,
    )

    # Add figure title
    fig.update_layout(
        title_text=f"NoisyStocks.com | Spurious stock correlation ({highest_corr.round(5)})"
    )

    # Set x-axis title
    fig.update_xaxes(title_text="date")

    # Set y-axes titles
    fig.update_yaxes(title_text=f"{stock_symbol} close price ($)", secondary_y=False)

    fig.update_yaxes(
        title_text=f"rainfall in {city}, {country_code} (mean mm per day)",
        secondary_y=True,
    )

    fig.update_layout(
        xaxis_tickformat="%d %B (%a)<br>%Y",  # only plot available x
    )
    fig.update_xaxes(nticks=x_axis_len)
    graph_json = fig.to_json(pretty=True)
    return (
        graph_json,
        city,
        country_code,
        price_direction,
        city_longitude,
        city_latitude,
    )


@flow
def create_folder(folder_url: Path):
    if not folder_exists(folder_url):
        # print(f"Folder {folder_url} does not exist, creating it.")
        Path.mkdir(folder_url, parents=True)
        if folder_exists(folder_url):
            return True  # Folder created
        else:
            raise ValueError("Folder should have been created, but was not.")
    else:
        print("Folder already exists")
        return False  # No folder created


@flow(task_runner=SequentialTaskRunner())
def pickle_object_to_path(object_to_save, folder_path: Path):
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
    content_db_conn_string: str = environ["NOISYSTOCKS_CONTENT_DB_CONNECTION_URL"],
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
    # TODO: refactor
    sql_alchemy_content_engine = db.create_engine(content_db_conn_string)
    connection = sql_alchemy_content_engine.connect()
    metadata = db.MetaData()
    website_table = db.Table(
        "website", metadata, autoload=True, autoload_with=sql_alchemy_content_engine
    )

    for corr_dict_file_path in corr_dict_pickle_file_paths:
        corr_dict = load_object_from_file_path(corr_dict_file_path)

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

            # TODO: refactor, this function is way overloaded; had no time to write decently
            (
                graph_json,
                city,
                country_code,
                stock_direction,
                city_longitude,
                city_latitude,
            ) = visualize_corr(
                pd_series_stocks=corr_dict[stock_symbol]["stock_pd_series"],
                pd_series_dataset=corr_dict[stock_symbol]["dataset_pd_series"],
                highest_corr=corr_dict[stock_symbol]["highest_corr"],
                stock_symbol=stock_symbol,
                dataset_uid=corr_dict[stock_symbol]["dataset_uid"],
                longitude=unfolded_indexes["longitude"],
                latitude=unfolded_indexes["latitude"],
            )

            file_hash = hash_file(filepath=corr_dict_file_path, algo_name="sha256")

            extra_stock_info = {
                "stock_symbol": stock_symbol,
                "stock_direction": stock_direction,
            }
            json_dict = {"graph_json": graph_json}
            geo_dict = {
                "city": city,
                "country_code": country_code,
                "city_longitude": city_longitude,
                "city_latitude": city_latitude,
            }
            pickle_name_dict = {
                "ingested_pickle_filename": corr_dict_file_path.name,
                "ingested_pickle_hash": file_hash,
            }  # note: pickle is not guaranteed to run deterministically
            # however, this is not required for this particular use case
            # the only purpose it to ensure the exact same pickle is found

            upsertion_query_values = {
                **extra_stock_info,
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
            insert_query = insert_query.on_conflict_do_nothing(
                index_elements=[
                    "stock_symbol",
                    "begin_date",
                    "end_date",
                    "dataset_database_name",
                ]
            )  # inserts the entry if there is no entry for the (composite) key
            connection.execute(insert_query)

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


@flow()
def create_markdown(stock_dict, image_file_path: Path):

    header_dict = {}

    # create folder page_bundle_path
    # year-month-day-{stock_symbol}

    stock_symbol = stock_dict["stock_symbol"]
    city = stock_dict["city"]
    country_code = stock_dict["country_code"]
    stock_direction = stock_dict["stock_direction"]
    publish_timestamp = stock_dict["publish_timestamp"]
    longitude = round(stock_dict["longitude"], 1)
    latitude = round(stock_dict["latitude"], 1)
    city_longitude = round(stock_dict["city_longitude"], 1)
    city_latitude = round(stock_dict["city_latitude"], 1)
    header_dict["title"] = f"{stock_symbol} and rainfall in {city}"
    header_dict["date"] = publish_timestamp.strftime(r"%Y-%m-%d %H:%M:%S")

    header_dict["categories"] = "spurious stock correlations"
    # FIXME: stock direction is broken, not sure why
    # if stock_direction == "up":
    # header_dict["categories"] = r"stock goes up 📈"
    # elif stock_direction == "down":
    # header_dict["categories"] = r"stock goes down 📉"
    # else:
    # header_dict["categories"] = r"NEUTRAL CHAOS ZONE"

    header_dict[
        "description"
    ] = f"The spurious correlation between stock {stock_symbol} and rainfall in {city}"

    header_cover_dict = {}

    header_cover_dict["image"] = image_file_path.name

    header_cover_dict[
        "alt"
    ] = f"chart showing the spurious stock correlation between stock {stock_symbol} and rainfall in {city}"

    header_cover_dict[
        "caption"
    ] = r"Credits: [Rainfall GPM IMERG dataset by NASA downloaded via Copernicus](https://cds.climate.copernicus.eu/cdsapp#!/dataset/insitu-gridded-observations-global-and-regional?tab=overview), [Historical stock price dataset by Boris Marjanovic downloaded via Kaggle](https://www.kaggle.com/datasets/borismarjanovic/price-volume-data-for-all-us-stocks-etfs), [Cities dataset from GeoNames](https://download.geonames.org/export/dump/)"
    # note there should be 4 spaces before image, alt & caption
    body = (
        f"Today, roughly 20 years ago, the chart for stock {stock_symbol} made a big move. What could have caused it? There could be a billion good reasons. We at NoisyStocks have no idea what those reasons are. Instead of a careful & nuanced analysis, we have calculated this chart using a special throw-spaghetti-at-a-wall-and-see-what-sticks algorithm. Our marvelous approach takes random variables and makes wildly spurious correlations."
        + linesep
    )

    shortcode_begin = r"{{< "
    shortcode_end = r" >}} "
    body_map_snippet = (
        f"# Where is {city}, {country_code}?"
        + linesep
        + shortcode_begin
        + 'leaflet-map zoom="2" mapHeight="500px" mapWidth="100%"'
        + f'mapLat="{latitude}" mapLon="{longitude}"'
        + shortcode_end
        + linesep
        + "    "
        + shortcode_begin
        + f'leaflet-marker marketLat="{city_latitude}" markerLon="{city_longitude}" markerContent="The city of {city}"'
        + shortcode_end
        + linesep
        + shortcode_begin
        + f'leaflet-marker marketLat="{latitude}" markerLon="{longitude}" markerContent="rainfall coordinates (longitude:{longitude}, latitude: {latitude})"'
        + shortcode_end
        + linesep
        + shortcode_begin
        + r"/leaflet-map"
        + shortcode_end
        + linesep
        + r"Note 1: The algorithm always chooses the nearest city with >1000 people. If the points happen to fall in the middle of the ocean, it will not be accurate. The actual rainfall coordinates are also shown on the map."
        + linesep
        + r'Note 2: Rainfall is defined as: "Time mean flux of rain, snow and hail measured as the height of the equivalent liquid water in a square meter per time interval."'
    )

    # add header to markdown
    markdown = ""
    markdown += "---" + linesep
    for key, item in header_dict.items():
        markdown += f"{key}: " + f'"{item}"' + linesep

    # add img to markdown
    markdown += r"cover:" + linesep
    for key, item in header_cover_dict.items():
        markdown += r"    " + f"{key}: " + f'"{item}"' + linesep

    # end of header
    markdown += "---" + linesep

    markdown += body

    markdown += body_map_snippet

    # Credit: Cities database from Geocities
    # Precipitation data from ...
    # Stock data from ...

    return markdown


@flow()
def export_plotly_graph(plotly_json, file_path: Path):

    fig = plotly.io.from_json(plotly_json)

    fig.write_image(file_path, width=1080, height=720)


@flow()
def export_markdown(markdown, page_bundle_path: Path):

    index_path = page_bundle_path / "index.md"
    with open(index_path, "w") as fp:
        fp.write(markdown)
        fp.close()


@flow(task_runner=SequentialTaskRunner())
def get_publish_content(content_db_conn_string, select_where_publish_ts_null: bool):
    """input: content_db_conn_string
    output: randomized nested dict {"random_row_index":{**rows_in_db}}"""
    # create sql_alchemy engine & query
    sql_alchemy_content_engine = db.create_engine(content_db_conn_string)
    connection = sql_alchemy_content_engine.connect()
    metadata = db.MetaData()
    website_table = db.Table(
        "website", metadata, autoload=True, autoload_with=sql_alchemy_content_engine
    )
    select_query = db.select([website_table])
    if select_where_publish_ts_null:
        select_query = select_query.where(
            website_table.columns.publish_timestamp.is_(None)
        )
    else:
        select_query = select_query.where(
            website_table.columns.publish_timestamp.is_not(None)
        )

    # print(str(select_query))

    # store results as a list per row
    query_result = connection.execute(select_query).all()

    # create nested dict, {"row_index":{**rows_in_db}}
    rows_dict = {}
    row_index = 0
    for row in query_result:
        row_as_dict = dict(row)
        # print(row_as_dict["stock_symbol"])
        rows_dict[row_index] = row_as_dict
        row_index += 1

    #    query_df = pd.DataFrame(query_result)
    #    query_df.columns = query_result[0].keys()
    #
    #    # shuffle the rows & reassign index numbers
    #    # shuffling could return a series; for safety make a df
    #    query_df = pd.DataFrame(query_df.sample(frac=1).reset_index(drop=True))
    #
    return rows_dict


@flow(task_runner=SequentialTaskRunner())
def calc_schedule_content(
    query_rows_dict, post_schedule_start_date: datetime, posts_per_day
):
    minutes_in_a_day = 24 * 60
    minutes_between_posts = minutes_in_a_day / posts_per_day
    timedelta_between_posts = timedelta(minutes=minutes_between_posts)

    publish_time_stamp = post_schedule_start_date.replace(
        hour=0, minute=0
    )  # always start at 0:00
    for stock_dict in query_rows_dict:
        query_rows_dict[stock_dict]["publish_timestamp"] = publish_time_stamp
        publish_time_stamp += timedelta_between_posts
    return query_rows_dict


@flow(task_runner=SequentialTaskRunner())
def upsert_website_content(content_db_conn_string, query_rows_dict):
    sql_alchemy_content_engine = db.create_engine(content_db_conn_string)
    connection = sql_alchemy_content_engine.connect()
    metadata = db.MetaData()
    website_table = db.Table(
        "website", metadata, autoload=True, autoload_with=sql_alchemy_content_engine
    )

    for stock_index in query_rows_dict:
        stock_row = query_rows_dict[stock_index]
        upsert_query = insert(website_table).values(stock_row)
        # do nothing if duplicate value
        upsert_query = upsert_query.on_conflict_do_update(
            index_elements=[
                "stock_symbol",
                "begin_date",
                "end_date",
                "dataset_database_name",
            ],
            set_=stock_row,
        )  # upserts, inserts a date if there is no entry for the (composite) key
        connection.execute(upsert_query)


@flow(task_runner=SequentialTaskRunner())
def create_website_content_files(query_rows_dict, website_content_folder_path: Path):

    # for every stock in the query_rows_dict

    page_bundle_paths = [x for x in website_content_folder_path.iterdir() if x.is_dir()]
    for stock in query_rows_dict:
        # create folder page_bundle_path
        # year-month-day-{stock_symbol}
        cur_stock = query_rows_dict[stock]

        stock_symbol = cur_stock["stock_symbol"]
        city = cur_stock["city"]
        country_code = cur_stock["country_code"]

        page_bundle_path = website_content_folder_path / (
            cur_stock["publish_timestamp"].strftime(r"%Y-%m-%d") + f"-{stock_symbol}"
        )

        # if already exists, skip

        if page_bundle_path in page_bundle_paths:
            continue

        create_folder(page_bundle_path)

        # FIXME: If the program crashes after folder creation, it will NOT output the content. 
        # TODO: Is there any way to make these combined actions atomic?

        image_path = (
            page_bundle_path
            / f"{stock_symbol}-corr-with-rainfall-in-{city}-{country_code}.webp"
        )

        # export graph image
        export_plotly_graph(plotly_json=cur_stock["graph_json"], file_path=image_path)

        markdown = create_markdown(stock_dict=cur_stock, image_file_path=image_path)

        export_markdown(markdown=markdown, page_bundle_path=page_bundle_path)


@flow(task_runner=SequentialTaskRunner())
def publish(
    content_db_conn_string,
    post_schedule_start_date: datetime,
    posts_per_day,
    website_content_folder_path: Path,
):

    rows_dict_where_pub_ts_null = get_publish_content(
        content_db_conn_string=content_db_conn_string, select_where_publish_ts_null=True
    )

    updated_rows_dict = calc_schedule_content(
        query_rows_dict=rows_dict_where_pub_ts_null,
        post_schedule_start_date=post_schedule_start_date,
        posts_per_day=posts_per_day,
    )

    upsert_website_content(
        content_db_conn_string=content_db_conn_string,
        query_rows_dict=updated_rows_dict,
    )

    rows_dict_where_pub_ts_not_null = get_publish_content(
        content_db_conn_string=content_db_conn_string,
        select_where_publish_ts_null=False,
    )

    create_website_content_files(
        query_rows_dict=rows_dict_where_pub_ts_not_null,
        website_content_folder_path=website_content_folder_path,
    )
