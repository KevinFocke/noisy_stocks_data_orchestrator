import io

import reverse_geocoder as rg  # Might need to be installed locally via pip
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task

"""Data Outflow Module
"""

# TODO: Set pandas backend to plotly https://plotly.com/python/pandas-backend/
# TODO: Static image export plotly https://plotly.com/python/static-image-export


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
def save_files(markdown):
    return


@flow(task_runner=SequentialTaskRunner())
def publish(historical_stock_data, best_fit):

    # Create visualization
    visualization = create_visualization()

    # Create files for export
    markdown = create_markdown_files(visualization)

    # Credit: Cities database from Geocities
    # Precipitation data from ...
    # Stock data from ...

    # Save files to disk
    save_files(markdown)

    return
