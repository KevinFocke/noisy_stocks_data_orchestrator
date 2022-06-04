from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

"""Data Outflow Module
"""

# TODO: Set pandas backend to plotly https://plotly.com/python/pandas-backend/


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

    # Save files to disk
    save_files(markdown)

    return
