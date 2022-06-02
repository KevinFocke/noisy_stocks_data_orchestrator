from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

"""Data Outflow Module
"""
# TODO: Set maximum filesize limit for images (10 MB?)
# TODO: Compress images exported by Plotly

# TODO: Decide where to put create_visualization


# TODO: Export chart images in 2 sizes:
# Thumbnail is 640 x 360 , should be saved in .webp
# Main image is 1600 x 900, should be saved in .webp


@task()
def create_visualization():
    return


@flow(task_runner=SequentialTaskRunner())
def publish(historical_stock_data, best_fit):
    # Create visualization

    # Upload to Strapi as draft

    create_visualization()
    return
