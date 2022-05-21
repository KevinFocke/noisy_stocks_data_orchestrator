from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task()
def create_visualization():
    return


@flow(task_runner=SequentialTaskRunner)
def publish(historical_stock_data, best_fit):
    # Create visualization
    
    # Upload to Strapi as draft

    create_visualization()
    return
