import random
import time

from prefect import flow, task
from prefect.task_runners import DaskTaskRunner


@task(retries=3, retry_delay_seconds=60)
def test_parallellization_helper_1(number):
    time.sleep(random.randint(0, 1))
    return number


@task()
def test_parallellization_helper_2(test_list):
    time.sleep(random.randint(0, 1))
    return test_list


@flow(task_runner=DaskTaskRunner())  # The subflow runs tasks in parallel
def test_parallellization():
    test_list = [test_parallellization_helper_1(number) for number in range(20)]
    test_parallellization_helper_2(
        test_list
    )  # Use wait_for=[test_list] to explicitly denote dependency
    return test_list


@flow()
def test_task_recovery():
    future = test_task_recovery_helper_1()  # Save prefect future
    final_state = future.wait(1)  # Wait one second max
    if final_state:
        # Take action if the task is done
        result = final_state.result()
        print(result)
        time.sleep(3)


@task()
def test_task_recovery_helper_1():
    return


@flow()
def test_flow():
    test_parallellization()
    test_task_recovery()


if __name__ == "__main__":
    test_flow()
