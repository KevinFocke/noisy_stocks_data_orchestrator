from prefect import flow, task


@flow
def etl_flow():
    # Based on queue saved in PostgresQL, import datasets

    return


@task
def Extract():

    return


@task
def Transform():
    return


@task
def Load():
    return
