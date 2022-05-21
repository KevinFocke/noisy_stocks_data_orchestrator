from prefect import flow, task


@flow()
def find_movers_and_shakers(collection="SandP"):

    # Create a dict of dicts. Key is stock name.
    # Check movers and shakers

    # check movers and shakers in which collection?

    stock_data = {
        "stock": "AAPL",
        "change_in_points": "100",
        "market_cap_in_millions": 500,
    }
    return stock_data


@task()
def get_historical_stock_data(stock_data: dict):
    return
