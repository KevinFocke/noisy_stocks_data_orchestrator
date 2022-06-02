import pandas as pd

# from data_structures import Stock
from noisy_stocks_data_orchestrator import __version__, main_flow


def test_version():
    assert __version__ == "0.1.0"


def test_sanity():
    """Can it find the module?"""
    assert main_flow.sanity_check() == "Module_Found"


def test_fixture_input_via_conftest(sanity_check_fixture):
    """
    conftest.py is used to supply fixtures accross test files
    """
    assert sanity_check_fixture == "testinput"


# def create_stock(stock_with_date_nan):
# stock = Stock(symbol="AAPL", time_series_df=stock_with_date_nan)
# assert type(stock) == Stock
# assert type(stock.symbol == String)
# assert type(stock.time_series_df == pd.DataFrame)
