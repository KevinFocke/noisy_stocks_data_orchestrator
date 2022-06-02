import pandas as pd
from noisy_stocks_data_orchestrator import __version__, main_flow
from noisy_stocks_data_orchestrator.customdatastructures import Stock

# For typechecking use isinstance()
# Start every test with test_


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


def test_create_stock(stock_with_date_nan):
    stock = Stock(symbol="AAPL", time_series_df=stock_with_date_nan)

    assert isinstance(stock, Stock)
    assert isinstance(stock.symbol, str)
    assert isinstance(stock.time_series_df, pd.DataFrame)
