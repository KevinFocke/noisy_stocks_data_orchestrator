import numpy as np
import pandas as pd
import pytest
from noisy_stocks_data_orchestrator import __version__, main_flow
from noisy_stocks_data_orchestrator.customdatastructures import Stock

from tests.conftest import (
    stock_with_duplicate_dates,
    stock_with_negative_closing_price,
    stock_with_unequal_rows,
)

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
    stock = stock_with_date_nan
    assert isinstance(stock, Stock)
    assert isinstance(stock.symbol, str)
    assert isinstance(stock.time_series_df, pd.DataFrame)


def test_date_nan_removed(stock_with_date_nan):
    index = stock_with_date_nan.time_series_df.index
    assert len(index) == 4


def test_stock_df_wrong_size_length():
    with pytest.raises(ValueError) as ve:
        stock_with_unequal_rows()

    # When testing exception message, convert it to a string
    assert str(ve.value) == "All arrays must be of the same length"


def test_stock_df_wrong_close_price():
    # with pytest.raises(TypeError) as se:
    stock = stock_with_negative_closing_price()
    closing_price_array = stock.time_series_df["close_price"].values
    expected_result = np.array([1.4])

    # assert closing_price_series.values.tolist() == [1.4]

    assert closing_price_array == expected_result
    # assert str(se.value) == "Schema"


def test_drop_duplicate_dates():
    stock = stock_with_duplicate_dates()
    closing_price_array = stock.time_series_df["close_price"].values
    expected_result = np.array([1.4, 1.3])
    assert np.array_equal(closing_price_array, expected_result)  # type: ignore
