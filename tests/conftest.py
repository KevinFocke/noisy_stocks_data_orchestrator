import pandas as pd
import pytest

"""
Single source of truth for fixtures across tests
"""

# Scopes of pytest fixtures decide how often they are run.
# Syntax: @pytest.fixture(scope="function")
# Unique polymorphism: You can call the class directly; it returns an object!
# The default scope of a fixture is function; this means:
# The fixture is a wrapper; setup before, teardown after.
# Other possible fixtures are: session, class, module
# You can also make a fixture apply to every test with autouse
# Syntax: @pytest.fixture(autouse=True)


@pytest.fixture
def sanity_check_fixture():
    return "testinput"


@pytest.fixture
def sanity_check_array():
    return [1, 1, 1, 1, 1]


@pytest.fixture
def stock_with_date_nan():
    return pd.DataFrame(
        {
            "timestamp": [
                "1996-10-04",
                "1980-02-05",
                "1970-02-05",
                "1950-02-07",
                pd.NaT,
            ],
            "close_price": [1.3, 1.4, 0, 1, 5],
        }
    )
