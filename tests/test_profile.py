import pandas as pd

from pydq import profile_dataframe


def test_profile_dataframe_returns_dict():
    """
    Basic smoke test:
    The function should run successfully and return a dict.
    """
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["x", "y", "z"]
    })

    result = profile_dataframe(df)

    assert isinstance(result, dict)


def test_profile_dataframe_has_columns_section():
    """
    The profiling result should contain a 'columns' section.
    """
    df = pd.DataFrame({
        "col1": [10, 20],
        "col2": [0.1, 0.2]
    })

    result = profile_dataframe(df)

    assert "columns" in result
    assert isinstance(result["columns"], list)
    assert len(result["columns"]) == 2


def test_profile_dataframe_column_names_present():
    """
    Each column entry should contain the correct column name.
    """
    df = pd.DataFrame({
        "num": [1, 2, 3, 4]
    })

    result = profile_dataframe(df)

    col = result["columns"][0]

    assert "name" in col
    assert col["name"] == "num"


def test_profile_dataframe_numeric_stats_exist():
    """
    Numeric columns should contain basic statistical fields.
    Only checks for field existence, not the actual values.
    """
    df = pd.DataFrame({
        "num": [1, 2, 3, 4]
    })

    result = profile_dataframe(df)
    col = result["columns"][0]

    for key in ("min", "max", "mean"):
        assert key in col
